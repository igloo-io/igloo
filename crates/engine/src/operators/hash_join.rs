//! Hash join physical operator

use crate::physical_plan::ExecutionPlan;
use arrow::record_batch::RecordBatch;
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{Schema, Field};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_expr::PhysicalExpr;
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use std::collections::HashMap;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;

/// Join type enumeration
#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

/// Physical operator for hash joins
#[derive(Debug)]
pub struct HashJoinExec {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    left_keys: Vec<Arc<dyn PhysicalExpr>>,
    right_keys: Vec<Arc<dyn PhysicalExpr>>,
    join_type: JoinType,
    schema: Arc<Schema>,
}

impl HashJoinExec {
    pub fn new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        left_keys: Vec<Arc<dyn PhysicalExpr>>,
        right_keys: Vec<Arc<dyn PhysicalExpr>>,
        join_type: JoinType,
    ) -> DataFusionResult<Self> {
        // Build output schema by combining left and right schemas
        let left_schema = left.schema();
        let right_schema = right.schema();
        
        let mut fields = Vec::new();
        
        // Add left fields
        for field in left_schema.fields() {
            fields.push(field.clone());
        }
        
        // Add right fields (with potential renaming to avoid conflicts)
        for field in right_schema.fields() {
            let mut new_field = field.clone();
            if left_schema.field_with_name(field.name()).is_ok() {
                new_field = Field::new(
                    &format!("right_{}", field.name()),
                    field.data_type().clone(),
                    field.is_nullable(),
                );
            }
            fields.push(new_field);
        }
        
        let schema = Arc::new(Schema::new(fields));
        
        Ok(Self {
            left,
            right,
            left_keys,
            right_keys,
            join_type,
            schema,
        })
    }

    pub fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

impl Clone for HashJoinExec {
    fn clone(&self) -> Self {
        Self {
            left: self.left.clone(),
            right: self.right.clone(),
            left_keys: self.left_keys.clone(),
            right_keys: self.right_keys.clone(),
            join_type: self.join_type.clone(),
            schema: self.schema.clone(),
        }
    }
}

#[async_trait::async_trait]
impl ExecutionPlan for HashJoinExec {
    async fn execute(&self) -> DataFusionResult<BoxStream<'static, DataFusionResult<RecordBatch>>> {
        // Build phase: consume right side and build hash table
        let mut right_stream = self.right.execute().await?;
        let mut hash_table: HashMap<Vec<u8>, Vec<RecordBatch>> = HashMap::new();
        let right_keys = self.right_keys.clone();
        
        while let Some(batch_result) = right_stream.next().await {
            let batch = batch_result?;
            
            // Extract join keys
            let mut key_arrays = Vec::new();
            for key_expr in &right_keys {
                let result = key_expr.evaluate(&batch)?;
                let array = result.into_array(batch.num_rows())?;
                key_arrays.push(array);
            }
            
            // Group rows by join key
            for row_idx in 0..batch.num_rows() {
                let mut key_bytes = Vec::new();
                for key_array in &key_arrays {
                    // Simple key serialization (in production, use proper hash function)
                    let value = format!("{:?}", key_array.slice(row_idx, 1));
                    key_bytes.extend_from_slice(value.as_bytes());
                }
                
                // Extract single row as RecordBatch
                let row_batch = batch.slice(row_idx, 1);
                hash_table.entry(key_bytes).or_insert_with(Vec::new).push(row_batch);
            }
        }
        
        // Probe phase: stream left side and probe hash table
        let left_stream = self.left.execute().await?;
        let left_keys = self.left_keys.clone();
        let schema = self.schema.clone();
        let join_type = self.join_type.clone();
        
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        
        tokio::spawn(async move {
            let mut left_stream = left_stream;
            
            while let Some(batch_result) = left_stream.next().await {
                match batch_result {
                    Ok(left_batch) => {
                        // Extract left join keys
                        let mut left_key_arrays = Vec::new();
                        for key_expr in &left_keys {
                            match key_expr.evaluate(&left_batch) {
                                Ok(result) => {
                                    match result.into_array(left_batch.num_rows()) {
                                        Ok(array) => left_key_arrays.push(array),
                                        Err(e) => {
                                            let _ = tx.send(Err(e)).await;
                                            return;
                                        }
                                    }
                                }
                                Err(e) => {
                                    let _ = tx.send(Err(e)).await;
                                    return;
                                }
                            }
                        }
                        
                        // Probe hash table for each left row
                        for left_row_idx in 0..left_batch.num_rows() {
                            let mut key_bytes = Vec::new();
                            for key_array in &left_key_arrays {
                                let value = format!("{:?}", key_array.slice(left_row_idx, 1));
                                key_bytes.extend_from_slice(value.as_bytes());
                            }
                            
                            let left_row = left_batch.slice(left_row_idx, 1);
                            
                            if let Some(right_batches) = hash_table.get(&key_bytes) {
                                // Join found - combine left and right rows
                                for right_batch in right_batches {
                                    match combine_batches(&left_row, right_batch, &schema) {
                                        Ok(joined_batch) => {
                                            if tx.send(Ok(joined_batch)).await.is_err() {
                                                return;
                                            }
                                        }
                                        Err(e) => {
                                            let _ = tx.send(Err(e)).await;
                                            return;
                                        }
                                    }
                                }
                            } else if join_type == JoinType::Left || join_type == JoinType::Full {
                                // Left join - emit left row with nulls for right side
                                match combine_with_nulls(&left_row, &schema, true) {
                                    Ok(joined_batch) => {
                                        if tx.send(Ok(joined_batch)).await.is_err() {
                                            return;
                                        }
                                    }
                                    Err(e) => {
                                        let _ = tx.send(Err(e)).await;
                                        return;
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                }
            }
        });
        
        Ok(Box::pin(ReceiverStream::new(rx)))
    }

    fn schema(&self) -> Arc<Schema> {
        self.schema.clone()
    }
}

fn combine_batches(
    left: &RecordBatch,
    right: &RecordBatch,
    output_schema: &Schema,
) -> DataFusionResult<RecordBatch> {
    let mut columns = Vec::new();
    
    // Add left columns
    for i in 0..left.num_columns() {
        columns.push(left.column(i).clone());
    }
    
    // Add right columns
    for i in 0..right.num_columns() {
        columns.push(right.column(i).clone());
    }
    
    RecordBatch::try_new(Arc::new(output_schema.clone()), columns)
        .map_err(|e| DataFusionError::ArrowError(e))
}

fn combine_with_nulls(
    left: &RecordBatch,
    output_schema: &Schema,
    left_side: bool,
) -> DataFusionResult<RecordBatch> {
    let mut columns = Vec::new();
    
    if left_side {
        // Add left columns
        for i in 0..left.num_columns() {
            columns.push(left.column(i).clone());
        }
        
        // Add null columns for right side
        let right_field_count = output_schema.fields().len() - left.num_columns();
        for i in 0..right_field_count {
            let field_idx = left.num_columns() + i;
            let field = output_schema.field(field_idx);
            let null_array = arrow::array::new_null_array(field.data_type(), 1);
            columns.push(null_array);
        }
    } else {
        // Add null columns for left side
        let left_field_count = output_schema.fields().len() - left.num_columns();
        for i in 0..left_field_count {
            let field = output_schema.field(i);
            let null_array = arrow::array::new_null_array(field.data_type(), 1);
            columns.push(null_array);
        }
        
        // Add right columns
        for i in 0..left.num_columns() {
            columns.push(left.column(i).clone());
        }
    }
    
    RecordBatch::try_new(Arc::new(output_schema.clone()), columns)
        .map_err(|e| DataFusionError::ArrowError(e))
}