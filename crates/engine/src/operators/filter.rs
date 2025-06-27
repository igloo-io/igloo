//! Filter physical operator

use crate::physical_plan::ExecutionPlan;
use arrow::record_batch::RecordBatch;
use arrow::compute::filter_record_batch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_expr::PhysicalExpr;
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use std::sync::Arc;

/// Physical operator for filtering
#[derive(Debug)]
pub struct FilterExec {
    input: Arc<dyn ExecutionPlan>,
    predicate: Arc<dyn PhysicalExpr>,
}

impl FilterExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, predicate: Arc<dyn PhysicalExpr>) -> Self {
        Self { input, predicate }
    }

    pub fn schema(&self) -> Arc<arrow::datatypes::Schema> {
        self.input.schema()
    }
}

impl Clone for FilterExec {
    fn clone(&self) -> Self {
        Self {
            input: self.input.clone(),
            predicate: self.predicate.clone(),
        }
    }
}

#[async_trait::async_trait]
impl ExecutionPlan for FilterExec {
    async fn execute(&self) -> DataFusionResult<BoxStream<'static, DataFusionResult<RecordBatch>>> {
        let input_stream = self.input.execute().await?;
        let predicate = self.predicate.clone();
        
        let filtered_stream = input_stream.try_filter_map(move |batch| {
            let predicate = predicate.clone();
            
            async move {
                let result = predicate.evaluate(&batch)?;
                let filter_array = result.into_array(batch.num_rows())?;
                
                let boolean_array = filter_array
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                    .ok_or_else(|| {
                        DataFusionError::Internal("Filter predicate must return boolean array".to_string())
                    })?;
                
                let filtered_batch = filter_record_batch(&batch, boolean_array)?;
                
                if filtered_batch.num_rows() > 0 {
                    Ok(Some(filtered_batch))
                } else {
                    Ok(None)
                }
            }
        });
        
        Ok(Box::pin(filtered_stream))
    }

    fn schema(&self) -> Arc<arrow::datatypes::Schema> {
        self.input.schema()
    }
}