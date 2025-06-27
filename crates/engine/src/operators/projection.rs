//! Projection physical operator

use crate::physical_plan::ExecutionPlan;
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_expr::PhysicalExpr;
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use std::sync::Arc;

/// Physical operator for projections
#[derive(Debug)]
pub struct ProjectionExec {
    input: Arc<dyn ExecutionPlan>,
    expressions: Vec<(Arc<dyn PhysicalExpr>, String)>,
    schema: Arc<arrow::datatypes::Schema>,
}

impl ProjectionExec {
    pub fn new(
        input: Arc<dyn ExecutionPlan>,
        expressions: Vec<(Arc<dyn PhysicalExpr>, String)>,
        schema: Arc<arrow::datatypes::Schema>,
    ) -> Self {
        Self {
            input,
            expressions,
            schema,
        }
    }

    pub fn schema(&self) -> Arc<arrow::datatypes::Schema> {
        self.schema.clone()
    }
}

impl Clone for ProjectionExec {
    fn clone(&self) -> Self {
        Self {
            input: self.input.clone(),
            expressions: self.expressions.clone(),
            schema: self.schema.clone(),
        }
    }
}

#[async_trait::async_trait]
impl ExecutionPlan for ProjectionExec {
    async fn execute(&self) -> DataFusionResult<BoxStream<'static, DataFusionResult<RecordBatch>>> {
        let input_stream = self.input.execute().await?;
        let expressions = self.expressions.clone();
        let schema = self.schema.clone();
        
        let projected_stream = input_stream.try_filter_map(move |batch| {
            let expressions = expressions.clone();
            let schema = schema.clone();
            
            async move {
                let mut columns = Vec::new();
                
                for (expr, _name) in &expressions {
                    let result = expr.evaluate(&batch)?;
                    let array = result.into_array(batch.num_rows())?;
                    columns.push(array);
                }
                
                let projected_batch = RecordBatch::try_new(schema, columns)?;
                Ok(Some(projected_batch))
            }
        });
        
        Ok(Box::pin(projected_stream))
    }

    fn schema(&self) -> Arc<arrow::datatypes::Schema> {
        self.schema.clone()
    }
}