//! Parquet scan physical operator

use crate::physical_plan::ExecutionPlan;
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use futures::stream::{BoxStream, StreamExt};
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use std::fs::File;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;

/// Physical operator for scanning Parquet files
#[derive(Debug, Clone)]
pub struct ParquetScanExec {
    file_path: String,
    schema: Arc<arrow::datatypes::Schema>,
    projection: Option<Vec<usize>>,
}

impl ParquetScanExec {
    pub fn new(
        file_path: String,
        schema: Arc<arrow::datatypes::Schema>,
        projection: Option<Vec<usize>>,
    ) -> Self {
        Self {
            file_path,
            schema,
            projection,
        }
    }

    pub fn schema(&self) -> Arc<arrow::datatypes::Schema> {
        self.schema.clone()
    }
}

#[async_trait::async_trait]
impl ExecutionPlan for ParquetScanExec {
    async fn execute(&self) -> DataFusionResult<BoxStream<'static, DataFusionResult<RecordBatch>>> {
        let file_path = self.file_path.clone();
        let projection = self.projection.clone();
        
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        
        tokio::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                let file = File::open(&file_path)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                
                let reader = ParquetFileArrowReader::try_new(file)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                
                let mut arrow_reader = reader.get_record_reader(1024)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                
                while let Some(batch_result) = arrow_reader.next() {
                    let batch = batch_result
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    
                    let final_batch = if let Some(ref proj) = projection {
                        let columns: Vec<_> = proj.iter()
                            .map(|&i| batch.column(i).clone())
                            .collect();
                        let projected_schema = Arc::new(batch.schema().project(proj)?);
                        RecordBatch::try_new(projected_schema, columns)?
                    } else {
                        batch
                    };
                    
                    if tx.send(Ok(final_batch)).await.is_err() {
                        break;
                    }
                }
                
                Ok::<(), DataFusionError>(())
            }).await;
            
            if let Err(e) = result {
                let _ = tx.send(Err(DataFusionError::External(Box::new(e)))).await;
            }
        });
        
        Ok(Box::pin(ReceiverStream::new(rx)))
    }

    fn schema(&self) -> Arc<arrow::datatypes::Schema> {
        self.schema.clone()
    }
}