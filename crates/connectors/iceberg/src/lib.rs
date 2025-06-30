//! Apache Iceberg connector for Igloo
//!
//! This is a basic implementation that reads Iceberg tables as Parquet files.
//! In a production system, this would use the full Iceberg metadata and manifest files.

use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use futures::stream::{BoxStream, StreamExt};
use igloo_engine::physical_plan::ExecutionPlan;
use parquet::arrow::{ArrowReader, ParquetFileArrowReader};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;

/// Physical operator for scanning Iceberg tables
#[derive(Debug, Clone)]
pub struct IcebergScanExec {
    table_path: String,
    schema: Arc<arrow::datatypes::Schema>,
    projection: Option<Vec<usize>>,
}

impl IcebergScanExec {
    pub fn new(
        table_path: String,
        schema: Arc<arrow::datatypes::Schema>,
        projection: Option<Vec<usize>>,
    ) -> Self {
        Self {
            table_path,
            schema,
            projection,
        }
    }

    pub fn schema(&self) -> Arc<arrow::datatypes::Schema> {
        self.schema.clone()
    }

    /// Discover Parquet files in the Iceberg table directory
    fn discover_data_files(&self) -> DataFusionResult<Vec<String>> {
        let table_path = Path::new(&self.table_path);
        let data_dir = table_path.join("data");
        
        if !data_dir.exists() {
            return Err(DataFusionError::External(Box::new(
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("Iceberg data directory not found: {}", data_dir.display())
                )
            )));
        }
        
        let mut parquet_files = Vec::new();
        
        // Recursively find all .parquet files
        fn find_parquet_files(dir: &Path, files: &mut Vec<String>) -> std::io::Result<()> {
            for entry in std::fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();
                
                if path.is_dir() {
                    find_parquet_files(&path, files)?;
                } else if path.extension().and_then(|s| s.to_str()) == Some("parquet") {
                    files.push(path.to_string_lossy().to_string());
                }
            }
            Ok(())
        }
        
        find_parquet_files(&data_dir, &mut parquet_files)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        
        Ok(parquet_files)
    }
}

#[async_trait::async_trait]
impl ExecutionPlan for IcebergScanExec {
    async fn execute(&self) -> DataFusionResult<BoxStream<'static, DataFusionResult<RecordBatch>>> {
        let parquet_files = self.discover_data_files()?;
        let projection = self.projection.clone();
        
        let (tx, rx) = tokio::sync::mpsc::channel(10);
        
        tokio::spawn(async move {
            for file_path in parquet_files {
                let projection = projection.clone();
                
                let result = tokio::task::spawn_blocking(move || {
                    let file = File::open(&file_path)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    
                    let reader = ParquetFileArrowReader::try_new(file)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    
                    let mut arrow_reader = reader.get_record_reader(1024)
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;
                    
                    let mut batches = Vec::new();
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
                        
                        batches.push(final_batch);
                    }
                    
                    Ok::<Vec<RecordBatch>, DataFusionError>(batches)
                }).await;
                
                match result {
                    Ok(Ok(batches)) => {
                        for batch in batches {
                            if tx.send(Ok(batch)).await.is_err() {
                                return;
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(DataFusionError::External(Box::new(e)))).await;
                        return;
                    }
                }
            }
        });
        
        Ok(Box::pin(ReceiverStream::new(rx)))
    }

    fn schema(&self) -> Arc<arrow::datatypes::Schema> {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::fs;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_iceberg_scan_empty_directory() {
        let temp_dir = TempDir::new().unwrap();
        let table_path = temp_dir.path().to_string_lossy().to_string();
        
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        
        let iceberg_scan = IcebergScanExec::new(table_path, schema, None);
        
        // Should fail because data directory doesn't exist
        let result = iceberg_scan.execute().await;
        assert!(result.is_err());
    }

    #[test]
    fn test_discover_data_files_no_directory() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
        ]));
        
        let iceberg_scan = IcebergScanExec::new("/nonexistent/path".to_string(), schema, None);
        let result = iceberg_scan.discover_data_files();
        assert!(result.is_err());
    }
}