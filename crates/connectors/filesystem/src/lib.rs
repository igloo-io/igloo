use std::fs;
use std::path::Path;
use std::pin::Pin;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::{self, Stream, StreamExt}; // Added StreamExt for map
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use igloo_connector::{Connector, Split}; // Assuming igloo_connector is the crate name

#[derive(Debug, Default)]
pub struct FilesystemConnector {}

#[async_trait]
impl Connector for FilesystemConnector {
    async fn get_splits(&self, table: &str) -> Result<Vec<Split>, arrow::error::ArrowError> {
        let path = Path::new(table);
        if !path.is_dir() {
            return Err(arrow::error::ArrowError::IoError(
                format!("Path '{}' is not a directory or does not exist.", table),
                std::io::Error::new(std::io::ErrorKind::NotFound, "Path not a directory"),
            ));
        }

        let mut splits = Vec::new();
        for entry in fs::read_dir(path).map_err(|e| {
            arrow::error::ArrowError::IoError(format!("Failed to read directory: {}", table), e)
        })? {
            let entry = entry.map_err(|e| {
                arrow::error::ArrowError::IoError(format!("Failed to read directory entry in {}", table), e)
            })?;
            let file_path = entry.path();
            if file_path.is_file() {
                if let Some(extension) = file_path.extension() {
                    if extension == "parquet" {
                        splits.push(Split {
                            uri: format!("file://{}", file_path.to_string_lossy()),
                        });
                    }
                }
            }
        }
        Ok(splits)
    }

    async fn read_split(
        &self,
        split: &Split,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<RecordBatch, arrow::error::ArrowError>> + Send>>, arrow::error::ArrowError> {
        // Remove "file://" prefix and handle potential errors if URI is malformed
        let file_path_str = split.uri.strip_prefix("file://").ok_or_else(|| {
            arrow::error::ArrowError::InvalidArgumentError(format!(
                "Invalid URI format for split: {}. Expected 'file:///path/to/file'",
                split.uri
            ))
        })?;

        let path = Path::new(file_path_str);
        if !path.exists() {
             return Err(arrow::error::ArrowError::IoError(
                format!("File does not exist at path: {}", file_path_str),
                std::io::Error::new(std::io::ErrorKind::NotFound, "File not found"),
            ));
        }


        let file = std::fs::File::open(path).map_err(|e| {
            arrow::error::ArrowError::IoError(format!("Failed to open file: {}", path.display()), e)
        })?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
            arrow::error::ArrowError::ParquetError(format!(
                "Failed to create ParquetRecordBatchReaderBuilder for {}: {}",
                path.display(),
                e
            ))
        })?;

        // Assuming all columns are read. Select/project pushdown would happen here.
        let reader = builder.build().map_err(|e| {
            arrow::error::ArrowError::ParquetError(format!(
                "Failed to build ParquetRecordBatchReader for {}: {}",
                path.display(),
                e
            ))
        })?;

        // The reader itself is an iterator of Result<RecordBatch, _>.
        // We need to convert this into a Stream.
        let stream = stream::iter(reader).map(|result| {
            result.map_err(|parquet_err| {
                arrow::error::ArrowError::ParquetError(format!(
                    "Failed to read RecordBatch from {}: {}",
                    path.display(),
                    parquet_err
                ))
            })
        });

        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use std::fs;
    use std::path::PathBuf;
    use tokio::runtime::Runtime; // For running async tests

    // Helper function to get the path to the test_data directory
    fn test_data_path() -> PathBuf {
        let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        path.push("test_data");
        path
    }

    // Helper function to get the path to the sample Parquet file
    fn sample_parquet_file_path() -> PathBuf {
        test_data_path().join("sample.parquet")
    }

    // Helper to ensure the test parquet file exists.
    fn ensure_test_file_exists() {
        let sample_file = sample_parquet_file_path();
        if !sample_file.exists() {
            // This is a fallback if the Python script didn't run or was not part of the subtask.
            // Ideally, the file creation should be guaranteed before tests run.
            // For this example, we'll panic if it's not there, assuming prior step created it.
            panic!("Test Parquet file not found at {:?}. Make sure it's created.", sample_file);
        }
    }

    #[test]
    fn test_get_splits_finds_parquet_file() {
        ensure_test_file_exists();
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let connector = FilesystemConnector::default();
            let test_dir = test_data_path();
            let test_dir_str = test_dir.to_str().unwrap();

            let splits_result = connector.get_splits(test_dir_str).await;
            assert!(splits_result.is_ok(), "get_splits failed: {:?}", splits_result.err());

            let splits = splits_result.unwrap();
            assert_eq!(splits.len(), 1, "Expected to find 1 split (file)");

            let expected_uri = format!("file://{}", sample_parquet_file_path().to_string_lossy());
            assert_eq!(splits[0].uri, expected_uri, "Split URI does not match expected");
        });
    }

    #[test]
    fn test_read_split_reads_correct_data() {
        ensure_test_file_exists();
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let connector = FilesystemConnector::default();
            let sample_file_uri = format!("file://{}", sample_parquet_file_path().to_string_lossy());
            let split = Split { uri: sample_file_uri };

            let stream_result = connector.read_split(&split).await;
            assert!(stream_result.is_ok(), "read_split failed: {:?}", stream_result.err());

            let mut stream = stream_result.unwrap();
            let mut record_batch_count = 0;

            while let Some(batch_result) = stream.next().await {
                assert!(batch_result.is_ok(), "Reading batch failed: {:?}", batch_result.err());
                let batch = batch_result.unwrap();
                record_batch_count += 1;

                assert_eq!(batch.num_columns(), 2, "Expected 2 columns");
                assert_eq!(batch.num_rows(), 3, "Expected 3 rows in the batch");

                // Check column names (optional, but good for verification)
                // Assuming schema is known: col1 (Int64), col2 (String)
                let schema = batch.schema();
                assert_eq!(schema.field(0).name(), "col1");
                assert_eq!(schema.field(1).name(), "col2");

                // Check data for col1
                let col1 = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Failed to downcast col1 to Int64Array");
                assert_eq!(col1.values(), &[1, 2, 3]);

                // Check data for col2
                let col2 = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Failed to downcast col2 to StringArray");
                let col2_values: Vec<&str> = col2.iter().map(|opt| opt.unwrap()).collect();
                assert_eq!(col2_values, &["A", "B", "C"]);
            }
            assert_eq!(record_batch_count, 1, "Expected one record batch from the sample file");
        });
    }

    #[test]
    fn test_get_splits_empty_if_no_parquet() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let connector = FilesystemConnector::default();
            let empty_dir = test_data_path().join("empty_dir");
            fs::create_dir_all(&empty_dir).unwrap(); // Ensure empty dir exists

            let splits_result = connector.get_splits(empty_dir.to_str().unwrap()).await;
            assert!(splits_result.is_ok());
            assert!(splits_result.unwrap().is_empty(), "Expected no splits from an empty directory");

            fs::remove_dir_all(&empty_dir).unwrap(); // Clean up
        });
    }

    #[test]
    fn test_get_splits_error_if_not_directory() {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            let connector = FilesystemConnector::default();
            // Try to list splits from a file instead of a directory
            let splits_result = connector.get_splits(sample_parquet_file_path().to_str().unwrap()).await;
            assert!(splits_result.is_err(), "Expected error when path is not a directory");
        });
    }
}
