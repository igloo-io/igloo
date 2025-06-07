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
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<RecordBatch, arrow::error::ArrowError>> + Send>>,
        arrow::error::ArrowError,
    > {
        // Ensure URI starts with "file://" and extract file path
        let file_path_str = split.uri.strip_prefix("file://").ok_or_else(|| {
            arrow::error::ArrowError::InvalidArgumentError(format!(
                "Invalid file URI: {}",
                split.uri
            ))
        })?;

        let path = Path::new(file_path_str);

        // Check if path exists before trying to open
        if !path.exists() {
            return Err(arrow::error::ArrowError::IoError(
                format!("File does not exist at path: {}", file_path_str),
                std::io::Error::new(std::io::ErrorKind::NotFound, "File not found"),
            ));
        }

        // Open the file
        let file = std::fs::File::open(path).map_err(|e| {
            arrow::error::ArrowError::IoError(format!("Failed to open file: {}", path.display()), e)
        })?;

        // Build the Parquet reader
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
            arrow::error::ArrowError::ParquetError(format!(
                "Failed to create ParquetRecordBatchReaderBuilder for {}: {}",
                path.display(),
                e
            ))
        })?;

        let reader = builder.build().map_err(|e| {
            arrow::error::ArrowError::ParquetError(format!(
                "Failed to build ParquetRecordBatchReader for {}: {}",
                path.display(),
                e
            ))
        })?;

        // Create a stream from the reader
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
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::arrow_writer::ArrowWriter;
    use std::fs::File;
    use std::sync::Arc; // For Arc<Schema>
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
        if let Err(e) = create_test_parquet_file(&sample_file) {
            panic!(
                "Failed to create test Parquet file at {:?}: {}",
                sample_file,
                e
            );
        }
    }

    fn create_test_parquet_file(path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
        if path.exists() {
            fs::remove_file(path)?; // Remove if it already exists to ensure fresh state
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?; // Ensure test_data directory exists
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Int64, false),
            Field::new("col2", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["A", "B", "C"])),
            ],
        )?;

        let file = File::create(path)?;
        let mut writer = ArrowWriter::try_new(file, schema, None)?;
        writer.write(&batch)?;
        writer.close()?;
        Ok(())
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
            assert!(splits_result.is_ok(), "FilesystemConnector::get_splits failed: {:?}", splits_result.err());

            let splits = splits_result.unwrap();
            assert_eq!(splits.len(), 1, "Expected to find 1 split (file), found {} splits. Splits: {:?}", splits.len(), splits);

            let expected_uri = format!("file://{}", sample_parquet_file_path().to_string_lossy());
            assert_eq!(splits[0].uri, expected_uri, "Split URI mismatch. Expected: '{}', Found: '{}'", expected_uri, splits[0].uri);
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
            assert!(stream_result.is_ok(), "FilesystemConnector::read_split failed for split {:?}: {:?}", split, stream_result.err());

            let mut stream = stream_result.unwrap();
            let mut record_batch_count = 0;

            while let Some(batch_result) = stream.next().await {
                assert!(batch_result.is_ok(), "Reading batch from stream for split {:?} failed: {:?}", split, batch_result.err());
                let batch = batch_result.unwrap();
                record_batch_count += 1;

                assert_eq!(batch.num_columns(), 2, "Expected 2 columns, found {}. Schema: {:?}", batch.num_columns(), batch.schema());
                assert_eq!(batch.num_rows(), 3, "Expected 3 rows in the batch, found {}.", batch.num_rows());

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
            assert_eq!(record_batch_count, 1, "Expected one record batch from file {:?}, but got {} batches.", split.uri, record_batch_count);
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
            assert!(splits_result.is_ok(), "get_splits on empty dir failed: {:?}", splits_result.err());
            let splits = splits_result.unwrap();
            assert!(splits.is_empty(), "Expected no splits from empty dir, found: {:?}", splits);

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
            assert!(splits_result.is_err(), "Expected error when get_splits path is not a directory, but got Ok({:?})", splits_result.ok());
        });
    }
}
