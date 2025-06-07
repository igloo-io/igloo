use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::SchemaRef; // For PhysicalOperator trait
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::{self, Stream, StreamExt, TryStreamExt}; // Added TryStreamExt for try_flatten

// Assuming igloo_connector::Split and igloo_connector::Connector are available
use igloo_connector::{Connector, Split};

/// Generic error type for the engine (can be expanded)
#[derive(Debug, thiserror::Error)] // Add thiserror = "1" to engine's Cargo.toml
pub enum EngineError {
    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    #[error("Connector error: {0}")]
    ConnectorError(String), // Simplified for now

    #[error("Execution error: {0}")]
    ExecutionError(String),
}

pub type Result<T> = std::result::Result<T, EngineError>;

/// A stream of RecordBatches
pub type RecordBatchStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;

/// PhysicalOperator is the basic unit of execution.
#[async_trait]
pub trait PhysicalOperator: Send + Sync {
    /// Returns the schema of the output of this operator.
    fn schema(&self) -> SchemaRef;

    /// Executes this operator, producing a stream of RecordBatches.
    async fn execute(&self) -> Result<RecordBatchStream>; // Changed to use local Result
}

/// ScanExec is a physical operator that scans data from a Connector.
pub struct ScanExec {
    splits: Vec<Split>,
    connector: Arc<dyn Connector>,
    output_schema: SchemaRef, // Schema of the data to be scanned
}

impl ScanExec {
    pub fn new(splits: Vec<Split>, connector: Arc<dyn Connector>, output_schema: SchemaRef) -> Self {
        Self {
            splits,
            connector,
            output_schema,
        }
    }
}

#[async_trait]
impl PhysicalOperator for ScanExec {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    async fn execute(&self) -> Result<RecordBatchStream> {
        if self.splits.is_empty() {
            // If there are no splits, return an empty stream with the correct schema.
            // This requires creating an empty RecordBatch with the schema, which is non-trivial.
            // For now, returning an empty stream directly.
            // Consider arrow::record_batch::RecordBatch::new_empty(self.output_schema.clone())
            // and stream::once(async { Ok(empty_batch) }) if needed.
            return Ok(Box::pin(stream::empty()));
        }

        let mut streams = Vec::with_capacity(self.splits.len());
        for split in &self.splits {
            // Clone connector Arc for each stream operation
            let connector_clone = self.connector.clone();
            let split_clone = split.clone(); // Clone split for the async block

            let stream_for_split = async move {
                connector_clone.read_split(&split_clone).await.map_err(|e| {
                    // Convert connector's error (ArrowError) to EngineError
                    EngineError::ConnectorError(format!("Failed to read split {:?}: {}", split_clone.uri, e))
                })
            };
            streams.push(stream_for_split);
        }

        // futures::future::try_join_all will execute all futures (getting streams) concurrently.
        let individual_streams: Vec<RecordBatchStream> =
            futures::future::try_join_all(streams).await?;

        // futures::stream::select_all merges multiple streams into one.
        // It polls all underlying streams and yields items as they become available.
        // If any underlying stream returns an error, select_all forwards it.
        let merged_stream = stream::select_all(individual_streams);

        Ok(Box::pin(merged_stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use igloo_connector_filesystem::FilesystemConnector; // For testing ScanExec
    use std::path::PathBuf;
    use tokio::runtime::Runtime;

    // Helper to get the path to the sample Parquet file created by igloo-connector-filesystem tests
    // This assumes a known relative path between crates during testing.
    // This is fragile; a better way might be to copy/create a test file within igloo-engine's own test setup.
    fn get_sample_parquet_path_for_engine_test() -> String {
        // Construct path like: igloo/crates/engine/../../crates/connectors/filesystem/test_data/sample.parquet
        let mut cargo_manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")); // .../igloo/crates/engine
        cargo_manifest_dir.pop(); // .../igloo/crates
        cargo_manifest_dir.pop(); // .../igloo
        let path = cargo_manifest_dir
            .join("crates")
            .join("connectors")
            .join("filesystem")
            .join("test_data")
            .join("sample.parquet");

        // Panic if file doesn't exist, as test depends on it.
        if !path.exists() {
            // This check will only run if tests are executed.
            // For --no-run, this won't be hit, but the test relies on the file's presence.
            // The filesystem tests *should* create this. If not, this test is invalid.
            // We assume the file exists for test compilation for now.
            // A more robust setup would involve the engine crate creating its own test data
            // or having a shared test data mechanism.
            println!("Warning: Test Parquet file not found at {:?}. Test will fail if run.", path);
            // To prevent panic during --no-run if somehow this code path was evaluated by compiler:
            // return "".to_string();
            // However, for actual test execution, we need the path or panic.
            // For now, we'll proceed assuming it's there for compilation phase.
            // If the file system test for igloo-connector-filesystem did not run or failed, this will fail.
        }
        path.to_string_lossy().into_owned()
    }

    #[test]
    fn test_scan_exec_with_filesystem_connector() {
        // This test will likely not compile/run in the current environment due to the 'half' issue.
        // It is written for completeness.

        // Ensure the test file exists before running the test logic.
        // This is a runtime check.
        let sample_file_path_str = get_sample_parquet_path_for_engine_test();
        if !PathBuf::from(&sample_file_path_str).exists() {
            panic!("Test Parquet file not found at {}. Ensure 'igloo-connector-filesystem' tests have created it or run its test data generation script.", sample_file_path_str);
        }


        let rt = Runtime::new().unwrap();
        rt.block_on(async {
            // 1. Define the expected schema for the Parquet file
            let expected_schema = Arc::new(Schema::new(vec![
                Field::new("col1", DataType::Int64, true), // Parquet typically makes columns nullable
                Field::new("col2", DataType::Utf8, true),
            ]));

            // 2. Setup the FilesystemConnector
            let connector: Arc<dyn Connector> = Arc::new(FilesystemConnector::default());

            // 3. Create a Split for the sample Parquet file
            // let sample_file_path_str = get_sample_parquet_path_for_engine_test(); // already called
            let split_uri = format!("file://{}", sample_file_path_str);
            let splits = vec![Split { uri: split_uri }];

            // 4. Create ScanExec
            let scan_exec = ScanExec::new(splits, connector, expected_schema.clone());

            // 5. Execute ScanExec
            let mut stream_result = scan_exec.execute().await;
            assert!(stream_result.is_ok(), "ScanExec execute failed: {:?}", stream_result.err());

            let mut stream = stream_result.unwrap();
            let mut record_batch_count = 0;

            if let Some(batch_result) = stream.next().await {
                assert!(batch_result.is_ok(), "Reading batch failed: {:?}", batch_result.err());
                let batch = batch_result.unwrap();
                record_batch_count += 1;

                assert_eq!(batch.num_columns(), 2, "Expected 2 columns");
                assert_eq!(batch.num_rows(), 3, "Expected 3 rows"); // sample.parquet has 3 rows

                // Verify schema of the batch
                assert_eq!(batch.schema(), expected_schema);

                // Verify data for col1
                let col1 = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("Failed to downcast col1");
                assert_eq!(col1.values(), &[1, 2, 3]);

                // Verify data for col2
                let col2 = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Failed to downcast col2");
                let col2_values: Vec<Option<&str>> = col2.iter().collect();
                assert_eq!(col2_values, vec![Some("A"), Some("B"), Some("C")]);

            } else {
                panic!("Stream was empty, expected one RecordBatch.");
            }

            // Ensure no more batches
            assert!(stream.next().await.is_none(), "Expected only one RecordBatch from the stream");
            assert_eq!(record_batch_count, 1, "Expected one record batch from the sample file");

        });
    }
}
