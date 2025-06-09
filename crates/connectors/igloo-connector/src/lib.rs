// In crates/connectors/igloo-connector/src/lib.rs
use arrow::record_batch::RecordBatch;
use futures::Stream;
use std::pin::Pin;

/// A Split is an opaque handle to a unit of work for a connector.
/// For a filesystem, this would be a single file path.
/// For a database, it could be a query or a range of primary keys.
#[derive(Debug, Clone)]
pub struct Split {
    pub uri: String, // e.g., "file:///path/to/data.parquet"
}

/// The core trait for any Igloo data source connector.
#[async_trait::async_trait]
pub trait Connector: Send + Sync {
    /// Given a table identifier, return a list of splits.
    async fn get_splits(&self, table: &str) -> Result<Vec<Split>, arrow::error::ArrowError>; // Assuming Result uses ArrowError for now

    /// Given a split, return a stream of Arrow RecordBatches.
    async fn read_split(
        &self,
        split: &Split,
    ) -> Result<
        Pin<Box<dyn Stream<Item = Result<RecordBatch, arrow::error::ArrowError>> + Send>>,
        arrow::error::ArrowError,
    >;
}
