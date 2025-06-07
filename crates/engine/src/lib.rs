pub mod physical_plan;
pub mod planner;

use arrow_array::RecordBatch;
use arrow_schema::ArrowError;
use futures::Stream;
use std::pin::Pin;

#[derive(Debug, thiserror::Error)]
pub enum ExecutionError {
    #[error("Arrow error: {0}")]
    ArrowError(#[from] ArrowError),
    #[error("Execution error: {0}")]
    General(String),
    // Add other error variants as they become necessary
}

pub type RecordBatchStream =
    Pin<Box<dyn Stream<Item = Result<RecordBatch, ExecutionError>> + Send>>;

// TODO: Implement query engine logic
