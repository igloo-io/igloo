//! Physical plan representation for query execution

use crate::operators::{FilterExec, ParquetScanExec, ProjectionExec, HashJoinExec};
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use futures::stream::BoxStream;
use std::sync::Arc;

/// Trait for executing physical plans
#[async_trait::async_trait]
pub trait ExecutionPlan: Send + Sync {
    /// Execute the plan and return a stream of record batches
    async fn execute(&self) -> DataFusionResult<BoxStream<'static, DataFusionResult<RecordBatch>>>;
    
    /// Get the schema of the output
    fn schema(&self) -> Arc<arrow::datatypes::Schema>;
}

/// Physical plan representation
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    ParquetScan(ParquetScanExec),
    Projection(ProjectionExec),
    Filter(FilterExec),
    HashJoin(HashJoinExec),
}

#[async_trait::async_trait]
impl ExecutionPlan for PhysicalPlan {
    async fn execute(&self) -> DataFusionResult<BoxStream<'static, DataFusionResult<RecordBatch>>> {
        match self {
            PhysicalPlan::ParquetScan(exec) => exec.execute().await,
            PhysicalPlan::Projection(exec) => exec.execute().await,
            PhysicalPlan::Filter(exec) => exec.execute().await,
            PhysicalPlan::HashJoin(exec) => exec.execute().await,
        }
    }

    fn schema(&self) -> Arc<arrow::datatypes::Schema> {
        match self {
            PhysicalPlan::ParquetScan(exec) => exec.schema(),
            PhysicalPlan::Projection(exec) => exec.schema(),
            PhysicalPlan::Filter(exec) => exec.schema(),
            PhysicalPlan::HashJoin(exec) => exec.schema(),
        }
    }
}

impl From<ParquetScanExec> for PhysicalPlan {
    fn from(exec: ParquetScanExec) -> Self {
        PhysicalPlan::ParquetScan(exec)
    }
}

impl From<ProjectionExec> for PhysicalPlan {
    fn from(exec: ProjectionExec) -> Self {
        PhysicalPlan::Projection(exec)
    }
}

impl From<FilterExec> for PhysicalPlan {
    fn from(exec: FilterExec) -> Self {
        PhysicalPlan::Filter(exec)
    }
}

impl From<HashJoinExec> for PhysicalPlan {
    fn from(exec: HashJoinExec) -> Self {
        PhysicalPlan::HashJoin(exec)
    }
}