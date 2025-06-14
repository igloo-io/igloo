//! Engine crate
//!
//! Implements the core query engine for Igloo.
//!
//! # Example
//! ```rust
//! // Example usage will go here once implemented
//! ```
//!
//! # TODO
//! Implement query engine logic

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext; // Changed this line

pub struct QueryEngine {
    ctx: SessionContext,
}

impl Default for QueryEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryEngine {
    pub fn new() -> Self {
        QueryEngine { ctx: SessionContext::new() }
    }

    // Modify this function
    pub async fn execute(&self, sql: &str) -> Vec<RecordBatch> {
        let df = self.ctx.sql(sql).await.expect("SQL execution failed");
        df.collect().await.expect("Failed to collect results")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array; // Changed from Int32Array
    use datafusion::arrow::datatypes::{DataType, Field, Schema}; // Changed this line
    use std::sync::Arc; // Added based on subtask description note

    #[tokio::test]
    async fn can_execute_simple_query() {
        // Arrange
        let engine = QueryEngine::new();
        let sql = "SELECT 42 as answer;";

        // Act
        let results = engine.execute(sql).await;

        // Assert
        assert_eq!(results.len(), 1, "Expected one RecordBatch");

        let batch = &results[0];

        // Check schema
        let expected_schema = Schema::new(vec![Field::new("answer", DataType::Int64, false)]); // Changed Int32 to Int64
        assert_eq!(batch.schema(), Arc::new(expected_schema), "Schema mismatch");

        // Check data
        assert_eq!(batch.num_rows(), 1, "Expected one row");
        let answer_column = batch
            .column_by_name("answer")
            .expect("answer column not found")
            .as_any()
            .downcast_ref::<Int64Array>() // Changed from Int32Array
            .expect("Failed to downcast to Int64Array"); // Changed message

        assert_eq!(answer_column.value(0), 42, "Incorrect value in answer column");
    }
}
