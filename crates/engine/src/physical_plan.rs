use arrow_schema::SchemaRef;
use std::sync::Arc; // Changed from arrow::datatypes::SchemaRef due to common module structure in arrow-rs

// Import the placeholder Expression from the planner module
use crate::planner::{evaluate_expression, Expression};
use crate::{ExecutionError, RecordBatchStream};
use arrow::compute::filter_record_batch; // Corrected path
use arrow_array::{BooleanArray, Int32Array, RecordBatch, StringArray}; // Removed ArrayRef
use arrow_schema::{DataType, Field, Schema}; // Removed ArrowError
use async_stream::try_stream;
use futures::stream; // Removed StreamExt

pub enum PhysicalPlan {
    /// Scans data from a source.
    ScanExec {
        schema: SchemaRef,
        // For now, it will generate dummy data. Later it will take partitions.
        // We might need to store table_name or projection here later.
    },
    /// Filters a stream of data.
    FilterExec {
        predicate: Expression,
        input: Arc<PhysicalPlan>,
        schema: SchemaRef, // FilterExec output schema is same as input
    },
    // Potentially ProjectionExec later
    // ProjectionExec {
    // input: Arc<PhysicalPlan>,
    // expr: Vec<Expression>, // These would be PhysicalExpressions
    // schema: SchemaRef,
    // },
}

impl PhysicalPlan {
    /// Each physical plan node must know the schema of the data it produces.
    pub fn schema(&self) -> SchemaRef {
        match self {
            PhysicalPlan::ScanExec { schema, .. } => Arc::clone(schema),
            PhysicalPlan::FilterExec { schema, .. } => Arc::clone(schema),
            // PhysicalPlan::ProjectionExec { schema, .. } => Arc::clone(schema),
        }
    }

    pub fn execute(&self) -> RecordBatchStream {
        match self {
            PhysicalPlan::ScanExec { schema: _ } => {
                // input schema is shadowed for now by dummy data schema
                // Define a simple schema for two columns: id (int) and name (string)
                let exec_schema = Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    Field::new("name", DataType::Utf8, false),
                ]));

                let batch1 = RecordBatch::try_new(
                    Arc::clone(&exec_schema),
                    vec![
                        Arc::new(Int32Array::from(vec![1, 2, 3, 6, 7])),
                        Arc::new(StringArray::from(vec![
                            "Alice", "Bob", "Charlie", "FilterMe", "KeepMe",
                        ])),
                    ],
                )
                .unwrap();

                let batch2 = RecordBatch::try_new(
                    Arc::clone(&exec_schema),
                    vec![
                        Arc::new(Int32Array::from(vec![4, 5, 8])),
                        Arc::new(StringArray::from(vec!["David", "Eve", "Another"])),
                    ],
                )
                .unwrap();

                let stream_items: Vec<Result<RecordBatch, ExecutionError>> =
                    vec![Ok(batch1), Ok(batch2)];

                Box::pin(stream::iter(stream_items))
            }
            PhysicalPlan::FilterExec {
                predicate,
                input,
                schema: _,
            } => {
                let input_stream = input.execute(); // Get stream from input plan
                let pred_clone = predicate.clone(); // Clone predicate for use in async block

                // Use try_stream to build the output stream
                Box::pin(try_stream! {
                    for await batch_result in input_stream {
                        let batch = batch_result?; // Propagate error if input stream fails

                        // Evaluate the predicate expression against the current batch
                        let filter_array = evaluate_expression(&batch, &pred_clone)
                            .map_err(ExecutionError::ArrowError)?; // Convert ArrowError to ExecutionError

                        // filter_record_batch expects a BooleanArray
                        let boolean_filter_array = filter_array
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .ok_or_else(|| ExecutionError::General("Filter predicate did not evaluate to a BooleanArray".to_string()))?;

                        // Apply the filter
                        let filtered_batch = filter_record_batch(&batch, boolean_filter_array)
                            .map_err(ExecutionError::ArrowError)?;

                        // Yield the filtered batch if it's not empty
                        if filtered_batch.num_rows() > 0 {
                            yield filtered_batch;
                        }
                    }
                })
            }
        }
    }
}
