use async_stream::try_stream;
use async_trait::async_trait;
use futures::stream::{Stream, StreamExt};
use igloo_engine::physical_plan::{ExecutionError, RecordBatch}; // RecordBatch is Vec<Vec<String>>
use std::pin::Pin;
// Removed unused std::sync::Arc

// 1. Define the local Operator trait
#[async_trait]
pub trait Operator: Send + Sync {
    async fn execute( // Changed signature to take Pin<&mut Self>
        self: Pin<&mut Self>,
    ) -> Result<Pin<Box<dyn Stream<Item = Result<RecordBatch, ExecutionError>> + Send + Sync>>, ExecutionError>;
}

// Helper function to slice a RecordBatch (Vec<Vec<String>>)
fn slice_record_batch(batch: &RecordBatch, offset: usize, limit: usize) -> RecordBatch {
    batch.iter().skip(offset).take(limit).cloned().collect()
}

pub struct LimitOperator {
    input: Pin<Box<dyn Operator + Send + Sync>>, // Changed to Pin<Box<...>>
    offset: usize,
    limit: usize,
}

// LimitOperator is Unpin because all its fields are Unpin.
// Pin<Box<T>> is Unpin. usize is Unpin.
impl Unpin for LimitOperator {}


impl LimitOperator {
    pub fn new(
        input: Pin<Box<dyn Operator + Send + Sync>>, // Changed to Pin<Box<...>>
        offset: usize,
        limit: usize,
    ) -> Self {
        Self {
            input,
            offset,
            limit,
        }
    }
}

// 2. Implement the local Operator trait for LimitOperator
#[async_trait]
impl Operator for LimitOperator {
    async fn execute( // Changed signature to take Pin<&mut Self>
        mut self: Pin<&mut Self>, // Add mut here
    ) -> Result<Pin<Box<dyn Stream<Item = Result<RecordBatch, ExecutionError>> + Send + Sync>>, ExecutionError> {
        // Since LimitOperator is Unpin, we can use get_mut to get &mut LimitOperator
        let this = self.as_mut().get_mut(); // self is Pin<&mut LimitOperator>, so self.as_mut() is Pin<&mut LimitOperator>
                                          // then .get_mut() gives &mut LimitOperator

        let offset = this.offset;
        let limit = this.limit;

        // this.input is Pin<Box<dyn Operator>>. Call as_mut() to get Pin<&mut dyn Operator> for execute.
        let mut input_stream = this.input.as_mut().execute().await?;

        let stream = try_stream! {
            let mut rows_seen: usize = 0;
            let mut rows_emitted: usize = 0;

            if limit == 0 {
                // If limit is 0, no rows should be emitted, and we might not even need to pull from input.
                // The stream will simply end.
                return; // try_stream specific return
            }

            while let Some(batch_result) = input_stream.next().await {
                let batch = batch_result?;

                if rows_emitted >= limit {
                    break; // Limit reached
                }

                let batch_row_count = batch.len();
                if batch_row_count == 0 {
                    continue; // Skip empty batches
                }

                // Current number of rows from input that are relevant to this operator's output
                let mut current_batch_relevant_offset = 0;

                // Handle offset
                if rows_seen < offset {
                    if rows_seen + batch_row_count <= offset {
                        // This entire batch is skipped
                        rows_seen += batch_row_count;
                        continue;
                    } else {
                        // Part of this batch is skipped
                        current_batch_relevant_offset = offset - rows_seen;
                        rows_seen = offset; // Offset is now met for future batches
                    }
                }
                // At this point, rows_seen >= offset, or part of current batch makes it so.
                // And current_batch_relevant_offset is how many rows to skip IN THIS BATCH.

                let rows_to_potentially_emit_from_this_batch = batch_row_count - current_batch_relevant_offset;
                if rows_to_potentially_emit_from_this_batch == 0 {
                    continue;
                }

                let rows_can_still_emit = limit - rows_emitted;
                let rows_to_emit_from_this_batch = std::cmp::min(rows_to_potentially_emit_from_this_batch, rows_can_still_emit);

                if rows_to_emit_from_this_batch > 0 {
                    let output_batch = slice_record_batch(&batch, current_batch_relevant_offset, rows_to_emit_from_this_batch);
                    rows_emitted += output_batch.len();
                    yield output_batch;
                }

                if rows_emitted >= limit {
                    break; // Limit reached
                }
            }
        };

        Ok(Box::pin(stream))
    }
}

// Temp fix for Arc<dyn Operator>::execute call:
// Removed the blanket impl for Arc<T> as Operator

// We no longer need RecordBatchStream trait here if Operator trait replaces it for LimitOperator's input/output.
// However, igloo_engine::physical_plan::RecordBatchStream is still used by the Operator trait's execute method's return type.
// So the types `ExecutionError`, `RecordBatch` are from `igloo_engine::physical_plan`.
// If this Operator trait is meant to *be* the RecordBatchStream, then LimitOperator should not impl RecordBatchStream.
// But the problem asks for `LimitOperator::execute` to use `try_stream!` which implies it's producing a stream,
// consistent with the `Operator::execute` signature.

// The original code had `LimitOperator` also implementing `igloo_engine::physical_plan::RecordBatchStream`.
// If that's still desired, that implementation needs to be added back or reconciled with this new `Operator` trait.
// For now, focusing on the new `Operator` trait implementation as requested.

#[cfg(test)]
mod tests {
    use super::*; // Imports Operator, LimitOperator, RecordBatch, ExecutionError, etc.
    use futures::stream::{self, StreamExt}; // For creating test streams
    use tokio; // For async test runtime

    // MockOperator for testing purposes
    struct MockOperator {
        name: String, // For debugging
        data_chunks: Vec<RecordBatch>,
        times_execute_called: usize, // To check execution semantics
    }

    impl MockOperator {
        fn new(data_chunks: Vec<RecordBatch>) -> Self {
            Self {
                name: "mock".to_string(),
                data_chunks,
                times_execute_called: 0,
            }
        }
        fn new_named(name: String, data_chunks: Vec<RecordBatch>) -> Self {
            Self {
                name,
                data_chunks,
                times_execute_called: 0,
            }
        }
    }

    #[async_trait]
    impl Operator for MockOperator {
        async fn execute( // Changed signature to take Pin<&mut Self>
            mut self: Pin<&mut Self>, // Added mut here
        ) -> Result<Pin<Box<dyn Stream<Item = Result<RecordBatch, ExecutionError>> + Send + Sync>>, ExecutionError> {
            // Get mutable access to MockOperator fields. This is safe because MockOperator can be !Unpin.
            // For a struct to be used with `Pin<&mut Self>` in async_trait methods,
            // it doesn't need to be Unpin itself, but accessing its fields requires care if it's not.
            // Here, we use `get_mut` which is unsafe but common in `Pin` interop.
            // A safe way is to use `pin_project_lite` or similar for field access.
            // For MockOperator, since it's test code and simple, we can assume it's okay or make it Unpin.
            // Let's make MockOperator Unpin for simplicity in tests.
            // (Adding `impl Unpin for MockOperator {}` if no !Unpin fields, or use `pin_project_lite`)
            // For now, let's assume direct mutable access is fine for the mock.
            // Use self.as_mut().get_mut() to avoid moving self, as per compiler suggestion.
            let this = self.as_mut().get_mut(); // This is unsafe if MockOperator is not Unpin.
                                               // MockOperator is Unpin because its fields are Unpin.
            this.times_execute_called += 1;
            // In a real scenario, if execute can be called multiple times,
            // it should ideally produce a fresh stream.
            // For these tests, we assume execute is called once per "query execution".
            // If it's called again on the same MockOperator instance, it will yield the same data again.
            // This is fine for how LimitOperator uses it (calls execute once on its input).

            // `this` is now `&mut MockOperator`, so we can access its fields directly.
            let stream_data = this.data_chunks.clone(); // Clone data for the stream
            let output_stream = stream::iter(stream_data.into_iter().map(Ok));

            // println!("[{}] MockOperator::execute called, times: {}", this.name, this.times_execute_called);
            // for batch in &self.data_chunks {
            //     println!("[{}]   Yielding batch of size: {}", self.name, batch.len());
            // }

            Ok(Box::pin(output_stream))
        }
    }

    // Helper to collect all batches from a stream
    async fn collect_stream_results(
        mut stream: Pin<Box<dyn Stream<Item = Result<RecordBatch, ExecutionError>> + Send + Sync>>,
    ) -> Result<Vec<RecordBatch>, ExecutionError> {
        let mut result_vec = Vec::new();
        while let Some(item_result) = stream.next().await {
            match item_result {
                Ok(batch) => result_vec.push(batch),
                Err(e) => return Err(e),
            }
        }
        Ok(result_vec)
    }

    // Helper to create a dummy RecordBatch with i32 data (as strings)
    fn create_batch_from_sequence(start: i32, count: i32) -> RecordBatch {
        if count == 0 {
            return Vec::new();
        }
        (start..start + count)
            .map(|i| vec![i.to_string(), (i * 10).to_string()]) // Each row has two columns
            .collect()
    }

    #[tokio::test]
    async fn test_simple_limit_no_offset() {
        let input_batch = create_batch_from_sequence(0, 100); // 100 rows
        let mock_op = MockOperator::new(vec![input_batch.clone()]);
        let mut limit_op = LimitOperator::new(Box::pin(mock_op), 0, 10);

        let mut pinned_op = std::pin::pin!(limit_op);
        let result_stream = Operator::execute(pinned_op.as_mut()).await.unwrap();
        let output_batches = collect_stream_results(result_stream).await.unwrap();

        assert_eq!(output_batches.len(), 1, "Should be one output batch");
        assert_eq!(output_batches[0].len(), 10, "Batch should have 10 rows");
        assert_eq!(output_batches[0][0], vec!["0".to_string(), "0".to_string()]); // First row data
        assert_eq!(output_batches[0][9], vec!["9".to_string(), "90".to_string()]); // Last row data
    }

    #[tokio::test]
    async fn test_limit_with_offset() {
        let input_batch = create_batch_from_sequence(0, 100); // 100 rows
        let mock_op = MockOperator::new(vec![input_batch.clone()]);
        let mut limit_op = LimitOperator::new(Box::pin(mock_op), 80, 15);

        let mut pinned_op = std::pin::pin!(limit_op);
        let result_stream = Operator::execute(pinned_op.as_mut()).await.unwrap();
        let output_batches = collect_stream_results(result_stream).await.unwrap();

        assert_eq!(output_batches.len(), 1, "Should be one output batch");
        assert_eq!(output_batches[0].len(), 15, "Batch should have 15 rows");
        // Rows should be 80-94 from original
        assert_eq!(output_batches[0][0], vec!["80".to_string(), "800".to_string()]);
        assert_eq!(output_batches[0][14], vec!["94".to_string(), "940".to_string()]);
    }

    #[tokio::test]
    async fn test_limit_spanning_batches() {
        let batch1 = create_batch_from_sequence(0, 50);  // Rows 0-49
        let batch2 = create_batch_from_sequence(50, 50); // Rows 50-99
        let mock_op = MockOperator::new(vec![batch1.clone(), batch2.clone()]);
        let mut limit_op = LimitOperator::new(Box::pin(mock_op), 40, 20);

        let mut pinned_op = std::pin::pin!(limit_op);
        let result_stream = Operator::execute(pinned_op.as_mut()).await.unwrap();
        let output_batches = collect_stream_results(result_stream).await.unwrap();

        assert_eq!(output_batches.len(), 2, "Should be two output batches");

        assert_eq!(output_batches[0].len(), 10, "First output batch should have 10 rows");
        assert_eq!(output_batches[0][0], vec!["40".to_string(), "400".to_string()]); // Row 40
        assert_eq!(output_batches[0][9], vec!["49".to_string(), "490".to_string()]); // Row 49

        assert_eq!(output_batches[1].len(), 10, "Second output batch should have 10 rows");
        assert_eq!(output_batches[1][0], vec!["50".to_string(), "500".to_string()]); // Row 50
        assert_eq!(output_batches[1][9], vec!["59".to_string(), "590".to_string()]); // Row 59
    }

    #[tokio::test]
    async fn test_offset_exact_batch_boundary() {
        let batch1 = create_batch_from_sequence(0, 50);
        let batch2 = create_batch_from_sequence(50, 50);
        let mock_op = MockOperator::new(vec![batch1.clone(), batch2.clone()]);
        let mut limit_op = LimitOperator::new(Box::pin(mock_op), 50, 10);

        let mut pinned_op = std::pin::pin!(limit_op);
        let result_stream = Operator::execute(pinned_op.as_mut()).await.unwrap();
        let output_batches = collect_stream_results(result_stream).await.unwrap();

        assert_eq!(output_batches.len(), 1);
        assert_eq!(output_batches[0].len(), 10);
        assert_eq!(output_batches[0][0], vec!["50".to_string(), "500".to_string()]); // Starts from row 50
    }

    #[tokio::test]
    async fn test_limit_exact_batch_boundary() {
        let batch1 = create_batch_from_sequence(0, 10);
        let batch2 = create_batch_from_sequence(10, 50);
        let mock_op = MockOperator::new(vec![batch1.clone(), batch2.clone()]);
        let mut limit_op = LimitOperator::new(Box::pin(mock_op), 0, 10);

        let mut pinned_op = std::pin::pin!(limit_op);
        let result_stream = Operator::execute(pinned_op.as_mut()).await.unwrap();
        let output_batches = collect_stream_results(result_stream).await.unwrap();

        assert_eq!(output_batches.len(), 1);
        assert_eq!(output_batches[0].len(), 10);
        assert_eq!(output_batches[0][9], vec!["9".to_string(), "90".to_string()]);
        // Check that input operator was not polled for the second batch unnecessarily.
        // This requires MockOperator to track how its execute or next was called.
        // The blanket impl for Arc<Operator> calls execute on Arc::get_mut(self.input)
        // This means the mock_op itself is not directly in LimitOperator after new().
        // To test this, we would need to pass Arc<Mutex<MockOperator>> or similar.
        // For now, this aspect (early termination of input) is harder to test with current Arc<dyn Operator> setup.
    }

    #[tokio::test]
    async fn test_limit_zero() {
        let input_batch = create_batch_from_sequence(0, 100);
        let mock_op = MockOperator::new(vec![input_batch.clone()]);
        let mut limit_op = LimitOperator::new(Box::pin(mock_op), 0, 0);

        let mut pinned_op = std::pin::pin!(limit_op);
        let result_stream = Operator::execute(pinned_op.as_mut()).await.unwrap();
        let output_batches = collect_stream_results(result_stream).await.unwrap();

        assert_eq!(output_batches.len(), 0, "Should be no output batches for limit 0");
    }

    #[tokio::test]
    async fn test_offset_greater_than_input_size() {
        let input_batch = create_batch_from_sequence(0, 50);
        let mock_op = MockOperator::new(vec![input_batch.clone()]);
        let mut limit_op = LimitOperator::new(Box::pin(mock_op), 100, 10);

        let mut pinned_op = std::pin::pin!(limit_op);
        let result_stream = Operator::execute(pinned_op.as_mut()).await.unwrap();
        let output_batches = collect_stream_results(result_stream).await.unwrap();

        assert_eq!(output_batches.len(), 0, "Should be no output batches");
    }

    #[tokio::test]
    async fn test_offset_plus_limit_greater_than_input_size() {
        let batch1 = create_batch_from_sequence(0, 30);
        let batch2 = create_batch_from_sequence(30, 30); // Total 60 rows
        let mock_op = MockOperator::new(vec![batch1, batch2]);
        let mut limit_op = LimitOperator::new(Box::pin(mock_op), 40, 30);

        let mut pinned_op = std::pin::pin!(limit_op);
        let result_stream = Operator::execute(pinned_op.as_mut()).await.unwrap();
        let output_batches = collect_stream_results(result_stream).await.unwrap();

        assert_eq!(output_batches.len(), 1); // All remaining rows fit in one output batch from batch2
        assert_eq!(output_batches[0].len(), 20);
        assert_eq!(output_batches[0][0], vec!["40".to_string(), "400".to_string()]);
        assert_eq!(output_batches[0][19], vec!["59".to_string(), "590".to_string()]);
    }

    #[tokio::test]
    async fn test_empty_input_batches() {
        let batch1 = create_batch_from_sequence(0, 10);
        let empty_batch = create_batch_from_sequence(0,0);
        let batch2 = create_batch_from_sequence(10, 10);
        let mock_op = MockOperator::new(vec![batch1, empty_batch, batch2]);
        let mut limit_op = LimitOperator::new(Box::pin(mock_op), 5, 10);

        let mut pinned_op = std::pin::pin!(limit_op);
        let result_stream = Operator::execute(pinned_op.as_mut()).await.unwrap();
        let output_batches = collect_stream_results(result_stream).await.unwrap();

        assert_eq!(output_batches.len(), 2);
        assert_eq!(output_batches[0].len(), 5);
        assert_eq!(output_batches[0][0], vec!["5".to_string(), "50".to_string()]);
        assert_eq!(output_batches[1].len(), 5);
        assert_eq!(output_batches[1][0], vec!["10".to_string(), "100".to_string()]);
    }

    #[tokio::test]
    async fn test_input_finishes_early_before_limit_met() {
        let input_batch = create_batch_from_sequence(0, 10); // Only 10 rows
        let mock_op = MockOperator::new(vec![input_batch.clone()]);
        let mut limit_op = LimitOperator::new(Box::pin(mock_op), 0, 20);

        let mut pinned_op = std::pin::pin!(limit_op);
        let result_stream = Operator::execute(pinned_op.as_mut()).await.unwrap();
        let output_batches = collect_stream_results(result_stream).await.unwrap();

        assert_eq!(output_batches.len(), 1);
        assert_eq!(output_batches[0].len(), 10); // Emits all it got
        assert_eq!(output_batches[0][0], vec!["0".to_string(), "0".to_string()]);
    }
}
