use crate::error::IglooError;
use arrow::record_batch::RecordBatch;
use futures::Stream;

#[async_trait::async_trait]
pub trait Operator {
    async fn execute(
        &self,
    ) -> Result<Box<dyn Stream<Item = Result<RecordBatch, IglooError>> + Send + Unpin>, IglooError>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use futures::stream::{self, TryStreamExt};
    use std::sync::Arc;

    struct MockOperator {
        data: Vec<RecordBatch>,
    }

    #[async_trait::async_trait]
    impl Operator for MockOperator {
        async fn execute(
            &self,
        ) -> Result<
            Box<dyn Stream<Item = Result<RecordBatch, IglooError>> + Send + Unpin>,
            IglooError,
        > {
            let stream = stream::iter(self.data.clone().into_iter().map(Ok));
            Ok(Box::new(stream))
        }
    }

    #[tokio::test]
    async fn test_mock_operator() {
        // Arrange
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch1 =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2, 3]))])
                .unwrap();
        let batch2 =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![4, 5, 6]))])
                .unwrap();
        let expected_batches = vec![batch1.clone(), batch2.clone()];

        let mock_operator = MockOperator { data: expected_batches.clone() };

        // Act
        let stream = mock_operator.execute().await.unwrap();
        let collected_batches: Vec<RecordBatch> = stream.try_collect::<Vec<_>>().await.unwrap();

        // Assert
        assert_eq!(collected_batches.len(), expected_batches.len());
        for (collected, expected) in collected_batches.iter().zip(expected_batches.iter()) {
            assert_eq!(collected.schema(), expected.schema());
            assert_eq!(collected.num_columns(), expected.num_columns());
            assert_eq!(collected.num_rows(), expected.num_rows());
            for i in 0..collected.num_columns() {
                assert_eq!(collected.column(i).as_ref(), expected.column(i).as_ref());
            }
        }
    }
}
