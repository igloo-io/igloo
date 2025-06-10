use arrow::record_batch::RecordBatch;
use futures::{Stream, StreamExt, TryStreamExt};
use igloo_core::{error::IglooError, operator::Operator};
use std::sync::Arc;

pub struct ProjectOperator {
    pub input: Arc<dyn Operator + Send + Sync>,
    pub projection: Vec<usize>,
}

#[async_trait::async_trait]
impl Operator for ProjectOperator {
    async fn execute(
        &self,
    ) -> Result<Box<dyn Stream<Item = Result<RecordBatch, IglooError>> + Send + Unpin>, IglooError>
    {
        let input_stream = self.input.execute().await?;
        let projection = self.projection.clone(); // Clone to move into the closure

        let output_stream = input_stream.map(move |record_batch_result| {
            record_batch_result.and_then(|record_batch| {
                record_batch.project(&projection).map_err(IglooError::Arrow)
            })
        });

        Ok(Box::new(output_stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use futures::stream::{self, TryStreamExt}; // Added TryStreamExt
    use std::sync::Arc;

    // MockOperator for testing purposes
    struct MockOperator {
        data: Vec<RecordBatch>,
    }

    #[async_trait] // Removed ::async_trait
    impl Operator for MockOperator {
        async fn execute(
            &self,
        ) -> Result<Box<dyn Stream<Item = Result<RecordBatch, IglooError>> + Send + Unpin>, IglooError>
        {
            let stream = stream::iter(self.data.clone().into_iter().map(Ok));
            Ok(Box::new(stream))
        }
    }

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )
        .unwrap();
        batch
    }

    #[tokio::test]
    async fn test_project_operator_selects_columns() {
        // Arrange
        let source_batch = create_test_batch();
        let mock_operator = Arc::new(MockOperator {
            data: vec![source_batch.clone()],
        });
        let projection_indices = vec![0, 2]; // Select columns "a" and "c"
        let project_operator = ProjectOperator {
            input: mock_operator,
            projection: projection_indices.clone(),
        };

        // Act
        let result_stream = project_operator.execute().await.unwrap();
        let result_batches: Vec<RecordBatch> = result_stream.try_collect().await.unwrap();
        let projected_batch = &result_batches[0];

        // Assert
        // 1. Schema of the output RecordBatch contains only the projected fields
        let expected_schema = Arc::new(Schema::new(vec![
            source_batch.schema().field(0).clone(), // "a"
            source_batch.schema().field(2).clone(), // "c"
        ]));
        assert_eq!(projected_batch.schema(), expected_schema);

        // 2. Number of columns in the output is correct
        assert_eq!(projected_batch.num_columns(), projection_indices.len());

        // 3. Data within the projected columns is identical to the source data
        assert_eq!(projected_batch.column(0).as_ref(), source_batch.column(0).as_ref()); // Column "a"
        assert_eq!(projected_batch.column(1).as_ref(), source_batch.column(2).as_ref()); // Column "c"
    }

    #[tokio::test]
    async fn test_project_operator_empty_projection() {
        // Arrange
        let source_batch = create_test_batch();
        let mock_operator = Arc::new(MockOperator {
            data: vec![source_batch.clone()],
        });
        let projection_indices = vec![]; // Select no columns
        let project_operator = ProjectOperator {
            input: mock_operator,
            projection: projection_indices.clone(),
        };

        // Act
        let result_stream = project_operator.execute().await.unwrap();
        let result_batches: Vec<RecordBatch> = result_stream.try_collect().await.unwrap();
        let projected_batch = &result_batches[0];

        // Assert
        let expected_schema = Arc::new(Schema::new(Vec::<Field>::new()));
        assert_eq!(projected_batch.schema(), expected_schema);
        assert_eq!(projected_batch.num_columns(), 0);
    }

    #[tokio::test]
    async fn test_project_operator_all_columns() {
        // Arrange
        let source_batch = create_test_batch();
        let mock_operator = Arc::new(MockOperator {
            data: vec![source_batch.clone()],
        });
        let projection_indices = vec![0, 1, 2]; // Select all columns
        let project_operator = ProjectOperator {
            input: mock_operator,
            projection: projection_indices.clone(),
        };

        // Act
        let result_stream = project_operator.execute().await.unwrap();
        let result_batches: Vec<RecordBatch> = result_stream.try_collect().await.unwrap();
        let projected_batch = &result_batches[0];

        // Assert
        assert_eq!(projected_batch.schema(), source_batch.schema());
        assert_eq!(projected_batch.num_columns(), 3);
        assert_eq!(projected_batch.column(0).as_ref(), source_batch.column(0).as_ref());
        assert_eq!(projected_batch.column(1).as_ref(), source_batch.column(1).as_ref());
        assert_eq!(projected_batch.column(2).as_ref(), source_batch.column(2).as_ref());
    }
}
