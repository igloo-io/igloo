//! Integration tests for the query engine

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use igloo_common::catalog::MemoryCatalog;
use igloo_engine::{QueryEngine, PhysicalPlanner};
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_end_to_end_parquet_query() {
    // Create a temporary directory for test data
    let temp_dir = TempDir::new().unwrap();
    let parquet_path = temp_dir.path().join("test_data.parquet");
    
    // Create sample data
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
    ]));
    
    let ids = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let names = StringArray::from(vec!["Alice", "Bob", "Charlie", "Diana", "Eve"]);
    let ages = Int32Array::from(vec![25, 30, 35, 28, 32]);
    
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(ids), Arc::new(names), Arc::new(ages)],
    ).unwrap();
    
    // Write to Parquet file
    let file = File::create(&parquet_path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    
    // Test the query engine
    let engine = QueryEngine::new();
    
    // Register the Parquet file as a table
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    
    let table_path = ListingTableUrl::parse(format!("file://{}", parquet_path.display())).unwrap();
    let config = ListingTableConfig::new(table_path)
        .with_listing_options(ListingOptions::new(Arc::new(ParquetFormat::default())));
    
    let table_provider = Arc::new(ListingTable::try_new(config).unwrap());
    engine.register_table("test_table", table_provider).unwrap();
    
    // Execute a query
    let sql = "SELECT name, age FROM test_table WHERE age > 30 ORDER BY age";
    let results = engine.execute(sql).await;
    
    // Verify results
    assert_eq!(results.len(), 1);
    let result_batch = &results[0];
    assert_eq!(result_batch.num_rows(), 2); // Charlie (35) and Eve (32)
    
    let names_column = result_batch.column_by_name("name").unwrap()
        .as_any().downcast_ref::<StringArray>().unwrap();
    let ages_column = result_batch.column_by_name("age").unwrap()
        .as_any().downcast_ref::<Int32Array>().unwrap();
    
    assert_eq!(names_column.value(0), "Eve");   // age 32
    assert_eq!(ages_column.value(0), 32);
    assert_eq!(names_column.value(1), "Charlie"); // age 35
    assert_eq!(ages_column.value(1), 35);
}

#[tokio::test]
async fn test_physical_planner_integration() {
    // Create catalog and session context
    let catalog = Arc::new(MemoryCatalog::new());
    let session_ctx = Arc::new(SessionContext::new());
    let planner = PhysicalPlanner::new(catalog.clone(), session_ctx.clone());
    
    // Create a simple logical plan
    let sql = "SELECT 42 as answer";
    let logical_plan = session_ctx.sql(sql).await.unwrap().into_optimized_plan().unwrap();
    
    // Convert to physical plan
    let physical_plan = planner.create_physical_plan(&logical_plan).await.unwrap();
    
    // Execute the physical plan
    let mut stream = physical_plan.execute().await.unwrap();
    let mut batches = Vec::new();
    
    use futures::StreamExt;
    while let Some(batch_result) = stream.next().await {
        batches.push(batch_result.unwrap());
    }
    
    // Verify results
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 1);
    
    let answer_column = batch.column_by_name("answer").unwrap()
        .as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(answer_column.value(0), 42);
}