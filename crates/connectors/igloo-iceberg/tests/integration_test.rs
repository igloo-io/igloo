use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::execution::context::{SessionContext, SessionStateTaskContext}; // Corrected import for SessionState
use datafusion::physical_plan::collect;
use igloo_iceberg::IcebergTableProvider; // Assuming this is the path
// Do not use iceberg_rust::catalog::Catalog directly, use specific catalog like MemoryCatalog
use iceberg_rust::catalog::memory::MemoryCatalog;
use iceberg_rust::object_store::local::LocalFileSystem;
use iceberg_rust::table::Table;
use iceberg_rust::table::transaction::TableTransaction; // Required for table creation
use iceberg_rust::spec::{
    manifest::{DataFile, FileFormat},
    partition::PartitionData,
    schema::Schema as IcebergSchemaInternal, // Alias to avoid conflict with DataFusion's Schema
    types::{PrimitiveType, Type as IcebergType},
    values::Value, // For potential future use with partition data
};
use iceberg_rust::spec::schema::NestedField; // Required for schema building

use std::fs;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;
use tokio;
use uuid::Uuid; // For generating unique IDs

// Helper function to create a minimal Parquet file from CSV for the test
async fn create_parquet_from_csv(
    csv_path: &Path,
    parquet_path: &Path,
    schema: Arc<Schema>, // DataFusion Arrow Schema
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = SessionContext::new();
    ctx.register_csv(
        "temp_csv",
        csv_path.to_str().unwrap(),
        datafusion::datasource::file_format::csv::CsvReadOptions::new().schema(&schema),
    )
    .await?;
    let df = ctx.sql("SELECT * FROM temp_csv").await?;
    df.write_parquet(parquet_path.to_str().unwrap(), datafusion::sql::DataFrameWriteOptions::default(), None)
      .await?;
    Ok(())
}

// Helper to set up a temporary Iceberg table structure for testing
async fn setup_test_iceberg_table(
    base_dir: &TempDir,
) -> Result<Arc<Table>, Box<dyn std::error::Error>> {
    let table_root_path = base_dir.path().join("test_table");
    fs::create_dir_all(table_root_path.join("data"))?;
    fs::create_dir_all(table_root_path.join("metadata"))?;

    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false), // Arrow: non-nullable
        Field::new("data", DataType::Utf8, true), // Arrow: nullable
    ]));

    // Create CSV data
    let csv_data_path = table_root_path.join("data/sample_data.csv");
    let mut wtr = csv::Writer::from_path(&csv_data_path)?;
    wtr.write_record(&["id", "data"])?;
    wtr.write_record(&["1", "apple"])?;
    wtr.write_record(&["2", "banana"])?;
    wtr.write_record(&["3", "cherry"])?;
    wtr.flush()?;

    // Convert CSV to Parquet
    let parquet_data_path = table_root_path.join("data/sample.parquet");
    create_parquet_from_csv(&csv_data_path, &parquet_data_path, arrow_schema.clone()).await?;
    let parquet_file_size = fs::metadata(&parquet_data_path)?.len();

    // Initialize ObjectStore for iceberg-rust
    let object_store = Arc::new(LocalFileSystem::new_with_prefix(table_root_path.clone())?);

    // Define Iceberg Schema
    let iceberg_schema = IcebergSchemaInternal::builder()
        .with_schema_id(0)
        .with_fields(vec![
            NestedField::required(1, "id", IcebergType::Primitive(PrimitiveType::Int)).into(),
            NestedField::optional(2, "data", IcebergType::Primitive(PrimitiveType::String)).into(),
        ])
        .build()?;

    // Use iceberg-rust to create actual manifest list and manifest files
    // This requires initializing a Catalog and performing a transaction.
    let mut catalog = MemoryCatalog::new("test_catalog", object_store.clone());
    catalog.initialize(Vec::new().into_iter().collect()).await?; // Initialize with no specific properties

    let table_namespace = iceberg_rust::catalog::Namespace::try_new(vec!["test_db".to_string()])?;
    let table_name = iceberg_rust::catalog::TableName::new("test_table");
    let table_identifier = iceberg_rust::catalog::TableIdentifier::new(table_namespace, table_name);

    let mut transaction = TableTransaction::new_table_transaction(
        &table_identifier,
        iceberg_schema,
        iceberg_rust::spec::partition::PartitionSpec::builder().with_spec_id(0).build()?,
        table_root_path.to_str().unwrap().to_string(), // location
        &mut catalog,
        object_store.clone(),
    )?;

    let data_file = DataFile::builder()
        .with_file_path(format!("{}/data/sample.parquet", table_root_path.to_str().unwrap()))
        .with_file_format(FileFormat::Parquet)
        .with_partition(PartitionData::empty(0)) // No partitioning
        .with_record_count(3)
        .with_file_size_in_bytes(parquet_file_size)
        // TODO: Add column stats if possible/easy, requires more detailed work
        .build()?;

    transaction.append_datafile(vec![data_file]);
    transaction.commit().await?; // This creates v1.metadata.json and manifest files

    // Load the table using iceberg-rust to verify and return
    let table = Table::load_file_system_table(table_root_path.to_str().unwrap(), &object_store).await?;
    Ok(Arc::new(table))
}


#[tokio::test]
async fn test_iceberg_table_provider_select_all() -> Result<(), Box<dyn std::error::Error>> {
    let base_dir = TempDir::new()?;
    let table_root_path = base_dir.path().join("test_table"); // Define for object store prefix
    let table = setup_test_iceberg_table(&base_dir).await?;

    // Create a new object store instance for the provider, scoped to the table root
    let provider_object_store = Arc::new(LocalFileSystem::new_with_prefix(&table_root_path)?);
    let provider = IcebergTableProvider::new(table.clone(), provider_object_store);

    let ctx = SessionContext::new();
    ctx.register_table("test_iceberg", Arc::new(provider))?;

    let df = ctx.sql("SELECT id, data FROM test_iceberg ORDER BY id").await?;
    let task_ctx = Arc::new(SessionStateTaskContext::new()); // Create task context
    let batches = collect(df.into_optimized_plan()?, task_ctx).await?;


    assert_eq!(batches.len(), 1, "Expected one RecordBatch");
    let batch = &batches[0];

    // Expected schema from DataFusion's perspective
    let expected_arrow_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false), // Matched to Iceberg: required, int
        Field::new("data", DataType::Utf8, true),  // Matched to Iceberg: optional, string
    ]));
    assert_eq!(batch.schema(), expected_arrow_schema, "Schema mismatch");

    assert_eq!(batch.num_columns(), 2);
    assert_eq!(batch.num_rows(), 3);

    let id_col = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    let data_col = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();

    assert_eq!(id_col.values(), &[1, 2, 3]);
    assert_eq!(data_col.value(0), "apple");
    assert_eq!(data_col.value(1), "banana");
    assert_eq!(data_col.value(2), "cherry");

    base_dir.close()?;
    Ok(())
}

#[tokio::test]
async fn test_iceberg_table_provider_projection() -> Result<(), Box<dyn std::error::Error>> {
    let base_dir = TempDir::new()?;
    let table_root_path = base_dir.path().join("test_table");
    let table = setup_test_iceberg_table(&base_dir).await?;
    let provider_object_store = Arc::new(LocalFileSystem::new_with_prefix(&table_root_path)?);

    let provider = IcebergTableProvider::new(table.clone(), provider_object_store);

    let ctx = SessionContext::new();
    ctx.register_table("test_iceberg_proj", Arc::new(provider))?;

    // Order by data to ensure consistent results for assertion, as scan might not preserve input order
    let df = ctx.sql("SELECT data FROM test_iceberg_proj ORDER BY data").await?;
    let task_ctx = Arc::new(SessionStateTaskContext::new());
    let batches = collect(df.into_optimized_plan()?, task_ctx).await?;

    assert_eq!(batches.len(), 1);
    let batch = &batches[0];

    let expected_arrow_schema = Arc::new(Schema::new(vec![
        Field::new("data", DataType::Utf8, true),
    ]));
    assert_eq!(batch.schema(), expected_arrow_schema, "Schema mismatch for projection");

    assert_eq!(batch.num_columns(), 1);
    assert_eq!(batch.num_rows(), 3);
    let data_col = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    // Values are apple, banana, cherry. Sorted: apple, banana, cherry
    assert_eq!(data_col.value(0), "apple");
    assert_eq!(data_col.value(1), "banana");
    assert_eq!(data_col.value(2), "cherry");

    base_dir.close()?;
    Ok(())
}
