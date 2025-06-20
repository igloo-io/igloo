use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::TableProvider;
use igloo_common::catalog::MemoryCatalog;
use std::sync::Arc;

fn create_mock_table_provider() -> Arc<dyn TableProvider> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    Arc::new(EmptyTable::new(schema))
}

#[test]
fn test_register_table() {
    let mut catalog = MemoryCatalog::new();
    let table_provider = create_mock_table_provider();
    catalog.register_table("test_table".to_string(), table_provider.clone());
    assert!(catalog.tables.contains_key("test_table"));
}

#[test]
fn test_get_table() {
    let mut catalog = MemoryCatalog::new();
    let table_provider = create_mock_table_provider();
    catalog.register_table("test_table".to_string(), table_provider.clone());
    let retrieved_table = catalog.get_table("test_table");
    assert!(retrieved_table.is_some());
}

#[test]
fn test_get_nonexistent_table() {
    let catalog = MemoryCatalog::new();
    let retrieved_table = catalog.get_table("nonexistent_table");
    assert!(retrieved_table.is_none());
}
