use std::sync::Arc;
use std::thread;

use arrow_schema::{DataType, Field, Schema as ArrowSchema};
use igloo_coordinator::catalog::{Catalog, Source, Table};

fn create_test_table(table_name: &str) -> Table {
    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        Field::new("column1", DataType::Int32, false),
        Field::new("column2", DataType::Utf8, true),
    ]));
    Table {
        name: table_name.to_string(),
        schema: arrow_schema,
        source: Source::Parquet, // Placeholder source
    }
}

#[test]
fn test_create_new_catalog() {
    let catalog = Catalog::new();
    assert_eq!(catalog.schemas.len(), 0);
}

#[test]
fn test_add_and_get_schema() {
    let catalog = Catalog::new();
    let schema_name = "test_schema";

    assert!(catalog.add_schema(schema_name).is_ok());
    assert!(catalog.get_schema(schema_name).is_some());
    assert_eq!(catalog.get_schema(schema_name).unwrap().name, schema_name);
}

#[test]
fn test_add_schema_already_exists() {
    let catalog = Catalog::new();
    let schema_name = "test_schema";
    assert!(catalog.add_schema(schema_name).is_ok());
    assert!(catalog.add_schema(schema_name).is_err());
}

#[test]
fn test_get_non_existent_schema() {
    let catalog = Catalog::new();
    assert!(catalog.get_schema("non_existent_schema").is_none());
}

#[test]
fn test_add_table_to_schema() {
    let catalog = Catalog::new();
    let schema_name = "test_schema";
    let table1 = create_test_table("table1");

    assert!(catalog.add_schema(schema_name).is_ok());
    assert!(catalog
        .add_table_to_schema(schema_name, table1.clone())
        .is_ok());

    let schema = catalog.get_schema(schema_name).unwrap();
    assert_eq!(schema.tables.len(), 1);
    assert!(schema.tables.contains_key(&table1.name));
    assert_eq!(schema.tables.get(&table1.name).unwrap().name, table1.name);
}

#[test]
fn test_add_table_to_non_existent_schema() {
    let catalog = Catalog::new();
    let table1 = create_test_table("table1");
    assert!(catalog
        .add_table_to_schema("non_existent_schema", table1)
        .is_err());
}

#[test]
fn test_add_table_already_exists_in_schema() {
    let catalog = Catalog::new();
    let schema_name = "test_schema";
    let table1 = create_test_table("table1");

    assert!(catalog.add_schema(schema_name).is_ok());
    assert!(catalog
        .add_table_to_schema(schema_name, table1.clone())
        .is_ok());
    assert!(catalog.add_table_to_schema(schema_name, table1).is_err());
}

#[test]
fn test_get_table_from_schema() {
    let catalog = Catalog::new();
    let schema_name = "test_schema";
    let table1_name = "table1";
    let table1 = create_test_table(table1_name);

    assert!(catalog.add_schema(schema_name).is_ok());
    assert!(catalog
        .add_table_to_schema(schema_name, table1.clone())
        .is_ok());

    let retrieved_table = catalog
        .get_table_from_schema(schema_name, table1_name)
        .unwrap();
    assert_eq!(retrieved_table.name, table1.name);
    assert_eq!(retrieved_table.schema, table1.schema);
}

#[test]
fn test_get_table_from_non_existent_schema() {
    let catalog = Catalog::new();
    assert!(catalog
        .get_table_from_schema("non_existent_schema", "any_table")
        .is_none());
}

#[test]
fn test_get_non_existent_table_from_schema() {
    let catalog = Catalog::new();
    let schema_name = "test_schema";
    assert!(catalog.add_schema(schema_name).is_ok());
    assert!(catalog
        .get_table_from_schema(schema_name, "non_existent_table")
        .is_none());
}

#[test]
fn test_concurrent_access() {
    let catalog = Arc::new(Catalog::new());
    let num_threads = 10;
    let tables_per_thread = 5;
    let mut handles = vec![];

    for i in 0..num_threads {
        let catalog_clone = Arc::clone(&catalog);
        let handle = thread::spawn(move || {
            let schema_name = format!("schema_{}", i);
            assert!(catalog_clone.add_schema(&schema_name).is_ok());

            for j in 0..tables_per_thread {
                let table_name = format!("table_{}_{}", i, j);
                let table = create_test_table(&table_name);
                assert!(catalog_clone
                    .add_table_to_schema(&schema_name, table)
                    .is_ok());
            }

            for j in 0..tables_per_thread {
                let table_name = format!("table_{}_{}", i, j);
                assert!(catalog_clone
                    .get_table_from_schema(&schema_name, &table_name)
                    .is_some());
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify all schemas and tables were added
    for i in 0..num_threads {
        let schema_name = format!("schema_{}", i);
        let schema = catalog.get_schema(&schema_name).unwrap();
        assert_eq!(schema.tables.len(), tables_per_thread);
        for j in 0..tables_per_thread {
            let table_name = format!("table_{}_{}", i, j);
            assert!(schema.tables.contains_key(&table_name));
        }
    }
}
