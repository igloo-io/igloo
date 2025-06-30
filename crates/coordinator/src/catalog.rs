use arrow_schema::SchemaRef;
use dashmap::DashMap;
use std::sync::Arc;

// Placeholder for the data source representation
#[derive(Debug, Clone)]
pub enum Source {
    Parquet,
    // TODO: Add other source types like CSV, etc.
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub schema: SchemaRef,
    pub source: Source, // For MVP, this can be simple. Later, it might be a trait object or more complex enum.
}

#[derive(Debug)]
pub struct Schema {
    pub name: String,
    pub tables: DashMap<String, Arc<Table>>,
}

impl Schema {
    pub fn new(name: String) -> Self {
        Self {
            name,
            tables: DashMap::new(),
        }
    }
}

#[derive(Debug)]
pub struct Catalog {
    pub schemas: DashMap<String, Arc<Schema>>,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            schemas: DashMap::new(),
        }
    }

    pub fn add_schema(&self, name: &str) -> Result<(), String> {
        if self.schemas.contains_key(name) {
            return Err(format!("Schema {} already exists", name));
        }
        let schema = Arc::new(Schema::new(name.to_string()));
        self.schemas.insert(name.to_string(), schema);
        Ok(())
    }

    pub fn get_schema(&self, name: &str) -> Option<Arc<Schema>> {
        self.schemas.get(name).map(|s| s.value().clone())
    }

    pub fn add_table_to_schema(&self, schema_name: &str, table: Table) -> Result<(), String> {
        match self.schemas.get(schema_name) {
            Some(schema) => {
                if schema.tables.contains_key(&table.name) {
                    return Err(format!(
                        "Table {} already exists in schema {}",
                        table.name, schema_name
                    ));
                }
                schema.tables.insert(table.name.clone(), Arc::new(table));
                Ok(())
            }
            None => Err(format!("Schema {} not found", schema_name)),
        }
    }

    pub fn get_table_from_schema(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Option<Arc<Table>> {
        self.schemas
            .get(schema_name)
            .and_then(|schema| schema.tables.get(table_name).map(|t| t.value().clone()))
    }
}
