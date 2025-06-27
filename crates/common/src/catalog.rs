use datafusion::datasource::TableProvider;
use std::collections::HashMap;
use std::sync::Arc;

// Basic Schema definition for now
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    // In a real system, this would contain column definitions (name, type, etc.)
    // For now, an empty struct is sufficient as a placeholder.
    // We can compare Arc<Schema> directly.
}

impl Schema {
    pub fn empty() -> Self {
        Self {}
    }
}

pub type SchemaRef = Arc<Schema>;

pub struct MemoryCatalog {
    pub tables: HashMap<String, Arc<dyn TableProvider>>,
    // We might want to store schemas here later, but for now,
    // the planner creates dummy schemas.
}

impl Default for MemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryCatalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    pub fn register_table(&mut self, name: String, provider: Arc<dyn TableProvider>) {
        self.tables.insert(name, provider);
    }

    pub fn get_table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables.get(name).cloned()
    }
}
