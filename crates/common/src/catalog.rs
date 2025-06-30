use datafusion::datasource::TableProvider;
use std::collections::HashMap;
use std::sync::Arc;

pub struct MemoryCatalog {
    pub tables: HashMap<String, Arc<dyn TableProvider>>,
}

impl Default for MemoryCatalog {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryCatalog {
    pub fn new() -> Self {
        Self { tables: HashMap::new() }
    }

    pub fn register_table(&mut self, name: String, provider: Arc<dyn TableProvider>) {
        self.tables.insert(name, provider);
    }

    pub fn get_table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables.get(name).cloned()
    }
}
