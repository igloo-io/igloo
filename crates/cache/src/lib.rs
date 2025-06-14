//! Cache crate
//!
//! Provides caching primitives and implementations for Igloo components.

use arrow::record_batch::RecordBatch;
use igloo_common::Error;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Configuration for the cache.
#[derive(Debug, Clone, Default)]
pub struct CacheConfig {
    pub capacity: Option<usize>,
    // Add more config options as needed
}

/// A cache for storing RecordBatches.
#[derive(Debug)]
pub struct Cache {
    data: RwLock<HashMap<String, Vec<RecordBatch>>>,
}

impl Default for Cache {
    fn default() -> Self {
        Self::new()
    }
}

impl Cache {
    /// Create a new cache.
    pub fn new() -> Self {
        info!("Creating new Cache");
        Self { data: RwLock::new(HashMap::new()) }
    }

    /// Get a value from the cache.
    pub async fn get(&self, key: &str) -> Option<Vec<RecordBatch>> {
        info!(key = %key, "Attempting to get value from cache");
        let data_guard = self.data.read().await;
        let value = data_guard.get(key).cloned();
        if value.is_some() {
            info!(key = %key, "Cache hit");
        } else {
            warn!(key = %key, "Cache miss");
        }
        value
    }

    /// Set a value in the cache.
    pub async fn put(&self, key: String, value: Vec<RecordBatch>) {
        info!(key = %key, "Setting value in cache");
        let mut data_guard = self.data.write().await;
        data_guard.insert(key, value);
    }
}

/// An in-memory cache for demonstration purposes.
#[cfg(feature = "in-memory")]
#[derive(Default)]
pub struct InMemoryCache {
    store: HashMap<String, String>,
}

#[cfg(feature = "in-memory")]
impl InMemoryCache {
    /// Create a new in-memory cache.
    pub fn new() -> Self {
        info!("Creating new InMemoryCache");
        Self { store: HashMap::new() }
    }
    /// Set a value in the cache.
    pub fn set(&mut self, key: &str, value: &str) {
        info!(key = %key, "Setting value in cache");
        self.store.insert(key.to_string(), value.to_string());
    }
    /// Get a value from the cache, or return an error if not found.
    pub fn get(&self, key: &str) -> Result<&String, Error> {
        if let Some(val) = self.store.get(key) {
            info!(key = %key, "Cache hit");
            Ok(val)
        } else {
            warn!(key = %key, "Cache miss");
            Err(Error::new("Key not found"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_sample_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ids = Int32Array::from(vec![1, 2, 3]);
        let names = StringArray::from(vec!["foo", "bar", "baz"]);
        RecordBatch::try_new(schema.clone(), vec![Arc::new(ids), Arc::new(names)]).unwrap()
    }

    #[tokio::test]
    async fn test_put_and_get() {
        let cache = Cache::new();
        let sample_batch = create_sample_batch();
        let data_to_store = vec![sample_batch.clone()];

        // Test putting data
        cache.put("test_key".to_string(), data_to_store.clone()).await;

        // Test getting existing data
        let retrieved_data_vec = cache.get("test_key").await.unwrap();
        assert_eq!(retrieved_data_vec.len(), 1);
        let retrieved_batch = &retrieved_data_vec[0];

        // Compare schemas
        assert_eq!(retrieved_batch.schema(), sample_batch.schema());
        // Compare number of columns and rows
        assert_eq!(retrieved_batch.num_columns(), sample_batch.num_columns());
        assert_eq!(retrieved_batch.num_rows(), sample_batch.num_rows());
        // Compare column data
        for i in 0..sample_batch.num_columns() {
            assert_eq!(retrieved_batch.column(i).as_ref(), sample_batch.column(i).as_ref());
        }

        // Test getting non-existent key
        assert!(
            cache.get("non_existent_key").await.is_none(),
            "Expected None for non-existent key"
        );
    }

    #[tokio::test]
    async fn test_thread_safety() {
        let cache = Arc::new(Cache::new());
        let mut tasks = vec![];

        let num_tasks: usize = 10;
        let ops_per_task: usize = 50;

        for i in 0..num_tasks {
            let cache_clone = Arc::clone(&cache);
            tasks.push(tokio::spawn(async move {
                for j in 0..ops_per_task {
                    let key = format!("key_task{}_op{}", i, j);
                    let sample_batch = create_sample_batch(); // Create fresh batch for each put
                    let data_to_store = vec![sample_batch.clone()];

                    // Perform a mix of put and get
                    if j % 2 == 0 {
                        // Put operation
                        cache_clone.put(key.clone(), data_to_store.clone()).await;
                        // Verify the put
                        let retrieved = cache_clone.get(&key).await;
                        assert!(retrieved.is_some(), "Failed to get after put in task {}", i);
                        if let Some(ret_vec) = retrieved {
                            assert_eq!(ret_vec.len(), 1);
                            assert_eq!(ret_vec[0].schema(), sample_batch.schema());
                            assert_eq!(ret_vec[0].num_columns(), sample_batch.num_columns());
                            assert_eq!(ret_vec[0].num_rows(), sample_batch.num_rows());
                        }
                    } else {
                        // Get operation (on a potentially existing or non-existing key)
                        let other_task_key = format!(
                            "key_task{}_op{}",
                            (i + 1) % num_tasks,
                            j.saturating_sub(1).max(0)
                        );
                        let _ = cache_clone.get(&other_task_key).await; // Just perform get
                    }
                }
            }));
        }

        for task in tasks {
            task.await.unwrap(); // Ensure task completes without panic
        }
    }

    #[test]
    #[cfg(feature = "in-memory")]
    fn test_in_memory_cache_set_get() {
        let mut cache = InMemoryCache::new();
        cache.set("key", "value");
        assert_eq!(cache.get("key").unwrap(), "value");
    }
}
