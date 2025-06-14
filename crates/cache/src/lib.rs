//! Cache crate
//!
//! Provides a thread-safe, in-memory key-value cache.
//! NOTE: This version uses a generic value type `V` due to current
//! build environment incompatibilities with the `arrow` crate.
//! It is intended to store `Vec<arrow::record_batch::RecordBatch>` once
//! the environment issue is resolved.

use std::collections::HashMap;
// use std::sync::Arc; // Required for Arc<V> if values are large, or just V if V is Clone. The provided template uses V directly.
use tokio::sync::RwLock;

// Ensure old structs like InMemoryCache, CacheConfig, and their tests are removed.
// Ensure old imports like igloo_common::Error, tracing, and arrow::record_batch::RecordBatch are removed if unused.

/// A thread-safe, in-memory cache.
///
/// Stores cloned values. Keys are Strings.
/// Intended to store `Vec<arrow::record_batch::RecordBatch>` in the future.
#[derive(Default)] // Added Default for easier Cache::default() if needed, though new() is primary.
pub struct Cache<V>
where
    V: Clone + Send + Sync + 'static,
{
    store: RwLock<HashMap<String, V>>,
}

impl<V> Cache<V>
where
    V: Clone + Send + Sync + 'static,
{
    /// Creates a new, empty cache.
    pub fn new() -> Self {
        Self {
            store: RwLock::new(HashMap::new()),
        }
    }

    /// Retrieves a clone of the value associated with the given key.
    ///
    /// Returns `None` if the key is not found.
    pub async fn get(&self, key: &str) -> Option<V> {
        let store_guard = self.store.read().await;
        store_guard.get(key).cloned()
    }

    /// Inserts a key-value pair into the cache.
    ///
    /// If the key already exists, the old value is overwritten.
    pub async fn put(&self, key: String, value: V) {
        let mut store_guard = self.store.write().await;
        store_guard.insert(key, value);
    }
}

// Old tests for InMemoryCache should be removed by this overwrite.

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc; // For Arc<Cache> in thread safety test

    #[tokio::test]
    async fn test_generic_cache_new() {
        let cache: Cache<String> = Cache::new();
        assert!(cache.store.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_generic_cache_put_get() {
        let cache = Cache::new();
        let key = "test_key".to_string();
        let value = "test_value".to_string();

        cache.put(key.clone(), value.clone()).await;
        let retrieved_value = cache.get(&key).await;

        assert_eq!(retrieved_value, Some(value));
    }

    #[tokio::test]
    async fn test_generic_cache_get_non_existent() {
        let cache: Cache<String> = Cache::new();
        let key = "non_existent_key";

        let retrieved_value = cache.get(key).await;
        assert_eq!(retrieved_value, None);
    }

    #[tokio::test]
    async fn test_generic_cache_put_overwrite() {
        let cache = Cache::new();
        let key = "overwrite_key".to_string();
        let value1 = "value1".to_string();
        let value2 = "value2".to_string();

        cache.put(key.clone(), value1.clone()).await;
        let retrieved_value1 = cache.get(&key).await;
        assert_eq!(retrieved_value1, Some(value1));

        cache.put(key.clone(), value2.clone()).await;
        let retrieved_value2 = cache.get(&key).await;
        assert_eq!(retrieved_value2, Some(value2));
    }

    // Test with a different type for V, e.g., i32
    #[tokio::test]
    async fn test_generic_cache_with_i32() {
        let cache = Cache::new();
        let key = "int_key".to_string();
        let value: i32 = 123;

        cache.put(key.clone(), value).await;
        let retrieved_value = cache.get(&key).await;
        assert_eq!(retrieved_value, Some(value));
    }

    #[tokio::test]
    async fn test_generic_cache_thread_safety() {
        let cache = Arc::new(Cache::<i32>::new());
        let mut task_handles = vec![];

        let num_tasks = 10;
        let ops_per_task = 10;

        for i in 0..num_tasks {
            let cache_clone = Arc::clone(&cache);
            let handle = tokio::spawn(async move {
                for j in 0..ops_per_task {
                    let key = format!("key_task{}_op{}", i, j);
                    cache_clone.put(key.clone(), (i * ops_per_task + j) as i32).await;
                }

                // Perform some get operations
                for j in 0..(ops_per_task / 2) { // Get half of what was put by this task
                    let key = format!("key_task{}_op{}", i, j);
                    let val = cache_clone.get(&key).await;
                    assert_eq!(val, Some((i * ops_per_task + j) as i32));
                }
            });
            task_handles.push(handle);
        }

        // Wait for all tasks to complete
        let results = futures::future::join_all(task_handles).await;
        for result in results {
            assert!(result.is_ok(), "Task panicked");
        }

        // Optional: Verify some values from the main thread to be sure
        let sample_key = "key_task0_op0".to_string();
        let sample_value = cache.get(&sample_key).await;
        if num_tasks > 0 && ops_per_task > 0 {
            assert_eq!(sample_value, Some(0i32));
        }

        let another_key = format!("key_task{}_op{}", num_tasks -1 , ops_per_task - 1);
        if num_tasks > 0 && ops_per_task > 0 {
             let another_value = cache.get(&another_key).await;
             assert_eq!(another_value, Some(((num_tasks - 1) * ops_per_task + (ops_per_task - 1)) as i32 ));
        }
    }
}
