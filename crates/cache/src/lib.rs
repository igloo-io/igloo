//! Cache crate
//!
//! Provides caching primitives and implementations for Igloo components.
//!
//! # Example
//! ```rust
//! use igloo_cache::InMemoryCache;
//! let mut cache = InMemoryCache::new();
//! cache.set("key", "value");
//! assert_eq!(cache.get("key").unwrap(), "value");
//! ```
//!
//! # TODO
//! Implement cache logic

use igloo_common::Error;
use std::collections::HashMap;
use tracing::{info, warn};

/// Configuration for the cache.
#[derive(Debug, Clone, Default)]
pub struct CacheConfig {
    pub capacity: Option<usize>,
    // Add more config options as needed
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
    #[test]
    #[cfg(feature = "in-memory")]
    fn test_in_memory_cache_set_get() {
        let mut cache = InMemoryCache::new();
        cache.set("key", "value");
        assert_eq!(cache.get("key").unwrap(), "value");
    }
}
