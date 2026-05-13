/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::{Arc, Mutex};

use datafusion::execution::cache::cache_manager::{
    CachedFileMetadataEntry, FileMetadataCache, FileMetadataCacheEntry,
};
use datafusion::execution::cache::cache_unit::DefaultFilesMetadataCache;
use datafusion::execution::cache::CacheAccessor;
use log::error;
use object_store::path::Path;

// Cache type constants
pub const CACHE_TYPE_METADATA: &str = "METADATA";
pub const CACHE_TYPE_STATS: &str = "STATISTICS";

// Helper function to log cache operations
fn log_cache_error(operation: &str, error: &str) {
    error!("[CACHE ERROR] {} operation failed: {}", operation, error);
}

// Wrapper to make Mutex<DefaultFilesMetadataCache> implement FileMetadataCache
pub struct MutexFileMetadataCache {
    pub inner: Mutex<DefaultFilesMetadataCache>,
}

impl MutexFileMetadataCache {
    pub fn new(cache: DefaultFilesMetadataCache) -> Self {
        Self {
            inner: Mutex::new(cache),
        }
    }

    pub fn clear_cache(&self) {
        if let Ok(cache) = self.inner.lock() {
            cache.clear();
        }
    }

    pub fn update_cache_limit(&self, new_limit: usize) {
        if let Ok(cache) = self.inner.lock() {
            cache.update_cache_limit(new_limit);
        }
    }

    pub fn get_cache_limit(&self) -> usize {
        if let Ok(cache) = self.inner.lock() {
            cache.cache_limit()
        } else {
            0
        }
    }
}

impl CacheAccessor<Path, CachedFileMetadataEntry> for MutexFileMetadataCache {
    fn get(&self, k: &Path) -> Option<CachedFileMetadataEntry> {
        match self.inner.lock() {
            Ok(cache) => {
                let result = cache.get(k);
                if result.is_some() {
                    let bt = std::backtrace::Backtrace::force_capture();
                    let bt_str = format!("{}", bt);
                    // Extract just the first few relevant frames
                    let caller: String = bt_str.lines()
                        .filter(|l| l.contains("opensearch_datafusion") || l.contains("datafusion"))
                        .take(3)
                        .collect::<Vec<_>>()
                        .join(" | ");
                    let _ = caller; // suppress unused warning while logs are commented out
                    // Per-file HIT/MISS logs disabled for clean perf benchmarks. Uncomment to debug.
                    // native_bridge_common::log_info!("[METADATA CACHE HIT] {} caller={}", k, caller);
                } else {
                    // native_bridge_common::log_info!("[METADATA CACHE MISS] {}", k);
                }
                result
            }
            Err(e) => {
                log_cache_error("get", &e.to_string());
                None
            }
        }
    }

    fn put(&self, k: &Path, v: CachedFileMetadataEntry) -> Option<CachedFileMetadataEntry> {
        match self.inner.lock() {
            Ok(cache) => cache.put(k, v),
            Err(e) => {
                log_cache_error("put", &e.to_string());
                None
            }
        }
    }

    fn remove(&self, k: &Path) -> Option<CachedFileMetadataEntry> {
        match self.inner.lock() {
            Ok(cache) => cache.remove(k),
            Err(e) => {
                log_cache_error("remove", &e.to_string());
                None
            }
        }
    }

    fn contains_key(&self, k: &Path) -> bool {
        match self.inner.lock() {
            Ok(cache) => cache.contains_key(k),
            Err(e) => {
                log_cache_error("contains_key", &e.to_string());
                false
            }
        }
    }

    fn len(&self) -> usize {
        match self.inner.lock() {
            Ok(cache) => cache.len(),
            Err(e) => {
                log_cache_error("len", &e.to_string());
                0
            }
        }
    }

    fn clear(&self) {
        match self.inner.lock() {
            Ok(cache) => cache.clear(),
            Err(e) => log_cache_error("clear", &e.to_string()),
        }
    }

    fn name(&self) -> String {
        match self.inner.lock() {
            Ok(cache) => cache.name(),
            Err(e) => {
                log_cache_error("name", &e.to_string());
                "cache_error".to_string()
            }
        }
    }
}

impl FileMetadataCache for MutexFileMetadataCache {
    fn cache_limit(&self) -> usize {
        match self.inner.lock() {
            Ok(cache) => cache.cache_limit(),
            Err(e) => {
                log_cache_error("cache_limit", &e.to_string());
                0
            }
        }
    }

    fn update_cache_limit(&self, limit: usize) {
        match self.inner.lock() {
            Ok(cache) => cache.update_cache_limit(limit),
            Err(e) => log_cache_error("update_cache_limit", &e.to_string()),
        }
    }

    fn list_entries(&self) -> std::collections::HashMap<Path, FileMetadataCacheEntry> {
        match self.inner.lock() {
            Ok(cache) => cache.list_entries(),
            Err(e) => {
                log_cache_error("list_entries", &e.to_string());
                std::collections::HashMap::new()
            }
        }
    }
}
