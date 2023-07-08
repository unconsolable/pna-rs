/*!
 * kvs: A key-value store
*/

#![deny(missing_docs)]
use std::collections::HashMap;

/// key-value store, both key and value are [`String`]
/// ```rust
/// use kvs::KvStore;
/// let mut store = KvStore::new();
/// store.set("key1".to_owned(), "value1".to_owned());
/// assert_eq!(store.get("key1".to_owned()), Some("value1".to_owned()));
/// store.remove("key1".to_owned());
/// assert_eq!(store.get("key1".to_owned()), None);
/// ```
pub struct KvStore {
    kv: HashMap<String, String>,
}

impl KvStore {
    /// create new [`KvStore`]
    pub fn new() -> Self {
        Self { kv: HashMap::new() }
    }

    /// set key to value mapping
    pub fn set(&mut self, key: String, value: String) {
        self.kv.insert(key, value);
    }

    /// get value via key
    pub fn get(&self, key: String) -> Option<String> {
        self.kv.get(&key).cloned()
    }

    /// remove key
    pub fn remove(&mut self, key: String) {
        self.kv.remove(&key);
    }
}

impl Default for KvStore {
    fn default() -> Self {
        Self::new()
    }
}
