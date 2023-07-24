/*!
 * engine trait
 */

use crate::Result;

/// kv engine trait
pub trait KvsEngine: Clone + Send + 'static {
    /// set a key-value pair
    fn set(&self, key: String, value: String) -> Result<()>;
    /// get value for a key
    fn get(&self, key: String) -> Result<Option<String>>;
    /// remove a key
    fn remove(&self, key: String) -> Result<()>;
}
