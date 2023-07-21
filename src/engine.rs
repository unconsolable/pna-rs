/*!
 * engine trait
 */

use crate::Result;

/// kv engine trait
pub trait KvsEngine {
    /// set a key-value pair
    fn set(&mut self, key: String, value: String) -> Result<()>;
    /// get value for a key
    fn get(&mut self, key: String) -> Result<Option<String>>;
    /// remove a key
    fn remove(&mut self, key: String) -> Result<()>;
}
