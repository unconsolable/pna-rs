/*!
 * kvs: A key-value store
*/

#![deny(missing_docs)]
pub mod result;
pub use result::{KvsError, Result};

pub mod kvstore;
pub use kvstore::KvStore;
