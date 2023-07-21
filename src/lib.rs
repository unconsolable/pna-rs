/*!
 * kvs: A key-value store
*/

#![deny(missing_docs)]
pub mod engine;
pub use engine::KvsEngine;

pub mod result;
pub use result::{KvsError, Result};

pub mod kvstore;
pub use kvstore::KvStore;

pub mod req_resp;
pub use req_resp::{Request, Response};

pub mod sled_kvs_engine;
pub use sled_kvs_engine::SledKvsEngine;
