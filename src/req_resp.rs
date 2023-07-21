/*!
 * request and response in network
 */
use serde::{Deserialize, Serialize};

/// request in network
#[derive(Serialize, Deserialize, Debug)]
pub enum Request {
    /// get value for key
    Get {
        /// key
        key: String,
    },
    /// set key-value pair
    Set {
        /// key
        key: String,
        /// value
        value: String,
    },
    /// remove key
    Rm {
        /// key
        key: String,
    },
}

#[derive(Serialize, Deserialize, Debug)]
/// response in network
pub struct Response {
    /// return value for get
    pub value: Option<String>,
    /// error string
    pub error: Option<String>,
}
