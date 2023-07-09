/*!
 * result wrapper
 */

use std::io;

use failure::Fail;

/// result type
pub type Result<T> = core::result::Result<T, KvsError>;

/// error kind in [`kvstore`]
#[derive(Fail, Debug)]
pub enum KvsError {
    /// key not found
    #[fail(display = "Key not found")]
    KeyNotFound,
    /// no begin command
    #[fail(display = "No begin command")]
    NoBeginCommand,
    /// serde_json error
    #[fail(display = "{}", _0)]
    SerdeJson(#[cause] serde_json::Error),
    /// std io error
    #[fail(display = "{}", _0)]
    StdIo(#[cause] io::Error),
}

impl From<serde_json::Error> for KvsError {
    fn from(value: serde_json::Error) -> Self {
        Self::SerdeJson(value)
    }
}

impl From<io::Error> for KvsError {
    fn from(value: io::Error) -> Self {
        Self::StdIo(value)
    }
}
