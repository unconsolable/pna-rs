/*!
 * result wrapper
 */

use std::{io, string};

use failure::Fail;

/// result type
pub type Result<T> = std::result::Result<T, KvsError>;

/// error kind in [`kvstore`]
#[derive(Fail, Debug)]
pub enum KvsError {
    /// key not found
    #[fail(display = "Key not found")]
    KeyNotFound,
    /// unmatched engine
    #[fail(display = "Unmatched engine")]
    UnmatchedEngine,
    /// serde_json error
    #[fail(display = "{}", _0)]
    SerdeJson(#[cause] serde_json::Error),
    /// std io error
    #[fail(display = "{}", _0)]
    StdIo(#[cause] io::Error),
    /// stderrlog error
    #[fail(display = "{}", _0)]
    StdErrLog(#[cause] log::SetLoggerError),
    /// sled error
    #[fail(display = "{}", _0)]
    Sled(#[cause] sled::Error),
    /// from utf8 error
    #[fail(display = "{}", _0)]
    FromUtf8(#[cause] string::FromUtf8Error),
    /// client error
    #[fail(display = "Client error")]
    ClientError,
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

impl From<log::SetLoggerError> for KvsError {
    fn from(value: log::SetLoggerError) -> Self {
        Self::StdErrLog(value)
    }
}

impl From<sled::Error> for KvsError {
    fn from(value: sled::Error) -> Self {
        Self::Sled(value)
    }
}

impl From<string::FromUtf8Error> for KvsError {
    fn from(value: string::FromUtf8Error) -> Self {
        Self::FromUtf8(value)
    }
}
