/*!
 * sled wrapper
 */

use sled::Db;

use crate::{KvsEngine, KvsError, Result};

/// A wrapper for sled
pub struct SledKvsEngine {
    /// sled db
    pub db: Db,
}

impl KvsEngine for SledKvsEngine {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        self.db.insert(key.as_bytes(), value.as_bytes())?;
        self.db.flush()?;
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        self.db
            .get(key)?
            .map(|x| -> Result<_> { Ok(String::from_utf8(x.to_vec())?) })
            .transpose()
    }

    fn remove(&mut self, key: String) -> Result<()> {
        self.db.remove(key)?.ok_or(KvsError::KeyNotFound)?;
        self.db.flush()?;
        Ok(())
    }
}
