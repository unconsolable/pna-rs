/*!
 * kvstore: key-value store
*/

use crate::result::{KvsError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::{
    collections::HashMap,
    fs::File,
    io::{self, BufReader, Seek, Write},
    path::PathBuf,
};

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
    kv: HashMap<String, usize>,
    file: File,
    offset: usize,
}

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

impl KvStore {
    /// set key to value mapping
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let mut json = Vec::new();
        let command = Command::Set { key, value };
        serde_json::to_writer(&mut json, &command)?;

        self.file.write_all(&json)?;

        if let Command::Set { key, .. } = command {
            self.kv.insert(key, self.offset);
            self.offset += json.len();
            return Ok(());
        }
        unreachable!()
    }

    /// get value via key
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let offset = match self.kv.get(&key) {
            Some(o) => *o as u64,
            None => return Ok(None),
        };

        self.file.seek(io::SeekFrom::Start(offset))?;
        let buf_reader = BufReader::new(&mut self.file);
        let mut command_iter = Deserializer::from_reader(buf_reader).into_iter::<Command>();

        Ok(Some(match command_iter.next() {
            Some(command) => match command? {
                Command::Set { key: _, value } => value,
                _ => unreachable!("should not be other command kinds"),
            },
            None => unreachable!("should not be None"),
        }))
    }

    /// remove key
    pub fn remove(&mut self, key: String) -> Result<()> {
        if !self.kv.contains_key(&key) {
            return Err(KvsError::KeyNotFound);
        }

        let mut json = Vec::new();
        let command = Command::Remove { key };
        serde_json::to_writer(&mut json, &command)?;

        self.file.write_all(&json)?;
        self.offset += json.len();

        if let Command::Remove { key } = command {
            self.kv.remove(&key);
            return Ok(());
        }
        unreachable!()
    }

    /// open a new [`KvStore`]
    /// `path` is a directory path
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let mut path: PathBuf = path.into();
        path.push("commands.json");

        let mut file = File::options()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;
        file.seek(io::SeekFrom::Start(0))?;
        let buf_reader = BufReader::new(&mut file);

        let mut kv = HashMap::new();

        let mut command_iter = Deserializer::from_reader(buf_reader).into_iter::<Command>();

        let mut offset = 0;
        while let Some(command) = command_iter.next() {
            match command? {
                Command::Set { key, .. } => {
                    kv.insert(key, offset);
                }
                Command::Remove { key } => {
                    kv.remove(&key);
                }
            }
            offset = command_iter.byte_offset();
        }

        Ok(Self { kv, file, offset })
    }
}
