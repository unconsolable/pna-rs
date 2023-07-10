/*!
 * kvstore: key-value store
*/

use crate::{KvsError, Result};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::{
    collections::{HashMap, HashSet},
    ffi::OsStr,
    fs::{self, File},
    io::{self, BufReader, BufWriter, Seek, Write},
    path::{Path, PathBuf},
};

const COMPACTION_THRESHOLD: u64 = 4 * 1024 * 1024;

/// key-value store, both key and value are [`String`]
/// ```rust
/// use kvs::{KvStore, Result};
/// let mut store = KvStore::open(".").unwrap();
/// assert!(store.set("key1".to_owned(), "value1".to_owned()).is_ok());
/// assert_eq!(store.get("key1".to_owned()).unwrap(), Some("value1".to_owned()));
/// assert!(store.remove("key1".to_owned()).is_ok());
/// assert_eq!(store.get("key1".to_owned()).unwrap(), None);
/// ```
pub struct KvStore {
    kv: HashMap<String, CommandOffset>,
    readers: HashMap<u64, BufReader<File>>,
    writer: BufWriter<File>,
    writer_offset: CommandOffset,
    uncompaction_size: u64,
    dir_path: PathBuf,
}

#[derive(Clone, Copy)]
struct CommandOffset {
    generation: u64,
    offset: u64,
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

        self.writer.write_all(&json)?;
        self.writer.flush()?;

        let key = match command {
            Command::Set { key, .. } => key,
            _ => unreachable!(),
        };
        self.kv.insert(key, self.writer_offset);
        self.writer_offset.offset += json.len() as u64;

        self.uncompaction_size += json.len() as u64;
        if self.uncompaction_size >= COMPACTION_THRESHOLD {
            self.compaction()?;
        }

        Ok(())
    }

    /// get value via key
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let CommandOffset { generation, offset } = match self.kv.get(&key) {
            Some(o) => *o,
            None => return Ok(None),
        };

        let reader = self
            .readers
            .get_mut(&generation)
            .expect("command reader not found");
        reader.seek(io::SeekFrom::Start(offset))?;

        let mut command_iter = Deserializer::from_reader(reader).into_iter::<Command>();
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

        self.writer.write_all(&json)?;
        self.writer_offset.offset += json.len() as u64;
        self.writer.flush()?;

        let key = match command {
            Command::Remove { key } => key,
            _ => unreachable!(),
        };
        self.kv.remove(&key);

        self.uncompaction_size += json.len() as u64;
        if self.uncompaction_size >= COMPACTION_THRESHOLD {
            self.compaction()?;
        }

        Ok(())
    }

    /// open a new [`KvStore`]
    /// `path` is a directory path
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path: PathBuf = path.into();
        fs::create_dir_all(path.as_path())?;

        let mut kv = HashMap::new();
        let mut readers = HashMap::new();
        let mut uncompaction_size = 0;
        let generations = Self::get_generations(path.as_path())?;
        let writer_generation = generations.iter().max().map_or(0, |x| x + 1);

        for generation in generations {
            let reader = Self::load_command_file(
                path.as_ref(),
                generation,
                &mut kv,
                &mut uncompaction_size,
            )?;
            readers.insert(generation, reader);
        }

        Ok(Self {
            kv,
            writer: Self::create_command_file(path.as_ref(), writer_generation, &mut readers)?,
            readers,
            writer_offset: CommandOffset {
                generation: writer_generation,
                offset: 0,
            },
            uncompaction_size,
            dir_path: path,
        })
    }

    fn compaction(&mut self) -> Result<()> {
        let mut kv = HashMap::new();
        let mut to_delete_generations = HashSet::new();

        let compaction_generation = self.writer_offset.generation + 1;
        let mut compaction_offset = CommandOffset {
            generation: compaction_generation,
            offset: 0,
        };
        let mut compaction_writer =
            Self::create_command_file(&self.dir_path, compaction_generation, &mut self.readers)?;

        for pair in &self.kv {
            let CommandOffset { generation, offset } = *pair.1;
            to_delete_generations.insert(generation);

            let reader = self
                .readers
                .get_mut(&generation)
                .expect("command reader not found");
            reader.seek(io::SeekFrom::Start(offset))?;

            let mut command_iter = Deserializer::from_reader(reader).into_iter::<Command>();
            let value = match command_iter.next() {
                Some(command) => match command? {
                    Command::Set { key: _, value } => value,
                    _ => unreachable!("should not be other command kinds"),
                },
                None => unreachable!("should not be None"),
            };

            let mut json = Vec::new();
            let command = Command::Set {
                key: pair.0.clone(),
                value,
            };
            serde_json::to_writer(&mut json, &command)?;

            compaction_writer.write_all(&json)?;

            let key = match command {
                Command::Set { key, .. } => key,
                _ => unreachable!(),
            };

            kv.insert(key, compaction_offset);
            compaction_offset.offset += json.len() as u64;
        }

        compaction_writer.flush()?;
        self.kv = kv;

        for generation in to_delete_generations {
            self.readers.remove(&generation);
            fs::remove_file(Self::convert_command_generation_path(
                self.dir_path.as_path(),
                generation,
            ))?;
        }

        let writer_offset = CommandOffset {
            generation: compaction_generation + 1,
            offset: 0,
        };
        let writer =
            Self::create_command_file(&self.dir_path, writer_offset.generation, &mut self.readers)?;

        (self.writer, self.writer_offset, self.uncompaction_size) = (writer, writer_offset, 0);
        Ok(())
    }

    fn get_generations(dir_path: &Path) -> Result<Vec<u64>> {
        let mut result: Vec<u64> = fs::read_dir(dir_path)?
            .flat_map(|res| -> Result<_> { Ok(res?.path()) })
            .filter(|path| path.is_file() && path.extension() == Some(OsStr::new("json")))
            .flat_map(|path| {
                path.file_name()
                    .and_then(OsStr::to_str)
                    .map(|s| s.trim_end_matches(".json"))
                    .map(str::parse::<u64>)
            })
            .flatten()
            .collect();

        result.sort_unstable();
        Ok(result)
    }

    fn load_command_file(
        dir_path: &Path,
        generation: u64,
        kv: &mut HashMap<String, CommandOffset>,
        uncompaction_size: &mut u64,
    ) -> Result<BufReader<File>> {
        let mut reader: BufReader<File> = BufReader::new(
            File::options()
                .read(true)
                .open(Self::convert_command_generation_path(dir_path, generation))?,
        );
        reader.seek(io::SeekFrom::Start(0))?;

        let mut command_iter = Deserializer::from_reader(&mut reader).into_iter::<Command>();

        let mut offset = 0;
        while let Some(command) = command_iter.next() {
            match command? {
                Command::Set { key, .. } => {
                    kv.insert(key, CommandOffset { generation, offset });
                }
                Command::Remove { key } => {
                    kv.remove(&key);
                }
            }
            offset = command_iter.byte_offset() as u64;
        }
        *uncompaction_size += offset;

        Ok(reader)
    }

    fn create_command_file(
        dir_path: &Path,
        generation: u64,
        readers: &mut HashMap<u64, BufReader<File>>,
    ) -> Result<BufWriter<File>> {
        let path = Self::convert_command_generation_path(dir_path, generation);
        let writer = BufWriter::new(
            File::options()
                .create(true)
                .append(true)
                .open(path.as_path())?,
        );

        let reader = BufReader::new(File::options().read(true).open(path.as_path())?);
        readers.insert(generation, reader);

        Ok(writer)
    }

    fn convert_command_generation_path(dir_path: &Path, generation: u64) -> PathBuf {
        dir_path.join(format!("{generation}.json"))
    }
}
