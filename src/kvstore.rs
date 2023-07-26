/*!
 * kvstore: key-value store
*/

use crate::{KvsEngine, KvsError, Result};
use crossbeam_skiplist::SkipMap;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::{
    collections::HashSet,
    ffi::OsStr,
    fs::{self, File},
    io::{self, BufReader, BufWriter, Seek, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

const COMPACTION_THRESHOLD: u64 = 4 * 1024 * 1024;

/// key-value store, both key and value are [`String`]
/// ```rust
/// use kvs::{KvStore, Result, KvsEngine};
/// let mut store = KvStore::open(".").unwrap();
/// assert!(store.set("key1".to_owned(), "value1".to_owned()).is_ok());
/// assert_eq!(store.get("key1".to_owned()).unwrap(), Some("value1".to_owned()));
/// assert!(store.remove("key1".to_owned()).is_ok());
/// assert_eq!(store.get("key1".to_owned()).unwrap(), None);
/// ```
#[derive(Clone)]
pub struct KvStore {
    kv: Arc<SkipMap<String, CommandOffset>>,
    reader: KvStoreReader,
    writer: Arc<Mutex<KvStoreWriter>>,
}

#[derive(Clone)]
struct KvStoreReader {
    dir_path: Arc<PathBuf>,
}

struct KvStoreWriter {
    kv: Arc<SkipMap<String, CommandOffset>>,
    writer: BufWriter<File>,
    writer_offset: CommandOffset,
    uncompaction_size: u64,
    dir_path: Arc<PathBuf>,
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

impl KvStoreReader {
    fn new(dir_path: Arc<PathBuf>) -> Self {
        Self { dir_path }
    }

    fn get(&self, command_offset: CommandOffset) -> Result<String> {
        let command_path =
            convert_command_generation_path(&self.dir_path, command_offset.generation);

        let mut reader: BufReader<File> =
            BufReader::new(File::options().read(true).open(command_path)?);
        reader.seek(io::SeekFrom::Start(command_offset.offset))?;

        let mut command_iter = Deserializer::from_reader(reader).into_iter::<Command>();
        Ok(match command_iter.next() {
            Some(command) => match command? {
                Command::Set { key: _, value } => value,
                _ => unreachable!("should not be other command kinds"),
            },
            None => unreachable!("should not be None"),
        })
    }
}

impl KvStoreWriter {
    fn new(
        kv: Arc<SkipMap<String, CommandOffset>>,
        dir_path: Arc<PathBuf>,
        writer_generation: u64,
        uncompaction_size: u64,
    ) -> Result<Self> {
        Ok(Self {
            kv,
            writer: Self::create_command_file(&dir_path, writer_generation)?,
            writer_offset: CommandOffset {
                generation: writer_generation,
                offset: 0,
            },
            uncompaction_size,
            dir_path,
        })
    }

    fn set(&mut self, key: String, value: String) -> Result<()> {
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

    fn remove(&mut self, key: String) -> Result<()> {
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

    fn compaction(&mut self) -> Result<()> {
        let mut to_delete_generations: HashSet<u64> = HashSet::new();

        let compaction_generation = self.writer_offset.generation + 1;
        let mut compaction_offset = CommandOffset {
            generation: compaction_generation,
            offset: 0,
        };
        let mut compaction_writer =
            Self::create_command_file(&self.dir_path, compaction_generation)?;
        let compaction_reader = KvStoreReader::new(self.dir_path.clone());

        for pair in self.kv.iter() {
            let command_offset = *pair.value();
            to_delete_generations.insert(command_offset.generation);

            let value = compaction_reader.get(command_offset)?;

            let mut json = Vec::new();
            let command = Command::Set {
                key: pair.key().clone(),
                value,
            };
            serde_json::to_writer(&mut json, &command)?;

            compaction_writer.write_all(&json)?;

            let key = match command {
                Command::Set { key, .. } => key,
                _ => unreachable!(),
            };

            self.kv.insert(key, compaction_offset);
            compaction_offset.offset += json.len() as u64;
        }

        compaction_writer.flush()?;

        for generation in to_delete_generations {
            fs::remove_file(convert_command_generation_path(
                self.dir_path.as_path(),
                generation,
            ))?;
        }

        let writer_offset = CommandOffset {
            generation: compaction_generation + 1,
            offset: 0,
        };
        let writer = Self::create_command_file(&self.dir_path, writer_offset.generation)?;

        (self.writer, self.writer_offset, self.uncompaction_size) = (writer, writer_offset, 0);
        Ok(())
    }

    fn create_command_file(dir_path: &Path, generation: u64) -> Result<BufWriter<File>> {
        let path = convert_command_generation_path(dir_path, generation);
        let writer = BufWriter::new(
            File::options()
                .create(true)
                .append(true)
                .open(path.as_path())?,
        );

        Ok(writer)
    }
}

fn convert_command_generation_path(dir_path: &Path, generation: u64) -> PathBuf {
    dir_path.join(format!("{generation}.json"))
}

impl KvStore {
    /// open a new [`KvStoreInner`]
    /// `path` is a directory path
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path: Arc<PathBuf> = Arc::new(path.into());
        fs::create_dir_all(path.as_path())?;

        let kv = Arc::new(SkipMap::new());
        let mut uncompaction_size = 0;
        let generations = Self::get_generations(path.as_path())?;
        let writer_generation = generations.iter().max().map_or(0, |x| x + 1);

        for generation in generations {
            Self::load_command_file(&path, generation, &kv, &mut uncompaction_size)?
        }

        Ok(Self {
            kv: kv.clone(),
            reader: KvStoreReader::new(path.clone()),
            writer: Arc::new(Mutex::new(KvStoreWriter::new(
                kv,
                path,
                writer_generation,
                uncompaction_size,
            )?)),
        })
    }

    fn load_command_file(
        dir_path: &Path,
        generation: u64,
        kv: &SkipMap<String, CommandOffset>,
        uncompaction_size: &mut u64,
    ) -> Result<()> {
        let mut reader: BufReader<File> = BufReader::new(
            File::options()
                .read(true)
                .open(convert_command_generation_path(dir_path, generation))?,
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
}

impl KvsEngine for KvStore {
    fn set(&self, key: String, value: String) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        writer.set(key, value)
    }

    fn get(&self, key: String) -> Result<Option<String>> {
        let command_offset = match self.kv.get(&key) {
            Some(o) => *o.value(),
            None => return Ok(None),
        };
        Ok(Some(self.reader.get(command_offset)?))
    }

    fn remove(&self, key: String) -> Result<()> {
        let mut writer = self.writer.lock().unwrap();
        writer.remove(key)
    }
}
