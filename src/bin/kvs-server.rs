use std::{
    env::current_dir,
    fmt::Display,
    fs,
    io::{BufReader, BufWriter, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener, TcpStream},
};

use clap::{Parser, ValueEnum};
use kvs::{KvStore, KvsEngine, KvsError, Request, Response, Result, SledKvsEngine};
use serde_json::Deserializer;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[arg(long, default_value_t = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4000))]
    addr: SocketAddr,
    #[arg(long, value_enum, default_value_t = Engine::Kvs)]
    engine: Engine,
}

#[derive(Clone, Copy, ValueEnum, PartialEq, Eq)]
enum Engine {
    Kvs,
    Sled,
}

impl Display for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Engine::Kvs => write!(f, "kvs"),
            Engine::Sled => write!(f, "sled"),
        }
    }
}

fn current_engine(cli_engine: Engine) -> Result<Engine> {
    let config_file = current_dir()?.join("engine");

    if !config_file.try_exists()? {
        fs::write(config_file, format!("{cli_engine}"))?;
        return Ok(cli_engine);
    }

    match fs::read_to_string(config_file)?.as_str() {
        "kvs" => Ok(Engine::Kvs),
        "sled" => Ok(Engine::Sled),
        _ => unreachable!(),
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    stderrlog::new()
        .verbosity(log::Level::Trace)
        .timestamp(stderrlog::Timestamp::Second)
        .module(module_path!())
        .init()?;
    log::debug!(
        "version: {}, engine: {}, address: {}",
        env!("CARGO_PKG_VERSION"),
        cli.engine,
        cli.addr
    );

    if current_engine(cli.engine)? != cli.engine {
        log::error!("unmatched engine");
        return Err(KvsError::UnmatchedEngine);
    }

    let listener = TcpListener::bind(cli.addr)?;
    match cli.engine {
        Engine::Kvs => run_engine(listener, KvStore::open(current_dir()?)?),
        Engine::Sled => run_engine(
            listener,
            SledKvsEngine {
                db: sled::open(current_dir()?)?,
            },
        ),
    }
}

fn run_engine(listener: TcpListener, mut kv: impl KvsEngine) -> Result<()> {
    for stream in listener.incoming() {
        let stream = stream?;
        log::debug!("receive a connection {}", stream.peer_addr()?);

        process(stream, &mut kv)?;
    }

    Ok(())
}

fn process(stream: TcpStream, kv: &mut impl KvsEngine) -> Result<()> {
    let reader = BufReader::new(&stream);
    let mut writer = BufWriter::new(&stream);

    let req_iter = Deserializer::from_reader(reader).into_iter::<Request>();

    for request in req_iter {
        let request = request?;
        log::debug!("request {:?}", request);

        let response = match request {
            Request::Get { key } => match kv.get(key) {
                Ok(value) => Response { value, error: None },
                Err(e) => Response {
                    value: None,
                    error: Some(e.to_string()),
                },
            },
            Request::Set { key, value } => match kv.set(key, value) {
                Ok(_) => Response {
                    value: None,
                    error: None,
                },
                Err(e) => Response {
                    value: None,
                    error: Some(e.to_string()),
                },
            },
            Request::Rm { key } => match kv.remove(key) {
                Ok(_) => Response {
                    value: None,
                    error: None,
                },
                Err(e) => Response {
                    value: None,
                    error: Some(e.to_string()),
                },
            },
        };
        log::debug!("response {:?}", response);

        let mut json = Vec::new();
        serde_json::to_writer(&mut json, &response)?;
        writer.write_all(&json)?;
        writer.flush()?;
    }

    Ok(())
}
