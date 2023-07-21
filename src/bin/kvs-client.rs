use std::{
    io::{BufReader, BufWriter, Write},
    net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream},
};

use clap::{Parser, Subcommand};
use kvs::{KvsError, Request, Response, Result};
use serde_json::Deserializer;

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Get {
        key: String,
        #[arg(long, default_value_t = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4000))]
        addr: SocketAddr,
    },
    Set {
        key: String,
        value: String,
        #[arg(long, default_value_t = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4000))]
        addr: SocketAddr,
    },
    Rm {
        key: String,
        #[arg(long, default_value_t = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 4000))]
        addr: SocketAddr,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Get { key, addr } => {
            let response = process_command(addr, Request::Get { key })?;

            if let Some(err) = response.error {
                eprintln!("error: {err}");
                return Err(KvsError::ClientError);
            }

            match response.value {
                Some(value) => println!("{value}"),
                None => println!("Key not found"),
            }
        }
        Commands::Set { key, value, addr } => {
            let response = process_command(addr, Request::Set { key, value })?;

            if let Some(err) = response.error {
                eprintln!("error: {err}");
                return Err(KvsError::ClientError);
            }
        }
        Commands::Rm { key, addr } => {
            let response = process_command(addr, Request::Rm { key })?;

            if let Some(err) = response.error {
                eprintln!("error: {err}");
                return Err(KvsError::ClientError);
            }
        }
    };

    Ok(())
}

fn process_command(addr: SocketAddr, request: Request) -> Result<Response> {
    let conn = TcpStream::connect(addr)?;
    let reader = BufReader::new(&conn);
    let mut writer = BufWriter::new(&conn);

    let mut json = Vec::new();
    serde_json::to_writer(&mut json, &request)?;
    writer.write_all(&json)?;
    writer.flush()?;

    let mut response_iter = Deserializer::from_reader(reader).into_iter::<Response>();
    let response = response_iter.next().expect("no response received")?;
    Ok(response)
}
