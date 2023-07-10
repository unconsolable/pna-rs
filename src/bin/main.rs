use clap::{Parser, Subcommand};
use kvs::{KvStore, KvsError, Result};

#[derive(Parser)]
#[command(version, about)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Get { key: String },
    Set { key: String, value: String },
    Rm { key: String },
}

fn main() -> Result<()> {
    let mut kv = KvStore::open(".")?;

    let cli = Cli::parse();
    match cli.command {
        Commands::Get { key } => {
            match kv.get(key)? {
                Some(value) => {
                    println!("{value}");
                }
                None => {
                    println!("Key not found");
                }
            }
            Ok(())
        }
        Commands::Set { key, value } => kv.set(key, value),
        Commands::Rm { key } => match kv.remove(key) {
            Err(KvsError::KeyNotFound) => {
                println!("Key not found");
                Err(KvsError::KeyNotFound)
            }
            other => other,
        },
    }
}
