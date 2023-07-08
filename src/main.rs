use clap::{Parser, Subcommand};

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

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::Get { .. } => unimplemented!("unimplemented"),
        Commands::Set { .. } => unimplemented!("unimplemented"),
        Commands::Rm { .. } => unimplemented!("unimplemented"),
    }
}
