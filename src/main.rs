use clap::{Parser, Subcommand};
use shuffly::{shuffle_jsonl, shuffle_file};

#[derive(Parser)]
#[command(name = "shuffly")]
#[command(about = "A CLI tool for shuffling JSONL files")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Shuffle JSONL from stdin
    Stdin,
    /// Shuffle a JSONL file
    File {
        /// Input file path
        path: String,
    },
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Commands::Stdin => {
            use std::io::Read;
            let mut input = String::new();
            std::io::stdin().read_to_string(&mut input).unwrap();
            let result = shuffle_jsonl(&input);
            println!("{}", result);
        }
        Commands::File { path } => {
            match shuffle_file(&path) {
                Ok(result) => println!("{}", result),
                Err(e) => eprintln!("Error: {}", e),
            }
        }
    }
}