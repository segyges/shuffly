use clap::Parser;
use shuffly::ShuffleConfig;
use std::fs;
use std::path::Path;

#[derive(Parser)]
#[command(name = "shuffly")]
#[command(about = "A CLI tool for shuffling JSONL files")]
struct Cli {
    /// Input files separated by colons (e.g., "file1.jsonl:file2.jsonl")
    #[arg(short = 'f', long, group = "input")]
    input_files: Option<String>,
    
    /// Directory containing .jsonl files to shuffle
    #[arg(short = 'd', long, group = "input")]
    input_dir: Option<String>,
    
    /// Output directory
    #[arg(short, long, default_value = ".")]
    output_dir: String,
    
    /// Output file name (without extension)
    #[arg(short = 'n', long, default_value = "shuffled")]
    output_name: String,
    
    /// Maximum size per output file in MB
    #[arg(short = 's', long, default_value_t = 4096)]
    max_size_mb: usize,
    
    /// Random seed for deterministic shuffling
    #[arg(long)]
    seed: Option<u64>,
}

fn collect_jsonl_files(dir: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut jsonl_files = Vec::new();
    let dir_path = Path::new(dir);
    
    if !dir_path.is_dir() {
        return Err(format!("'{}' is not a directory", dir).into());
    }
    
    for entry in fs::read_dir(dir_path)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.is_file() {
            if let Some(extension) = path.extension() {
                if extension == "jsonl" {
                    if let Some(path_str) = path.to_str() {
                        jsonl_files.push(path_str.to_string());
                    }
                }
            }
        }
    }
    
    if jsonl_files.is_empty() {
        return Err(format!("No .jsonl files found in directory '{}'", dir).into());
    }
    
    jsonl_files.sort(); // For consistent ordering
    Ok(jsonl_files)
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    
    // Determine input files
    let input_files = match (cli.input_files, cli.input_dir) {
        (Some(files), None) => files,
        (None, Some(dir)) => {
            match collect_jsonl_files(&dir) {
                Ok(files) => files.join(":"),
                Err(e) => {
                    eprintln!("Error reading directory: {}", e);
                    std::process::exit(1);
                }
            }
        }
        (None, None) => {
            eprintln!("Error: Must specify either --input-files or --input-dir");
            std::process::exit(1);
        }
        (Some(_), Some(_)) => {
            eprintln!("Error: Cannot specify both --input-files and --input-dir");
            std::process::exit(1);
        }
    };
    
    let config = match ShuffleConfig::new(
        &input_files,
        &cli.output_dir,
        &cli.output_name,
        cli.max_size_mb,
        cli.seed,
    ) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };
    
    match shuffly::shuffle_jsonl(&config).await {
        Ok(output_files) => {
            println!("Successfully created {} output files:", output_files.len());
            for file in output_files {
                println!("  {}", file.display());
            }
        }
        Err(e) => {
            eprintln!("Error during shuffling: {}", e);
            std::process::exit(1);
        }
    }
}