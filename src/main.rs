use clap::Parser;
use shuffly::ShuffleConfig;
use std::fs;
use std::path::{Path, PathBuf};

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

    #[arg(long, default_value = "\n")]
    delimiter: String,

		#[arg(long, default_value = "jsonl")]
		file_extension: String,
    
    /// Random seed for deterministic shuffling
    #[arg(long)]
    seed: Option<u64>,
}

fn collect_files_by_extension(dir: &str, extension: &str) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let mut files = Vec::new();
    let dir_path = Path::new(dir);
    
    if !dir_path.is_dir() {
        return Err(format!("'{}' is not a directory", dir).into());
    }
    
    let target_extension = format!(".{}", extension);
    let target_extension_gz = format!(".{}.gz", extension);
    
    for entry in fs::read_dir(dir_path)? {
        let entry = entry?;
        let path = entry.path();
        
        if path.is_file() {
            let path_str = path.to_string_lossy();
            
            if path_str.ends_with(&target_extension) || path_str.ends_with(&target_extension_gz) {
                files.push(path);
            }
        }
    }
    
    if files.is_empty() {
        return Err(format!("No .{} files found in directory '{}'", extension, dir).into());
    }
    
    files.sort(); // For consistent ordering
    Ok(files)
}

fn parse_input_files(input_str: &str) -> Result<Vec<PathBuf>, Box<dyn std::error::Error>> {
    let files: Vec<PathBuf> = input_str
        .split(':')
        .map(|s| PathBuf::from(s.trim()))
        .collect();
    
    // Validate all files exist
    for file in &files {
        if !file.exists() {
            return Err(format!("Input file not found: {}", file.display()).into());
        }
    }
    
    Ok(files)
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    
    // Determine input files - parse them here in the CLI layer
    let input_files = match (cli.input_files, cli.input_dir) {
        (Some(files_str), None) => {
            match parse_input_files(&files_str) {
                Ok(files) => files,
                Err(e) => {
                    eprintln!("Error parsing input files: {}", e);
                    std::process::exit(1);
                }
            }
        }
        (None, Some(dir)) => {
            match collect_files_by_extension(&dir, &cli.file_extension) {
                Ok(files) => files,
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
        input_files,  // Pass Vec<PathBuf> directly
        &cli.output_dir,
        &cli.output_name,
        cli.max_size_mb,
        &cli.delimiter,     // Pass delimiter
        &cli.file_extension, // Pass file extension
        cli.seed,
    ) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };
    
    match shuffly::shuffle_files(&config).await {
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