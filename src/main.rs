use clap::Parser;
use shuffly::ShuffleConfig;

#[derive(Parser)]
#[command(name = "shuffly")]
#[command(about = "A CLI tool for shuffling JSONL files")]
struct Cli {
    /// Input files separated by colons (e.g., "file1.jsonl:file2.jsonl")
    #[arg(short, long)]
    input: String,
    
    /// Output directory
    #[arg(short, long, default_value = ".")]
    output_dir: String,
    
    /// Output file name (without extension)
    #[arg(short = 'n', long, default_value = "shuffled")]
    output_name: String,
    
    /// Maximum size per output file in MB
    #[arg(short = 's', long, default_value_t = 100)]
    max_size_mb: usize,
}

fn main() {
    let cli = Cli::parse();
    
    let config = match ShuffleConfig::new(
        &cli.input,
        &cli.output_dir,
        &cli.output_name,
        cli.max_size_mb,
    ) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("Error: {}", e);
            std::process::exit(1);
        }
    };
    
    match shuffly::shuffle_jsonl(&config) {
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