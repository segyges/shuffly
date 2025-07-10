use std::path::{Path, PathBuf};
use std::fs;
use std::io;
use async_compression::tokio::bufread::GzipDecoder;
use tokio::fs::File;
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use rand::prelude::*;
use rand::rngs::StdRng;
use rand::{SeedableRng, rng, RngCore};

#[derive(Debug, Clone)]
pub struct ShuffleConfig {
    pub input_files: Vec<PathBuf>,
    pub output_dir: PathBuf,
    pub output_name: String,
    pub max_size_mb: usize,
    pub seed: Option<u64>,
}

impl ShuffleConfig {
    pub fn new(
        input_files_str: &str,
        output_dir: &str,
        output_name: &str,
        max_size_mb: usize,
        seed: Option<u64>,
    ) -> Result<Self, io::Error> {
        let input_files = parse_input_files(input_files_str)?;
        let output_dir = PathBuf::from(output_dir);
        
        // Validate output directory exists or can be created
        if !output_dir.exists() {
            fs::create_dir_all(&output_dir)?;
        }
        
        Ok(ShuffleConfig {
            input_files,
            output_dir,
            output_name: output_name.to_string(),
            max_size_mb,
            seed,
        })
    }
}

fn parse_input_files(input_str: &str) -> Result<Vec<PathBuf>, io::Error> {
    let files: Vec<PathBuf> = input_str
        .split(':')
        .map(|s| PathBuf::from(s.trim()))
        .collect();
    
    // Validate all files exist
    for file in &files {
        if !file.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Input file not found: {}", file.display())
            ));
        }
    }
    
    Ok(files)
}

pub async fn shuffle_jsonl(config: &ShuffleConfig) -> Result<Vec<PathBuf>, io::Error> {
    // Phase 1: Distribute lines from input files to temporary files
    let temp_files = phase_1_distribute(config).await?;
    
    // Phase 2: Shuffle each temp file and write to final output files
    let output_files = phase_2_shuffle_and_write(config, temp_files).await?;
    
    Ok(output_files)
}

async fn phase_1_distribute(config: &ShuffleConfig) -> Result<Vec<PathBuf>, io::Error> {
    println!("Phase 1: Distributing lines to temporary files...");
    
    // Estimate number of output files based on total input size
    let total_input_size = estimate_total_input_size(&config.input_files).await?;
    let max_size_bytes = config.max_size_mb * 1024 * 1024;
    let estimated_num_files = ((total_input_size + max_size_bytes - 1) / max_size_bytes).max(1);
    
    println!("Estimated {} output files needed", estimated_num_files);
    
    // Create temp files
    let mut temp_files = Vec::new();
    let mut temp_writers = Vec::new();
    
    for i in 0..estimated_num_files {
        let temp_path = config.output_dir.join(format!(".{}_temp_{:04}.jsonl", config.output_name, i));
        let file = File::create(&temp_path).await?;
        let writer = BufWriter::new(file);
        temp_files.push(temp_path);
        temp_writers.push(writer);
    }
    
    // Initialize RNG with seed for deterministic behavior
    let mut rng: Box<dyn RngCore> = match config.seed {
        Some(seed) => Box::new(StdRng::seed_from_u64(seed)),
        None => Box::new(rng()),
    };
    let mut total_lines = 0;
    
    // Process input files in sorted order for deterministic behavior
    let mut sorted_input_files = config.input_files.clone();
    sorted_input_files.sort();
			
	// Process each input file
		for input_file in &sorted_input_files {
				println!("Processing {}", input_file.display());
				
				let file = File::open(input_file).await?;
				let buf_reader = BufReader::new(file);
				
				// Create a boxed reader that can handle both cases
				let reader: Box<dyn AsyncBufRead + Unpin> = if input_file.extension().and_then(|s| s.to_str()) == Some("gz") {
						Box::new(BufReader::new(GzipDecoder::new(buf_reader)))
				} else {
						Box::new(buf_reader)
				};
				
				let mut lines = reader.lines();
				
				while let Some(line) = lines.next_line().await? {
						if !line.trim().is_empty() {
								// Randomly assign to one of the temp files
								let temp_index = rng.random_range(0..temp_writers.len());
								temp_writers[temp_index].write_all(line.as_bytes()).await?;
								temp_writers[temp_index].write_all(b"\n").await?;
								total_lines += 1;
						}
				}
		}
				
    // Flush and close all temp writers
    for mut writer in temp_writers {
        writer.flush().await?;
    }
    
    println!("Phase 1 complete: {} lines distributed across {} temp files", total_lines, temp_files.len());
    
    Ok(temp_files)
}

async fn phase_2_shuffle_and_write(
    config: &ShuffleConfig,
    temp_files: Vec<PathBuf>,
) -> Result<Vec<PathBuf>, io::Error> {
    println!("Phase 2: Shuffling temp files and writing final output...");
    
    let mut output_files = Vec::new();
    let mut rng: Box<dyn RngCore> = match config.seed {
        Some(seed) => Box::new(StdRng::seed_from_u64(seed.wrapping_add(1))), // Different seed for phase 2
        None => Box::new(rng()),
    };
    
    for (i, temp_file) in temp_files.iter().enumerate() {
        // Read all lines from this temp file
        let mut lines = Vec::new();
        let file = File::open(temp_file).await?;
        let reader = BufReader::new(file);
        let mut line_stream = reader.lines();
        
        while let Some(line) = line_stream.next_line().await? {
            if !line.trim().is_empty() {
                lines.push(line);
            }
        }
        
        // Skip empty temp files
        if lines.is_empty() {
            continue;
        }
        
        // Shuffle the lines
        lines.shuffle(&mut rng);
        
        // Write to final output file
        let output_filename = if temp_files.len() == 1 {
            format!("{}.jsonl", config.output_name)
        } else {
            format!("{}_part_{:04}.jsonl", config.output_name, i + 1)
        };
        
        let output_path = config.output_dir.join(output_filename);
        let output_file = File::create(&output_path).await?;
        let mut writer = BufWriter::new(output_file);
        
        for line in &lines {
            writer.write_all(line.as_bytes()).await?;
            writer.write_all(b"\n").await?;
        }
        
        writer.flush().await?;
        output_files.push(output_path.clone());
        
        println!("Wrote {} lines to {}", lines.len(), output_path.display());
        
        // Clean up temp file
        tokio::fs::remove_file(temp_file).await?;
    }
    
    println!("Phase 2 complete: {} final output files created", output_files.len());
    
    Ok(output_files)
}

async fn estimate_total_input_size(input_files: &[PathBuf]) -> Result<usize, io::Error> {
    let mut total_size = 0;
    for file in input_files {
        let metadata = tokio::fs::metadata(file).await?;
        total_size += metadata.len() as usize;
    }
    Ok(total_size)
}

pub async fn count_lines_in_file(file_path: &Path) -> Result<usize, io::Error> {
    let file = File::open(file_path).await?;
    let reader = BufReader::new(file);
    let mut lines = reader.lines();
    let mut count = 0;
    
    while let Some(line) = lines.next_line().await? {
        if !line.trim().is_empty() {
            count += 1;
        }
    }
    
    Ok(count)
}