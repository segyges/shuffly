use std::path::{Path, PathBuf};
use std::fs;
use std::io;

#[derive(Debug, Clone)]
pub struct ShuffleConfig {
    pub input_files: Vec<PathBuf>,
    pub output_dir: PathBuf,
    pub output_name: String,
    pub max_size_mb: usize,
}

impl ShuffleConfig {
    pub fn new(
        input_files_str: &str,
        output_dir: &str,
        output_name: &str,
        max_size_mb: usize,
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

pub fn shuffle_jsonl(config: &ShuffleConfig) -> Result<Vec<PathBuf>, io::Error> {
    // Read all lines from all input files
    let mut all_lines = Vec::new();
    
    for file in &config.input_files {
        let content = fs::read_to_string(file)?;
        for line in content.lines() {
            if !line.trim().is_empty() {
                all_lines.push(line.to_string());
            }
        }
    }
    
    println!("Read {} lines from {} files", all_lines.len(), config.input_files.len());
    
    // TODO: Implement actual shuffling of all_lines
    // For now, just calculate output files
    
    let max_size_bytes = config.max_size_mb * 1024 * 1024;
    let estimated_total_size: usize = all_lines.iter().map(|line| line.len() + 1).sum(); // +1 for newline
    
    let num_output_files = if estimated_total_size <= max_size_bytes {
        1
    } else {
        (estimated_total_size + max_size_bytes - 1) / max_size_bytes // Ceiling division
    };
    
    let mut output_files = Vec::new();
    for i in 0..num_output_files {
        let filename = if num_output_files == 1 {
            format!("{}.jsonl", config.output_name)
        } else {
            format!("{}_part_{:03}.jsonl", config.output_name, i + 1)
        };
        
        let output_path = config.output_dir.join(filename);
        output_files.push(output_path);
    }
    
    println!("Would shuffle {} lines into {} output files", 
             all_lines.len(), output_files.len());
    println!("Estimated total size: {} bytes", estimated_total_size);
    println!("Output files: {:?}", output_files);
    
    Ok(output_files)
}

pub fn count_lines_in_file(file_path: &Path) -> Result<usize, io::Error> {
    let content = fs::read_to_string(file_path)?;
    Ok(content.lines().filter(|line| !line.trim().is_empty()).count())
}