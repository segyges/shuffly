use std::path::{Path, PathBuf};
use std::fs;
use std::io::{self, Write};
use rand::seq::SliceRandom;
use rand::rng;

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
    
    if all_lines.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "No lines found in input files"
        ));
    }
    
    // Shuffle the lines
    let mut rng = rng();
    all_lines.shuffle(&mut rng);
    println!("Shuffled {} lines", all_lines.len());
    
    // Calculate how to split the lines across output files
    let max_size_bytes = config.max_size_mb * 1024 * 1024;
    let mut output_files = Vec::new();
    let mut current_size = 0;
    let mut current_file_lines = Vec::new();
    let mut file_index = 0;
    
    for line in all_lines {
        let line_size = line.len() + 1; // +1 for newline
        
        // Check if adding this line would exceed the size limit
        if current_size + line_size > max_size_bytes && !current_file_lines.is_empty() {
            // Write current file
            let output_path = write_output_file(
                &config.output_dir,
                &config.output_name,
                file_index,
                &current_file_lines,
            )?;
            output_files.push(output_path);
            
            // Start new file
            current_file_lines.clear();
            current_size = 0;
            file_index += 1;
        }
        
        current_file_lines.push(line);
        current_size += line_size;
    }
    
    // Write the last file if it has content
    if !current_file_lines.is_empty() {
        let output_path = write_output_file(
            &config.output_dir,
            &config.output_name,
            file_index,
            &current_file_lines,
        )?;
        output_files.push(output_path);
    }
    
    println!("Successfully wrote {} output files", output_files.len());
    
    Ok(output_files)
}

fn write_output_file(
    output_dir: &Path,
    output_name: &str,
    file_index: usize,
    lines: &[String],
) -> Result<PathBuf, io::Error> {
    let filename = if file_index == 0 && lines.len() < 1000000 { // Heuristic for single file
        format!("{}.jsonl", output_name)
    } else {
        format!("{}_part_{:04}.jsonl", output_name, file_index + 1)
    };
    
    let output_path = output_dir.join(filename);
    let mut file = fs::File::create(&output_path)?;
    
    for line in lines {
        writeln!(file, "{}", line)?;
    }
    
    println!("Wrote {} lines to {}", lines.len(), output_path.display());
    
    Ok(output_path)
}

pub fn count_lines_in_file(file_path: &Path) -> Result<usize, io::Error> {
    let content = fs::read_to_string(file_path)?;
    Ok(content.lines().filter(|line| !line.trim().is_empty()).count())
}