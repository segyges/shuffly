use std::collections::HashMap;
use std::path::{PathBuf};
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

struct LineBuffer {
    lines: Vec<(usize, String)>, // (temp_file_index, line_content)
    total_size: usize,
}

impl LineBuffer {
    fn new() -> Self {
        Self {
            lines: Vec::new(),
            total_size: 0,
        }
    }
    
    fn add_line(&mut self, temp_index: usize, line: String) {
        self.total_size += line.len();
        self.lines.push((temp_index, line));
    }
    
    fn is_full(&self, max_size: usize) -> bool {
        self.total_size >= max_size
    }
    
    fn clear(&mut self) {
        self.lines.clear();
        self.total_size = 0;
    }
    
    fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }
}

async fn flush_line_buffer(
    buffer: &mut LineBuffer,
    temp_files: &[PathBuf],
    max_open_files: usize,
) -> Result<(), io::Error> {
    if buffer.is_empty() {
        return Ok(());
    }
    
    // Group lines by temp file index
    let mut lines_by_file: HashMap<usize, Vec<String>> = HashMap::new();
    for (temp_index, line) in buffer.lines.drain(..) {
        lines_by_file.entry(temp_index).or_default().push(line);
    }
    
    // Process files in batches to limit open file descriptors
    let file_indices: Vec<usize> = lines_by_file.keys().cloned().collect();
    
    for chunk in file_indices.chunks(max_open_files) {
        let mut writers = Vec::new();
        let mut indices = Vec::new();
        
        // Open files in this batch
        for &file_idx in chunk {
            let file = tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&temp_files[file_idx])
                .await?;
            writers.push(BufWriter::new(file));
            indices.push(file_idx);
        }
        
        // Write all lines for files in this batch
        for (writer_idx, &file_idx) in indices.iter().enumerate() {
            if let Some(lines) = lines_by_file.get(&file_idx) {
                for line in lines {
                    writers[writer_idx].write_all(line.as_bytes()).await?;
                    writers[writer_idx].write_all(b"\n").await?;
                }
            }
        }
        
        // Flush and close all writers in this batch
        for mut writer in writers {
            writer.flush().await?;
        }
    }
    
    buffer.clear();
    Ok(())
}

async fn phase_1_distribute(config: &ShuffleConfig) -> Result<Vec<PathBuf>, io::Error> {
    println!("Phase 1: Distributing lines to temporary files...");
    
    // Estimate number of output files based on total input size
    let total_input_size = estimate_total_input_size(&config.input_files).await?;
    let max_size_bytes = config.max_size_mb * 1024 * 1024;
    let estimated_num_files = ((total_input_size + max_size_bytes - 1) / max_size_bytes).max(1);
    
    println!("Estimated {} output files needed", estimated_num_files);
    
    // Create temp file paths (but don't open them yet)
    let mut temp_files = Vec::new();
    for i in 0..estimated_num_files {
        let temp_path = config.output_dir.join(format!(".{}_temp_{:04}.jsonl", config.output_name, i));
        temp_files.push(temp_path);
    }
    
    // Configuration for batched processing
    const MAX_OPEN_INPUT_FILES: usize = 16;
    const MAX_OPEN_OUTPUT_FILES: usize = 128;
    const MAX_BUFFER_SIZE: usize = 1024 * 1024 * 1024; // 1GB
    
    // Initialize RNG with seed for deterministic behavior
    let mut rng: Box<dyn RngCore> = match config.seed {
        Some(seed) => Box::new(StdRng::seed_from_u64(seed)),
        None => Box::new(rng()),
    };
    let mut total_lines = 0;
    let mut line_buffer = LineBuffer::new();
    
    // Process input files in sorted order for deterministic behavior
    let mut sorted_input_files = config.input_files.clone();
    sorted_input_files.sort();
    
    // Process input files in batches
    for input_batch in sorted_input_files.chunks(MAX_OPEN_INPUT_FILES) {
        let mut readers = Vec::new();
        
        // Open input files in this batch
        for input_file in input_batch {
            println!("Processing {}", input_file.display());
            
            let file = File::open(input_file).await?;
            let buf_reader = BufReader::new(file);
            
            // Create a boxed reader that can handle both cases
            let reader: Box<dyn AsyncBufRead + Unpin> = if input_file.extension().and_then(|s| s.to_str()) == Some("gz") {
                Box::new(BufReader::new(GzipDecoder::new(buf_reader)))
            } else {
                Box::new(buf_reader)
            };
            
            readers.push(reader.lines());
        }
        
        // Round-robin through readers in this batch
        let mut active_readers = (0..readers.len()).collect::<Vec<_>>();
        
        while !active_readers.is_empty() {
            let mut finished_readers = Vec::new();
            
            for (idx, &reader_idx) in active_readers.iter().enumerate() {
                // Try to read a line from this reader
                if let Some(line) = readers[reader_idx].next_line().await? {
                    if !line.trim().is_empty() {
                        // Randomly assign to one of the temp files
                        let temp_index = rng.random_range(0..temp_files.len());
                        line_buffer.add_line(temp_index, line);
                        total_lines += 1;
                        
                        // Check if buffer is full
                        if line_buffer.is_full(MAX_BUFFER_SIZE) {
                            flush_line_buffer(&mut line_buffer, &temp_files, MAX_OPEN_OUTPUT_FILES).await?;
                        }
                    }
                } else {
                    // This reader is finished
                    finished_readers.push(idx);
                }
            }
            
            // Remove finished readers (in reverse order to maintain indices)
            for &idx in finished_readers.iter().rev() {
                active_readers.remove(idx);
            }
        }
    }
    
    // Flush any remaining lines in the buffer
    if !line_buffer.is_empty() {
        flush_line_buffer(&mut line_buffer, &temp_files, MAX_OPEN_OUTPUT_FILES).await?;
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