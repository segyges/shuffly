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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_parse_input_files_valid() {
        let temp_dir = TempDir::new().unwrap();
        
        // Create test files
        let file1 = temp_dir.path().join("test1.jsonl");
        let file2 = temp_dir.path().join("test2.jsonl");
        fs::write(&file1, "test content").unwrap();
        fs::write(&file2, "test content").unwrap();
        
        let input_str = format!("{}:{}", file1.display(), file2.display());
        let result = parse_input_files(&input_str).unwrap();
        
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], file1);
        assert_eq!(result[1], file2);
    }

    #[test]
    fn test_parse_input_files_with_spaces() {
        let temp_dir = TempDir::new().unwrap();
        
        // Create test files
        let file1 = temp_dir.path().join("test1.jsonl");
        let file2 = temp_dir.path().join("test2.jsonl");
        fs::write(&file1, "test content").unwrap();
        fs::write(&file2, "test content").unwrap();
        
        // Test with spaces around colons
        let input_str = format!("{} : {}", file1.display(), file2.display());
        let result = parse_input_files(&input_str).unwrap();
        
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], file1);
        assert_eq!(result[1], file2);
    }

    #[test]
    fn test_parse_input_files_nonexistent() {
        let input_str = "/nonexistent/file1.jsonl:/nonexistent/file2.jsonl";
        let result = parse_input_files(input_str);
        
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Input file not found"));
    }

    #[test]
    fn test_parse_input_files_single_file() {
        let temp_dir = TempDir::new().unwrap();
        let file1 = temp_dir.path().join("test.jsonl");
        fs::write(&file1, "test content").unwrap();
        
        let input_str = file1.to_string_lossy().to_string();
        let result = parse_input_files(&input_str).unwrap();
        
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], file1);
    }

    #[test]
    fn test_collect_files_by_extension_jsonl() {
        let temp_dir = TempDir::new().unwrap();
        
        // Create various test files
        let jsonl_file1 = temp_dir.path().join("test1.jsonl");
        let jsonl_file2 = temp_dir.path().join("test2.jsonl");
        let gz_file = temp_dir.path().join("test3.jsonl.gz");
        let txt_file = temp_dir.path().join("test4.txt");
        
        fs::write(&jsonl_file1, "test content").unwrap();
        fs::write(&jsonl_file2, "test content").unwrap();
        fs::write(&gz_file, "test content").unwrap();
        fs::write(&txt_file, "test content").unwrap();
        
        let result = collect_files_by_extension(temp_dir.path().to_str().unwrap(), "jsonl").unwrap();
        
        assert_eq!(result.len(), 3); // Should include both .jsonl and .jsonl.gz files
        assert!(result.contains(&jsonl_file1));
        assert!(result.contains(&jsonl_file2));
        assert!(result.contains(&gz_file));
        assert!(!result.iter().any(|p| p == &txt_file));
    }

    #[test]
    fn test_collect_files_by_extension_csv() {
        let temp_dir = TempDir::new().unwrap();
        
        // Create various test files
        let csv_file1 = temp_dir.path().join("test1.csv");
        let csv_file2 = temp_dir.path().join("test2.csv");
        let csv_gz_file = temp_dir.path().join("test3.csv.gz");
        let jsonl_file = temp_dir.path().join("test4.jsonl");
        
        fs::write(&csv_file1, "test content").unwrap();
        fs::write(&csv_file2, "test content").unwrap();
        fs::write(&csv_gz_file, "test content").unwrap();
        fs::write(&jsonl_file, "test content").unwrap();
        
        let result = collect_files_by_extension(temp_dir.path().to_str().unwrap(), "csv").unwrap();
        
        assert_eq!(result.len(), 3); // Should include both .csv and .csv.gz files
        assert!(result.contains(&csv_file1));
        assert!(result.contains(&csv_file2));
        assert!(result.contains(&csv_gz_file));
        assert!(!result.iter().any(|p| p == &jsonl_file));
    }

    #[test]
    fn test_collect_files_by_extension_sorted() {
        let temp_dir = TempDir::new().unwrap();
        
        // Create files in non-alphabetical order
        let file_c = temp_dir.path().join("c.jsonl");
        let file_a = temp_dir.path().join("a.jsonl");
        let file_b = temp_dir.path().join("b.jsonl");
        
        fs::write(&file_c, "test content").unwrap();
        fs::write(&file_a, "test content").unwrap();
        fs::write(&file_b, "test content").unwrap();
        
        let result = collect_files_by_extension(temp_dir.path().to_str().unwrap(), "jsonl").unwrap();
        
        assert_eq!(result.len(), 3);
        // Should be sorted alphabetically
        assert_eq!(result[0], file_a);
        assert_eq!(result[1], file_b);
        assert_eq!(result[2], file_c);
    }

    #[test]
    fn test_collect_files_by_extension_empty_directory() {
        let temp_dir = TempDir::new().unwrap();
        
        let result = collect_files_by_extension(temp_dir.path().to_str().unwrap(), "jsonl");
        
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No .jsonl files found"));
    }

    #[test]
    fn test_collect_files_by_extension_nonexistent_directory() {
        let result = collect_files_by_extension("/nonexistent/directory", "jsonl");
        
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("is not a directory"));
    }

    #[test]
    fn test_collect_files_by_extension_ignores_subdirectories() {
        let temp_dir = TempDir::new().unwrap();
        
        // Create a file in the main directory
        let main_file = temp_dir.path().join("main.jsonl");
        fs::write(&main_file, "test content").unwrap();
        
        // Create a subdirectory with a file
        let subdir = temp_dir.path().join("subdir");
        fs::create_dir(&subdir).unwrap();
        let sub_file = subdir.join("sub.jsonl");
        fs::write(&sub_file, "test content").unwrap();
        
        let result = collect_files_by_extension(temp_dir.path().to_str().unwrap(), "jsonl").unwrap();
        
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], main_file);
        assert!(!result.iter().any(|p| p == &sub_file));
    }

    #[test]
    fn test_collect_files_by_extension_case_sensitive() {
        let temp_dir = TempDir::new().unwrap();
        
        // Create files with different case extensions
        let lower_file = temp_dir.path().join("test.jsonl");
        let upper_file = temp_dir.path().join("test.JSONL");
        
        fs::write(&lower_file, "test content").unwrap();
        fs::write(&upper_file, "test content").unwrap();
        
        let result = collect_files_by_extension(temp_dir.path().to_str().unwrap(), "jsonl").unwrap();
        
        // Should only match lowercase extension
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], lower_file);
    }

    #[test]
    fn test_collect_files_by_extension_partial_match() {
        let temp_dir = TempDir::new().unwrap();
        
        // Create files that partially match the extension
        let partial_file = temp_dir.path().join("test.jsonlines");
        let exact_file = temp_dir.path().join("test.jsonl");
        
        fs::write(&partial_file, "test content").unwrap();
        fs::write(&exact_file, "test content").unwrap();
        
        let result = collect_files_by_extension(temp_dir.path().to_str().unwrap(), "jsonl").unwrap();
        
        // Should only match exact extension
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], exact_file);
    }
}