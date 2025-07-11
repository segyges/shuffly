mod shuffle;

// Re-export your core functions
pub use shuffle::*;

// Python bindings - only when pyo3 feature enabled
#[cfg(feature = "pyo3")]
use pyo3::prelude::*;
#[cfg(feature = "pyo3")]
use std::path::PathBuf;

#[cfg(feature = "pyo3")]
#[pyfunction]
#[pyo3(name = "shuffle_files")]
fn shuffle_files_py(
    input_files: Vec<String>,  // Changed from &str to Vec<String>
    output_dir: &str,
    output_name: &str,
    max_size_mb: usize,
    delimiter: Option<&str>,      // Added delimiter parameter
    file_extension: Option<&str>, // Added file extension parameter
    seed: Option<u64>,           // Added seed parameter
) -> PyResult<Vec<String>> {
    // Convert string paths to PathBuf
    let input_pathbufs: Vec<PathBuf> = input_files.into_iter().map(PathBuf::from).collect();
    
    let config = shuffle::ShuffleConfig::new(
        input_pathbufs,
        output_dir,
        output_name,
        max_size_mb,
        delimiter.unwrap_or("\n"),        // Default to newline
        file_extension.unwrap_or("jsonl"), // Default to jsonl
        seed,
    ).map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    
    // Use tokio runtime for async function
    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
    
    let output_files = rt.block_on(shuffle::shuffle_files(&config))
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    
    Ok(output_files.into_iter().map(|p| p.to_string_lossy().to_string()).collect())
}

#[cfg(feature = "pyo3")]
#[pymodule]
fn shuffly(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(shuffle_files_py, m)?)?;
    Ok(())
}