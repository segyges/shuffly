mod shuffle;

// Re-export your core functions
pub use shuffle::*;

// Python bindings - only when pyo3 feature enabled
#[cfg(feature = "pyo3")]
use pyo3::prelude::*;

#[cfg(feature = "pyo3")]
#[pyfunction]
#[pyo3(name = "shuffle_jsonl")]
fn shuffle_jsonl_py(
    input_files: &str,
    output_dir: &str,
    output_name: &str,
    max_size_mb: usize,
) -> PyResult<Vec<String>> {
    let config = shuffle::ShuffleConfig::new(input_files, output_dir, output_name, max_size_mb)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    
    let output_files = shuffle::shuffle_jsonl(&config)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
    
    Ok(output_files.into_iter().map(|p| p.to_string_lossy().to_string()).collect())
}

#[cfg(feature = "pyo3")]
#[pymodule]
fn shuffly(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(shuffle_jsonl_py, m)?)?;
    Ok(())
}