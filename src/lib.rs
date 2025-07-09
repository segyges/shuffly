mod shuffle;

// Re-export your core functions
pub use shuffle::*;

// Python bindings - only when pyo3 feature enabled
#[cfg(feature = "pyo3")]
use pyo3::prelude::*;

#[cfg(feature = "pyo3")]
#[pyfunction]
#[pyo3(name = "shuffle_jsonl")]  // Python name
fn shuffle_jsonl_py(input: &str) -> PyResult<String> {
    Ok(shuffle::shuffle_jsonl(input))
}

#[cfg(feature = "pyo3")]
#[pyfunction]
#[pyo3(name = "shuffle_file")]  // Python name
fn shuffle_file_py(file_path: &str) -> PyResult<String> {
    shuffle::shuffle_file(file_path)
        .map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))
}

#[cfg(feature = "pyo3")]
#[pymodule]
fn shuffly(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(shuffle_jsonl_py, m)?)?;
    m.add_function(wrap_pyfunction!(shuffle_file_py, m)?)?;
    Ok(())
}