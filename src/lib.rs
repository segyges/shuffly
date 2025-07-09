#[cfg(feature = "pyo3")]
use pyo3::prelude::*;

#[cfg(feature = "pyo3")]
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok(sum_numbers(a, b).to_string())
}

#[cfg(feature = "pyo3")]
#[pymodule]
fn shuffly(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    Ok(())
}

// Core function - always available
pub fn sum_numbers(a: usize, b: usize) -> usize {
    a + b
}
