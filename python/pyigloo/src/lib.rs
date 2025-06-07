use pyo3::prelude::*;

/// A Python module implemented in Rust.
#[pymodule]
fn pyigloo(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hello, m)?)?;
    // We will add the IglooClient class here later
    Ok(())
}

/// A simple function to test the bindings.
#[pyfunction]
fn hello() -> PyResult<String> {
    Ok("Hello from pyigloo!".to_string())
}
