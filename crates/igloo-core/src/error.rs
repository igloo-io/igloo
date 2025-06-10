use thiserror::Error;

#[derive(Debug, Error)]
pub enum IglooError {
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Execution error: {0}")]
    Execution(String),
}
