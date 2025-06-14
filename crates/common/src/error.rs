use thiserror::Error;

/// Unified error type for Igloo crates.
use sqlparser::parser::ParserError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("An unknown error occurred: {0}")]
    Unknown(String),
    #[error("I/O error: {0}")]
    Io(String),
    #[error("Execution error: {0}")]
    Execution(String),
    #[error("Operation not supported: {0}")]
    NotSupported(String),
    // Add more error variants as needed
    #[error("SQL parsing error: {0}")]
    SqlParser(#[from] ParserError),
}

pub type Result<T> = std::result::Result<T, Error>;

// The new() method might need to be re-evaluated or removed if Unknown is not the primary default.
// For now, keeping it as is.
impl Error {
    pub fn new(msg: &str) -> Self {
        Error::Unknown(msg.to_string())
    }
}
