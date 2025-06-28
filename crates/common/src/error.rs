use thiserror::Error;

/// Unified error type for Igloo crates.
use sqlparser::parser::ParserError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("An unknown error occurred: {0}")]
    Unknown(String),
    #[error("SQL parsing error: {0}")]
    Parse(String), // Changed from SqlParser(#[from] ParserError) to match usage
    #[error("Planning error: {0}")]
    Plan(String),
    // Add more error variants as needed
    #[error("SQL parser error: {0}")] // Kept a separate variant for direct sqlparser errors if needed elsewhere
    SqlParser(#[from] ParserError),
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn new(msg: &str) -> Self {
        Error::Unknown(msg.to_string())
    }
}
