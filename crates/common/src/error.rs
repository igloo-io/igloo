use thiserror::Error;

/// Unified error type for Igloo crates.
use sqlparser::parser::ParserError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("An unknown error occurred: {0}")]
    Unknown(String),
    // Add more error variants as needed
    #[error("SQL parsing error: {0}")]
    SqlParser(#[from] ParserError),
}

pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn new(msg: &str) -> Self {
        Error::Unknown(msg.to_string())
    }
}
