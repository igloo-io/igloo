use thiserror::Error;

/// Unified error type for Igloo crates.
#[derive(Debug, Error)]
pub enum Error {
    #[error("An unknown error occurred: {0}")]
    Unknown(String),
    // Add more error variants as needed
}

impl Error {
    pub fn new(msg: &str) -> Self {
        Error::Unknown(msg.to_string())
    }
}
