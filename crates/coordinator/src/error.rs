use std::net::AddrParseError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum CoordinatorError {
    #[error("Failed to load configuration")]
    Config(#[from] config::ConfigError),

    #[error("Invalid server address in configuration")]
    AddrParse(#[from] AddrParseError),

    #[error("gRPC transport error")]
    Transport(#[from] tonic::transport::Error),

    #[error("Worker ID not found: {0}")]
    WorkerNotFound(String),

    #[error("An internal error occurred: {0}")]
    Internal(String),
}
