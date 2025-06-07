use std::net::AddrParseError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum WorkerError {
    #[error("Failed to load configuration")]
    Config(#[from] config::ConfigError),

    #[error("Invalid worker server address in configuration")]
    WorkerAddrParse(#[from] AddrParseError),

    // Note: tonic::transport::Error is for server-side listening.
    // For client-side connection errors, tonic::Status is often the direct error,
    // or it's wrapped in a general gRPC error.
    #[error("gRPC client connection error")]
    ClientConnection(#[from] tonic::transport::Error), // For CoordinatorServiceClient::connect

    #[error("gRPC call failed: {0}")]
    RpcError(#[from] tonic::Status),

    #[error("Failed to register worker after multiple retries")]
    RegistrationFailed,

    #[error("Failed to connect to coordinator after multiple retries")]
    ConnectionFailed,

    #[error("An internal error occurred: {0}")]
    Internal(String),
}
