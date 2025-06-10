//! Common crate
//!
//! Shared utilities, types, and error handling for Igloo.
//!
//! # Example
//! ```rust
//! use igloo_common::Error;
//! let err = Error::new("example error");
//! ```
// TODO: Shared utilities, types, and error handling

pub mod error;
pub use error::Error;
