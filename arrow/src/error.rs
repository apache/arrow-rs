//! Defines `ArrowError` for representing failures in various Arrow operations.

pub use arrow_schema::ArrowError;

/// A specialized `Result` type for Arrow operations.
pub type Result<T> = std::result::Result<T, ArrowError>;
