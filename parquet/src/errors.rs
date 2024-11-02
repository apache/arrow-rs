// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Common Parquet errors and macros.

use std::error::Error;
use std::{cell, io, result, str};

#[cfg(feature = "arrow")]
use arrow_schema::ArrowError;

/// Parquet error enumeration
// Note: we don't implement PartialEq as the semantics for the
// external variant are not well defined (#4469)
#[derive(Debug)]
pub enum ParquetError {
    /// General Parquet error.
    /// Returned when code violates normal workflow of working with Parquet files.
    General(String),
    /// "Not yet implemented" Parquet error.
    /// Returned when functionality is not yet available.
    NYI(String),
    /// "End of file" Parquet error.
    /// Returned when IO related failures occur, e.g. when there are not enough bytes to
    /// decode.
    EOF(String),
    #[cfg(feature = "arrow")]
    /// Arrow error.
    /// Returned when reading into arrow or writing from arrow.
    ArrowError(String),
    /// Error when the requested column index is more than the
    /// number of columns in the row group
    IndexOutOfBound(usize, usize),
    /// An external error variant
    External(Box<dyn Error + Send + Sync>),
}

impl std::fmt::Display for ParquetError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self {
            ParquetError::General(message) => {
                write!(fmt, "Parquet error: {message}")
            }
            ParquetError::NYI(message) => write!(fmt, "NYI: {message}"),
            ParquetError::EOF(message) => write!(fmt, "EOF: {message}"),
            #[cfg(feature = "arrow")]
            ParquetError::ArrowError(message) => write!(fmt, "Arrow: {message}"),
            ParquetError::IndexOutOfBound(index, ref bound) => {
                write!(fmt, "Index {index} out of bound: {bound}")
            }
            ParquetError::External(e) => write!(fmt, "External: {e}"),
        }
    }
}

impl Error for ParquetError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ParquetError::External(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

impl From<io::Error> for ParquetError {
    fn from(e: io::Error) -> ParquetError {
        ParquetError::External(Box::new(e))
    }
}

#[cfg(any(feature = "snap", test))]
impl From<snap::Error> for ParquetError {
    fn from(e: snap::Error) -> ParquetError {
        ParquetError::External(Box::new(e))
    }
}

impl From<thrift::Error> for ParquetError {
    fn from(e: thrift::Error) -> ParquetError {
        ParquetError::External(Box::new(e))
    }
}

impl From<cell::BorrowMutError> for ParquetError {
    fn from(e: cell::BorrowMutError) -> ParquetError {
        ParquetError::External(Box::new(e))
    }
}

impl From<str::Utf8Error> for ParquetError {
    fn from(e: str::Utf8Error) -> ParquetError {
        ParquetError::External(Box::new(e))
    }
}
#[cfg(feature = "arrow")]
impl From<ArrowError> for ParquetError {
    fn from(e: ArrowError) -> ParquetError {
        ParquetError::External(Box::new(e))
    }
}

#[cfg(feature = "object_store")]
impl From<object_store::Error> for ParquetError {
    fn from(e: object_store::Error) -> ParquetError {
        ParquetError::External(Box::new(e))
    }
}

/// A specialized `Result` for Parquet errors.
pub type Result<T, E = ParquetError> = result::Result<T, E>;

// ----------------------------------------------------------------------
// Conversion from `ParquetError` to other types of `Error`s

impl From<ParquetError> for io::Error {
    fn from(e: ParquetError) -> Self {
        io::Error::new(io::ErrorKind::Other, e)
    }
}

// ----------------------------------------------------------------------
// Convenient macros for different errors

macro_rules! general_err {
    ($fmt:expr) => (ParquetError::General($fmt.to_owned()));
    ($fmt:expr, $($args:expr),*) => (ParquetError::General(format!($fmt, $($args),*)));
    ($e:expr, $fmt:expr) => (ParquetError::General($fmt.to_owned(), $e));
    ($e:ident, $fmt:expr, $($args:tt),*) => (
        ParquetError::General(&format!($fmt, $($args),*), $e));
}

macro_rules! nyi_err {
    ($fmt:expr) => (ParquetError::NYI($fmt.to_owned()));
    ($fmt:expr, $($args:expr),*) => (ParquetError::NYI(format!($fmt, $($args),*)));
}

macro_rules! eof_err {
    ($fmt:expr) => (ParquetError::EOF($fmt.to_owned()));
    ($fmt:expr, $($args:expr),*) => (ParquetError::EOF(format!($fmt, $($args),*)));
}

#[cfg(feature = "arrow")]
macro_rules! arrow_err {
    ($fmt:expr) => (ParquetError::ArrowError($fmt.to_owned()));
    ($fmt:expr, $($args:expr),*) => (ParquetError::ArrowError(format!($fmt, $($args),*)));
    ($e:expr, $fmt:expr) => (ParquetError::ArrowError($fmt.to_owned(), $e));
    ($e:ident, $fmt:expr, $($args:tt),*) => (
        ParquetError::ArrowError(&format!($fmt, $($args),*), $e));
}

// ----------------------------------------------------------------------
// Convert parquet error into other errors

#[cfg(feature = "arrow")]
impl From<ParquetError> for ArrowError {
    fn from(p: ParquetError) -> Self {
        Self::ParquetError(format!("{p}"))
    }
}
