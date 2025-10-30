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

//! Common Avro errors and macros.

use core::num::TryFromIntError;
use std::error::Error;
use std::string::FromUtf8Error;
use std::{cell, io, result, str};
use arrow_schema::ArrowError;

/// Avro error enumeration

#[derive(Debug)]
#[non_exhaustive]
pub enum AvroError {
    /// General Avro error.
    /// Returned when code violates normal workflow of working with Avro data.
    General(String),
    /// "Not yet implemented" Avro error.
    /// Returned when functionality is not yet available.
    NYI(String),
    /// "End of file" Avro error.
    /// Returned when IO related failures occur, e.g. when there are not enough bytes to
    /// decode.
    EOF(String),
    /// Arrow error.
    /// Returned when reading into arrow or writing from arrow.
    ArrowError(String),
    /// Error when the requested index is more than the
    /// number of items expected
    IndexOutOfBound(usize, usize),
    /// An external error variant
    External(Box<dyn Error + Send + Sync>),
    /// Returned when a function needs more data to complete properly. The `usize` field indicates
    /// the total number of bytes required, not the number of additional bytes.
    NeedMoreData(usize),
    /// Returned when a function needs more data to complete properly.
    /// The `Range<u64>` indicates the range of bytes that are needed.
    NeedMoreDataRange(std::ops::Range<u64>),
}

impl std::fmt::Display for AvroError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self {
            AvroError::General(message) => {
                write!(fmt, "Avro error: {message}")
            }
            AvroError::NYI(message) => write!(fmt, "NYI: {message}"),
            AvroError::EOF(message) => write!(fmt, "EOF: {message}"),
            AvroError::ArrowError(message) => write!(fmt, "Arrow: {message}"),
            AvroError::IndexOutOfBound(index, bound) => {
                write!(fmt, "Index {index} out of bound: {bound}")
            }
            AvroError::External(e) => write!(fmt, "External: {e}"),
            AvroError::NeedMoreData(needed) => write!(fmt, "NeedMoreData: {needed}"),
            AvroError::NeedMoreDataRange(range) => {
                write!(fmt, "NeedMoreDataRange: {}..{}", range.start, range.end)
            }
        }
    }
}

impl Error for AvroError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            AvroError::External(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

impl From<TryFromIntError> for AvroError {
    fn from(e: TryFromIntError) -> AvroError {
        AvroError::General(format!("Integer overflow: {e}"))
    }
}

impl From<io::Error> for AvroError {
    fn from(e: io::Error) -> AvroError {
        AvroError::External(Box::new(e))
    }
}

impl From<cell::BorrowMutError> for AvroError {
    fn from(e: cell::BorrowMutError) -> AvroError {
        AvroError::External(Box::new(e))
    }
}

impl From<str::Utf8Error> for AvroError {
    fn from(e: str::Utf8Error) -> AvroError {
        AvroError::External(Box::new(e))
    }
}

impl From<FromUtf8Error> for AvroError {
    fn from(e: FromUtf8Error) -> AvroError {
        AvroError::External(Box::new(e))
    }
}

impl From<ArrowError> for AvroError {
    fn from(e: ArrowError) -> AvroError {
        AvroError::External(Box::new(e))
    }
}

/// A specialized `Result` for Avro errors.
pub type Result<T, E = AvroError> = result::Result<T, E>;

// ----------------------------------------------------------------------
// Conversion from `AvroError` to other types of `Error`s

impl From<AvroError> for io::Error {
    fn from(e: AvroError) -> Self {
        io::Error::other(e)
    }
}

// ----------------------------------------------------------------------
// Convenient macros for different errors

macro_rules! general_err {
    ($fmt:expr) => (AvroError::General($fmt.to_owned()));
    ($fmt:expr, $($args:expr),*) => (AvroError::General(format!($fmt, $($args),*)));
    ($e:expr, $fmt:expr) => (AvroError::General($fmt.to_owned(), $e));
    ($e:ident, $fmt:expr, $($args:tt),*) => (
        AvroError::General(&format!($fmt, $($args),*), $e));
}

macro_rules! nyi_err {
    ($fmt:expr) => (AvroError::NYI($fmt.to_owned()));
    ($fmt:expr, $($args:expr),*) => (AvroError::NYI(format!($fmt, $($args),*)));
}

macro_rules! eof_err {
    ($fmt:expr) => (AvroError::EOF($fmt.to_owned()));
    ($fmt:expr, $($args:expr),*) => (AvroError::EOF(format!($fmt, $($args),*)));
}

macro_rules! arrow_err {
    ($fmt:expr) => (AvroError::ArrowError($fmt.to_owned()));
    ($fmt:expr, $($args:expr),*) => (AvroError::ArrowError(format!($fmt, $($args),*)));
    ($e:expr, $fmt:expr) => (AvroError::ArrowError($fmt.to_owned(), $e));
    ($e:ident, $fmt:expr, $($args:tt),*) => (
        AvroError::ArrowError(&format!($fmt, $($args),*), $e));
}

// ----------------------------------------------------------------------
// Convert avro error into other errors

impl From<AvroError> for ArrowError {
    fn from(p: AvroError) -> Self {
        Self::AvroError(format!("{p}"))
    }
}

