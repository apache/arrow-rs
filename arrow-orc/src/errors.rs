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

//! ORC errors and related utility macros.

use arrow_schema::ArrowError;
use prost::DecodeError;
use std::error::Error;
use std::{io, result, str};

// TODO: more specific errors
#[derive(Debug)]
pub enum OrcError {
    /// Generic error
    General(String),
    /// When couldn't convert to/from Arrow schema
    SchemaConversion(String),
    /// When file doesn't conform to expected spec
    Corrupted(String),
    /// Functionality not yet implemented
    NotYetImplemented(String),
    /// External error
    External(Box<dyn Error + Send + Sync>),
}

impl std::fmt::Display for OrcError {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        match &self {
            Self::General(m) => write!(fmt, "ORC error: {m}"),
            Self::SchemaConversion(m) => write!(fmt, "ORC schema error: {m}"),
            Self::Corrupted(m) => write!(fmt, "ORC file out of specification: {m}"),
            Self::NotYetImplemented(m) => {
                write!(fmt, "ORC feature not yet implemented: {m}")
            }
            Self::External(m) => write!(fmt, "External: {m}"),
        }
    }
}

impl Error for OrcError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::External(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

impl From<io::Error> for OrcError {
    fn from(e: io::Error) -> Self {
        Self::External(Box::new(e))
    }
}

impl From<snap::Error> for OrcError {
    fn from(e: snap::Error) -> Self {
        Self::External(Box::new(e))
    }
}

impl From<lzokay_native::Error> for OrcError {
    fn from(e: lzokay_native::Error) -> Self {
        Self::External(Box::new(e))
    }
}

impl From<str::Utf8Error> for OrcError {
    fn from(e: str::Utf8Error) -> Self {
        Self::External(Box::new(e))
    }
}

impl From<lz4_flex::block::DecompressError> for OrcError {
    fn from(e: lz4_flex::block::DecompressError) -> Self {
        Self::External(Box::new(e))
    }
}

impl From<ArrowError> for OrcError {
    fn from(e: ArrowError) -> Self {
        Self::External(Box::new(e))
    }
}

impl From<DecodeError> for OrcError {
    fn from(e: DecodeError) -> Self {
        Self::External(Box::new(e))
    }
}

/// A specialized `Result` for ORC errors.
pub type Result<T, E = OrcError> = result::Result<T, E>;

// ----------------------------------------------------------------------
// Convenient macros for different errors

macro_rules! general_err {
    ($fmt:expr) => (crate::errors::OrcError::General($fmt.to_owned()));
    ($fmt:expr, $($args:expr),*) => (crate::errors::OrcError::General(format!($fmt, $($args),*)));
    ($e:expr, $fmt:expr) => (crate::errors::OrcError::General($fmt.to_owned(), $e));
    ($e:ident, $fmt:expr, $($args:tt),*) => (
        crate::errors::OrcError::General(&format!($fmt, $($args),*), $e));
}

macro_rules! nyi_err {
    ($fmt:expr) => (crate::errors::OrcError::NotYetImplemented($fmt.to_owned()));
    ($fmt:expr, $($args:expr),*) => (crate::errors::OrcError::NotYetImplemented(format!($fmt, $($args),*)));
}

// ----------------------------------------------------------------------
// Convert ORC error into other errors

impl From<OrcError> for ArrowError {
    fn from(p: OrcError) -> Self {
        Self::OrcError(format!("{p}"))
    }
}

impl From<OrcError> for std::io::Error {
    fn from(value: OrcError) -> Self {
        std::io::Error::new(std::io::ErrorKind::Other, value)
    }
}
