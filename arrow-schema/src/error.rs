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

//! Defines `ArrowError` for representing failures in various Arrow operations.
use std::fmt::{Debug, Display, Formatter};
use std::io::Write;

use std::error::Error;

/// Many different operations in the `arrow` crate return this error type.
#[derive(Debug)]
pub enum ArrowError {
    /// Returned when functionality is not yet available.
    NotYetImplemented(String),
    /// Wraps an external error.
    ExternalError(Box<dyn Error + Send + Sync>),
    /// Error during casting from one type to another.
    CastError(String),
    /// Memory or buffer error.
    MemoryError(String),
    /// Error during parsing from a string.
    ParseError(String),
    /// Error during schema-related operations.
    SchemaError(String),
    /// Error during computation.
    ComputeError(String),
    /// Error during division by zero.
    DivideByZero,
    /// Error when an arithmetic operation overflows.
    ArithmeticOverflow(String),
    /// Error during CSV-related operations.
    CsvError(String),
    /// Error during JSON-related operations.
    JsonError(String),
    /// Error during IO operations.
    IoError(String, std::io::Error),
    /// Error during IPC operations in `arrow-ipc` or `arrow-flight`.
    IpcError(String),
    /// Error indicating that an unexpected or bad argument was passed to a function.
    InvalidArgumentError(String),
    /// Error during Parquet operations.
    ParquetError(String),
    /// Error during import or export to/from the C Data Interface
    CDataInterface(String),
    /// Error when a dictionary key is bigger than the key type
    DictionaryKeyOverflowError,
    /// Error when the run end index in a REE array is bigger than the array length
    RunEndIndexOverflowError,
}

impl ArrowError {
    /// Wraps an external error in an `ArrowError`.
    pub fn from_external_error(error: Box<dyn Error + Send + Sync>) -> Self {
        Self::ExternalError(error)
    }
}

impl From<std::io::Error> for ArrowError {
    fn from(error: std::io::Error) -> Self {
        ArrowError::IoError(error.to_string(), error)
    }
}

impl From<std::str::Utf8Error> for ArrowError {
    fn from(error: std::str::Utf8Error) -> Self {
        ArrowError::ParseError(error.to_string())
    }
}

impl From<std::string::FromUtf8Error> for ArrowError {
    fn from(error: std::string::FromUtf8Error) -> Self {
        ArrowError::ParseError(error.to_string())
    }
}

impl<W: Write> From<std::io::IntoInnerError<W>> for ArrowError {
    fn from(error: std::io::IntoInnerError<W>) -> Self {
        ArrowError::IoError(error.to_string(), error.into())
    }
}

impl Display for ArrowError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ArrowError::NotYetImplemented(source) => {
                write!(f, "Not yet implemented: {}", &source)
            }
            ArrowError::ExternalError(source) => write!(f, "External error: {}", &source),
            ArrowError::CastError(desc) => write!(f, "Cast error: {desc}"),
            ArrowError::MemoryError(desc) => write!(f, "Memory error: {desc}"),
            ArrowError::ParseError(desc) => write!(f, "Parser error: {desc}"),
            ArrowError::SchemaError(desc) => write!(f, "Schema error: {desc}"),
            ArrowError::ComputeError(desc) => write!(f, "Compute error: {desc}"),
            ArrowError::ArithmeticOverflow(desc) => write!(f, "Arithmetic overflow: {desc}"),
            ArrowError::DivideByZero => write!(f, "Divide by zero error"),
            ArrowError::CsvError(desc) => write!(f, "Csv error: {desc}"),
            ArrowError::JsonError(desc) => write!(f, "Json error: {desc}"),
            ArrowError::IoError(desc, _) => write!(f, "Io error: {desc}"),
            ArrowError::IpcError(desc) => write!(f, "Ipc error: {desc}"),
            ArrowError::InvalidArgumentError(desc) => {
                write!(f, "Invalid argument error: {desc}")
            }
            ArrowError::ParquetError(desc) => {
                write!(f, "Parquet argument error: {desc}")
            }
            ArrowError::CDataInterface(desc) => {
                write!(f, "C Data interface error: {desc}")
            }
            ArrowError::DictionaryKeyOverflowError => {
                write!(f, "Dictionary key bigger than the key type")
            }
            ArrowError::RunEndIndexOverflowError => {
                write!(f, "Run end encoded array index overflow error")
            }
        }
    }
}

impl Error for ArrowError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ArrowError::ExternalError(source) => Some(source.as_ref()),
            ArrowError::IoError(_, source) => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn error_source() {
        let e1 = ArrowError::DivideByZero;
        assert!(e1.source().is_none());

        // one level of wrapping
        let e2 = ArrowError::ExternalError(Box::new(e1));
        let source = e2.source().unwrap().downcast_ref::<ArrowError>().unwrap();
        assert!(matches!(source, ArrowError::DivideByZero));

        // two levels of wrapping
        let e3 = ArrowError::ExternalError(Box::new(e2));
        let source = e3
            .source()
            .unwrap()
            .downcast_ref::<ArrowError>()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<ArrowError>()
            .unwrap();

        assert!(matches!(source, ArrowError::DivideByZero));
    }
}
