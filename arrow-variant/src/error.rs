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

//! Error types for the arrow-variant crate

use arrow_schema::ArrowError;
use std::error::Error as StdError;
use std::fmt::{Display, Formatter, Result as FmtResult};

/// Error type for operations in this crate
#[derive(Debug)]
pub enum Error {
    /// Error when parsing metadata
    InvalidMetadata(String),

    /// Error when parsing JSON
    JsonParse(serde_json::Error),

    /// Error when creating a Variant
    VariantCreation(String),

    /// Error when reading a Variant
    VariantRead(String),

    /// Error when creating a VariantArray
    VariantArrayCreation(ArrowError),

    /// Error for empty input
    EmptyInput,
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            Error::InvalidMetadata(msg) => write!(f, "Invalid metadata: {}", msg),
            Error::JsonParse(err) => write!(f, "JSON parse error: {}", err),
            Error::VariantCreation(msg) => write!(f, "Failed to create Variant: {}", msg),
            Error::VariantRead(msg) => write!(f, "Failed to read Variant: {}", msg),
            Error::VariantArrayCreation(err) => write!(f, "Failed to create VariantArray: {}", err),
            Error::EmptyInput => write!(f, "Empty input"),
        }
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Error::JsonParse(err) => Some(err),
            Error::VariantArrayCreation(err) => Some(err),
            _ => None,
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::JsonParse(err)
    }
}

impl From<ArrowError> for Error {
    fn from(err: ArrowError) -> Self {
        Error::VariantArrayCreation(err)
    }
}

impl From<Error> for ArrowError {
    fn from(err: Error) -> Self {
        ArrowError::ExternalError(Box::new(err))
    }
} 