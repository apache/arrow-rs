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
use thiserror::Error;

/// Error type for operations in this crate
#[derive(Debug, Error)]
pub enum Error {
    /// Error when parsing metadata
    #[error("Invalid metadata: {0}")]
    InvalidMetadata(String),

    /// Error when parsing JSON
    #[error("JSON parse error: {0}")]
    JsonParse(#[from] serde_json::Error),

    /// Error when creating a Variant
    #[error("Failed to create Variant: {0}")]
    VariantCreation(String),

    /// Error when reading a Variant
    #[error("Failed to read Variant: {0}")]
    VariantRead(String),

    /// Error when creating a VariantArray
    #[error("Failed to create VariantArray: {0}")]
    VariantArrayCreation(#[from] ArrowError),

    /// Error for empty input
    #[error("Empty input")]
    EmptyInput,
}

impl From<Error> for ArrowError {
    fn from(err: Error) -> Self {
        ArrowError::ExternalError(Box::new(err))
    }
} 