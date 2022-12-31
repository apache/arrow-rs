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

use arrow_schema::ArrowError;

/// Errors for the Apache Arrow Flight crate
#[derive(Debug)]
pub enum FlightError {
    /// Underlying arrow error
    Arrow(ArrowError),
    /// Returned when functionality is not yet available.
    NotYetImplemented(String),
    /// Error from the underlying tonic library
    Tonic(tonic::Status),
    /// Some unexpected message was received
    ProtocolError(String),
    /// An error occured during decoding
    DecodeError(String),
    /// Some other (opaque) error
    ExternalError(Box<dyn std::error::Error + Send + Sync>),
}

impl FlightError {
    pub fn protocol(message: impl Into<String>) -> Self {
        Self::ProtocolError(message.into())
    }

    /// Wraps an external error in an `ArrowError`.
    pub fn from_external_error(error: Box<dyn std::error::Error + Send + Sync>) -> Self {
        Self::ExternalError(error)
    }
}

impl std::fmt::Display for FlightError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // TODO better format / error
        write!(f, "{:?}", self)
    }
}

impl std::error::Error for FlightError {}

impl From<tonic::Status> for FlightError {
    fn from(status: tonic::Status) -> Self {
        Self::Tonic(status)
    }
}

impl From<ArrowError> for FlightError {
    fn from(value: ArrowError) -> Self {
        Self::Arrow(value)
    }
}

// default conversion from FlightError to tonic treats everything
// other than `Status` as an internal error
impl From<FlightError> for tonic::Status {
    fn from(value: FlightError) -> Self {
        match value {
            FlightError::Arrow(e) => tonic::Status::internal(e.to_string()),
            FlightError::NotYetImplemented(e) => tonic::Status::internal(e),
            FlightError::Tonic(status) => status,
            FlightError::ProtocolError(e) => tonic::Status::internal(e),
            FlightError::DecodeError(e) => tonic::Status::internal(e),
            FlightError::ExternalError(e) => tonic::Status::internal(e.to_string()),
        }
    }
}

pub type Result<T> = std::result::Result<T, FlightError>;
