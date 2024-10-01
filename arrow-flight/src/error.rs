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

use std::error::Error;

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
    /// An error occurred during decoding
    DecodeError(String),
    /// External error that can provide source of error by calling `Error::source`.
    ExternalError(Box<dyn Error + Send + Sync>),
}

impl FlightError {
    /// Generate a new `FlightError::ProtocolError` variant.
    pub fn protocol(message: impl Into<String>) -> Self {
        Self::ProtocolError(message.into())
    }

    /// Wraps an external error in an `ArrowError`.
    pub fn from_external_error(error: Box<dyn Error + Send + Sync>) -> Self {
        Self::ExternalError(error)
    }
}

impl std::fmt::Display for FlightError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlightError::Arrow(source) => write!(f, "Arrow error: {}", source),
            FlightError::NotYetImplemented(desc) => write!(f, "Not yet implemented: {}", desc),
            FlightError::Tonic(source) => write!(f, "Tonic error: {}", source),
            FlightError::ProtocolError(desc) => write!(f, "Protocol error: {}", desc),
            FlightError::DecodeError(desc) => write!(f, "Decode error: {}", desc),
            FlightError::ExternalError(source) => write!(f, "External error: {}", source),
        }
    }
}

impl Error for FlightError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            FlightError::Arrow(source) => Some(source),
            FlightError::Tonic(source) => Some(source),
            FlightError::ExternalError(source) => Some(source.as_ref()),
            _ => None,
        }
    }
}

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

/// Result type for the Apache Arrow Flight crate
pub type Result<T> = std::result::Result<T, FlightError>;

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn error_source() {
        let e1 = FlightError::DecodeError("foo".into());
        assert!(e1.source().is_none());

        // one level of wrapping
        let e2 = FlightError::ExternalError(Box::new(e1));
        let source = e2.source().unwrap().downcast_ref::<FlightError>().unwrap();
        assert!(matches!(source, FlightError::DecodeError(_)));

        let e3 = FlightError::ExternalError(Box::new(e2));
        let source = e3
            .source()
            .unwrap()
            .downcast_ref::<FlightError>()
            .unwrap()
            .source()
            .unwrap()
            .downcast_ref::<FlightError>()
            .unwrap();

        assert!(matches!(source, FlightError::DecodeError(_)));
    }

    #[test]
    fn error_through_arrow() {
        // flight error that wraps an arrow error that wraps a flight error
        let e1 = FlightError::DecodeError("foo".into());
        let e2 = ArrowError::ExternalError(Box::new(e1));
        let e3 = FlightError::ExternalError(Box::new(e2));

        // ensure we can find the lowest level error by following source()
        let mut root_error: &dyn Error = &e3;
        while let Some(source) = root_error.source() {
            // walk the next level
            root_error = source;
        }

        let source = root_error.downcast_ref::<FlightError>().unwrap();
        assert!(matches!(source, FlightError::DecodeError(_)));
    }
}
