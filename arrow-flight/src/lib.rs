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

//! A native Rust implementation of [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html)
//! for exchanging [Arrow](https://arrow.apache.org) data between processes.
//!
//! Please see the [arrow-flight crates.io](https://crates.io/crates/arrow-flight)
//! page for feature flags and more information.
//!
//! # Overview
//!
//! This crate contains:
//!
//! 1. Low level [prost] generated structs
//!    for Flight gRPC protobuf messages, such as [`FlightData`], [`FlightInfo`],
//!    [`Location`] and [`Ticket`].
//!
//! 2. Low level [tonic] generated [`flight_service_client`] and
//!    [`flight_service_server`].
//!
//! 3. Experimental support for [Flight SQL] in [`sql`]. Requires the
//!    `flight-sql-experimental` feature of this crate to be activated.
//!
//! [Flight SQL]: https://arrow.apache.org/docs/format/FlightSql.html
#![allow(rustdoc::invalid_html_tags)]
#![warn(missing_docs)]

use arrow_ipc::{convert, writer, writer::EncodedData, writer::IpcWriteOptions};
use arrow_schema::{ArrowError, Schema};

use arrow_ipc::convert::try_schema_from_ipc_buffer;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use prost_types::Timestamp;
use std::{fmt, ops::Deref};

type ArrowResult<T> = std::result::Result<T, ArrowError>;

#[allow(clippy::all)]
mod gen {
    // Since this file is auto-generated, we suppress all warnings
    #![allow(missing_docs)]
    include!("arrow.flight.protocol.rs");
}

/// Defines a `Flight` for generation or retrieval.
pub mod flight_descriptor {
    use super::gen;
    pub use gen::flight_descriptor::DescriptorType;
}

/// Low Level [tonic] [`FlightServiceClient`](gen::flight_service_client::FlightServiceClient).
pub mod flight_service_client {
    use super::gen;
    pub use gen::flight_service_client::FlightServiceClient;
}

/// Low Level [tonic] [`FlightServiceServer`](gen::flight_service_server::FlightServiceServer)
/// and [`FlightService`](gen::flight_service_server::FlightService).
pub mod flight_service_server {
    use super::gen;
    pub use gen::flight_service_server::FlightService;
    pub use gen::flight_service_server::FlightServiceServer;
}

/// Mid Level [`FlightClient`]
pub mod client;
pub use client::FlightClient;

/// Decoder to create [`RecordBatch`](arrow_array::RecordBatch) streams from [`FlightData`] streams.
/// See [`FlightRecordBatchStream`](decode::FlightRecordBatchStream).
pub mod decode;

/// Encoder to create [`FlightData`] streams from [`RecordBatch`](arrow_array::RecordBatch) streams.
/// See [`FlightDataEncoderBuilder`](encode::FlightDataEncoderBuilder).
pub mod encode;

/// Common error types
pub mod error;

pub use gen::Action;
pub use gen::ActionType;
pub use gen::BasicAuth;
pub use gen::CancelFlightInfoRequest;
pub use gen::CancelFlightInfoResult;
pub use gen::CancelStatus;
pub use gen::Criteria;
pub use gen::Empty;
pub use gen::FlightData;
pub use gen::FlightDescriptor;
pub use gen::FlightEndpoint;
pub use gen::FlightInfo;
pub use gen::HandshakeRequest;
pub use gen::HandshakeResponse;
pub use gen::Location;
pub use gen::PollInfo;
pub use gen::PutResult;
pub use gen::RenewFlightEndpointRequest;
pub use gen::Result;
pub use gen::SchemaResult;
pub use gen::Ticket;

/// Helper to extract HTTP/gRPC trailers from a tonic stream.
mod trailers;

pub mod utils;

#[cfg(feature = "flight-sql-experimental")]
pub mod sql;
mod streams;

use flight_descriptor::DescriptorType;

/// SchemaAsIpc represents a pairing of a `Schema` with IpcWriteOptions
pub struct SchemaAsIpc<'a> {
    /// Data type representing a schema and its IPC write options
    pub pair: (&'a Schema, &'a IpcWriteOptions),
}

/// IpcMessage represents a `Schema` in the format expected in
/// `FlightInfo.schema`
#[derive(Debug)]
pub struct IpcMessage(pub Bytes);

// Useful conversion functions

fn flight_schema_as_encoded_data(arrow_schema: &Schema, options: &IpcWriteOptions) -> EncodedData {
    let data_gen = writer::IpcDataGenerator::default();
    let mut dict_tracker =
        writer::DictionaryTracker::new_with_preserve_dict_id(false, options.preserve_dict_id());
    data_gen.schema_to_bytes_with_dictionary_tracker(arrow_schema, &mut dict_tracker, options)
}

fn flight_schema_as_flatbuffer(schema: &Schema, options: &IpcWriteOptions) -> IpcMessage {
    let encoded_data = flight_schema_as_encoded_data(schema, options);
    IpcMessage(encoded_data.ipc_message.into())
}

// Implement a bunch of useful traits for various conversions, displays,
// etc...

// Deref

impl Deref for IpcMessage {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> Deref for SchemaAsIpc<'a> {
    type Target = (&'a Schema, &'a IpcWriteOptions);

    fn deref(&self) -> &Self::Target {
        &self.pair
    }
}

// Display...

/// Limits the output of value to limit...
fn limited_fmt(f: &mut fmt::Formatter<'_>, value: &[u8], limit: usize) -> fmt::Result {
    if value.len() > limit {
        write!(f, "{:?}", &value[..limit])
    } else {
        write!(f, "{:?}", &value)
    }
}

impl fmt::Display for FlightData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FlightData {{")?;
        write!(f, " descriptor: ")?;
        match &self.flight_descriptor {
            Some(d) => write!(f, "{d}")?,
            None => write!(f, "None")?,
        };
        write!(f, ", header: ")?;
        limited_fmt(f, &self.data_header, 8)?;
        write!(f, ", metadata: ")?;
        limited_fmt(f, &self.app_metadata, 8)?;
        write!(f, ", body: ")?;
        limited_fmt(f, &self.data_body, 8)?;
        write!(f, " }}")
    }
}

impl fmt::Display for FlightDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FlightDescriptor {{")?;
        write!(f, " type: ")?;
        match self.r#type() {
            DescriptorType::Cmd => {
                write!(f, "cmd, value: ")?;
                limited_fmt(f, &self.cmd, 8)?;
            }
            DescriptorType::Path => {
                write!(f, "path: [")?;
                let mut sep = "";
                for element in &self.path {
                    write!(f, "{sep}{element}")?;
                    sep = ", ";
                }
                write!(f, "]")?;
            }
            DescriptorType::Unknown => {
                write!(f, "unknown")?;
            }
        }
        write!(f, " }}")
    }
}

impl fmt::Display for FlightEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FlightEndpoint {{")?;
        write!(f, " ticket: ")?;
        match &self.ticket {
            Some(value) => write!(f, "{value}"),
            None => write!(f, " None"),
        }?;
        write!(f, ", location: [")?;
        let mut sep = "";
        for location in &self.location {
            write!(f, "{sep}{location}")?;
            sep = ", ";
        }
        write!(f, "]")?;
        write!(f, ", expiration_time:")?;
        match &self.expiration_time {
            Some(value) => write!(f, " {value}"),
            None => write!(f, " None"),
        }?;
        write!(f, ", app_metadata: ")?;
        limited_fmt(f, &self.app_metadata, 8)?;
        write!(f, " }}")
    }
}

impl fmt::Display for FlightInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ipc_message = IpcMessage(self.schema.clone());
        let schema: Schema = ipc_message.try_into().map_err(|_err| fmt::Error)?;
        write!(f, "FlightInfo {{")?;
        write!(f, " schema: {schema}")?;
        write!(f, ", descriptor:")?;
        match &self.flight_descriptor {
            Some(d) => write!(f, " {d}"),
            None => write!(f, " None"),
        }?;
        write!(f, ", endpoint: [")?;
        let mut sep = "";
        for endpoint in &self.endpoint {
            write!(f, "{sep}{endpoint}")?;
            sep = ", ";
        }
        write!(f, "], total_records: {}", self.total_records)?;
        write!(f, ", total_bytes: {}", self.total_bytes)?;
        write!(f, ", ordered: {}", self.ordered)?;
        write!(f, ", app_metadata: ")?;
        limited_fmt(f, &self.app_metadata, 8)?;
        write!(f, " }}")
    }
}

impl fmt::Display for PollInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PollInfo {{")?;
        write!(f, " info:")?;
        match &self.info {
            Some(value) => write!(f, " {value}"),
            None => write!(f, " None"),
        }?;
        write!(f, ", descriptor:")?;
        match &self.flight_descriptor {
            Some(d) => write!(f, " {d}"),
            None => write!(f, " None"),
        }?;
        write!(f, ", progress:")?;
        match &self.progress {
            Some(value) => write!(f, " {value}"),
            None => write!(f, " None"),
        }?;
        write!(f, ", expiration_time:")?;
        match &self.expiration_time {
            Some(value) => write!(f, " {value}"),
            None => write!(f, " None"),
        }?;
        write!(f, " }}")
    }
}

impl fmt::Display for CancelFlightInfoRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CancelFlightInfoRequest {{")?;
        write!(f, " info: ")?;
        match &self.info {
            Some(value) => write!(f, "{value}")?,
            None => write!(f, "None")?,
        };
        write!(f, " }}")
    }
}

impl fmt::Display for CancelFlightInfoResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CancelFlightInfoResult {{")?;
        write!(f, " status: {}", self.status().as_str_name())?;
        write!(f, " }}")
    }
}

impl fmt::Display for RenewFlightEndpointRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RenewFlightEndpointRequest {{")?;
        write!(f, " endpoint: ")?;
        match &self.endpoint {
            Some(value) => write!(f, "{value}")?,
            None => write!(f, "None")?,
        };
        write!(f, " }}")
    }
}

impl fmt::Display for Location {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Location {{")?;
        write!(f, " uri: ")?;
        write!(f, "{}", self.uri)
    }
}

impl fmt::Display for Ticket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Ticket {{")?;
        write!(f, " ticket: ")?;
        write!(f, "{}", BASE64_STANDARD.encode(&self.ticket))
    }
}

// From...

impl From<EncodedData> for FlightData {
    fn from(data: EncodedData) -> Self {
        FlightData {
            data_header: data.ipc_message.into(),
            data_body: data.arrow_data.into(),
            ..Default::default()
        }
    }
}

impl From<SchemaAsIpc<'_>> for FlightData {
    fn from(schema_ipc: SchemaAsIpc) -> Self {
        let IpcMessage(vals) = flight_schema_as_flatbuffer(schema_ipc.0, schema_ipc.1);
        FlightData {
            data_header: vals,
            ..Default::default()
        }
    }
}

impl TryFrom<SchemaAsIpc<'_>> for SchemaResult {
    type Error = ArrowError;

    fn try_from(schema_ipc: SchemaAsIpc) -> ArrowResult<Self> {
        // According to the definition from `Flight.proto`
        // The schema of the dataset in its IPC form:
        //   4 bytes - an optional IPC_CONTINUATION_TOKEN prefix
        //   4 bytes - the byte length of the payload
        //   a flatbuffer Message whose header is the Schema
        let IpcMessage(vals) = schema_to_ipc_format(schema_ipc)?;
        Ok(SchemaResult { schema: vals })
    }
}

impl TryFrom<SchemaAsIpc<'_>> for IpcMessage {
    type Error = ArrowError;

    fn try_from(schema_ipc: SchemaAsIpc) -> ArrowResult<Self> {
        schema_to_ipc_format(schema_ipc)
    }
}

fn schema_to_ipc_format(schema_ipc: SchemaAsIpc) -> ArrowResult<IpcMessage> {
    let pair = *schema_ipc;
    let encoded_data = flight_schema_as_encoded_data(pair.0, pair.1);

    let mut schema = vec![];
    writer::write_message(&mut schema, encoded_data, pair.1)?;
    Ok(IpcMessage(schema.into()))
}

impl TryFrom<&FlightData> for Schema {
    type Error = ArrowError;
    fn try_from(data: &FlightData) -> ArrowResult<Self> {
        convert::try_schema_from_flatbuffer_bytes(&data.data_header[..]).map_err(|err| {
            ArrowError::ParseError(format!(
                "Unable to convert flight data to Arrow schema: {err}"
            ))
        })
    }
}

impl TryFrom<FlightInfo> for Schema {
    type Error = ArrowError;

    fn try_from(value: FlightInfo) -> ArrowResult<Self> {
        value.try_decode_schema()
    }
}

impl TryFrom<IpcMessage> for Schema {
    type Error = ArrowError;

    fn try_from(value: IpcMessage) -> ArrowResult<Self> {
        try_schema_from_ipc_buffer(&value)
    }
}

impl TryFrom<&SchemaResult> for Schema {
    type Error = ArrowError;
    fn try_from(data: &SchemaResult) -> ArrowResult<Self> {
        try_schema_from_ipc_buffer(&data.schema)
    }
}

impl TryFrom<SchemaResult> for Schema {
    type Error = ArrowError;
    fn try_from(data: SchemaResult) -> ArrowResult<Self> {
        (&data).try_into()
    }
}

// FlightData, FlightDescriptor, etc..

impl FlightData {
    /// Create a new [`FlightData`].
    ///
    /// # See Also
    ///
    /// See [`FlightDataEncoderBuilder`] for a higher level API to
    /// convert a stream of [`RecordBatch`]es to [`FlightData`]s
    ///
    /// # Example:
    ///
    /// ```
    /// # use bytes::Bytes;
    /// # use arrow_flight::{FlightData, FlightDescriptor};
    /// # fn encode_data() -> Bytes { Bytes::new() } // dummy data
    /// // Get encoded Arrow IPC data:
    /// let data_body: Bytes = encode_data();
    /// // Create the FlightData message
    /// let flight_data = FlightData::new()
    ///   .with_descriptor(FlightDescriptor::new_cmd("the command"))
    ///   .with_app_metadata("My apps metadata")
    ///   .with_data_body(data_body);
    /// ```
    ///
    /// [`FlightDataEncoderBuilder`]: crate::encode::FlightDataEncoderBuilder
    /// [`RecordBatch`]: arrow_array::RecordBatch
    pub fn new() -> Self {
        Default::default()
    }

    /// Add a [`FlightDescriptor`] describing the data
    pub fn with_descriptor(mut self, flight_descriptor: FlightDescriptor) -> Self {
        self.flight_descriptor = Some(flight_descriptor);
        self
    }

    /// Add a data header
    pub fn with_data_header(mut self, data_header: impl Into<Bytes>) -> Self {
        self.data_header = data_header.into();
        self
    }

    /// Add a data body. See [`IpcDataGenerator`] to create this data.
    ///
    /// [`IpcDataGenerator`]: arrow_ipc::writer::IpcDataGenerator
    pub fn with_data_body(mut self, data_body: impl Into<Bytes>) -> Self {
        self.data_body = data_body.into();
        self
    }

    /// Add optional application specific metadata to the message
    pub fn with_app_metadata(mut self, app_metadata: impl Into<Bytes>) -> Self {
        self.app_metadata = app_metadata.into();
        self
    }
}

impl FlightDescriptor {
    /// Create a new opaque command [`CMD`] `FlightDescriptor` to generate a dataset.
    ///
    /// [`CMD`]: https://github.com/apache/arrow/blob/6bd31f37ae66bd35594b077cb2f830be57e08acd/format/Flight.proto#L224-L227
    pub fn new_cmd(cmd: impl Into<Bytes>) -> Self {
        FlightDescriptor {
            r#type: DescriptorType::Cmd.into(),
            cmd: cmd.into(),
            ..Default::default()
        }
    }

    /// Create a new named path [`PATH`] `FlightDescriptor` that identifies a dataset
    ///
    /// [`PATH`]: https://github.com/apache/arrow/blob/6bd31f37ae66bd35594b077cb2f830be57e08acd/format/Flight.proto#L217-L222
    pub fn new_path(path: Vec<String>) -> Self {
        FlightDescriptor {
            r#type: DescriptorType::Path.into(),
            path,
            ..Default::default()
        }
    }
}

impl FlightInfo {
    /// Create a new, empty `FlightInfo`, describing where to fetch flight data
    ///
    ///
    /// # Example:
    /// ```
    /// # use arrow_flight::{FlightInfo, Ticket, FlightDescriptor, FlightEndpoint};
    /// # use arrow_schema::{Schema, Field, DataType};
    /// # fn get_schema() -> Schema {
    /// #   Schema::new(vec![
    /// #     Field::new("a", DataType::Utf8, false),
    /// #   ])
    /// # }
    /// #
    /// // Create a new FlightInfo
    /// let flight_info = FlightInfo::new()
    ///   // Encode the Arrow schema
    ///   .try_with_schema(&get_schema())
    ///   .expect("encoding failed")
    ///   .with_endpoint(
    ///      FlightEndpoint::new()
    ///        .with_ticket(Ticket::new("ticket contents")
    ///      )
    ///    )
    ///   .with_descriptor(FlightDescriptor::new_cmd("RUN QUERY"));
    /// ```
    pub fn new() -> FlightInfo {
        FlightInfo {
            schema: Bytes::new(),
            flight_descriptor: None,
            endpoint: vec![],
            ordered: false,
            // Flight says "Set these to -1 if unknown."
            //
            // https://github.com/apache/arrow-rs/blob/17ca4d51d0490f9c65f5adde144f677dbc8300e7/format/Flight.proto#L287-L289
            total_records: -1,
            total_bytes: -1,
            app_metadata: Bytes::new(),
        }
    }

    /// Try and convert the data in this  `FlightInfo` into a [`Schema`]
    pub fn try_decode_schema(self) -> ArrowResult<Schema> {
        let msg = IpcMessage(self.schema);
        msg.try_into()
    }

    /// Specify the schema for the response.
    ///
    /// Note this takes the arrow [`Schema`] (not the IPC schema) and
    /// encodes it using the default IPC options.
    ///
    /// Returns an error if `schema` can not be encoded into IPC form.
    pub fn try_with_schema(mut self, schema: &Schema) -> ArrowResult<Self> {
        let options = IpcWriteOptions::default();
        let IpcMessage(schema) = SchemaAsIpc::new(schema, &options).try_into()?;
        self.schema = schema;
        Ok(self)
    }

    /// Add specific a endpoint for fetching the data
    pub fn with_endpoint(mut self, endpoint: FlightEndpoint) -> Self {
        self.endpoint.push(endpoint);
        self
    }

    /// Add a [`FlightDescriptor`] describing what this data is
    pub fn with_descriptor(mut self, flight_descriptor: FlightDescriptor) -> Self {
        self.flight_descriptor = Some(flight_descriptor);
        self
    }

    /// Set the number of records in the result, if known
    pub fn with_total_records(mut self, total_records: i64) -> Self {
        self.total_records = total_records;
        self
    }

    /// Set the number of bytes in the result, if known
    pub fn with_total_bytes(mut self, total_bytes: i64) -> Self {
        self.total_bytes = total_bytes;
        self
    }

    /// Specify if the response is [ordered] across endpoints
    ///
    /// [ordered]: https://github.com/apache/arrow-rs/blob/17ca4d51d0490f9c65f5adde144f677dbc8300e7/format/Flight.proto#L269-L275
    pub fn with_ordered(mut self, ordered: bool) -> Self {
        self.ordered = ordered;
        self
    }

    /// Add optional application specific metadata to the message
    pub fn with_app_metadata(mut self, app_metadata: impl Into<Bytes>) -> Self {
        self.app_metadata = app_metadata.into();
        self
    }
}

impl PollInfo {
    /// Create a new, empty [`PollInfo`], providing information for a long-running query
    ///
    /// # Example:
    /// ```
    /// # use arrow_flight::{FlightInfo, PollInfo, FlightDescriptor};
    /// # use prost_types::Timestamp;
    /// // Create a new PollInfo
    /// let poll_info = PollInfo::new()
    ///   .with_info(FlightInfo::new())
    ///   .with_descriptor(FlightDescriptor::new_cmd("RUN QUERY"))
    ///   .try_with_progress(0.5)
    ///   .expect("progress should've been valid")
    ///   .with_expiration_time(
    ///     "1970-01-01".parse().expect("invalid timestamp")
    ///   );
    /// ```
    pub fn new() -> Self {
        Self {
            info: None,
            flight_descriptor: None,
            progress: None,
            expiration_time: None,
        }
    }

    /// Add the current available results for the poll call as a [`FlightInfo`]
    pub fn with_info(mut self, info: FlightInfo) -> Self {
        self.info = Some(info);
        self
    }

    /// Add a [`FlightDescriptor`] that the client should use for the next poll call,
    /// if the query is not yet complete
    pub fn with_descriptor(mut self, flight_descriptor: FlightDescriptor) -> Self {
        self.flight_descriptor = Some(flight_descriptor);
        self
    }

    /// Set the query progress if known. Must be in the range [0.0, 1.0] else this will
    /// return an error
    pub fn try_with_progress(mut self, progress: f64) -> ArrowResult<Self> {
        if !(0.0..=1.0).contains(&progress) {
            return Err(ArrowError::InvalidArgumentError(format!(
                "PollInfo progress must be in the range [0.0, 1.0], got {progress}"
            )));
        }
        self.progress = Some(progress);
        Ok(self)
    }

    /// Specify expiration time for this request
    pub fn with_expiration_time(mut self, expiration_time: Timestamp) -> Self {
        self.expiration_time = Some(expiration_time);
        self
    }
}

impl<'a> SchemaAsIpc<'a> {
    /// Create a new `SchemaAsIpc` from a `Schema` and `IpcWriteOptions`
    pub fn new(schema: &'a Schema, options: &'a IpcWriteOptions) -> Self {
        SchemaAsIpc {
            pair: (schema, options),
        }
    }
}

impl CancelFlightInfoRequest {
    /// Create a new [`CancelFlightInfoRequest`], providing the [`FlightInfo`]
    /// of the query to cancel.
    pub fn new(info: FlightInfo) -> Self {
        Self { info: Some(info) }
    }
}

impl CancelFlightInfoResult {
    /// Create a new [`CancelFlightInfoResult`] from the provided [`CancelStatus`].
    pub fn new(status: CancelStatus) -> Self {
        Self {
            status: status as i32,
        }
    }
}

impl RenewFlightEndpointRequest {
    /// Create a new [`RenewFlightEndpointRequest`], providing the [`FlightEndpoint`]
    /// for which is being requested an extension of its expiration.
    pub fn new(endpoint: FlightEndpoint) -> Self {
        Self {
            endpoint: Some(endpoint),
        }
    }
}

impl Action {
    /// Create a new Action with type and body
    pub fn new(action_type: impl Into<String>, body: impl Into<Bytes>) -> Self {
        Self {
            r#type: action_type.into(),
            body: body.into(),
        }
    }
}

impl Result {
    /// Create a new Result with the specified body
    pub fn new(body: impl Into<Bytes>) -> Self {
        Self { body: body.into() }
    }
}

impl Ticket {
    /// Create a new `Ticket`
    ///
    /// # Example
    ///
    /// ```
    /// # use arrow_flight::Ticket;
    /// let ticket = Ticket::new("SELECT * from FOO");
    /// ```
    pub fn new(ticket: impl Into<Bytes>) -> Self {
        Self {
            ticket: ticket.into(),
        }
    }
}

impl FlightEndpoint {
    /// Create a new, empty `FlightEndpoint` that represents a location
    /// to retrieve Flight results.
    ///
    /// # Example
    /// ```
    /// # use arrow_flight::{FlightEndpoint, Ticket};
    /// #
    /// // Specify the client should fetch results from this server
    /// let endpoint = FlightEndpoint::new()
    ///   .with_ticket(Ticket::new("the ticket"));
    ///
    /// // Specify the client should fetch results from either
    /// // `http://example.com` or `https://example.com`
    /// let endpoint = FlightEndpoint::new()
    ///   .with_ticket(Ticket::new("the ticket"))
    ///   .with_location("http://example.com")
    ///   .with_location("https://example.com");
    /// ```
    pub fn new() -> FlightEndpoint {
        Default::default()
    }

    /// Set the [`Ticket`] used to retrieve data from the endpoint
    pub fn with_ticket(mut self, ticket: Ticket) -> Self {
        self.ticket = Some(ticket);
        self
    }

    /// Add a location `uri` to this endpoint. Note each endpoint can
    /// have multiple locations.
    ///
    /// If no `uri` is specified, the [Flight Spec] says:
    ///
    /// ```text
    /// * If the list is empty, the expectation is that the ticket can only
    /// * be redeemed on the current service where the ticket was
    /// * generated.
    /// ```
    /// [Flight Spec]: https://github.com/apache/arrow-rs/blob/17ca4d51d0490f9c65f5adde144f677dbc8300e7/format/Flight.proto#L307C2-L312
    pub fn with_location(mut self, uri: impl Into<String>) -> Self {
        self.location.push(Location { uri: uri.into() });
        self
    }

    /// Specify expiration time for this stream
    pub fn with_expiration_time(mut self, expiration_time: Timestamp) -> Self {
        self.expiration_time = Some(expiration_time);
        self
    }

    /// Add optional application specific metadata to the message
    pub fn with_app_metadata(mut self, app_metadata: impl Into<Bytes>) -> Self {
        self.app_metadata = app_metadata.into();
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_ipc::MetadataVersion;
    use arrow_schema::{DataType, Field, TimeUnit};

    struct TestVector(Vec<u8>, usize);

    impl fmt::Display for TestVector {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            limited_fmt(f, &self.0, self.1)
        }
    }

    #[test]
    fn it_creates_flight_descriptor_command() {
        let expected_cmd = "my_command".as_bytes();
        let fd = FlightDescriptor::new_cmd(expected_cmd.to_vec());
        assert_eq!(fd.r#type(), DescriptorType::Cmd);
        assert_eq!(fd.cmd, expected_cmd.to_vec());
    }

    #[test]
    fn it_accepts_equal_output() {
        let input = TestVector(vec![91; 10], 10);

        let actual = format!("{input}");
        let expected = format!("{:?}", vec![91; 10]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn it_accepts_short_output() {
        let input = TestVector(vec![91; 6], 10);

        let actual = format!("{input}");
        let expected = format!("{:?}", vec![91; 6]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn it_accepts_long_output() {
        let input = TestVector(vec![91; 10], 9);

        let actual = format!("{input}");
        let expected = format!("{:?}", vec![91; 9]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn ser_deser_schema_result() {
        let schema = Schema::new(vec![
            Field::new("c1", DataType::Utf8, false),
            Field::new("c2", DataType::Float64, true),
            Field::new("c3", DataType::UInt32, false),
            Field::new("c4", DataType::Boolean, true),
            Field::new("c5", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("c6", DataType::Time32(TimeUnit::Second), false),
        ]);
        // V5 with write_legacy_ipc_format = false
        // this will write the continuation marker
        let option = IpcWriteOptions::default();
        let schema_ipc = SchemaAsIpc::new(&schema, &option);
        let result: SchemaResult = schema_ipc.try_into().unwrap();
        let des_schema: Schema = (&result).try_into().unwrap();
        assert_eq!(schema, des_schema);

        // V4 with write_legacy_ipc_format = true
        // this will not write the continuation marker
        let option = IpcWriteOptions::try_new(8, true, MetadataVersion::V4).unwrap();
        let schema_ipc = SchemaAsIpc::new(&schema, &option);
        let result: SchemaResult = schema_ipc.try_into().unwrap();
        let des_schema: Schema = (&result).try_into().unwrap();
        assert_eq!(schema, des_schema);
    }
}
