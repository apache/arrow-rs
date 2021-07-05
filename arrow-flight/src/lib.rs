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

use arrow::datatypes::Schema;
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::ipc::{
    convert, size_prefixed_root_as_message, writer, writer::EncodedData,
    writer::IpcWriteOptions,
};

use std::{
    convert::{TryFrom, TryInto},
    fmt,
    ops::Deref,
};

mod gen {
    include!("arrow.flight.protocol.rs");
}

pub mod flight_descriptor {
    use super::gen;
    pub use gen::flight_descriptor::DescriptorType;
}

pub mod flight_service_client {
    use super::gen;
    pub use gen::flight_service_client::FlightServiceClient;
}

pub mod flight_service_server {
    use super::gen;
    pub use gen::flight_service_server::FlightService;
    pub use gen::flight_service_server::FlightServiceServer;
}

pub use gen::Action;
pub use gen::ActionType;
pub use gen::BasicAuth;
pub use gen::Criteria;
pub use gen::Empty;
pub use gen::FlightData;
pub use gen::FlightDescriptor;
pub use gen::FlightEndpoint;
pub use gen::FlightInfo;
pub use gen::HandshakeRequest;
pub use gen::HandshakeResponse;
pub use gen::Location;
pub use gen::PutResult;
pub use gen::Result;
pub use gen::SchemaResult;
pub use gen::Ticket;

pub mod utils;

use flight_descriptor::DescriptorType;

/// SchemaAsIpc represents a pairing of a `Schema` with IpcWriteOptions
pub struct SchemaAsIpc<'a> {
    pub pair: (&'a Schema, &'a IpcWriteOptions),
}

/// IpcMessage represents a `Schema` in the format expected in
/// `FlightInfo.schema`
#[derive(Debug)]
pub struct IpcMessage(pub Vec<u8>);

// Useful conversion functions

fn flight_schema_as_encoded_data(
    arrow_schema: &Schema,
    options: &IpcWriteOptions,
) -> EncodedData {
    let data_gen = writer::IpcDataGenerator::default();
    data_gen.schema_to_bytes(arrow_schema, options)
}

fn flight_schema_as_flatbuffer(schema: &Schema, options: &IpcWriteOptions) -> IpcMessage {
    let encoded_data = flight_schema_as_encoded_data(schema, options);
    IpcMessage(encoded_data.ipc_message)
}

// Implement a bunch of useful traits for various conversions, displays,
// etc...

// Deref

impl Deref for IpcMessage {
    type Target = Vec<u8>;

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
            Some(d) => write!(f, "{}", d)?,
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
                    write!(f, "{}{}", sep, element)?;
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
            Some(value) => write!(f, "{}", value),
            None => write!(f, " none"),
        }?;
        write!(f, ", location: [")?;
        let mut sep = "";
        for location in &self.location {
            write!(f, "{}{}", sep, location)?;
            sep = ", ";
        }
        write!(f, "]")?;
        write!(f, " }}")
    }
}

impl fmt::Display for FlightInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ipc_message = IpcMessage(self.schema.clone());
        let schema: Schema = ipc_message.try_into().map_err(|_err| fmt::Error)?;
        write!(f, "FlightInfo {{")?;
        write!(f, " schema: {}", schema)?;
        write!(f, ", descriptor:")?;
        match &self.flight_descriptor {
            Some(d) => write!(f, " {}", d),
            None => write!(f, " None"),
        }?;
        write!(f, ", endpoint: [")?;
        let mut sep = "";
        for endpoint in &self.endpoint {
            write!(f, "{}{}", sep, endpoint)?;
            sep = ", ";
        }
        write!(f, "], total_records: {}", self.total_records)?;
        write!(f, ", total_bytes: {}", self.total_bytes)?;
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
        write!(f, "{}", base64::encode(&self.ticket))
    }
}

// From...

impl From<EncodedData> for FlightData {
    fn from(data: EncodedData) -> Self {
        FlightData {
            data_header: data.ipc_message,
            data_body: data.arrow_data,
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

impl From<SchemaAsIpc<'_>> for SchemaResult {
    fn from(schema_ipc: SchemaAsIpc) -> Self {
        let IpcMessage(vals) = flight_schema_as_flatbuffer(schema_ipc.0, schema_ipc.1);
        SchemaResult { schema: vals }
    }
}

// TryFrom...

impl TryFrom<i32> for DescriptorType {
    type Error = ArrowError;

    fn try_from(value: i32) -> ArrowResult<Self> {
        value.try_into()
    }
}

impl TryFrom<SchemaAsIpc<'_>> for IpcMessage {
    type Error = ArrowError;

    fn try_from(schema_ipc: SchemaAsIpc) -> ArrowResult<Self> {
        let pair = *schema_ipc;
        let encoded_data = flight_schema_as_encoded_data(pair.0, pair.1);

        let mut schema = vec![];
        arrow::ipc::writer::write_message(&mut schema, encoded_data, pair.1)?;
        Ok(IpcMessage(schema))
    }
}

impl TryFrom<&FlightData> for Schema {
    type Error = ArrowError;
    fn try_from(data: &FlightData) -> ArrowResult<Self> {
        convert::schema_from_bytes(&data.data_header[..]).map_err(|err| {
            ArrowError::ParseError(format!(
                "Unable to convert flight data to Arrow schema: {}",
                err
            ))
        })
    }
}

impl TryFrom<FlightInfo> for Schema {
    type Error = ArrowError;

    fn try_from(value: FlightInfo) -> ArrowResult<Self> {
        let msg = IpcMessage(value.schema);
        msg.try_into()
    }
}

impl TryFrom<IpcMessage> for Schema {
    type Error = ArrowError;

    fn try_from(value: IpcMessage) -> ArrowResult<Self> {
        // CONTINUATION TAKES 4 BYTES
        // SIZE TAKES 4 BYTES (so read msg as size prefixed)
        let msg = size_prefixed_root_as_message(&value.0[4..]).map_err(|err| {
            ArrowError::ParseError(format!(
                "Unable to convert flight info to a message: {}",
                err
            ))
        })?;
        let ipc_schema = msg.header_as_schema().ok_or_else(|| {
            ArrowError::ParseError(
                "Unable to convert flight info to a schema".to_string(),
            )
        })?;
        Ok(convert::fb_to_schema(ipc_schema))
    }
}

impl TryFrom<&SchemaResult> for Schema {
    type Error = ArrowError;
    fn try_from(data: &SchemaResult) -> ArrowResult<Self> {
        convert::schema_from_bytes(&data.schema[..]).map_err(|err| {
            ArrowError::ParseError(format!(
                "Unable to convert schema result to Arrow schema: {}",
                err
            ))
        })
    }
}

// FlightData, FlightDescriptor, etc..

impl FlightData {
    pub fn new(
        flight_descriptor: Option<FlightDescriptor>,
        message: IpcMessage,
        app_metadata: Vec<u8>,
        data_body: Vec<u8>,
    ) -> Self {
        let IpcMessage(vals) = message;
        FlightData {
            flight_descriptor,
            data_header: vals,
            app_metadata,
            data_body,
        }
    }
}

impl FlightDescriptor {
    pub fn new_cmd(cmd: Vec<u8>) -> Self {
        FlightDescriptor {
            r#type: DescriptorType::Cmd.into(),
            cmd,
            ..Default::default()
        }
    }

    pub fn new_path(path: Vec<String>) -> Self {
        FlightDescriptor {
            r#type: DescriptorType::Path.into(),
            path,
            ..Default::default()
        }
    }
}

impl FlightInfo {
    pub fn new(
        message: IpcMessage,
        flight_descriptor: Option<FlightDescriptor>,
        endpoint: Vec<FlightEndpoint>,
        total_records: i64,
        total_bytes: i64,
    ) -> Self {
        let IpcMessage(vals) = message;
        FlightInfo {
            schema: vals,
            flight_descriptor,
            endpoint,
            total_records,
            total_bytes,
        }
    }
}

impl<'a> SchemaAsIpc<'a> {
    pub fn new(schema: &'a Schema, options: &'a IpcWriteOptions) -> Self {
        SchemaAsIpc {
            pair: (schema, options),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        let actual = format!("{}", input);
        let expected = format!("{:?}", vec![91; 10]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn it_accepts_short_output() {
        let input = TestVector(vec![91; 6], 10);

        let actual = format!("{}", input);
        let expected = format!("{:?}", vec![91; 6]);
        assert_eq!(actual, expected);
    }

    #[test]
    fn it_accepts_long_output() {
        let input = TestVector(vec![91; 10], 9);

        let actual = format!("{}", input);
        let expected = format!("{:?}", vec![91; 9]);
        assert_eq!(actual, expected);
    }
}
