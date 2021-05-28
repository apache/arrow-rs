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

//! Utilities to assist with reading and writing Arrow data as Flight messages

use crate::{
    flight_descriptor::DescriptorType, FlightData, FlightDescriptor, FlightInfo,
    SchemaResult,
};

use arrow::array::ArrayRef;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::{ArrowError, Result};
use arrow::ipc::{
    convert, reader, size_prefixed_root_as_message, writer, writer::EncodedData,
    writer::IpcWriteOptions,
};
use arrow::record_batch::RecordBatch;
use std::{
    cmp::min,
    convert::{TryFrom, TryInto},
    fmt,
    ops::Deref,
};

/// Convert a `RecordBatch` to a vector of `FlightData` representing the bytes of the dictionaries
/// and a `FlightData` representing the bytes of the batch's values
pub fn flight_data_from_arrow_batch(
    batch: &RecordBatch,
    options: &IpcWriteOptions,
) -> (Vec<FlightData>, FlightData) {
    let data_gen = writer::IpcDataGenerator::default();
    let mut dictionary_tracker = writer::DictionaryTracker::new(false);

    let (encoded_dictionaries, encoded_batch) = data_gen
        .encoded_batch(batch, &mut dictionary_tracker, &options)
        .expect("DictionaryTracker configured above to not error on replacement");

    let flight_dictionaries = encoded_dictionaries.into_iter().map(Into::into).collect();
    let flight_batch = encoded_batch.into();

    (flight_dictionaries, flight_batch)
}

impl From<EncodedData> for FlightData {
    fn from(data: EncodedData) -> Self {
        FlightData {
            data_header: data.ipc_message,
            data_body: data.arrow_data,
            ..Default::default()
        }
    }
}

/// Convert a `Schema` to `SchemaResult` by converting to an IPC message
#[deprecated(
    since = "4.2.0",
    note = "Use From trait, e.g.: (schema, options).into()"
)]
pub fn flight_schema_from_arrow_schema(
    schema: &Schema,
    options: &IpcWriteOptions,
) -> SchemaResult {
    (schema, options).into()
}

/// Convert a `Schema` to `FlightData` by converting to an IPC message
#[deprecated(
    since = "4.2.0",
    note = "Use From trait, e.g.: (schema, options).into()"
)]
pub fn flight_data_from_arrow_schema(
    schema: &Schema,
    options: &IpcWriteOptions,
) -> FlightData {
    (schema, options).into()
}

/// Convert a `Schema` to bytes in the format expected in `FlightInfo.schema`
#[deprecated(
    since = "4.2.0",
    note = "Use From trait, e.g.: (schema, options).into()"
)]
pub fn ipc_message_from_arrow_schema(
    schema: &Schema,
    options: &IpcWriteOptions,
) -> Result<Vec<u8>> {
    let message = (schema, options).try_into()?;
    let IpcMessage(vals) = message;
    Ok(vals)
}

fn flight_schema_as_flatbuffer(schema: &Schema, options: &IpcWriteOptions) -> IpcMessage {
    let encoded_data = flight_schema_as_encoded_data(schema, options);
    IpcMessage(encoded_data.ipc_message)
}

fn flight_schema_as_encoded_data(
    arrow_schema: &Schema,
    options: &IpcWriteOptions,
) -> EncodedData {
    let data_gen = writer::IpcDataGenerator::default();
    data_gen.schema_to_bytes(arrow_schema, options)
}

/// Try convert `FlightData` into an Arrow Schema
///
/// Returns an error if the `FlightData` header is not a valid IPC schema
impl TryFrom<&FlightData> for Schema {
    type Error = ArrowError;
    fn try_from(data: &FlightData) -> Result<Self> {
        convert::schema_from_bytes(&data.data_header[..]).map_err(|err| {
            ArrowError::ParseError(format!(
                "Unable to convert flight data to Arrow schema: {}",
                err
            ))
        })
    }
}

/// Try convert `SchemaResult` into an Arrow Schema
///
/// Returns an error if the `FlightData` header is not a valid IPC schema
impl TryFrom<&SchemaResult> for Schema {
    type Error = ArrowError;
    fn try_from(data: &SchemaResult) -> Result<Self> {
        convert::schema_from_bytes(&data.schema[..]).map_err(|err| {
            ArrowError::ParseError(format!(
                "Unable to convert schema result to Arrow schema: {}",
                err
            ))
        })
    }
}

/// Convert a FlightData message to a RecordBatch
pub fn flight_data_to_arrow_batch(
    data: &FlightData,
    schema: SchemaRef,
    dictionaries_by_field: &[Option<ArrayRef>],
) -> Result<RecordBatch> {
    // check that the data_header is a record batch message
    let message = arrow::ipc::root_as_message(&data.data_header[..]).map_err(|err| {
        ArrowError::ParseError(format!("Unable to get root as message: {:?}", err))
    })?;

    message
        .header_as_record_batch()
        .ok_or_else(|| {
            ArrowError::ParseError(
                "Unable to convert flight data header to a record batch".to_string(),
            )
        })
        .map(|batch| {
            reader::read_record_batch(
                &data.data_body,
                batch,
                schema,
                &dictionaries_by_field,
            )
        })?
}

// TODO: add more explicit conversion that exposes flight descriptor and metadata options

// Implement a bunch of useful traits for various conversions, displays,
// etc...

// Display...

impl fmt::Display for FlightData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "descriptor: {:?}", self.flight_descriptor)?;
        let limit = min(8, self.data_header.len());
        write!(f, ", header: {:?}", &self.data_header[..limit])?;
        let limit = min(8, self.app_metadata.len());
        write!(f, ", metadata: {:?}", &self.app_metadata[..limit])?;
        let limit = min(8, self.data_body.len());
        write!(f, ", body: {:?}", &self.data_body[..limit])
    }
}

impl fmt::Display for FlightDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "type: ")?;
        match self.r#type() {
            DescriptorType::Cmd => {
                let limit = min(8, self.cmd.len());
                write!(f, "cmd, value: {:?}", &self.cmd[..limit])
            }
            DescriptorType::Path => {
                write!(f, "path, value: {:?}", self.path)
            }
            DescriptorType::Unknown => {
                write!(f, "unknown")
            }
        }
    }
}

// From...

impl From<(&Schema, &IpcWriteOptions)> for FlightData {
    fn from(pair: (&Schema, &IpcWriteOptions)) -> Self {
        let IpcMessage(vals) = flight_schema_as_flatbuffer(pair.0, pair.1);
        FlightData {
            data_header: vals,
            ..Default::default()
        }
    }
}

impl From<(&Schema, &IpcWriteOptions)> for SchemaResult {
    fn from(pair: (&Schema, &IpcWriteOptions)) -> Self {
        let IpcMessage(vals) = flight_schema_as_flatbuffer(pair.0, pair.1);
        SchemaResult { schema: vals }
    }
}

// TryFrom...

impl TryFrom<i32> for DescriptorType {
    type Error = ArrowError;

    fn try_from(value: i32) -> Result<Self> {
        match value {
            0 => Ok(DescriptorType::Unknown),
            1 => Ok(DescriptorType::Cmd),
            2 => Ok(DescriptorType::Path),
            _ => Err(ArrowError::InvalidArgumentError(format!(
                "invalid descriptor type: {}",
                value
            ))),
        }
    }
}

impl TryFrom<(&Schema, &IpcWriteOptions)> for IpcMessage {
    type Error = ArrowError;

    fn try_from(pair: (&Schema, &IpcWriteOptions)) -> Result<Self> {
        let encoded_data = flight_schema_as_encoded_data(pair.0, pair.1);

        let mut schema = vec![];
        arrow::ipc::writer::write_message(&mut schema, encoded_data, pair.1)?;
        Ok(IpcMessage(schema))
    }
}

impl TryFrom<FlightInfo> for Schema {
    type Error = ArrowError;

    fn try_from(value: FlightInfo) -> Result<Self> {
        let msg = IpcMessage(value.schema);
        msg.try_into()
    }
}

impl TryFrom<IpcMessage> for Schema {
    type Error = ArrowError;

    fn try_from(value: IpcMessage) -> Result<Self> {
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

// FlightDescriptor, etc..

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

/// IpcMessage wraps a Vec<u8>
struct IpcMessage(Vec<u8>);

impl Deref for IpcMessage {
    type Target = Vec<u8>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_creates_flight_descriptor_command() {
        let expected_cmd = "my_command".as_bytes();
        let fd = FlightDescriptor::new_cmd(expected_cmd.to_vec());
        assert_eq!(fd.r#type(), DescriptorType::Cmd);
        assert_eq!(fd.cmd, expected_cmd.to_vec());
        println!("{}", fd);
    }
}
