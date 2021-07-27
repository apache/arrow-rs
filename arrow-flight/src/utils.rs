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

use crate::{FlightData, IpcMessage, SchemaAsIpc, SchemaResult};

use arrow::array::ArrayRef;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::error::{ArrowError, Result};
use arrow::ipc::{reader, writer, writer::IpcWriteOptions};
use arrow::record_batch::RecordBatch;
use std::convert::TryInto;

/// Convert a `RecordBatch` to a vector of `FlightData` representing the bytes of the dictionaries
/// and a `FlightData` representing the bytes of the batch's values
pub fn flight_data_from_arrow_batch(
    batch: &RecordBatch,
    options: &IpcWriteOptions,
) -> (Vec<FlightData>, FlightData) {
    let data_gen = writer::IpcDataGenerator::default();
    let mut dictionary_tracker = writer::DictionaryTracker::new(false);

    let (encoded_dictionaries, encoded_batch) = data_gen
        .encoded_batch(batch, &mut dictionary_tracker, options)
        .expect("DictionaryTracker configured above to not error on replacement");

    let flight_dictionaries = encoded_dictionaries.into_iter().map(Into::into).collect();
    let flight_batch = encoded_batch.into();

    (flight_dictionaries, flight_batch)
}

/// Convert `FlightData` (with supplied schema and dictionaries) to an arrow `RecordBatch`.
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
                dictionaries_by_field,
            )
        })?
}

/// Convert a `Schema` to `SchemaResult` by converting to an IPC message
#[deprecated(
    since = "4.4.0",
    note = "Use From trait, e.g.: SchemaAsIpc::new(schema, options).into()"
)]
pub fn flight_schema_from_arrow_schema(
    schema: &Schema,
    options: &IpcWriteOptions,
) -> SchemaResult {
    SchemaAsIpc::new(schema, options).into()
}

/// Convert a `Schema` to `FlightData` by converting to an IPC message
#[deprecated(
    since = "4.4.0",
    note = "Use From trait, e.g.: SchemaAsIpc::new(schema, options).into()"
)]
pub fn flight_data_from_arrow_schema(
    schema: &Schema,
    options: &IpcWriteOptions,
) -> FlightData {
    SchemaAsIpc::new(schema, options).into()
}

/// Convert a `Schema` to bytes in the format expected in `FlightInfo.schema`
#[deprecated(
    since = "4.4.0",
    note = "Use TryFrom trait, e.g.: SchemaAsIpc::new(schema, options).try_into()"
)]
pub fn ipc_message_from_arrow_schema(
    schema: &Schema,
    options: &IpcWriteOptions,
) -> Result<Vec<u8>> {
    let message = SchemaAsIpc::new(schema, options).try_into()?;
    let IpcMessage(vals) = message;
    Ok(vals)
}
