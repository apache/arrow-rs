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
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch};
use arrow_buffer::Buffer;
use arrow_ipc::convert::fb_to_schema;
use arrow_ipc::{reader, root_as_message, writer, writer::IpcWriteOptions};
use arrow_schema::{ArrowError, Schema, SchemaRef};

/// Convert a `RecordBatch` to a vector of `FlightData` representing the bytes of the dictionaries
/// and a `FlightData` representing the bytes of the batch's values
#[deprecated(
    since = "30.0.0",
    note = "Use IpcDataGenerator directly with DictionaryTracker to avoid re-sending dictionaries"
)]
pub fn flight_data_from_arrow_batch(
    batch: &RecordBatch,
    options: &IpcWriteOptions,
) -> (Vec<FlightData>, FlightData) {
    let data_gen = writer::IpcDataGenerator::default();
    let mut dictionary_tracker =
        writer::DictionaryTracker::new_with_preserve_dict_id(false, options.preserve_dict_id());

    let (encoded_dictionaries, encoded_batch) = data_gen
        .encoded_batch(batch, &mut dictionary_tracker, options)
        .expect("DictionaryTracker configured above to not error on replacement");

    let flight_dictionaries = encoded_dictionaries.into_iter().map(Into::into).collect();
    let flight_batch = encoded_batch.into();

    (flight_dictionaries, flight_batch)
}

/// Convert a slice of wire protocol `FlightData`s into a vector of `RecordBatch`es
pub fn flight_data_to_batches(flight_data: &[FlightData]) -> Result<Vec<RecordBatch>, ArrowError> {
    let schema = flight_data.first().ok_or_else(|| {
        ArrowError::CastError("Need at least one FlightData for schema".to_string())
    })?;
    let message = root_as_message(&schema.data_header[..])
        .map_err(|_| ArrowError::CastError("Cannot get root as message".to_string()))?;

    let ipc_schema: arrow_ipc::Schema = message
        .header_as_schema()
        .ok_or_else(|| ArrowError::CastError("Cannot get header as Schema".to_string()))?;
    let schema = fb_to_schema(ipc_schema);
    let schema = Arc::new(schema);

    let mut batches = vec![];
    let dictionaries_by_id = HashMap::new();
    for datum in flight_data[1..].iter() {
        let batch = flight_data_to_arrow_batch(datum, schema.clone(), &dictionaries_by_id)?;
        batches.push(batch);
    }
    Ok(batches)
}

/// Convert `FlightData` (with supplied schema and dictionaries) to an arrow `RecordBatch`.
pub fn flight_data_to_arrow_batch(
    data: &FlightData,
    schema: SchemaRef,
    dictionaries_by_id: &HashMap<i64, ArrayRef>,
) -> Result<RecordBatch, ArrowError> {
    // check that the data_header is a record batch message
    let message = arrow_ipc::root_as_message(&data.data_header[..])
        .map_err(|err| ArrowError::ParseError(format!("Unable to get root as message: {err:?}")))?;

    message
        .header_as_record_batch()
        .ok_or_else(|| {
            ArrowError::ParseError(
                "Unable to convert flight data header to a record batch".to_string(),
            )
        })
        .map(|batch| {
            reader::read_record_batch(
                &Buffer::from(data.data_body.as_ref()),
                batch,
                schema,
                dictionaries_by_id,
                None,
                &message.version(),
            )
        })?
}

/// Convert a `Schema` to `SchemaResult` by converting to an IPC message
#[deprecated(
    since = "4.4.0",
    note = "Use From trait, e.g.: SchemaAsIpc::new(schema, options).try_into()"
)]
pub fn flight_schema_from_arrow_schema(
    schema: &Schema,
    options: &IpcWriteOptions,
) -> Result<SchemaResult, ArrowError> {
    SchemaAsIpc::new(schema, options).try_into()
}

/// Convert a `Schema` to `FlightData` by converting to an IPC message
#[deprecated(
    since = "4.4.0",
    note = "Use From trait, e.g.: SchemaAsIpc::new(schema, options).into()"
)]
pub fn flight_data_from_arrow_schema(schema: &Schema, options: &IpcWriteOptions) -> FlightData {
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
) -> Result<Bytes, ArrowError> {
    let message = SchemaAsIpc::new(schema, options).try_into()?;
    let IpcMessage(vals) = message;
    Ok(vals)
}

/// Convert `RecordBatch`es to wire protocol `FlightData`s
pub fn batches_to_flight_data(
    schema: &Schema,
    batches: Vec<RecordBatch>,
) -> Result<Vec<FlightData>, ArrowError> {
    let options = IpcWriteOptions::default();
    let schema_flight_data: FlightData = SchemaAsIpc::new(schema, &options).into();
    let mut dictionaries = vec![];
    let mut flight_data = vec![];

    let data_gen = writer::IpcDataGenerator::default();
    let mut dictionary_tracker =
        writer::DictionaryTracker::new_with_preserve_dict_id(false, options.preserve_dict_id());

    for batch in batches.iter() {
        let (encoded_dictionaries, encoded_batch) =
            data_gen.encoded_batch(batch, &mut dictionary_tracker, &options)?;

        dictionaries.extend(encoded_dictionaries.into_iter().map(Into::into));
        flight_data.push(encoded_batch.into());
    }

    let mut stream = Vec::with_capacity(1 + dictionaries.len() + flight_data.len());

    stream.push(schema_flight_data);
    stream.extend(dictionaries);
    stream.extend(flight_data);
    let flight_data = stream;
    Ok(flight_data)
}
