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

use std::{collections::VecDeque, fmt::Debug, pin::Pin, sync::Arc, task::Poll};

use crate::{error::Result, FlightData, SchemaAsIpc};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use futures::{ready, stream::BoxStream, Stream, StreamExt};

/// Creates a [`Stream`](futures::Stream) of [`FlightData`]s from a
/// `Stream` of [`Result`]<[`RecordBatch`], [`FlightError`]>.
///
/// This can be used to implement [`FlightService::do_get`] in an
/// Arrow Flight implementation;
///
/// # Caveats
///   1. [`DictionaryArray`](arrow_array::array::DictionaryArray)s
///   are converted to their underlying types prior to transport, due to
///   <https://github.com/apache/arrow-rs/issues/3389>.
///
/// # Example
/// ```no_run
/// # use std::sync::Arc;
/// # use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
/// # async fn f() {
/// # let c1 = UInt32Array::from(vec![1, 2, 3, 4, 5, 6]);
/// # let record_batch = RecordBatch::try_from_iter(vec![
/// #      ("a", Arc::new(c1) as ArrayRef)
/// #   ])
/// #   .expect("cannot create record batch");
/// use arrow_flight::encode::FlightDataEncoderBuilder;
///
/// // Get an input stream of Result<RecordBatch, FlightError>
/// let input_stream = futures::stream::iter(vec![Ok(record_batch)]);
///
/// // Build a stream of `Result<FlightData>` (e.g. to return for do_get)
/// let flight_data_stream = FlightDataEncoderBuilder::new()
///  .build(input_stream);
///
/// // Create a tonic `Response` that can be returned from a Flight server
/// let response = tonic::Response::new(flight_data_stream);
/// # }
/// ```
///
/// [`FlightService::do_get`]: crate::flight_service_server::FlightService::do_get
/// [`FlightError`]: crate::error::FlightError
#[derive(Debug)]
pub struct FlightDataEncoderBuilder {
    /// The maximum approximate target message size in bytes
    /// (see details on [`Self::with_max_flight_data_size`]).
    max_flight_data_size: usize,
    /// Ipc writer options
    options: IpcWriteOptions,
    /// Metadata to add to the schema message
    app_metadata: Bytes,
}

/// Default target size for encoded [`FlightData`].
///
/// Note this value would normally be 4MB, but the size calculation is
/// somewhat inexact, so we set it to 2MB.
pub const GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES: usize = 2097152;

impl Default for FlightDataEncoderBuilder {
    fn default() -> Self {
        Self {
            max_flight_data_size: GRPC_TARGET_MAX_FLIGHT_SIZE_BYTES,
            options: IpcWriteOptions::default(),
            app_metadata: Bytes::new(),
        }
    }
}

impl FlightDataEncoderBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the (approximate) maximum size, in bytes, of the
    /// [`FlightData`] produced by this encoder. Defaults to 2MB.
    ///
    /// Since there is often a maximum message size for gRPC messages
    /// (typically around 4MB), this encoder splits up [`RecordBatch`]s
    /// (preserving order) into multiple [`FlightData`] objects to
    /// limit the size individual messages sent via gRPC.
    ///
    /// The size is approximate because of the additional encoding
    /// overhead on top of the underlying data buffers themselves.
    pub fn with_max_flight_data_size(mut self, max_flight_data_size: usize) -> Self {
        self.max_flight_data_size = max_flight_data_size;
        self
    }

    /// Specify application specific metadata included in the
    /// [`FlightData::app_metadata`] field of the the first Schema
    /// message
    pub fn with_metadata(mut self, app_metadata: Bytes) -> Self {
        self.app_metadata = app_metadata;
        self
    }

    /// Set the [`IpcWriteOptions`] used to encode the [`RecordBatch`]es for transport.
    pub fn with_options(mut self, options: IpcWriteOptions) -> Self {
        self.options = options;
        self
    }

    /// Return a [`Stream`](futures::Stream) of [`FlightData`],
    /// consuming self. More details on [`FlightDataEncoder`]
    pub fn build<S>(self, input: S) -> FlightDataEncoder
    where
        S: Stream<Item = Result<RecordBatch>> + Send + 'static,
    {
        let Self {
            max_flight_data_size,
            options,
            app_metadata,
        } = self;

        FlightDataEncoder::new(input.boxed(), max_flight_data_size, options, app_metadata)
    }
}

/// Stream that encodes a stream of record batches to flight data.
///
/// See [`FlightDataEncoderBuilder`] for details and example.
pub struct FlightDataEncoder {
    /// Input stream
    inner: BoxStream<'static, Result<RecordBatch>>,
    /// schema, set after the first batch
    schema: Option<SchemaRef>,
    /// Target maximum size of flight data
    /// (see details on [`FlightDataEncoderBuilder::with_max_flight_data_size`]).
    max_flight_data_size: usize,
    /// do the encoding / tracking of dictionaries
    encoder: FlightIpcEncoder,
    /// optional metadata to add to schema FlightData
    app_metadata: Option<Bytes>,
    /// data queued up to send but not yet sent
    queue: VecDeque<FlightData>,
    /// Is this stream done (inner is empty or errored)
    done: bool,
}

impl FlightDataEncoder {
    fn new(
        inner: BoxStream<'static, Result<RecordBatch>>,
        max_flight_data_size: usize,
        options: IpcWriteOptions,
        app_metadata: Bytes,
    ) -> Self {
        Self {
            inner,
            schema: None,
            max_flight_data_size,
            encoder: FlightIpcEncoder::new(options),
            app_metadata: Some(app_metadata),
            queue: VecDeque::new(),
            done: false,
        }
    }

    /// Place the `FlightData` in the queue to send
    fn queue_message(&mut self, data: FlightData) {
        self.queue.push_back(data);
    }

    /// Place the `FlightData` in the queue to send
    fn queue_messages(&mut self, datas: impl IntoIterator<Item = FlightData>) {
        for data in datas {
            self.queue_message(data)
        }
    }

    /// Encodes batch into one or more `FlightData` messages in self.queue
    fn encode_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let schema = match &self.schema {
            Some(schema) => schema.clone(),
            None => {
                let batch_schema = batch.schema();
                // The first message is the schema message, and all
                // batches have the same schema
                let schema = Arc::new(prepare_schema_for_flight(&batch_schema));
                let mut schema_flight_data = self.encoder.encode_schema(&schema);

                // attach any metadata requested
                if let Some(app_metadata) = self.app_metadata.take() {
                    schema_flight_data.app_metadata = app_metadata;
                }
                self.queue_message(schema_flight_data);
                // remember schema
                self.schema = Some(schema.clone());
                schema
            }
        };

        // encode the batch
        let batch = prepare_batch_for_flight(&batch, schema)?;

        for batch in split_batch_for_grpc_response(batch, self.max_flight_data_size) {
            let (flight_dictionaries, flight_batch) =
                self.encoder.encode_batch(&batch)?;

            self.queue_messages(flight_dictionaries);
            self.queue_message(flight_batch);
        }

        Ok(())
    }
}

impl Stream for FlightDataEncoder {
    type Item = Result<FlightData>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            if self.done && self.queue.is_empty() {
                return Poll::Ready(None);
            }

            // Any messages queued to send?
            if let Some(data) = self.queue.pop_front() {
                return Poll::Ready(Some(Ok(data)));
            }

            // Get next batch
            let batch = ready!(self.inner.poll_next_unpin(cx));

            match batch {
                None => {
                    // inner is done
                    self.done = true;
                    // queue must also be empty so we are done
                    assert!(self.queue.is_empty());
                    return Poll::Ready(None);
                }
                Some(Err(e)) => {
                    // error from inner
                    self.done = true;
                    self.queue.clear();
                    return Poll::Ready(Some(Err(e)));
                }
                Some(Ok(batch)) => {
                    // had data, encode into the queue
                    if let Err(e) = self.encode_batch(batch) {
                        self.done = true;
                        self.queue.clear();
                        return Poll::Ready(Some(Err(e)));
                    }
                }
            }
        }
    }
}

/// Prepare an arrow Schema for transport over the Arrow Flight protocol
///
/// Convert dictionary types to underlying types
///
/// See hydrate_dictionary for more information
fn prepare_schema_for_flight(schema: &Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Dictionary(_, value_type) => Field::new(
                field.name(),
                value_type.as_ref().clone(),
                field.is_nullable(),
            )
            .with_metadata(field.metadata().clone()),
            _ => field.clone(),
        })
        .collect();

    Schema::new(fields)
}

/// Split [`RecordBatch`] so it hopefully fits into a gRPC response.
///
/// Data is zero-copy sliced into batches.
///
/// Note: this method does not take into account already sliced
/// arrays: <https://github.com/apache/arrow-rs/issues/3407>
fn split_batch_for_grpc_response(
    batch: RecordBatch,
    max_flight_data_size: usize,
) -> Vec<RecordBatch> {
    let size = batch
        .columns()
        .iter()
        .map(|col| col.get_buffer_memory_size())
        .sum::<usize>();

    let n_batches = (size / max_flight_data_size
        + usize::from(size % max_flight_data_size != 0))
    .max(1);
    let rows_per_batch = (batch.num_rows() / n_batches).max(1);
    let mut out = Vec::with_capacity(n_batches + 1);

    let mut offset = 0;
    while offset < batch.num_rows() {
        let length = (rows_per_batch).min(batch.num_rows() - offset);
        out.push(batch.slice(offset, length));

        offset += length;
    }

    out
}

/// The data needed to encode a stream of flight data, holding on to
/// shared Dictionaries.
///
/// TODO: at allow dictionaries to be flushed / avoid building them
///
/// TODO limit on the number of dictionaries???
struct FlightIpcEncoder {
    options: IpcWriteOptions,
    data_gen: IpcDataGenerator,
    dictionary_tracker: DictionaryTracker,
}

impl FlightIpcEncoder {
    fn new(options: IpcWriteOptions) -> Self {
        let error_on_replacement = true;
        Self {
            options,
            data_gen: IpcDataGenerator::default(),
            dictionary_tracker: DictionaryTracker::new(error_on_replacement),
        }
    }

    /// Encode a schema as a FlightData
    fn encode_schema(&self, schema: &Schema) -> FlightData {
        SchemaAsIpc::new(schema, &self.options).into()
    }

    /// Convert a `RecordBatch` to a Vec of `FlightData` representing
    /// dictionaries and a `FlightData` representing the batch
    fn encode_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<(Vec<FlightData>, FlightData)> {
        let (encoded_dictionaries, encoded_batch) = self.data_gen.encoded_batch(
            batch,
            &mut self.dictionary_tracker,
            &self.options,
        )?;

        let flight_dictionaries =
            encoded_dictionaries.into_iter().map(Into::into).collect();
        let flight_batch = encoded_batch.into();

        Ok((flight_dictionaries, flight_batch))
    }
}

/// Prepares a RecordBatch for transport over the Arrow Flight protocol
///
/// This means:
///
/// 1. Hydrates any dictionaries to its underlying type. See
/// hydrate_dictionary for more information.
///
fn prepare_batch_for_flight(
    batch: &RecordBatch,
    schema: SchemaRef,
) -> Result<RecordBatch> {
    let columns = batch
        .columns()
        .iter()
        .map(hydrate_dictionary)
        .collect::<Result<Vec<_>>>()?;

    Ok(RecordBatch::try_new(schema, columns)?)
}

/// Hydrates a dictionary to its underlying type
///
/// An IPC response, streaming or otherwise, defines its schema up front
/// which defines the mapping from dictionary IDs. It then sends these
/// dictionaries over the wire.
///
/// This requires identifying the different dictionaries in use, assigning
/// them IDs, and sending new dictionaries, delta or otherwise, when needed
///
/// See also:
/// * <https://github.com/apache/arrow-rs/issues/1206>
///
/// For now we just hydrate the dictionaries to their underlying type
fn hydrate_dictionary(array: &ArrayRef) -> Result<ArrayRef> {
    let arr = if let DataType::Dictionary(_, value) = array.data_type() {
        arrow_cast::cast(array, value)?
    } else {
        Arc::clone(array)
    };
    Ok(arr)
}

#[cfg(test)]
mod tests {
    use arrow::{
        array::{UInt32Array, UInt8Array},
        compute::concat_batches,
    };
    use arrow_array::UInt64Array;

    use super::*;

    #[test]
    /// ensure only the batch's used data (not the allocated data) is sent
    /// <https://github.com/apache/arrow-rs/issues/208>
    fn test_encode_flight_data() {
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let c1 = UInt32Array::from(vec![1, 2, 3, 4, 5, 6]);

        let batch = RecordBatch::try_from_iter(vec![("a", Arc::new(c1) as ArrayRef)])
            .expect("cannot create record batch");
        let schema = batch.schema();

        let (_, baseline_flight_batch) = make_flight_data(&batch, &options);

        let big_batch = batch.slice(0, batch.num_rows() - 1);
        let optimized_big_batch =
            prepare_batch_for_flight(&big_batch, Arc::clone(&schema))
                .expect("failed to optimize");
        let (_, optimized_big_flight_batch) =
            make_flight_data(&optimized_big_batch, &options);

        assert_eq!(
            baseline_flight_batch.data_body.len(),
            optimized_big_flight_batch.data_body.len()
        );

        let small_batch = batch.slice(0, 1);
        let optimized_small_batch =
            prepare_batch_for_flight(&small_batch, Arc::clone(&schema))
                .expect("failed to optimize");
        let (_, optimized_small_flight_batch) =
            make_flight_data(&optimized_small_batch, &options);

        assert!(
            baseline_flight_batch.data_body.len()
                > optimized_small_flight_batch.data_body.len()
        );
    }

    pub fn make_flight_data(
        batch: &RecordBatch,
        options: &IpcWriteOptions,
    ) -> (Vec<FlightData>, FlightData) {
        let data_gen = IpcDataGenerator::default();
        let mut dictionary_tracker = DictionaryTracker::new(false);

        let (encoded_dictionaries, encoded_batch) = data_gen
            .encoded_batch(batch, &mut dictionary_tracker, options)
            .expect("DictionaryTracker configured above to not error on replacement");

        let flight_dictionaries =
            encoded_dictionaries.into_iter().map(Into::into).collect();
        let flight_batch = encoded_batch.into();

        (flight_dictionaries, flight_batch)
    }

    #[test]
    fn test_split_batch_for_grpc_response() {
        let max_flight_data_size = 1024;

        // no split
        let c = UInt32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let batch = RecordBatch::try_from_iter(vec![("a", Arc::new(c) as ArrayRef)])
            .expect("cannot create record batch");
        let split = split_batch_for_grpc_response(batch.clone(), max_flight_data_size);
        assert_eq!(split.len(), 1);
        assert_eq!(batch, split[0]);

        // split once
        let n_rows = max_flight_data_size + 1;
        assert!(n_rows % 2 == 1, "should be an odd number");
        let c =
            UInt8Array::from((0..n_rows).map(|i| (i % 256) as u8).collect::<Vec<_>>());
        let batch = RecordBatch::try_from_iter(vec![("a", Arc::new(c) as ArrayRef)])
            .expect("cannot create record batch");
        let split = split_batch_for_grpc_response(batch.clone(), max_flight_data_size);
        assert_eq!(split.len(), 3);
        assert_eq!(
            split.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            n_rows
        );
        assert_eq!(concat_batches(&batch.schema(), &split).unwrap(), batch);
    }

    #[test]
    fn test_split_batch_for_grpc_response_sizes() {
        // 2000 8 byte entries into 2k pieces: 8 chunks of 250 rows
        verify_split(2000, 2 * 1024, vec![250, 250, 250, 250, 250, 250, 250, 250]);

        // 2000 8 byte entries into 4k pieces: 4 chunks of 500 rows
        verify_split(2000, 4 * 1024, vec![500, 500, 500, 500]);

        // 2023 8 byte entries into 3k pieces does not divide evenly
        verify_split(2023, 3 * 1024, vec![337, 337, 337, 337, 337, 337, 1]);

        // 10 8 byte entries into 1 byte pieces means each rows gets its own
        verify_split(10, 1, vec![1, 1, 1, 1, 1, 1, 1, 1, 1, 1]);

        // 10 8 byte entries into 1k byte pieces means one piece
        verify_split(10, 1024, vec![10]);
    }

    /// Creates a UInt64Array of 8 byte integers with input_rows rows
    /// `max_flight_data_size_bytes` pieces and verifies the row counts in
    /// those pieces
    fn verify_split(
        num_input_rows: u64,
        max_flight_data_size_bytes: usize,
        expected_sizes: Vec<usize>,
    ) {
        let array: UInt64Array = (0..num_input_rows).collect();

        let batch = RecordBatch::try_from_iter(vec![("a", Arc::new(array) as ArrayRef)])
            .expect("cannot create record batch");

        let input_rows = batch.num_rows();

        let split =
            split_batch_for_grpc_response(batch.clone(), max_flight_data_size_bytes);
        let sizes: Vec<_> = split.iter().map(|batch| batch.num_rows()).collect();
        let output_rows: usize = sizes.iter().sum();

        assert_eq!(sizes, expected_sizes, "mismatch for {batch:?}");
        assert_eq!(input_rows, output_rows, "mismatch for {batch:?}");
    }

    // test sending record batches
    // test sending record batches with multiple different dictionaries
}
