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

use crate::{error::Result, FlightData, FlightDescriptor, SchemaAsIpc};

use arrow_array::{Array, ArrayRef, RecordBatch, RecordBatchOptions, UnionArray};
use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};

use arrow_schema::{DataType, Field, FieldRef, Fields, Schema, SchemaRef, UnionMode};
use bytes::Bytes;
use futures::{ready, stream::BoxStream, Stream, StreamExt};

/// Creates a [`Stream`] of [`FlightData`]s from a
/// `Stream` of [`Result`]<[`RecordBatch`], [`FlightError`]>.
///
/// This can be used to implement [`FlightService::do_get`] in an
/// Arrow Flight implementation;
///
/// This structure encodes a stream of `Result`s rather than `RecordBatch`es  to
/// propagate errors from streaming execution, where the generation of the
/// `RecordBatch`es is incremental, and an error may occur even after
/// several have already been successfully produced.
///
/// # Caveats
/// 1. When [`DictionaryHandling`] is [`DictionaryHandling::Hydrate`],
///    [`DictionaryArray`]s are converted to their underlying types prior to
///    transport.
///    When [`DictionaryHandling`] is [`DictionaryHandling::Resend`], Dictionary [`FlightData`] is sent with every
///    [`RecordBatch`] that contains a [`DictionaryArray`](arrow_array::array::DictionaryArray).
///    See <https://github.com/apache/arrow-rs/issues/3389>.
///
/// [`DictionaryArray`]: arrow_array::array::DictionaryArray
///
/// # Example
/// ```no_run
/// # use std::sync::Arc;
/// # use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
/// # async fn f() {
/// # let c1 = UInt32Array::from(vec![1, 2, 3, 4, 5, 6]);
/// # let batch = RecordBatch::try_from_iter(vec![
/// #      ("a", Arc::new(c1) as ArrayRef)
/// #   ])
/// #   .expect("cannot create record batch");
/// use arrow_flight::encode::FlightDataEncoderBuilder;
///
/// // Get an input stream of Result<RecordBatch, FlightError>
/// let input_stream = futures::stream::iter(vec![Ok(batch)]);
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
/// # Example: Sending `Vec<RecordBatch>`
///
/// You can create a [`Stream`] to pass to [`Self::build`] from an existing
/// `Vec` of `RecordBatch`es like this:
///
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{ArrayRef, RecordBatch, UInt32Array};
/// # async fn f() {
/// # fn make_batches() -> Vec<RecordBatch> {
/// #   let c1 = UInt32Array::from(vec![1, 2, 3, 4, 5, 6]);
/// #   let batch = RecordBatch::try_from_iter(vec![
/// #      ("a", Arc::new(c1) as ArrayRef)
/// #   ])
/// #   .expect("cannot create record batch");
/// #   vec![batch.clone(), batch.clone()]
/// # }
/// use arrow_flight::encode::FlightDataEncoderBuilder;
///
/// // Get batches that you want to send via Flight
/// let batches: Vec<RecordBatch> = make_batches();
///
/// // Create an input stream of Result<RecordBatch, FlightError>
/// let input_stream = futures::stream::iter(
///   batches.into_iter().map(Ok)
/// );
///
/// // Build a stream of `Result<FlightData>` (e.g. to return for do_get)
/// let flight_data_stream = FlightDataEncoderBuilder::new()
///  .build(input_stream);
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
    /// Optional schema, if known before data.
    schema: Option<SchemaRef>,
    /// Optional flight descriptor, if known before data.
    descriptor: Option<FlightDescriptor>,
    /// Deterimines how `DictionaryArray`s are encoded for transport.
    /// See [`DictionaryHandling`] for more information.
    dictionary_handling: DictionaryHandling,
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
            schema: None,
            descriptor: None,
            dictionary_handling: DictionaryHandling::Hydrate,
        }
    }
}

impl FlightDataEncoderBuilder {
    /// Create a new [`FlightDataEncoderBuilder`].
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

    /// Set [`DictionaryHandling`] for encoder
    pub fn with_dictionary_handling(mut self, dictionary_handling: DictionaryHandling) -> Self {
        self.dictionary_handling = dictionary_handling;
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

    /// Specify a schema for the RecordBatches being sent. If a schema
    /// is not specified, an encoded Schema message will be sent when
    /// the first [`RecordBatch`], if any, is encoded. Some clients
    /// expect a Schema message even if there is no data sent.
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = Some(schema);
        self
    }

    /// Specify a flight descriptor in the first FlightData message.
    pub fn with_flight_descriptor(mut self, descriptor: Option<FlightDescriptor>) -> Self {
        self.descriptor = descriptor;
        self
    }

    /// Takes a [`Stream`] of [`Result<RecordBatch>`] and returns a [`Stream`]
    /// of [`FlightData`], consuming self.
    ///
    /// See example on [`Self`] and [`FlightDataEncoder`] for more details
    pub fn build<S>(self, input: S) -> FlightDataEncoder
    where
        S: Stream<Item = Result<RecordBatch>> + Send + 'static,
    {
        let Self {
            max_flight_data_size,
            options,
            app_metadata,
            schema,
            descriptor,
            dictionary_handling,
        } = self;

        FlightDataEncoder::new(
            input.boxed(),
            schema,
            max_flight_data_size,
            options,
            app_metadata,
            descriptor,
            dictionary_handling,
        )
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
    /// cleared after the first FlightData message is sent
    descriptor: Option<FlightDescriptor>,
    /// Deterimines how `DictionaryArray`s are encoded for transport.
    /// See [`DictionaryHandling`] for more information.
    dictionary_handling: DictionaryHandling,
}

impl FlightDataEncoder {
    fn new(
        inner: BoxStream<'static, Result<RecordBatch>>,
        schema: Option<SchemaRef>,
        max_flight_data_size: usize,
        options: IpcWriteOptions,
        app_metadata: Bytes,
        descriptor: Option<FlightDescriptor>,
        dictionary_handling: DictionaryHandling,
    ) -> Self {
        let mut encoder = Self {
            inner,
            schema: None,
            max_flight_data_size,
            encoder: FlightIpcEncoder::new(
                options,
                dictionary_handling != DictionaryHandling::Resend,
            ),
            app_metadata: Some(app_metadata),
            queue: VecDeque::new(),
            done: false,
            descriptor,
            dictionary_handling,
        };

        // If schema is known up front, enqueue it immediately
        if let Some(schema) = schema {
            encoder.encode_schema(&schema);
        }

        encoder
    }

    /// Place the `FlightData` in the queue to send
    fn queue_message(&mut self, mut data: FlightData) {
        if let Some(descriptor) = self.descriptor.take() {
            data.flight_descriptor = Some(descriptor);
        }
        self.queue.push_back(data);
    }

    /// Place the `FlightData` in the queue to send
    fn queue_messages(&mut self, datas: impl IntoIterator<Item = FlightData>) {
        for data in datas {
            self.queue_message(data)
        }
    }

    /// Encodes schema as a [`FlightData`] in self.queue.
    /// Updates `self.schema` and returns the new schema
    fn encode_schema(&mut self, schema: &SchemaRef) -> SchemaRef {
        // The first message is the schema message, and all
        // batches have the same schema
        let send_dictionaries = self.dictionary_handling == DictionaryHandling::Resend;
        let schema = Arc::new(prepare_schema_for_flight(
            schema,
            &mut self.encoder.dictionary_tracker,
            send_dictionaries,
        ));
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

    /// Encodes batch into one or more `FlightData` messages in self.queue
    fn encode_batch(&mut self, batch: RecordBatch) -> Result<()> {
        let schema = match &self.schema {
            Some(schema) => schema.clone(),
            // encode the schema if this is the first time we have seen it
            None => self.encode_schema(batch.schema_ref()),
        };

        let batch = match self.dictionary_handling {
            DictionaryHandling::Resend => batch,
            DictionaryHandling::Hydrate => hydrate_dictionaries(&batch, schema)?,
        };

        for batch in split_batch_for_grpc_response(batch, self.max_flight_data_size) {
            let (flight_dictionaries, flight_batch) = self.encoder.encode_batch(&batch)?;

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

/// Defines how a [`FlightDataEncoder`] encodes [`DictionaryArray`]s
///
/// [`DictionaryArray`]: arrow_array::DictionaryArray
///
/// In the arrow flight protocol dictionary values and keys are sent as two separate messages.
/// When a sender is encoding a [`RecordBatch`] containing ['DictionaryArray'] columns, it will
/// first send a dictionary batch (a batch with header `MessageHeader::DictionaryBatch`) containing
/// the dictionary values. The receiver is responsible for reading this batch and maintaining state that associates
/// those dictionary values with the corresponding array using the `dict_id` as a key.
///
/// After sending the dictionary batch the sender will send the array data in a batch with header `MessageHeader::RecordBatch`.
/// For any dictionary array batches in this message, the encoded flight message will only contain the dictionary keys. The receiver
/// is then responsible for rebuilding the `DictionaryArray` on the client side using the dictionary values from the DictionaryBatch message
/// and the keys from the RecordBatch message.
///
/// For example, if we have a batch with a `TypedDictionaryArray<'_, UInt32Type, Utf8Type>` (a dictionary array where they keys are `u32` and the
/// values are `String`), then the DictionaryBatch will contain a `StringArray` and the RecordBatch will contain a `UInt32Array`.
///
/// Note that since `dict_id` defined in the `Schema` is used as a key to associate dictionary values to their arrays it is required that each
/// `DictionaryArray` in a `RecordBatch` have a unique `dict_id`.
///
/// The current implementation does not support "delta" dictionaries so a new dictionary batch will be sent each time the encoder sees a
/// dictionary which is not pointer-equal to the previously observed dictionary for a given `dict_id`.
///
/// For clients which may not support `DictionaryEncoding`, the `DictionaryHandling::Hydrate` method will bypass the process defined above
/// and "hydrate" any `DictionaryArray` in the batch to their underlying value type (e.g. `TypedDictionaryArray<'_, UInt32Type, Utf8Type>` will
/// be sent as a `StringArray`). With this method all data will be sent in ``MessageHeader::RecordBatch` messages and the batch schema
/// will be adjusted so that all dictionary encoded fields are changed to fields of the dictionary value type.
#[derive(Debug, PartialEq)]
pub enum DictionaryHandling {
    /// Expands to the underlying type (default). This likely sends more data
    /// over the network but requires less memory (dictionaries are not tracked)
    /// and is more compatible with other arrow flight client implementations
    /// that may not support `DictionaryEncoding`
    ///
    /// See also:
    /// * <https://github.com/apache/arrow-rs/issues/1206>
    Hydrate,
    /// Send dictionary FlightData with every RecordBatch that contains a
    /// [`DictionaryArray`]. See [`Self::Hydrate`] for more tradeoffs. No
    /// attempt is made to skip sending the same (logical) dictionary values
    /// twice.
    ///
    /// [`DictionaryArray`]: arrow_array::DictionaryArray
    ///
    /// This requires identifying the different dictionaries in use and assigning
    //  them unique IDs
    Resend,
}

fn prepare_field_for_flight(
    field: &FieldRef,
    dictionary_tracker: &mut DictionaryTracker,
    send_dictionaries: bool,
) -> Field {
    match field.data_type() {
        DataType::List(inner) => Field::new_list(
            field.name(),
            prepare_field_for_flight(inner, dictionary_tracker, send_dictionaries),
            field.is_nullable(),
        )
        .with_metadata(field.metadata().clone()),
        DataType::LargeList(inner) => Field::new_list(
            field.name(),
            prepare_field_for_flight(inner, dictionary_tracker, send_dictionaries),
            field.is_nullable(),
        )
        .with_metadata(field.metadata().clone()),
        DataType::Struct(fields) => {
            let new_fields: Vec<Field> = fields
                .iter()
                .map(|f| prepare_field_for_flight(f, dictionary_tracker, send_dictionaries))
                .collect();
            Field::new_struct(field.name(), new_fields, field.is_nullable())
                .with_metadata(field.metadata().clone())
        }
        DataType::Union(fields, mode) => {
            let (type_ids, new_fields): (Vec<i8>, Vec<Field>) = fields
                .iter()
                .map(|(type_id, f)| {
                    (
                        type_id,
                        prepare_field_for_flight(f, dictionary_tracker, send_dictionaries),
                    )
                })
                .unzip();

            Field::new_union(field.name(), type_ids, new_fields, *mode)
        }
        DataType::Dictionary(_, value_type) => {
            if !send_dictionaries {
                Field::new(
                    field.name(),
                    value_type.as_ref().clone(),
                    field.is_nullable(),
                )
                .with_metadata(field.metadata().clone())
            } else {
                let dict_id = dictionary_tracker.set_dict_id(field.as_ref());

                Field::new_dict(
                    field.name(),
                    field.data_type().clone(),
                    field.is_nullable(),
                    dict_id,
                    field.dict_is_ordered().unwrap_or_default(),
                )
                .with_metadata(field.metadata().clone())
            }
        }
        _ => field.as_ref().clone(),
    }
}

/// Prepare an arrow Schema for transport over the Arrow Flight protocol
///
/// Convert dictionary types to underlying types
///
/// See hydrate_dictionary for more information
fn prepare_schema_for_flight(
    schema: &Schema,
    dictionary_tracker: &mut DictionaryTracker,
    send_dictionaries: bool,
) -> Schema {
    let fields: Fields = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Dictionary(_, value_type) => {
                if !send_dictionaries {
                    Field::new(
                        field.name(),
                        value_type.as_ref().clone(),
                        field.is_nullable(),
                    )
                    .with_metadata(field.metadata().clone())
                } else {
                    let dict_id = dictionary_tracker.set_dict_id(field.as_ref());
                    Field::new_dict(
                        field.name(),
                        field.data_type().clone(),
                        field.is_nullable(),
                        dict_id,
                        field.dict_is_ordered().unwrap_or_default(),
                    )
                    .with_metadata(field.metadata().clone())
                }
            }
            tpe if tpe.is_nested() => {
                prepare_field_for_flight(field, dictionary_tracker, send_dictionaries)
            }
            _ => field.as_ref().clone(),
        })
        .collect();

    Schema::new(fields).with_metadata(schema.metadata().clone())
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

    let n_batches =
        (size / max_flight_data_size + usize::from(size % max_flight_data_size != 0)).max(1);
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
    fn new(options: IpcWriteOptions, error_on_replacement: bool) -> Self {
        let preserve_dict_id = options.preserve_dict_id();
        Self {
            options,
            data_gen: IpcDataGenerator::default(),
            dictionary_tracker: DictionaryTracker::new_with_preserve_dict_id(
                error_on_replacement,
                preserve_dict_id,
            ),
        }
    }

    /// Encode a schema as a FlightData
    fn encode_schema(&self, schema: &Schema) -> FlightData {
        SchemaAsIpc::new(schema, &self.options).into()
    }

    /// Convert a `RecordBatch` to a Vec of `FlightData` representing
    /// dictionaries and a `FlightData` representing the batch
    fn encode_batch(&mut self, batch: &RecordBatch) -> Result<(Vec<FlightData>, FlightData)> {
        let (encoded_dictionaries, encoded_batch) =
            self.data_gen
                .encoded_batch(batch, &mut self.dictionary_tracker, &self.options)?;

        let flight_dictionaries = encoded_dictionaries.into_iter().map(Into::into).collect();
        let flight_batch = encoded_batch.into();

        Ok((flight_dictionaries, flight_batch))
    }
}

/// Hydrates any dictionaries arrays in `batch` to its underlying type. See
/// hydrate_dictionary for more information.
fn hydrate_dictionaries(batch: &RecordBatch, schema: SchemaRef) -> Result<RecordBatch> {
    let columns = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, c)| hydrate_dictionary(c, field.data_type()))
        .collect::<Result<Vec<_>>>()?;

    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));

    Ok(RecordBatch::try_new_with_options(
        schema, columns, &options,
    )?)
}

/// Hydrates a dictionary to its underlying type.
fn hydrate_dictionary(array: &ArrayRef, data_type: &DataType) -> Result<ArrayRef> {
    let arr = match (array.data_type(), data_type) {
        (DataType::Union(_, UnionMode::Sparse), DataType::Union(fields, UnionMode::Sparse)) => {
            let union_arr = array.as_any().downcast_ref::<UnionArray>().unwrap();

            Arc::new(UnionArray::try_new(
                fields.clone(),
                union_arr.type_ids().clone(),
                None,
                fields
                    .iter()
                    .map(|(type_id, field)| {
                        Ok(arrow_cast::cast(
                            union_arr.child(type_id),
                            field.data_type(),
                        )?)
                    })
                    .collect::<Result<Vec<_>>>()?,
            )?)
        }
        (_, data_type) => arrow_cast::cast(array, data_type)?,
    };
    Ok(arr)
}

#[cfg(test)]
mod tests {
    use crate::decode::{DecodedPayload, FlightDataDecoder};
    use arrow_array::builder::{
        GenericByteDictionaryBuilder, ListBuilder, StringDictionaryBuilder, StructBuilder,
    };
    use arrow_array::*;
    use arrow_array::{cast::downcast_array, types::*};
    use arrow_buffer::ScalarBuffer;
    use arrow_cast::pretty::pretty_format_batches;
    use arrow_ipc::MetadataVersion;
    use arrow_schema::{UnionFields, UnionMode};
    use std::collections::HashMap;

    use super::*;

    #[test]
    /// ensure only the batch's used data (not the allocated data) is sent
    /// <https://github.com/apache/arrow-rs/issues/208>
    fn test_encode_flight_data() {
        // use 8-byte alignment - default alignment is 64 which produces bigger ipc data
        let options = IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap();
        let c1 = UInt32Array::from(vec![1, 2, 3, 4, 5, 6]);

        let batch = RecordBatch::try_from_iter(vec![("a", Arc::new(c1) as ArrayRef)])
            .expect("cannot create record batch");
        let schema = batch.schema_ref();

        let (_, baseline_flight_batch) = make_flight_data(&batch, &options);

        let big_batch = batch.slice(0, batch.num_rows() - 1);
        let optimized_big_batch =
            hydrate_dictionaries(&big_batch, Arc::clone(schema)).expect("failed to optimize");
        let (_, optimized_big_flight_batch) = make_flight_data(&optimized_big_batch, &options);

        assert_eq!(
            baseline_flight_batch.data_body.len(),
            optimized_big_flight_batch.data_body.len()
        );

        let small_batch = batch.slice(0, 1);
        let optimized_small_batch =
            hydrate_dictionaries(&small_batch, Arc::clone(schema)).expect("failed to optimize");
        let (_, optimized_small_flight_batch) = make_flight_data(&optimized_small_batch, &options);

        assert!(
            baseline_flight_batch.data_body.len() > optimized_small_flight_batch.data_body.len()
        );
    }

    #[tokio::test]
    async fn test_dictionary_hydration() {
        let arr1: DictionaryArray<UInt16Type> = vec!["a", "a", "b"].into_iter().collect();
        let arr2: DictionaryArray<UInt16Type> = vec!["c", "c", "d"].into_iter().collect();

        let schema = Arc::new(Schema::new(vec![Field::new_dictionary(
            "dict",
            DataType::UInt16,
            DataType::Utf8,
            false,
        )]));
        let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr1)]).unwrap();
        let batch2 = RecordBatch::try_new(schema, vec![Arc::new(arr2)]).unwrap();

        let stream = futures::stream::iter(vec![Ok(batch1), Ok(batch2)]);

        let encoder = FlightDataEncoderBuilder::default().build(stream);
        let mut decoder = FlightDataDecoder::new(encoder);
        let expected_schema = Schema::new(vec![Field::new("dict", DataType::Utf8, false)]);
        let expected_schema = Arc::new(expected_schema);
        let mut expected_arrays = vec![
            StringArray::from(vec!["a", "a", "b"]),
            StringArray::from(vec!["c", "c", "d"]),
        ]
        .into_iter();
        while let Some(decoded) = decoder.next().await {
            let decoded = decoded.unwrap();
            match decoded.payload {
                DecodedPayload::None => {}
                DecodedPayload::Schema(s) => assert_eq!(s, expected_schema),
                DecodedPayload::RecordBatch(b) => {
                    assert_eq!(b.schema(), expected_schema);
                    let expected_array = expected_arrays.next().unwrap();
                    let actual_array = b.column_by_name("dict").unwrap();
                    let actual_array = downcast_array::<StringArray>(actual_array);

                    assert_eq!(actual_array, expected_array);
                }
            }
        }
    }

    #[tokio::test]
    async fn test_dictionary_resend() {
        let arr1: DictionaryArray<UInt16Type> = vec!["a", "a", "b"].into_iter().collect();
        let arr2: DictionaryArray<UInt16Type> = vec!["c", "c", "d"].into_iter().collect();

        let schema = Arc::new(Schema::new(vec![Field::new_dictionary(
            "dict",
            DataType::UInt16,
            DataType::Utf8,
            false,
        )]));
        let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr1)]).unwrap();
        let batch2 = RecordBatch::try_new(schema, vec![Arc::new(arr2)]).unwrap();

        verify_flight_round_trip(vec![batch1, batch2]).await;
    }

    #[tokio::test]
    async fn test_multiple_dictionaries_resend() {
        // Create a schema with two dictionary fields that have the same dict ID
        let schema = Arc::new(Schema::new(vec![
            Field::new_dictionary("dict_1", DataType::UInt16, DataType::Utf8, false),
            Field::new_dictionary("dict_2", DataType::UInt16, DataType::Utf8, false),
        ]));

        let arr_one_1: Arc<DictionaryArray<UInt16Type>> =
            Arc::new(vec!["a", "a", "b"].into_iter().collect());
        let arr_one_2: Arc<DictionaryArray<UInt16Type>> =
            Arc::new(vec!["c", "c", "d"].into_iter().collect());
        let arr_two_1: Arc<DictionaryArray<UInt16Type>> =
            Arc::new(vec!["b", "a", "c"].into_iter().collect());
        let arr_two_2: Arc<DictionaryArray<UInt16Type>> =
            Arc::new(vec!["k", "d", "e"].into_iter().collect());
        let batch1 =
            RecordBatch::try_new(schema.clone(), vec![arr_one_1.clone(), arr_one_2.clone()])
                .unwrap();
        let batch2 =
            RecordBatch::try_new(schema.clone(), vec![arr_two_1.clone(), arr_two_2.clone()])
                .unwrap();

        verify_flight_round_trip(vec![batch1, batch2]).await;
    }

    #[tokio::test]
    async fn test_dictionary_list_hydration() {
        let mut builder = ListBuilder::new(StringDictionaryBuilder::<UInt16Type>::new());

        builder.append_value(vec![Some("a"), None, Some("b")]);

        let arr1 = builder.finish();

        builder.append_value(vec![Some("c"), None, Some("d")]);

        let arr2 = builder.finish();

        let schema = Arc::new(Schema::new(vec![Field::new_list(
            "dict_list",
            Field::new_dictionary("item", DataType::UInt16, DataType::Utf8, true),
            true,
        )]));

        let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr1)]).unwrap();
        let batch2 = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr2)]).unwrap();

        let stream = futures::stream::iter(vec![Ok(batch1), Ok(batch2)]);

        let encoder = FlightDataEncoderBuilder::default().build(stream);

        let mut decoder = FlightDataDecoder::new(encoder);
        let expected_schema = Schema::new(vec![Field::new_list(
            "dict_list",
            Field::new("item", DataType::Utf8, true),
            true,
        )]);

        let expected_schema = Arc::new(expected_schema);

        let mut expected_arrays = vec![
            StringArray::from_iter(vec![Some("a"), None, Some("b")]),
            StringArray::from_iter(vec![Some("c"), None, Some("d")]),
        ]
        .into_iter();

        while let Some(decoded) = decoder.next().await {
            let decoded = decoded.unwrap();
            match decoded.payload {
                DecodedPayload::None => {}
                DecodedPayload::Schema(s) => assert_eq!(s, expected_schema),
                DecodedPayload::RecordBatch(b) => {
                    assert_eq!(b.schema(), expected_schema);
                    let expected_array = expected_arrays.next().unwrap();
                    let list_array =
                        downcast_array::<ListArray>(b.column_by_name("dict_list").unwrap());
                    let elem_array = downcast_array::<StringArray>(list_array.value(0).as_ref());

                    assert_eq!(elem_array, expected_array);
                }
            }
        }
    }

    #[tokio::test]
    async fn test_dictionary_list_resend() {
        let mut builder = ListBuilder::new(StringDictionaryBuilder::<UInt16Type>::new());

        builder.append_value(vec![Some("a"), None, Some("b")]);

        let arr1 = builder.finish();

        builder.append_value(vec![Some("c"), None, Some("d")]);

        let arr2 = builder.finish();

        let schema = Arc::new(Schema::new(vec![Field::new_list(
            "dict_list",
            Field::new_dictionary("item", DataType::UInt16, DataType::Utf8, true),
            true,
        )]));

        let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr1)]).unwrap();
        let batch2 = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr2)]).unwrap();

        verify_flight_round_trip(vec![batch1, batch2]).await;
    }

    #[tokio::test]
    async fn test_dictionary_struct_hydration() {
        let struct_fields = vec![Field::new_list(
            "dict_list",
            Field::new_dictionary("item", DataType::UInt16, DataType::Utf8, true),
            true,
        )];

        let mut struct_builder = StructBuilder::new(
            struct_fields.clone(),
            vec![Box::new(builder::ListBuilder::new(
                StringDictionaryBuilder::<UInt16Type>::new(),
            ))],
        );

        struct_builder
            .field_builder::<ListBuilder<GenericByteDictionaryBuilder<UInt16Type,GenericStringType<i32>>>>(0)
            .unwrap()
            .append_value(vec![Some("a"), None, Some("b")]);

        struct_builder.append(true);

        let arr1 = struct_builder.finish();

        struct_builder
            .field_builder::<ListBuilder<GenericByteDictionaryBuilder<UInt16Type,GenericStringType<i32>>>>(0)
            .unwrap()
            .append_value(vec![Some("c"), None, Some("d")]);
        struct_builder.append(true);

        let arr2 = struct_builder.finish();

        let schema = Arc::new(Schema::new(vec![Field::new_struct(
            "struct",
            struct_fields,
            true,
        )]));

        let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr1)]).unwrap();
        let batch2 = RecordBatch::try_new(schema, vec![Arc::new(arr2)]).unwrap();

        let stream = futures::stream::iter(vec![Ok(batch1), Ok(batch2)]);

        let encoder = FlightDataEncoderBuilder::default().build(stream);

        let mut decoder = FlightDataDecoder::new(encoder);
        let expected_schema = Schema::new(vec![Field::new_struct(
            "struct",
            vec![Field::new_list(
                "dict_list",
                Field::new("item", DataType::Utf8, true),
                true,
            )],
            true,
        )]);

        let expected_schema = Arc::new(expected_schema);

        let mut expected_arrays = vec![
            StringArray::from_iter(vec![Some("a"), None, Some("b")]),
            StringArray::from_iter(vec![Some("c"), None, Some("d")]),
        ]
        .into_iter();

        while let Some(decoded) = decoder.next().await {
            let decoded = decoded.unwrap();
            match decoded.payload {
                DecodedPayload::None => {}
                DecodedPayload::Schema(s) => assert_eq!(s, expected_schema),
                DecodedPayload::RecordBatch(b) => {
                    assert_eq!(b.schema(), expected_schema);
                    let expected_array = expected_arrays.next().unwrap();
                    let struct_array =
                        downcast_array::<StructArray>(b.column_by_name("struct").unwrap());
                    let list_array = downcast_array::<ListArray>(struct_array.column(0));

                    let elem_array = downcast_array::<StringArray>(list_array.value(0).as_ref());

                    assert_eq!(elem_array, expected_array);
                }
            }
        }
    }

    #[tokio::test]
    async fn test_dictionary_struct_resend() {
        let struct_fields = vec![Field::new_list(
            "dict_list",
            Field::new_dictionary("item", DataType::UInt16, DataType::Utf8, true),
            true,
        )];

        let mut struct_builder = StructBuilder::new(
            struct_fields.clone(),
            vec![Box::new(builder::ListBuilder::new(
                StringDictionaryBuilder::<UInt16Type>::new(),
            ))],
        );

        struct_builder.field_builder::<ListBuilder<GenericByteDictionaryBuilder<UInt16Type,GenericStringType<i32>>>>(0).unwrap().append_value(vec![Some("a"), None, Some("b")]);
        struct_builder.append(true);

        let arr1 = struct_builder.finish();

        struct_builder.field_builder::<ListBuilder<GenericByteDictionaryBuilder<UInt16Type,GenericStringType<i32>>>>(0).unwrap().append_value(vec![Some("c"), None, Some("d")]);
        struct_builder.append(true);

        let arr2 = struct_builder.finish();

        let schema = Arc::new(Schema::new(vec![Field::new_struct(
            "struct",
            struct_fields,
            true,
        )]));

        let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr1)]).unwrap();
        let batch2 = RecordBatch::try_new(schema, vec![Arc::new(arr2)]).unwrap();

        verify_flight_round_trip(vec![batch1, batch2]).await;
    }

    #[tokio::test]
    async fn test_dictionary_union_hydration() {
        let struct_fields = vec![Field::new_list(
            "dict_list",
            Field::new_dictionary("item", DataType::UInt16, DataType::Utf8, true),
            true,
        )];

        let union_fields = [
            (
                0,
                Arc::new(Field::new_list(
                    "dict_list",
                    Field::new_dictionary("item", DataType::UInt16, DataType::Utf8, true),
                    true,
                )),
            ),
            (
                1,
                Arc::new(Field::new_struct("struct", struct_fields.clone(), true)),
            ),
            (2, Arc::new(Field::new("string", DataType::Utf8, true))),
        ]
        .into_iter()
        .collect::<UnionFields>();

        let struct_fields = vec![Field::new_list(
            "dict_list",
            Field::new_dictionary("item", DataType::UInt16, DataType::Utf8, true),
            true,
        )];

        let mut builder = builder::ListBuilder::new(StringDictionaryBuilder::<UInt16Type>::new());

        builder.append_value(vec![Some("a"), None, Some("b")]);

        let arr1 = builder.finish();

        let type_id_buffer = [0].into_iter().collect::<ScalarBuffer<i8>>();
        let arr1 = UnionArray::try_new(
            union_fields.clone(),
            type_id_buffer,
            None,
            vec![
                Arc::new(arr1) as Arc<dyn Array>,
                new_null_array(union_fields.iter().nth(1).unwrap().1.data_type(), 1),
                new_null_array(union_fields.iter().nth(2).unwrap().1.data_type(), 1),
            ],
        )
        .unwrap();

        builder.append_value(vec![Some("c"), None, Some("d")]);

        let arr2 = Arc::new(builder.finish());
        let arr2 = StructArray::new(struct_fields.clone().into(), vec![arr2], None);

        let type_id_buffer = [1].into_iter().collect::<ScalarBuffer<i8>>();
        let arr2 = UnionArray::try_new(
            union_fields.clone(),
            type_id_buffer,
            None,
            vec![
                new_null_array(union_fields.iter().next().unwrap().1.data_type(), 1),
                Arc::new(arr2),
                new_null_array(union_fields.iter().nth(2).unwrap().1.data_type(), 1),
            ],
        )
        .unwrap();

        let type_id_buffer = [2].into_iter().collect::<ScalarBuffer<i8>>();
        let arr3 = UnionArray::try_new(
            union_fields.clone(),
            type_id_buffer,
            None,
            vec![
                new_null_array(union_fields.iter().next().unwrap().1.data_type(), 1),
                new_null_array(union_fields.iter().nth(1).unwrap().1.data_type(), 1),
                Arc::new(StringArray::from(vec!["e"])),
            ],
        )
        .unwrap();

        let (type_ids, union_fields): (Vec<_>, Vec<_>) = union_fields
            .iter()
            .map(|(type_id, field_ref)| (type_id, (*Arc::clone(field_ref)).clone()))
            .unzip();
        let schema = Arc::new(Schema::new(vec![Field::new_union(
            "union",
            type_ids.clone(),
            union_fields.clone(),
            UnionMode::Sparse,
        )]));

        let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr1)]).unwrap();
        let batch2 = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr2)]).unwrap();
        let batch3 = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr3)]).unwrap();

        let stream = futures::stream::iter(vec![Ok(batch1), Ok(batch2), Ok(batch3)]);

        let encoder = FlightDataEncoderBuilder::default().build(stream);

        let mut decoder = FlightDataDecoder::new(encoder);

        let hydrated_struct_fields = vec![Field::new_list(
            "dict_list",
            Field::new("item", DataType::Utf8, true),
            true,
        )];

        let hydrated_union_fields = vec![
            Field::new_list("dict_list", Field::new("item", DataType::Utf8, true), true),
            Field::new_struct("struct", hydrated_struct_fields.clone(), true),
            Field::new("string", DataType::Utf8, true),
        ];

        let expected_schema = Schema::new(vec![Field::new_union(
            "union",
            type_ids.clone(),
            hydrated_union_fields,
            UnionMode::Sparse,
        )]);

        let expected_schema = Arc::new(expected_schema);

        let mut expected_arrays = vec![
            StringArray::from_iter(vec![Some("a"), None, Some("b")]),
            StringArray::from_iter(vec![Some("c"), None, Some("d")]),
            StringArray::from(vec!["e"]),
        ]
        .into_iter();

        let mut batch = 0;
        while let Some(decoded) = decoder.next().await {
            let decoded = decoded.unwrap();
            match decoded.payload {
                DecodedPayload::None => {}
                DecodedPayload::Schema(s) => assert_eq!(s, expected_schema),
                DecodedPayload::RecordBatch(b) => {
                    assert_eq!(b.schema(), expected_schema);
                    let expected_array = expected_arrays.next().unwrap();
                    let union_arr =
                        downcast_array::<UnionArray>(b.column_by_name("union").unwrap());

                    let elem_array = match batch {
                        0 => {
                            let list_array = downcast_array::<ListArray>(union_arr.child(0));
                            downcast_array::<StringArray>(list_array.value(0).as_ref())
                        }
                        1 => {
                            let struct_array = downcast_array::<StructArray>(union_arr.child(1));
                            let list_array = downcast_array::<ListArray>(struct_array.column(0));

                            downcast_array::<StringArray>(list_array.value(0).as_ref())
                        }
                        _ => downcast_array::<StringArray>(union_arr.child(2)),
                    };

                    batch += 1;

                    assert_eq!(elem_array, expected_array);
                }
            }
        }
    }

    #[tokio::test]
    async fn test_dictionary_union_resend() {
        let struct_fields = vec![Field::new_list(
            "dict_list",
            Field::new_dictionary("item", DataType::UInt16, DataType::Utf8, true),
            true,
        )];

        let union_fields = [
            (
                0,
                Arc::new(Field::new_list(
                    "dict_list",
                    Field::new_dictionary("item", DataType::UInt16, DataType::Utf8, true),
                    true,
                )),
            ),
            (
                1,
                Arc::new(Field::new_struct("struct", struct_fields.clone(), true)),
            ),
            (2, Arc::new(Field::new("string", DataType::Utf8, true))),
        ]
        .into_iter()
        .collect::<UnionFields>();

        let struct_fields = vec![Field::new_list(
            "dict_list",
            Field::new_dictionary("item", DataType::UInt16, DataType::Utf8, true),
            true,
        )];

        let mut builder = builder::ListBuilder::new(StringDictionaryBuilder::<UInt16Type>::new());

        builder.append_value(vec![Some("a"), None, Some("b")]);

        let arr1 = builder.finish();

        let type_id_buffer = [0].into_iter().collect::<ScalarBuffer<i8>>();
        let arr1 = UnionArray::try_new(
            union_fields.clone(),
            type_id_buffer,
            None,
            vec![
                Arc::new(arr1) as Arc<dyn Array>,
                new_null_array(union_fields.iter().nth(1).unwrap().1.data_type(), 1),
                new_null_array(union_fields.iter().nth(2).unwrap().1.data_type(), 1),
            ],
        )
        .unwrap();

        builder.append_value(vec![Some("c"), None, Some("d")]);

        let arr2 = Arc::new(builder.finish());
        let arr2 = StructArray::new(struct_fields.clone().into(), vec![arr2], None);

        let type_id_buffer = [1].into_iter().collect::<ScalarBuffer<i8>>();
        let arr2 = UnionArray::try_new(
            union_fields.clone(),
            type_id_buffer,
            None,
            vec![
                new_null_array(union_fields.iter().next().unwrap().1.data_type(), 1),
                Arc::new(arr2),
                new_null_array(union_fields.iter().nth(2).unwrap().1.data_type(), 1),
            ],
        )
        .unwrap();

        let type_id_buffer = [2].into_iter().collect::<ScalarBuffer<i8>>();
        let arr3 = UnionArray::try_new(
            union_fields.clone(),
            type_id_buffer,
            None,
            vec![
                new_null_array(union_fields.iter().next().unwrap().1.data_type(), 1),
                new_null_array(union_fields.iter().nth(1).unwrap().1.data_type(), 1),
                Arc::new(StringArray::from(vec!["e"])),
            ],
        )
        .unwrap();

        let (type_ids, union_fields): (Vec<_>, Vec<_>) = union_fields
            .iter()
            .map(|(type_id, field_ref)| (type_id, (*Arc::clone(field_ref)).clone()))
            .unzip();
        let schema = Arc::new(Schema::new(vec![Field::new_union(
            "union",
            type_ids.clone(),
            union_fields.clone(),
            UnionMode::Sparse,
        )]));

        let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr1)]).unwrap();
        let batch2 = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr2)]).unwrap();
        let batch3 = RecordBatch::try_new(schema.clone(), vec![Arc::new(arr3)]).unwrap();

        verify_flight_round_trip(vec![batch1, batch2, batch3]).await;
    }

    async fn verify_flight_round_trip(mut batches: Vec<RecordBatch>) {
        let expected_schema = batches.first().unwrap().schema();

        let encoder = FlightDataEncoderBuilder::default()
            .with_options(IpcWriteOptions::default().with_preserve_dict_id(false))
            .with_dictionary_handling(DictionaryHandling::Resend)
            .build(futures::stream::iter(batches.clone().into_iter().map(Ok)));

        let mut expected_batches = batches.drain(..);

        let mut decoder = FlightDataDecoder::new(encoder);
        while let Some(decoded) = decoder.next().await {
            let decoded = decoded.unwrap();
            match decoded.payload {
                DecodedPayload::None => {}
                DecodedPayload::Schema(s) => assert_eq!(s, expected_schema),
                DecodedPayload::RecordBatch(b) => {
                    let expected_batch = expected_batches.next().unwrap();
                    assert_eq!(b, expected_batch);
                }
            }
        }
    }

    #[test]
    fn test_schema_metadata_encoded() {
        let schema = Schema::new(vec![Field::new("data", DataType::Int32, false)]).with_metadata(
            HashMap::from([("some_key".to_owned(), "some_value".to_owned())]),
        );

        let mut dictionary_tracker = DictionaryTracker::new_with_preserve_dict_id(false, true);

        let got = prepare_schema_for_flight(&schema, &mut dictionary_tracker, false);
        assert!(got.metadata().contains_key("some_key"));
    }

    #[test]
    fn test_encode_no_column_batch() {
        let batch = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(10)),
        )
        .expect("cannot create record batch");

        hydrate_dictionaries(&batch, batch.schema()).expect("failed to optimize");
    }

    pub fn make_flight_data(
        batch: &RecordBatch,
        options: &IpcWriteOptions,
    ) -> (Vec<FlightData>, FlightData) {
        let data_gen = IpcDataGenerator::default();
        let mut dictionary_tracker = DictionaryTracker::new_with_preserve_dict_id(false, true);

        let (encoded_dictionaries, encoded_batch) = data_gen
            .encoded_batch(batch, &mut dictionary_tracker, options)
            .expect("DictionaryTracker configured above to not error on replacement");

        let flight_dictionaries = encoded_dictionaries.into_iter().map(Into::into).collect();
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
        let c = UInt8Array::from((0..n_rows).map(|i| (i % 256) as u8).collect::<Vec<_>>());
        let batch = RecordBatch::try_from_iter(vec![("a", Arc::new(c) as ArrayRef)])
            .expect("cannot create record batch");
        let split = split_batch_for_grpc_response(batch.clone(), max_flight_data_size);
        assert_eq!(split.len(), 3);
        assert_eq!(
            split.iter().map(|batch| batch.num_rows()).sum::<usize>(),
            n_rows
        );
        let a = pretty_format_batches(&split).unwrap().to_string();
        let b = pretty_format_batches(&[batch]).unwrap().to_string();
        assert_eq!(a, b);
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

        let split = split_batch_for_grpc_response(batch.clone(), max_flight_data_size_bytes);
        let sizes: Vec<_> = split.iter().map(RecordBatch::num_rows).collect();
        let output_rows: usize = sizes.iter().sum();

        assert_eq!(sizes, expected_sizes, "mismatch for {batch:?}");
        assert_eq!(input_rows, output_rows, "mismatch for {batch:?}");
    }

    // test sending record batches
    // test sending record batches with multiple different dictionaries

    #[tokio::test]
    async fn flight_data_size_even() {
        let s1 = StringArray::from_iter_values(std::iter::repeat(".10 bytes.").take(1024));
        let i1 = Int16Array::from_iter_values(0..1024);
        let s2 = StringArray::from_iter_values(std::iter::repeat("6bytes").take(1024));
        let i2 = Int64Array::from_iter_values(0..1024);

        let batch = RecordBatch::try_from_iter(vec![
            ("s1", Arc::new(s1) as _),
            ("i1", Arc::new(i1) as _),
            ("s2", Arc::new(s2) as _),
            ("i2", Arc::new(i2) as _),
        ])
        .unwrap();

        verify_encoded_split(batch, 112).await;
    }

    #[tokio::test]
    async fn flight_data_size_uneven_variable_lengths() {
        // each row has a longer string than the last with increasing lengths 0 --> 1024
        let array = StringArray::from_iter_values((0..1024).map(|i| "*".repeat(i)));
        let batch = RecordBatch::try_from_iter(vec![("data", Arc::new(array) as _)]).unwrap();

        // overage is much higher than ideal
        // https://github.com/apache/arrow-rs/issues/3478
        verify_encoded_split(batch, 4304).await;
    }

    #[tokio::test]
    async fn flight_data_size_large_row() {
        // batch with individual that can each exceed the batch size
        let array1 = StringArray::from_iter_values(vec![
            "*".repeat(500),
            "*".repeat(500),
            "*".repeat(500),
            "*".repeat(500),
        ]);
        let array2 = StringArray::from_iter_values(vec![
            "*".to_string(),
            "*".repeat(1000),
            "*".repeat(2000),
            "*".repeat(4000),
        ]);

        let array3 = StringArray::from_iter_values(vec![
            "*".to_string(),
            "*".to_string(),
            "*".repeat(1000),
            "*".repeat(2000),
        ]);

        let batch = RecordBatch::try_from_iter(vec![
            ("a1", Arc::new(array1) as _),
            ("a2", Arc::new(array2) as _),
            ("a3", Arc::new(array3) as _),
        ])
        .unwrap();

        // 5k over limit (which is 2x larger than limit of 5k)
        // overage is much higher than ideal
        // https://github.com/apache/arrow-rs/issues/3478
        verify_encoded_split(batch, 5800).await;
    }

    #[tokio::test]
    async fn flight_data_size_string_dictionary() {
        // Small dictionary (only 2 distinct values ==> 2 entries in dictionary)
        let array: DictionaryArray<Int32Type> = (1..1024)
            .map(|i| match i % 3 {
                0 => Some("value0"),
                1 => Some("value1"),
                _ => None,
            })
            .collect();

        let batch = RecordBatch::try_from_iter(vec![("a1", Arc::new(array) as _)]).unwrap();

        verify_encoded_split(batch, 160).await;
    }

    #[tokio::test]
    async fn flight_data_size_large_dictionary() {
        // large dictionary (all distinct values ==> 1024 entries in dictionary)
        let values: Vec<_> = (1..1024).map(|i| "**".repeat(i)).collect();

        let array: DictionaryArray<Int32Type> = values.iter().map(|s| Some(s.as_str())).collect();

        let batch = RecordBatch::try_from_iter(vec![("a1", Arc::new(array) as _)]).unwrap();

        // overage is much higher than ideal
        // https://github.com/apache/arrow-rs/issues/3478
        verify_encoded_split(batch, 3328).await;
    }

    #[tokio::test]
    async fn flight_data_size_large_dictionary_repeated_non_uniform() {
        // large dictionary (1024 distinct values) that are used throughout the array
        let values = StringArray::from_iter_values((0..1024).map(|i| "******".repeat(i)));
        let keys = Int32Array::from_iter_values((0..3000).map(|i| (3000 - i) % 1024));
        let array = DictionaryArray::new(keys, Arc::new(values));

        let batch = RecordBatch::try_from_iter(vec![("a1", Arc::new(array) as _)]).unwrap();

        // overage is much higher than ideal
        // https://github.com/apache/arrow-rs/issues/3478
        verify_encoded_split(batch, 5280).await;
    }

    #[tokio::test]
    async fn flight_data_size_multiple_dictionaries() {
        // high cardinality
        let values1: Vec<_> = (1..1024).map(|i| "**".repeat(i)).collect();
        // highish cardinality
        let values2: Vec<_> = (1..1024).map(|i| "**".repeat(i % 10)).collect();
        // medium cardinality
        let values3: Vec<_> = (1..1024).map(|i| "**".repeat(i % 100)).collect();

        let array1: DictionaryArray<Int32Type> = values1.iter().map(|s| Some(s.as_str())).collect();
        let array2: DictionaryArray<Int32Type> = values2.iter().map(|s| Some(s.as_str())).collect();
        let array3: DictionaryArray<Int32Type> = values3.iter().map(|s| Some(s.as_str())).collect();

        let batch = RecordBatch::try_from_iter(vec![
            ("a1", Arc::new(array1) as _),
            ("a2", Arc::new(array2) as _),
            ("a3", Arc::new(array3) as _),
        ])
        .unwrap();

        // overage is much higher than ideal
        // https://github.com/apache/arrow-rs/issues/3478
        verify_encoded_split(batch, 4128).await;
    }

    /// Return size, in memory of flight data
    fn flight_data_size(d: &FlightData) -> usize {
        let flight_descriptor_size = d
            .flight_descriptor
            .as_ref()
            .map(|descriptor| {
                let path_len: usize = descriptor.path.iter().map(|p| p.as_bytes().len()).sum();

                std::mem::size_of_val(descriptor) + descriptor.cmd.len() + path_len
            })
            .unwrap_or(0);

        flight_descriptor_size + d.app_metadata.len() + d.data_body.len() + d.data_header.len()
    }

    /// Coverage for <https://github.com/apache/arrow-rs/issues/3478>
    ///
    /// Encodes the specified batch using several values of
    /// `max_flight_data_size` between 1K to 5K and ensures that the
    /// resulting size of the flight data stays within the limit
    /// + `allowed_overage`
    ///
    /// `allowed_overage` is how far off the actual data encoding is
    /// from the target limit that was set. It is an improvement when
    /// the allowed_overage decreses.
    ///
    /// Note this overhead will likely always be greater than zero to
    /// account for encoding overhead such as IPC headers and padding.
    ///
    ///
    async fn verify_encoded_split(batch: RecordBatch, allowed_overage: usize) {
        let num_rows = batch.num_rows();

        // Track the overall required maximum overage
        let mut max_overage_seen = 0;

        for max_flight_data_size in [1024, 2021, 5000] {
            println!("Encoding {num_rows} with a maximum size of {max_flight_data_size}");

            let mut stream = FlightDataEncoderBuilder::new()
                .with_max_flight_data_size(max_flight_data_size)
                // use 8-byte alignment - default alignment is 64 which produces bigger ipc data
                .with_options(IpcWriteOptions::try_new(8, false, MetadataVersion::V5).unwrap())
                .build(futures::stream::iter([Ok(batch.clone())]));

            let mut i = 0;
            while let Some(data) = stream.next().await.transpose().unwrap() {
                let actual_data_size = flight_data_size(&data);

                let actual_overage = if actual_data_size > max_flight_data_size {
                    actual_data_size - max_flight_data_size
                } else {
                    0
                };

                assert!(
                    actual_overage <= allowed_overage,
                    "encoded data[{i}]: actual size {actual_data_size}, \
                         actual_overage: {actual_overage} \
                         allowed_overage: {allowed_overage}"
                );

                i += 1;

                max_overage_seen = max_overage_seen.max(actual_overage)
            }
        }

        // ensure that the specified overage is exactly the maxmium so
        // that when the splitting logic improves, the tests must be
        // updated to reflect the better logic
        assert_eq!(
            allowed_overage, max_overage_seen,
            "Specified overage was too high"
        );
    }
}
