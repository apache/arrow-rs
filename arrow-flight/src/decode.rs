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

use crate::{trailers::LazyTrailers, utils::flight_data_to_arrow_batch, FlightData};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_buffer::Buffer;
use arrow_schema::{Schema, SchemaRef};
use bytes::Bytes;
use futures::{ready, stream::BoxStream, Stream, StreamExt};
use std::{collections::HashMap, fmt::Debug, pin::Pin, sync::Arc, task::Poll};
use tonic::metadata::MetadataMap;

use crate::error::{FlightError, Result};

/// Decodes a [Stream] of [`FlightData`] back into
/// [`RecordBatch`]es. This can be used to decode the response from an
/// Arrow Flight server
///
/// # Note
/// To access the lower level Flight messages (e.g. to access
/// [`FlightData::app_metadata`]), you can call [`Self::into_inner`]
/// and use the [`FlightDataDecoder`] directly.
///
/// # Example:
/// ```no_run
/// # async fn f() -> Result<(), arrow_flight::error::FlightError>{
/// # use bytes::Bytes;
/// // make a do_get request
/// use arrow_flight::{
///   error::Result,
///   decode::FlightRecordBatchStream,
///   Ticket,
///   flight_service_client::FlightServiceClient
/// };
/// use tonic::transport::Channel;
/// use futures::stream::{StreamExt, TryStreamExt};
///
/// let client: FlightServiceClient<Channel> = // make client..
/// # unimplemented!();
///
/// let request = tonic::Request::new(
///   Ticket { ticket: Bytes::new() }
/// );
///
/// // Get a stream of FlightData;
/// let flight_data_stream = client
///   .do_get(request)
///   .await?
///   .into_inner();
///
/// // Decode stream of FlightData to RecordBatches
/// let record_batch_stream = FlightRecordBatchStream::new_from_flight_data(
///   // convert tonic::Status to FlightError
///   flight_data_stream.map_err(|e| e.into())
/// );
///
/// // Read back RecordBatches
/// while let Some(batch) = record_batch_stream.next().await {
///   match batch {
///     Ok(batch) => { /* process batch */ },
///     Err(e) => { /* handle error */ },
///   };
/// }
///
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct FlightRecordBatchStream {
    /// Optional grpc header metadata.
    headers: MetadataMap,

    /// Optional grpc trailer metadata.
    trailers: Option<LazyTrailers>,

    inner: FlightDataDecoder,
}

impl FlightRecordBatchStream {
    /// Create a new [`FlightRecordBatchStream`] from a decoded stream
    pub fn new(inner: FlightDataDecoder) -> Self {
        Self {
            inner,
            headers: MetadataMap::default(),
            trailers: None,
        }
    }

    /// Create a new [`FlightRecordBatchStream`] from a stream of [`FlightData`]
    pub fn new_from_flight_data<S>(inner: S) -> Self
    where
        S: Stream<Item = Result<FlightData>> + Send + 'static,
    {
        Self {
            inner: FlightDataDecoder::new(inner),
            headers: MetadataMap::default(),
            trailers: None,
        }
    }

    /// Record response headers.
    pub fn with_headers(self, headers: MetadataMap) -> Self {
        Self { headers, ..self }
    }

    /// Record response trailers.
    pub fn with_trailers(self, trailers: LazyTrailers) -> Self {
        Self {
            trailers: Some(trailers),
            ..self
        }
    }

    /// Headers attached to this stream.
    pub fn headers(&self) -> &MetadataMap {
        &self.headers
    }

    /// Trailers attached to this stream.
    ///
    /// Note that this will return `None` until the entire stream is consumed.
    /// Only after calling `next()` returns `None`, might any available trailers be returned.
    pub fn trailers(&self) -> Option<MetadataMap> {
        self.trailers.as_ref().and_then(|trailers| trailers.get())
    }

    /// Has a message defining the schema been received yet?
    #[deprecated = "use schema().is_some() instead"]
    pub fn got_schema(&self) -> bool {
        self.schema().is_some()
    }

    /// Return schema for the stream, if it has been received
    pub fn schema(&self) -> Option<&SchemaRef> {
        self.inner.schema()
    }

    /// Consume self and return the wrapped [`FlightDataDecoder`]
    pub fn into_inner(self) -> FlightDataDecoder {
        self.inner
    }
}

impl futures::Stream for FlightRecordBatchStream {
    type Item = Result<RecordBatch>;

    /// Returns the next [`RecordBatch`] available in this stream, or `None` if
    /// there are no further results available.
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            let had_schema = self.schema().is_some();
            let res = ready!(self.inner.poll_next_unpin(cx));
            match res {
                // Inner exhausted
                None => {
                    return Poll::Ready(None);
                }
                Some(Err(e)) => {
                    return Poll::Ready(Some(Err(e)));
                }
                // translate data
                Some(Ok(data)) => match data.payload {
                    DecodedPayload::Schema(_) if had_schema => {
                        return Poll::Ready(Some(Err(FlightError::protocol(
                            "Unexpectedly saw multiple Schema messages in FlightData stream",
                        ))));
                    }
                    DecodedPayload::Schema(_) => {
                        // Need next message, poll inner again
                    }
                    DecodedPayload::RecordBatch(batch) => {
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    DecodedPayload::None => {
                        // Need next message
                    }
                },
            }
        }
    }
}

/// Wrapper around a stream of [`FlightData`] that handles the details
/// of decoding low level Flight messages into [`Schema`] and
/// [`RecordBatch`]es, including details such as dictionaries.
///
/// # Protocol Details
///
/// The client handles flight messages as followes:
///
/// - **None:** This message has no effect. This is useful to
///   transmit metadata without any actual payload.
///
/// - **Schema:** The schema is (re-)set. Dictionaries are cleared and
///   the decoded schema is returned.
///
/// - **Dictionary Batch:** A new dictionary for a given column is registered. An existing
///   dictionary for the same column will be overwritten. This
///   message is NOT visible.
///
/// - **Record Batch:** Record batch is created based on the current
///   schema and dictionaries. This fails if no schema was transmitted
///   yet.
///
/// All other message types (at the time of writing: e.g. tensor and
/// sparse tensor) lead to an error.
///
/// Example usecases
///
/// 1. Using this low level stream it is possible to receive a steam
///    of RecordBatches in FlightData that have different schemas by
///    handling multiple schema messages separately.
pub struct FlightDataDecoder {
    /// Underlying data stream
    response: BoxStream<'static, Result<FlightData>>,
    /// Decoding state
    state: Option<FlightStreamState>,
    /// Seen the end of the inner stream?
    done: bool,
}

impl Debug for FlightDataDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlightDataDecoder")
            .field("response", &"<stream>")
            .field("state", &self.state)
            .field("done", &self.done)
            .finish()
    }
}

impl FlightDataDecoder {
    /// Create a new wrapper around the stream of [`FlightData`]
    pub fn new<S>(response: S) -> Self
    where
        S: Stream<Item = Result<FlightData>> + Send + 'static,
    {
        Self {
            state: None,
            response: response.boxed(),
            done: false,
        }
    }

    /// Returns the current schema for this stream
    pub fn schema(&self) -> Option<&SchemaRef> {
        self.state.as_ref().map(|state| &state.schema)
    }

    /// Extracts flight data from the next message, updating decoding
    /// state as necessary.
    fn extract_message(&mut self, data: FlightData) -> Result<Option<DecodedFlightData>> {
        use arrow_ipc::MessageHeader;
        let message = arrow_ipc::root_as_message(&data.data_header[..])
            .map_err(|e| FlightError::DecodeError(format!("Error decoding root message: {e}")))?;

        match message.header_type() {
            MessageHeader::NONE => Ok(Some(DecodedFlightData::new_none(data))),
            MessageHeader::Schema => {
                let schema = Schema::try_from(&data)
                    .map_err(|e| FlightError::DecodeError(format!("Error decoding schema: {e}")))?;

                let schema = Arc::new(schema);
                let dictionaries_by_field = HashMap::new();

                self.state = Some(FlightStreamState {
                    schema: Arc::clone(&schema),
                    dictionaries_by_field,
                });
                Ok(Some(DecodedFlightData::new_schema(data, schema)))
            }
            MessageHeader::DictionaryBatch => {
                let state = if let Some(state) = self.state.as_mut() {
                    state
                } else {
                    return Err(FlightError::protocol(
                        "Received DictionaryBatch prior to Schema",
                    ));
                };

                let buffer = Buffer::from_bytes(data.data_body.into());
                let dictionary_batch = message.header_as_dictionary_batch().ok_or_else(|| {
                    FlightError::protocol(
                        "Could not get dictionary batch from DictionaryBatch message",
                    )
                })?;

                arrow_ipc::reader::read_dictionary(
                    &buffer,
                    dictionary_batch,
                    &state.schema,
                    &mut state.dictionaries_by_field,
                    &message.version(),
                )
                .map_err(|e| {
                    FlightError::DecodeError(format!("Error decoding ipc dictionary: {e}"))
                })?;

                // Updated internal state, but no decoded message
                Ok(None)
            }
            MessageHeader::RecordBatch => {
                let state = if let Some(state) = self.state.as_ref() {
                    state
                } else {
                    return Err(FlightError::protocol(
                        "Received RecordBatch prior to Schema",
                    ));
                };

                let batch = flight_data_to_arrow_batch(
                    &data,
                    Arc::clone(&state.schema),
                    &state.dictionaries_by_field,
                )
                .map_err(|e| {
                    FlightError::DecodeError(format!("Error decoding ipc RecordBatch: {e}"))
                })?;

                Ok(Some(DecodedFlightData::new_record_batch(data, batch)))
            }
            other => {
                let name = other.variant_name().unwrap_or("UNKNOWN");
                Err(FlightError::protocol(format!("Unexpected message: {name}")))
            }
        }
    }
}

impl futures::Stream for FlightDataDecoder {
    type Item = Result<DecodedFlightData>;
    /// Returns the result of decoding the next [`FlightData`] message
    /// from the server, or `None` if there are no further results
    /// available.
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        loop {
            let res = ready!(self.response.poll_next_unpin(cx));

            return Poll::Ready(match res {
                None => {
                    self.done = true;
                    None // inner is exhausted
                }
                Some(data) => Some(match data {
                    Err(e) => Err(e),
                    Ok(data) => match self.extract_message(data) {
                        Ok(Some(extracted)) => Ok(extracted),
                        Ok(None) => continue, // Need next input message
                        Err(e) => Err(e),
                    },
                }),
            });
        }
    }
}

/// tracks the state needed to reconstruct [`RecordBatch`]es from a
/// streaming flight response.
#[derive(Debug)]
struct FlightStreamState {
    schema: SchemaRef,
    dictionaries_by_field: HashMap<i64, ArrayRef>,
}

/// FlightData and the decoded payload (Schema, RecordBatch), if any
#[derive(Debug)]
pub struct DecodedFlightData {
    /// The original FlightData message
    pub inner: FlightData,
    /// The decoded payload
    pub payload: DecodedPayload,
}

impl DecodedFlightData {
    /// Create a new DecodedFlightData with no payload
    pub fn new_none(inner: FlightData) -> Self {
        Self {
            inner,
            payload: DecodedPayload::None,
        }
    }

    /// Create a new DecodedFlightData with a [`Schema`] payload
    pub fn new_schema(inner: FlightData, schema: SchemaRef) -> Self {
        Self {
            inner,
            payload: DecodedPayload::Schema(schema),
        }
    }

    /// Create a new [`DecodedFlightData`] with a [`RecordBatch`] payload
    pub fn new_record_batch(inner: FlightData, batch: RecordBatch) -> Self {
        Self {
            inner,
            payload: DecodedPayload::RecordBatch(batch),
        }
    }

    /// Return the metadata field of the inner flight data
    pub fn app_metadata(&self) -> Bytes {
        self.inner.app_metadata.clone()
    }
}

/// The result of decoding [`FlightData`]
#[derive(Debug)]
pub enum DecodedPayload {
    /// None (no data was sent in the corresponding FlightData)
    None,

    /// A decoded Schema message
    Schema(SchemaRef),

    /// A decoded Record batch.
    RecordBatch(RecordBatch),
}
