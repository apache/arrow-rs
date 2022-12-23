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

use crate::{
    flight_service_client::FlightServiceClient, utils::flight_data_to_arrow_batch,
    FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, Ticket,
};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::Schema;
use bytes::Bytes;
use futures::{future::ready, ready, stream, StreamExt};
use std::{collections::HashMap, convert::TryFrom, pin::Pin, sync::Arc, task::Poll};
use tonic::{metadata::MetadataMap, transport::Channel, Streaming};

use crate::error::{FlightError, Result};

/// A "Mid level" [Apache Arrow Flight](https://arrow.apache.org/docs/format/Flight.html) client.
///
/// [`FlightClient`] is intended as a convenience for interactions
/// with Arrow Flight servers. For more direct control, such as access
/// to the response headers, use  [`FlightServiceClient`] directly
/// via methods such as [`Self::inner`] or [`Self::into_inner`].
///
/// # Example:
/// ```no_run
/// # async fn run() {
/// # use arrow_flight::FlightClient;
/// # use bytes::Bytes;
/// use tonic::transport::Channel;
/// let channel = Channel::from_static("http://localhost:1234")
///   .connect()
///   .await
///   .expect("error connecting");
///
/// let mut client = FlightClient::new(channel);
///
/// // Send 'Hi' bytes as the handshake request to the server
/// let response = client
///   .handshake(Bytes::from("Hi"))
///   .await
///   .expect("error handshaking");
///
/// // Expect the server responded with 'Ho'
/// assert_eq!(response, Bytes::from("Ho"));
/// # }
/// ```
#[derive(Debug)]
pub struct FlightClient {
    /// Optional grpc header metadata to include with each request
    metadata: MetadataMap,

    /// The inner client
    inner: FlightServiceClient<Channel>,
}

impl FlightClient {
    /// Creates a client client with the provided [`Channel`](tonic::transport::Channel)
    pub fn new(channel: Channel) -> Self {
        Self::new_from_inner(FlightServiceClient::new(channel))
    }

    /// Creates a new higher level client with the provided lower level client
    pub fn new_from_inner(inner: FlightServiceClient<Channel>) -> Self {
        Self {
            metadata: MetadataMap::new(),
            inner,
        }
    }

    /// Return a reference to gRPC metadata included with each request
    pub fn metadata(&self) -> &MetadataMap {
        &self.metadata
    }

    /// Return a reference to gRPC metadata included with each request
    ///
    /// These headers can be used, for example, to include
    /// authorization or other application specific headers.
    pub fn metadata_mut(&mut self) -> &mut MetadataMap {
        &mut self.metadata
    }

    /// Add the specified header with value to all subsequent
    /// requests. See [`Self::metadata_mut`] for fine grained control.
    pub fn add_header(&mut self, key: &str, value: &str) -> Result<()> {
        let key = tonic::metadata::MetadataKey::<_>::from_bytes(key.as_bytes())
            .map_err(|e| FlightError::ExternalError(Box::new(e)))?;

        let value = value
            .parse()
            .map_err(|e| FlightError::ExternalError(Box::new(e)))?;

        // ignore previous value
        self.metadata.insert(key, value);

        Ok(())
    }

    /// Return a reference to the underlying tonic
    /// [`FlightServiceClient`]
    pub fn inner(&self) -> &FlightServiceClient<Channel> {
        &self.inner
    }

    /// Return a mutable reference to the underlying tonic
    /// [`FlightServiceClient`]
    pub fn inner_mut(&mut self) -> &mut FlightServiceClient<Channel> {
        &mut self.inner
    }

    /// Consume this client and return the underlying tonic
    /// [`FlightServiceClient`]
    pub fn into_inner(self) -> FlightServiceClient<Channel> {
        self.inner
    }

    /// Perform an Arrow Flight handshake with the server, sending
    /// `payload` as the [`HandshakeRequest`] payload and returning
    /// the [`HandshakeResponse`](crate::HandshakeResponse)
    /// bytes returned from the server
    ///
    /// See [`FlightClient`] docs for an example.
    pub async fn handshake(&mut self, payload: impl Into<Bytes>) -> Result<Bytes> {
        let request = HandshakeRequest {
            protocol_version: 0,
            payload: payload.into(),
        };

        // apply headers, etc
        let request = self.make_request(stream::once(ready(request)));

        let mut response_stream = self.inner.handshake(request).await?.into_inner();

        if let Some(response) = response_stream.next().await.transpose()? {
            // check if there is another response
            if response_stream.next().await.is_some() {
                return Err(FlightError::protocol(
                    "Got unexpected second response from handshake",
                ));
            }

            Ok(response.payload)
        } else {
            Err(FlightError::protocol("No response from handshake"))
        }
    }

    /// Make a `DoGet` call to the server with the provided ticket,
    /// returning a [`FlightRecordBatchStream`] for reading
    /// [`RecordBatch`]es.
    ///
    /// # Example:
    /// ```no_run
    /// # async fn run() {
    /// # use bytes::Bytes;
    /// # use arrow_flight::FlightClient;
    /// # use arrow_flight::Ticket;
    /// # use arrow_array::RecordBatch;
    /// # use tonic::transport::Channel;
    /// # use futures::stream::TryStreamExt;
    /// # let channel = Channel::from_static("http://localhost:1234")
    /// #  .connect()
    /// #  .await
    /// #  .expect("error connecting");
    /// # let ticket = Ticket { ticket: Bytes::from("foo") };
    /// let mut client = FlightClient::new(channel);
    ///
    /// // Invoke a do_get request on the server with a previously
    /// // received Ticket
    ///
    /// let response = client
    ///    .do_get(ticket)
    ///    .await
    ///    .expect("error invoking do_get");
    ///
    /// // Use try_collect to get the RecordBatches from the server
    /// let batches: Vec<RecordBatch> = response
    ///    .try_collect()
    ///    .await
    ///    .expect("no stream errors");
    /// # }
    /// ```
    pub async fn do_get(&mut self, ticket: Ticket) -> Result<FlightRecordBatchStream> {
        let request = self.make_request(ticket);

        let response = self.inner.do_get(request).await?.into_inner();

        let flight_data_stream = FlightDataStream::new(response);
        Ok(FlightRecordBatchStream::new(flight_data_stream))
    }

    /// Make a `GetFlightInfo` call to the server with the provided
    /// [`FlightDescriptor`] and return the [`FlightInfo`] from the
    /// server. The [`FlightInfo`] can be used with [`Self::do_get`]
    /// to retrieve the requested batches.
    ///
    /// # Example:
    /// ```no_run
    /// # async fn run() {
    /// # use arrow_flight::FlightClient;
    /// # use arrow_flight::FlightDescriptor;
    /// # use tonic::transport::Channel;
    /// # let channel = Channel::from_static("http://localhost:1234")
    /// #   .connect()
    /// #   .await
    /// #   .expect("error connecting");
    /// let mut client = FlightClient::new(channel);
    ///
    /// // Send a 'CMD' request to the server
    /// let request = FlightDescriptor::new_cmd(b"MOAR DATA".to_vec());
    /// let flight_info = client
    ///   .get_flight_info(request)
    ///   .await
    ///   .expect("error handshaking");
    ///
    /// // retrieve the first endpoint from the returned flight info
    /// let ticket = flight_info
    ///   .endpoint[0]
    ///   // Extract the ticket
    ///   .ticket
    ///   .clone()
    ///   .expect("expected ticket");
    ///
    /// // Retrieve the corresponding RecordBatch stream with do_get
    /// let data = client
    ///   .do_get(ticket)
    ///   .await
    ///   .expect("error fetching data");
    /// # }
    /// ```
    pub async fn get_flight_info(
        &mut self,
        descriptor: FlightDescriptor,
    ) -> Result<FlightInfo> {
        let request = self.make_request(descriptor);

        let response = self.inner.get_flight_info(request).await?.into_inner();
        Ok(response)
    }

    // TODO other methods
    // list_flights
    // get_schema
    // do_put
    // do_action
    // list_actions
    // do_exchange

    /// return a Request, adding any configured metadata
    fn make_request<T>(&self, t: T) -> tonic::Request<T> {
        // Pass along metadata
        let mut request = tonic::Request::new(t);
        *request.metadata_mut() = self.metadata.clone();
        request
    }
}

/// A stream of [`RecordBatch`]es from from an Arrow Flight server.
///
/// To access the lower level Flight messages directly, consider
/// calling [`Self::into_inner`] and using the [`FlightDataStream`]
/// directly.
#[derive(Debug)]
pub struct FlightRecordBatchStream {
    inner: FlightDataStream,
    got_schema: bool,
}

impl FlightRecordBatchStream {
    pub fn new(inner: FlightDataStream) -> Self {
        Self {
            inner,
            got_schema: false,
        }
    }

    /// Has a message defining the schema been received yet?
    pub fn got_schema(&self) -> bool {
        self.got_schema
    }

    /// Consume self and return the wrapped [`FlightDataStream`]
    pub fn into_inner(self) -> FlightDataStream {
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
                    DecodedPayload::Schema(_) if self.got_schema => {
                        return Poll::Ready(Some(Err(FlightError::protocol(
                            "Unexpectedly saw multiple Schema messages in FlightData stream",
                        ))));
                    }
                    DecodedPayload::Schema(_) => {
                        self.got_schema = true;
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
/// of RecordBatches in FlightData that have different schemas by
/// handling multiple schema messages separately.
#[derive(Debug)]
pub struct FlightDataStream {
    /// Underlying data stream
    response: Streaming<FlightData>,
    /// Decoding state
    state: Option<FlightStreamState>,
    /// seen the end of the inner stream?
    done: bool,
}

impl FlightDataStream {
    /// Create a new wrapper around the stream of FlightData
    pub fn new(response: Streaming<FlightData>) -> Self {
        Self {
            state: None,
            response,
            done: false,
        }
    }

    /// Extracts flight data from the next message, updating decoding
    /// state as necessary.
    fn extract_message(&mut self, data: FlightData) -> Result<Option<DecodedFlightData>> {
        use arrow_ipc::MessageHeader;
        let message = arrow_ipc::root_as_message(&data.data_header[..]).map_err(|e| {
            FlightError::DecodeError(format!("Error decoding root message: {e}"))
        })?;

        match message.header_type() {
            MessageHeader::NONE => Ok(Some(DecodedFlightData::new_none(data))),
            MessageHeader::Schema => {
                let schema = Schema::try_from(&data).map_err(|e| {
                    FlightError::DecodeError(format!("Error decoding schema: {e}"))
                })?;

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

                let buffer: arrow_buffer::Buffer = data.data_body.into();
                let dictionary_batch =
                    message.header_as_dictionary_batch().ok_or_else(|| {
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
                    FlightError::DecodeError(format!(
                        "Error decoding ipc dictionary: {e}"
                    ))
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
                    FlightError::DecodeError(format!(
                        "Error decoding ipc RecordBatch: {e}"
                    ))
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

impl futures::Stream for FlightDataStream {
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
                    Err(e) => Err(FlightError::Tonic(e)),
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
    schema: Arc<Schema>,
    dictionaries_by_field: HashMap<i64, ArrayRef>,
}

/// FlightData and the decoded payload (Schema, RecordBatch), if any
#[derive(Debug)]
pub struct DecodedFlightData {
    pub inner: FlightData,
    pub payload: DecodedPayload,
}

impl DecodedFlightData {
    pub fn new_none(inner: FlightData) -> Self {
        Self {
            inner,
            payload: DecodedPayload::None,
        }
    }

    pub fn new_schema(inner: FlightData, schema: Arc<Schema>) -> Self {
        Self {
            inner,
            payload: DecodedPayload::Schema(schema),
        }
    }

    pub fn new_record_batch(inner: FlightData, batch: RecordBatch) -> Self {
        Self {
            inner,
            payload: DecodedPayload::RecordBatch(batch),
        }
    }

    /// return the metadata field of the inner flight data
    pub fn app_metadata(&self) -> &[u8] {
        &self.inner.app_metadata
    }
}

/// The result of decoding [`FlightData`]
#[derive(Debug)]
pub enum DecodedPayload {
    /// None (no data was sent in the corresponding FlightData)
    None,

    /// A decoded Schema message
    Schema(Arc<Schema>),

    /// A decoded Record batch.
    RecordBatch(RecordBatch),
}
