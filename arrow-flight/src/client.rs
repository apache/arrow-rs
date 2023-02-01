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

use std::{
    sync::{Arc, Mutex},
    task::Poll,
};

use crate::{
    decode::FlightRecordBatchStream, flight_service_client::FlightServiceClient, Action,
    ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, PutResult, Ticket,
};
use arrow_schema::Schema;
use bytes::Bytes;
use futures::{
    future::ready,
    ready,
    stream::{self, BoxStream},
    Stream, StreamExt, TryStreamExt,
};
use tonic::{metadata::MetadataMap, transport::Channel};

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
    /// [`RecordBatch`](arrow_array::RecordBatch)es.
    ///
    /// # Note
    ///
    /// To access the returned [`FlightData`] use
    /// [`FlightRecordBatchStream::into_inner()`]
    ///
    /// # Example:
    /// ```no_run
    /// # async fn run() {
    /// # use bytes::Bytes;
    /// # use arrow_flight::FlightClient;
    /// # use arrow_flight::Ticket;
    /// # use arrow_array::RecordBatch;
    /// # use futures::stream::TryStreamExt;
    /// # let channel: tonic::transport::Channel = unimplemented!();
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

        let response_stream = self
            .inner
            .do_get(request)
            .await?
            .into_inner()
            .map_err(FlightError::Tonic);

        Ok(FlightRecordBatchStream::new_from_flight_data(
            response_stream,
        ))
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
    /// # let channel: tonic::transport::Channel = unimplemented!();
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

    /// Make a `DoPut` call to the server with the provided
    /// [`Stream`](futures::Stream) of [`FlightData`] and returning a
    /// stream of [`PutResult`].
    ///
    /// # Note
    ///
    /// The input stream is [`Result`] so that this can be connected
    /// to a streaming data source (such as [`FlightDataEncoder`])
    /// without having to buffer. If the input stream returns an error
    /// that error will not be sent to the server, instead it will be
    /// placed into the result stream and the server connection
    /// terminated.
    ///
    /// # Example:
    /// ```no_run
    /// # async fn run() {
    /// # use futures::{TryStreamExt, StreamExt};
    /// # use std::sync::Arc;
    /// # use arrow_array::UInt64Array;
    /// # use arrow_array::RecordBatch;
    /// # use arrow_flight::{FlightClient, FlightDescriptor, PutResult};
    /// # use arrow_flight::encode::FlightDataEncoderBuilder;
    /// # let batch = RecordBatch::try_from_iter(vec![
    /// #  ("col2", Arc::new(UInt64Array::from_iter([10, 23, 33])) as _)
    /// # ]).unwrap();
    /// # let channel: tonic::transport::Channel = unimplemented!();
    /// let mut client = FlightClient::new(channel);
    ///
    /// // encode the batch as a stream of `FlightData`
    /// let flight_data_stream = FlightDataEncoderBuilder::new()
    ///   .build(futures::stream::iter(vec![Ok(batch)]));
    ///
    /// // send the stream and get the results as `PutResult`
    /// let response: Vec<PutResult>= client
    ///   .do_put(flight_data_stream)
    ///   .await
    ///   .unwrap()
    ///   .try_collect() // use TryStreamExt to collect stream
    ///   .await
    ///   .expect("error calling do_put");
    /// # }
    /// ```
    pub async fn do_put<S: Stream<Item = Result<FlightData>> + Send + 'static>(
        &mut self,
        request: S,
    ) -> Result<BoxStream<'static, Result<PutResult>>> {
        let ok_stream = FallibleStream::new(request.boxed());
        let builder = ok_stream.builder();

        // send ok result to the server
        let request = self.make_request(ok_stream);

        let response_stream = self
            .inner
            .do_put(request)
            .await?
            .into_inner()
            .map_err(FlightError::Tonic);

        // combine the response from the server and any error from the client
        Ok(builder.build(response_stream.boxed()))
    }

    /// Make a `DoExchange` call to the server with the provided
    /// [`Stream`](futures::Stream) of [`FlightData`] and returning a
    /// stream of [`FlightData`].
    ///
    /// # Example:
    /// ```no_run
    /// # async fn run() {
    /// # use futures::{TryStreamExt, StreamExt};
    /// # use std::sync::Arc;
    /// # use arrow_array::UInt64Array;
    /// # use arrow_array::RecordBatch;
    /// # use arrow_flight::{FlightClient, FlightDescriptor, PutResult};
    /// # use arrow_flight::encode::FlightDataEncoderBuilder;
    /// # let batch = RecordBatch::try_from_iter(vec![
    /// #  ("col2", Arc::new(UInt64Array::from_iter([10, 23, 33])) as _)
    /// # ]).unwrap();
    /// # let channel: tonic::transport::Channel = unimplemented!();
    /// let mut client = FlightClient::new(channel);
    ///
    /// // encode the batch as a stream of `FlightData`
    /// let flight_data_stream = FlightDataEncoderBuilder::new()
    ///   .build(futures::stream::iter(vec![Ok(batch)]))
    ///   // data encoder return Results, but do_exchange requires FlightData
    ///   .map(|batch|batch.unwrap());
    ///
    /// // send the stream and get the results as `RecordBatches`
    /// let response: Vec<RecordBatch> = client
    ///   .do_exchange(flight_data_stream)
    ///   .await
    ///   .unwrap()
    ///   .try_collect() // use TryStreamExt to collect stream
    ///   .await
    ///   .expect("error calling do_exchange");
    /// # }
    /// ```
    pub async fn do_exchange<S: Stream<Item = FlightData> + Send + 'static>(
        &mut self,
        request: S,
    ) -> Result<FlightRecordBatchStream> {
        let request = self.make_request(request);

        let response = self
            .inner
            .do_exchange(request)
            .await?
            .into_inner()
            .map_err(FlightError::Tonic);

        Ok(FlightRecordBatchStream::new_from_flight_data(response))
    }

    /// Make a `ListFlights` call to the server with the provided
    /// critera and returning a [`Stream`](futures::Stream) of [`FlightInfo`].
    ///
    /// # Example:
    /// ```no_run
    /// # async fn run() {
    /// # use futures::TryStreamExt;
    /// # use bytes::Bytes;
    /// # use arrow_flight::{FlightInfo, FlightClient};
    /// # let channel: tonic::transport::Channel = unimplemented!();
    /// let mut client = FlightClient::new(channel);
    ///
    /// // Send 'Name=Foo' bytes as the "expression" to the server
    /// // and gather the returned FlightInfo
    /// let responses: Vec<FlightInfo> = client
    ///   .list_flights(Bytes::from("Name=Foo"))
    ///   .await
    ///   .expect("error listing flights")
    ///   .try_collect() // use TryStreamExt to collect stream
    ///   .await
    ///   .expect("error gathering flights");
    /// # }
    /// ```
    pub async fn list_flights(
        &mut self,
        expression: impl Into<Bytes>,
    ) -> Result<BoxStream<'static, Result<FlightInfo>>> {
        let request = Criteria {
            expression: expression.into(),
        };

        let request = self.make_request(request);

        let response = self
            .inner
            .list_flights(request)
            .await?
            .into_inner()
            .map_err(FlightError::Tonic);

        Ok(response.boxed())
    }

    /// Make a `GetSchema` call to the server with the provided
    /// [`FlightDescriptor`] and returning the associated [`Schema`].
    ///
    /// # Example:
    /// ```no_run
    /// # async fn run() {
    /// # use bytes::Bytes;
    /// # use arrow_flight::{FlightDescriptor, FlightClient};
    /// # use arrow_schema::Schema;
    /// # let channel: tonic::transport::Channel = unimplemented!();
    /// let mut client = FlightClient::new(channel);
    ///
    /// // Request the schema result of a 'CMD' request to the server
    /// let request = FlightDescriptor::new_cmd(b"MOAR DATA".to_vec());
    ///
    /// let schema: Schema = client
    ///   .get_schema(request)
    ///   .await
    ///   .expect("error making request");
    /// # }
    /// ```
    pub async fn get_schema(
        &mut self,
        flight_descriptor: FlightDescriptor,
    ) -> Result<Schema> {
        let request = self.make_request(flight_descriptor);

        let schema_result = self.inner.get_schema(request).await?.into_inner();

        // attempt decode from IPC
        let schema: Schema = schema_result.try_into()?;

        Ok(schema)
    }

    /// Make a `ListActions` call to the server and returning a
    /// [`Stream`](futures::Stream) of [`ActionType`].
    ///
    /// # Example:
    /// ```no_run
    /// # async fn run() {
    /// # use futures::TryStreamExt;
    /// # use arrow_flight::{ActionType, FlightClient};
    /// # use arrow_schema::Schema;
    /// # let channel: tonic::transport::Channel = unimplemented!();
    /// let mut client = FlightClient::new(channel);
    ///
    /// // List available actions on the server:
    /// let actions: Vec<ActionType> = client
    ///   .list_actions()
    ///   .await
    ///   .expect("error listing actions")
    ///   .try_collect() // use TryStreamExt to collect stream
    ///   .await
    ///   .expect("error gathering actions");
    /// # }
    /// ```
    pub async fn list_actions(
        &mut self,
    ) -> Result<BoxStream<'static, Result<ActionType>>> {
        let request = self.make_request(Empty {});

        let action_stream = self
            .inner
            .list_actions(request)
            .await?
            .into_inner()
            .map_err(FlightError::Tonic);

        Ok(action_stream.boxed())
    }

    /// Make a `DoAction` call to the server and returning a
    /// [`Stream`](futures::Stream) of opaque [`Bytes`].
    ///
    /// # Example:
    /// ```no_run
    /// # async fn run() {
    /// # use bytes::Bytes;
    /// # use futures::TryStreamExt;
    /// # use arrow_flight::{Action, FlightClient};
    /// # use arrow_schema::Schema;
    /// # let channel: tonic::transport::Channel = unimplemented!();
    /// let mut client = FlightClient::new(channel);
    ///
    /// let request = Action::new("my_action", "the body");
    ///
    /// // Make a request to run the action on the server
    /// let results: Vec<Bytes> = client
    ///   .do_action(request)
    ///   .await
    ///   .expect("error executing acton")
    ///   .try_collect() // use TryStreamExt to collect stream
    ///   .await
    ///   .expect("error gathering action results");
    /// # }
    /// ```
    pub async fn do_action(
        &mut self,
        action: Action,
    ) -> Result<BoxStream<'static, Result<Bytes>>> {
        let request = self.make_request(action);

        let result_stream = self
            .inner
            .do_action(request)
            .await?
            .into_inner()
            .map_err(FlightError::Tonic)
            .map(|r| {
                r.map(|r| {
                    // unwrap inner bytes
                    let crate::Result { body } = r;
                    body
                })
            });

        Ok(result_stream.boxed())
    }

    /// return a Request, adding any configured metadata
    fn make_request<T>(&self, t: T) -> tonic::Request<T> {
        // Pass along metadata
        let mut request = tonic::Request::new(t);
        *request.metadata_mut() = self.metadata.clone();
        request
    }
}

/// A stream that reads `Results`, and passes along the OK variants,
/// and saves any Errors seen to be forward along with responses
///
/// If the input stream produces an an error, the error is saved in `err`
/// and this stream is ended (the inner is not pollled any more)
struct FallibleStream {
    input_stream: BoxStream<'static, Result<FlightData>>,
    err: Arc<Mutex<Option<FlightError>>>,
    done: bool,
}

impl FallibleStream {
    fn new(input_stream: BoxStream<'static, Result<FlightData>>) -> Self {
        Self {
            input_stream,
            done: false,
            err: Arc::new(Mutex::new(None)),
        }
    }

    /// Returns a builder for wrapping result streams
    fn builder(&self) -> StreamWrapperBuilder {
        StreamWrapperBuilder {
            maybe_err: Arc::clone(&self.err),
        }
    }
}

impl Stream for FallibleStream {
    type Item = FlightData;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }

        match ready!(self.input_stream.poll_next_unpin(cx)) {
            Some(data) => match data {
                Ok(ok) => Poll::Ready(Some(ok)),
                Err(e) => {
                    *self.err.lock().expect("non poisoned") = Some(e);
                    self.done = true;
                    Poll::Ready(None)
                }
            },
            // input stream was done
            None => {
                self.done = true;
                Poll::Ready(None)
            }
        }
    }
}

/// A builder for wrapping server result streams that return either
/// the error from the provided client stream or the error from the server
struct StreamWrapperBuilder {
    maybe_err: Arc<Mutex<Option<FlightError>>>,
}

impl StreamWrapperBuilder {
    /// wraps response stream to return items from response_stream or
    /// the client stream error, if any
    /// Produce a stream that reads results from the server, first
    /// checking to see if the client stream generated an error
    fn build(
        self,
        response_stream: BoxStream<'static, Result<PutResult>>,
    ) -> BoxStream<'static, Result<PutResult>> {
        let state = StreamAndError {
            maybe_err: self.maybe_err,
            response_stream,
        };

        futures::stream::unfold(state, |mut state| async move {
            state.next().await.map(|item| (item, state))
        })
        .boxed()
    }
}

struct StreamAndError {
    // error from a FallableStream
    maybe_err: Arc<Mutex<Option<FlightError>>>,
    response_stream: BoxStream<'static, Result<PutResult>>,
}

impl StreamAndError {
    /// get the next result to pass along
    async fn next(&mut self) -> Option<Result<PutResult>> {
        // if the client made an error return that
        let next_item = self.maybe_err.lock().expect("non poisoned").take();
        if let Some(e) = next_item {
            return Some(Err(e));
        }
        // otherwise return the next item from the server, if any
        else {
            self.response_stream.next().await
        }
    }
}
