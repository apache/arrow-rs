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
    decode::FlightRecordBatchStream,
    flight_service_client::FlightServiceClient,
    gen::{CancelFlightInfoRequest, CancelFlightInfoResult, RenewFlightEndpointRequest},
    trailers::extract_lazy_trailers,
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, PollInfo, PutResult, Ticket,
};
use arrow_schema::Schema;
use bytes::Bytes;
use futures::{
    future::ready,
    stream::{self, BoxStream},
    Stream, StreamExt, TryStreamExt,
};
use prost::Message;
use tonic::{metadata::MetadataMap, transport::Channel};

use crate::error::{FlightError, Result};
use crate::streams::{FallibleRequestStream, FallibleTonicResponseStream};

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
    /// Creates a client client with the provided [`Channel`]
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

        let (md, response_stream, _ext) = self.inner.do_get(request).await?.into_parts();
        let (response_stream, trailers) = extract_lazy_trailers(response_stream);

        Ok(FlightRecordBatchStream::new_from_flight_data(
            response_stream.map_err(FlightError::Tonic),
        )
        .with_headers(md)
        .with_trailers(trailers))
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
    pub async fn get_flight_info(&mut self, descriptor: FlightDescriptor) -> Result<FlightInfo> {
        let request = self.make_request(descriptor);

        let response = self.inner.get_flight_info(request).await?.into_inner();
        Ok(response)
    }

    /// Make a `PollFlightInfo` call to the server with the provided
    /// [`FlightDescriptor`] and return the [`PollInfo`] from the
    /// server.
    ///
    /// The `info` field of the [`PollInfo`] can be used with
    /// [`Self::do_get`] to retrieve the requested batches.
    ///
    /// If the `flight_descriptor` field of the [`PollInfo`] is
    /// `None` then the `info` field represents the complete results.
    ///
    /// If the `flight_descriptor` field is some [`FlightDescriptor`]
    /// then the `info` field has incomplete results, and the client
    /// should call this method again with the new `flight_descriptor`
    /// to get the updated status.
    ///
    /// The `expiration_time`, if set, represents the expiration time
    /// of the `flight_descriptor`, after which the server may not accept
    /// this retry descriptor and may cancel the query.
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
    /// let poll_info = client
    ///   .poll_flight_info(request)
    ///   .await
    ///   .expect("error handshaking");
    ///
    /// // retrieve the first endpoint from the returned poll info
    /// let ticket = poll_info
    ///   .info
    ///   .expect("expected flight info")
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
    pub async fn poll_flight_info(&mut self, descriptor: FlightDescriptor) -> Result<PollInfo> {
        let request = self.make_request(descriptor);

        let response = self.inner.poll_flight_info(request).await?.into_inner();
        Ok(response)
    }

    /// Make a `DoPut` call to the server with the provided
    /// [`Stream`] of [`FlightData`] and returning a
    /// stream of [`PutResult`].
    ///
    /// # Note
    ///
    /// The input stream is [`Result`] so that this can be connected
    /// to a streaming data source, such as [`FlightDataEncoder`](crate::encode::FlightDataEncoder),
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
        let (sender, receiver) = futures::channel::oneshot::channel();

        // Intercepts client errors and sends them to the oneshot channel above
        let request = Box::pin(request); // Pin to heap
        let request_stream = FallibleRequestStream::new(sender, request);

        let request = self.make_request(request_stream);
        let response_stream = self.inner.do_put(request).await?.into_inner();

        // Forwards errors from the error oneshot with priority over responses from server
        let response_stream = Box::pin(response_stream);
        let error_stream = FallibleTonicResponseStream::new(receiver, response_stream);

        // combine the response from the server and any error from the client
        Ok(error_stream.boxed())
    }

    /// Make a `DoExchange` call to the server with the provided
    /// [`Stream`] of [`FlightData`] and returning a
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
    ///   .build(futures::stream::iter(vec![Ok(batch)]));
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
    pub async fn do_exchange<S: Stream<Item = Result<FlightData>> + Send + 'static>(
        &mut self,
        request: S,
    ) -> Result<FlightRecordBatchStream> {
        let (sender, receiver) = futures::channel::oneshot::channel();

        let request = Box::pin(request);
        // Intercepts client errors and sends them to the oneshot channel above
        let request_stream = FallibleRequestStream::new(sender, request);

        let request = self.make_request(request_stream);
        let response_stream = self.inner.do_exchange(request).await?.into_inner();

        let response_stream = Box::pin(response_stream);
        let error_stream = FallibleTonicResponseStream::new(receiver, response_stream);

        // combine the response from the server and any error from the client
        Ok(FlightRecordBatchStream::new_from_flight_data(error_stream))
    }

    /// Make a `ListFlights` call to the server with the provided
    /// criteria and returning a [`Stream`] of [`FlightInfo`].
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
    pub async fn get_schema(&mut self, flight_descriptor: FlightDescriptor) -> Result<Schema> {
        let request = self.make_request(flight_descriptor);

        let schema_result = self.inner.get_schema(request).await?.into_inner();

        // attempt decode from IPC
        let schema: Schema = schema_result.try_into()?;

        Ok(schema)
    }

    /// Make a `ListActions` call to the server and returning a
    /// [`Stream`] of [`ActionType`].
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
    pub async fn list_actions(&mut self) -> Result<BoxStream<'static, Result<ActionType>>> {
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
    /// [`Stream`] of opaque [`Bytes`].
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
    pub async fn do_action(&mut self, action: Action) -> Result<BoxStream<'static, Result<Bytes>>> {
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

    /// Make a `CancelFlightInfo` call to the server and return
    /// a [`CancelFlightInfoResult`].
    ///
    /// # Example:
    /// ```no_run
    /// # async fn run() {
    /// # use arrow_flight::{CancelFlightInfoRequest, FlightClient, FlightDescriptor};
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
    /// // Cancel the query
    /// let request = CancelFlightInfoRequest::new(flight_info);
    /// let result = client
    ///   .cancel_flight_info(request)
    ///   .await
    ///   .expect("error cancelling");
    /// # }
    /// ```
    pub async fn cancel_flight_info(
        &mut self,
        request: CancelFlightInfoRequest,
    ) -> Result<CancelFlightInfoResult> {
        let action = Action::new("CancelFlightInfo", request.encode_to_vec());
        let response = self.do_action(action).await?.try_next().await?;
        let response = response.ok_or(FlightError::protocol(
            "Received no response for cancel_flight_info call",
        ))?;
        CancelFlightInfoResult::decode(response)
            .map_err(|e| FlightError::DecodeError(e.to_string()))
    }

    /// Make a `RenewFlightEndpoint` call to the server and return
    /// the renewed [`FlightEndpoint`].
    ///
    /// # Example:
    /// ```no_run
    /// # async fn run() {
    /// # use arrow_flight::{FlightClient, FlightDescriptor, RenewFlightEndpointRequest};
    /// # let channel: tonic::transport::Channel = unimplemented!();
    /// let mut client = FlightClient::new(channel);
    ///
    /// // Send a 'CMD' request to the server
    /// let request = FlightDescriptor::new_cmd(b"MOAR DATA".to_vec());
    /// let flight_endpoint = client
    ///   .get_flight_info(request)
    ///   .await
    ///   .expect("error handshaking")
    ///   .endpoint[0];
    ///
    /// // Renew the endpoint
    /// let request = RenewFlightEndpointRequest::new(flight_endpoint);
    /// let flight_endpoint = client
    ///   .renew_flight_endpoint(request)
    ///   .await
    ///   .expect("error renewing");
    /// # }
    /// ```
    pub async fn renew_flight_endpoint(
        &mut self,
        request: RenewFlightEndpointRequest,
    ) -> Result<FlightEndpoint> {
        let action = Action::new("RenewFlightEndpoint", request.encode_to_vec());
        let response = self.do_action(action).await?.try_next().await?;
        let response = response.ok_or(FlightError::protocol(
            "Received no response for renew_flight_endpoint call",
        ))?;
        FlightEndpoint::decode(response).map_err(|e| FlightError::DecodeError(e.to_string()))
    }

    /// return a Request, adding any configured metadata
    fn make_request<T>(&self, t: T) -> tonic::Request<T> {
        // Pass along metadata
        let mut request = tonic::Request::new(t);
        *request.metadata_mut() = self.metadata.clone();
        request
    }
}
