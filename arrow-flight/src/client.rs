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
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, PollInfo, PutResult, Ticket,
    decode::FlightRecordBatchStream,
    flight_service_client::FlightServiceClient,
    r#gen::{CancelFlightInfoRequest, CancelFlightInfoResult, RenewFlightEndpointRequest},
    trailers::extract_lazy_trailers,
};
use arrow_schema::Schema;
use bytes::Bytes;
use futures::{
    Stream, StreamExt, TryStreamExt,
    future::ready,
    stream::{self, BoxStream},
};
use prost::Message;
use tonic::codegen::{Body, StdError};
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
pub struct FlightClient<T = Channel> {
    /// Optional grpc header metadata to include with each request
    metadata: MetadataMap,

    /// The inner client
    inner: FlightServiceClient<T>,
}

impl<T> FlightClient<T>
where
    T: tonic::client::GrpcService<tonic::body::Body>,
    T::Error: Into<StdError>,
    T::ResponseBody: Body<Data = Bytes> + std::marker::Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + std::marker::Send,
{
    /// Creates a client with the provided transport
    pub fn new(inner: T) -> Self {
        Self::new_from_inner(FlightServiceClient::new(inner))
    }

    /// Creates a new higher level client with the provided lower level client
    pub fn new_from_inner(inner: FlightServiceClient<T>) -> Self {
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
    pub fn inner(&self) -> &FlightServiceClient<T> {
        &self.inner
    }

    /// Return a mutable reference to the underlying tonic
    /// [`FlightServiceClient`]
    pub fn inner_mut(&mut self) -> &mut FlightServiceClient<T> {
        &mut self.inner
    }

    /// Consume this client and return the underlying tonic
    /// [`FlightServiceClient`]
    pub fn into_inner(self) -> FlightServiceClient<T> {
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
            response_stream.map_err(|status| status.into()),
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
            .map_err(|status| status.into());

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
            .map_err(|status| status.into());

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
            .map_err(|status| status.into())
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
    fn make_request<R>(&self, t: R) -> tonic::Request<R> {
        // Pass along metadata
        let mut request = tonic::Request::new(t);
        *request.metadata_mut() = self.metadata.clone();
        request
    }
}

#[cfg(test)]
mod tests {
    use super::FlightClient;
    use crate::encode::FlightDataEncoderBuilder;
    use crate::flight_service_server::{FlightService, FlightServiceServer};
    use crate::{
        Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
        HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    };
    use arrow_array::{RecordBatch, UInt64Array};
    use bytes::Bytes;
    use futures::{StreamExt, TryStreamExt, stream::BoxStream};
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;
    use tonic::metadata::MetadataMap;
    use tonic::service::interceptor::InterceptedService;
    use tonic::transport::Channel;
    use tonic::{Request, Response, Status, Streaming};
    use uuid::Uuid;

    /// Minimal `FlightService` that records request metadata and serves a
    /// configured `do_get` response. Other RPCs return `Unimplemented`.
    #[derive(Debug, Clone, Default)]
    struct InterceptorTestServer {
        state: Arc<Mutex<InterceptorTestState>>,
    }

    #[derive(Debug, Default)]
    struct InterceptorTestState {
        do_get_request: Option<Ticket>,
        do_get_response: Option<Vec<Result<RecordBatch, Status>>>,
        last_request_metadata: Option<MetadataMap>,
    }

    impl InterceptorTestServer {
        fn save_metadata<T>(&self, request: &Request<T>) {
            self.state.lock().unwrap().last_request_metadata = Some(request.metadata().clone());
        }

        fn set_do_get_response(&self, response: Vec<Result<RecordBatch, Status>>) {
            self.state.lock().unwrap().do_get_response = Some(response);
        }

        fn take_do_get_request(&self) -> Option<Ticket> {
            self.state.lock().unwrap().do_get_request.take()
        }

        fn take_last_request_metadata(&self) -> Option<MetadataMap> {
            self.state.lock().unwrap().last_request_metadata.take()
        }
    }

    #[tonic::async_trait]
    impl FlightService for InterceptorTestServer {
        type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
        type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
        type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
        type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
        type DoActionStream = BoxStream<'static, Result<crate::Result, Status>>;
        type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
        type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

        async fn do_get(
            &self,
            request: Request<Ticket>,
        ) -> Result<Response<Self::DoGetStream>, Status> {
            self.save_metadata(&request);
            let mut state = self.state.lock().unwrap();
            state.do_get_request = Some(request.into_inner());

            let batches = state
                .do_get_response
                .take()
                .ok_or_else(|| Status::internal("no do_get response configured"))?;
            let batch_stream = futures::stream::iter(batches).map_err(Into::into);
            let stream = FlightDataEncoderBuilder::new()
                .build(batch_stream)
                .map_err(Into::into);
            Ok(Response::new(stream.boxed()))
        }

        async fn handshake(
            &self,
            _: Request<Streaming<HandshakeRequest>>,
        ) -> Result<Response<Self::HandshakeStream>, Status> {
            Err(Status::unimplemented(""))
        }
        async fn list_flights(
            &self,
            _: Request<Criteria>,
        ) -> Result<Response<Self::ListFlightsStream>, Status> {
            Err(Status::unimplemented(""))
        }
        async fn get_flight_info(
            &self,
            _: Request<FlightDescriptor>,
        ) -> Result<Response<FlightInfo>, Status> {
            Err(Status::unimplemented(""))
        }
        async fn poll_flight_info(
            &self,
            _: Request<FlightDescriptor>,
        ) -> Result<Response<PollInfo>, Status> {
            Err(Status::unimplemented(""))
        }
        async fn get_schema(
            &self,
            _: Request<FlightDescriptor>,
        ) -> Result<Response<SchemaResult>, Status> {
            Err(Status::unimplemented(""))
        }
        async fn do_put(
            &self,
            _: Request<Streaming<FlightData>>,
        ) -> Result<Response<Self::DoPutStream>, Status> {
            Err(Status::unimplemented(""))
        }
        async fn do_action(
            &self,
            _: Request<Action>,
        ) -> Result<Response<Self::DoActionStream>, Status> {
            Err(Status::unimplemented(""))
        }
        async fn list_actions(
            &self,
            _: Request<Empty>,
        ) -> Result<Response<Self::ListActionsStream>, Status> {
            Err(Status::unimplemented(""))
        }
        async fn do_exchange(
            &self,
            _: Request<Streaming<FlightData>>,
        ) -> Result<Response<Self::DoExchangeStream>, Status> {
            Err(Status::unimplemented(""))
        }
    }

    /// Spawns the test server on a background task and exposes a connected channel.
    struct InterceptorTestFixture {
        shutdown: Option<tokio::sync::oneshot::Sender<()>>,
        addr: SocketAddr,
        handle: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
    }

    impl InterceptorTestFixture {
        async fn new(server: FlightServiceServer<InterceptorTestServer>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (tx, rx) = tokio::sync::oneshot::channel();
            let shutdown_future = async move {
                rx.await.ok();
            };
            let serve = tonic::transport::Server::builder()
                .timeout(Duration::from_secs(30))
                .add_service(server)
                .serve_with_incoming_shutdown(
                    tokio_stream::wrappers::TcpListenerStream::new(listener),
                    shutdown_future,
                );
            let handle = tokio::task::spawn(serve);
            Self {
                shutdown: Some(tx),
                addr,
                handle: Some(handle),
            }
        }

        async fn channel(&self) -> Channel {
            let url = format!("http://{}", self.addr);
            tonic::transport::Endpoint::from_shared(url)
                .expect("valid endpoint")
                .timeout(Duration::from_secs(30))
                .connect()
                .await
                .expect("error connecting to server")
        }

        async fn shutdown_and_wait(mut self) {
            if let Some(tx) = self.shutdown.take() {
                tx.send(()).expect("server quit early");
            }
            if let Some(handle) = self.handle.take() {
                handle
                    .await
                    .expect("task join error (panic?)")
                    .expect("server error at shutdown");
            }
        }
    }

    /// Integration test: a tonic [`Channel`] wrapped in an [`InterceptedService`]
    /// that injects a custom header is passed to [`FlightClient`], and the server
    /// observes the header on the request.
    #[tokio::test]
    async fn test_flight_client_with_intercepted_channel_passes_custom_header() {
        let test_server = InterceptorTestServer::default();
        let fixture =
            InterceptorTestFixture::new(FlightServiceServer::new(test_server.clone())).await;

        let channel = fixture.channel().await;

        let header_name = "x-random-header";
        let header_value = format!("random-{}", Uuid::new_v4());
        let header_value_for_interceptor = header_value.clone();

        let interceptor = move |mut req: Request<()>| -> Result<Request<()>, Status> {
            req.metadata_mut().insert(
                header_name,
                header_value_for_interceptor
                    .parse()
                    .expect("valid metadata value"),
            );
            Ok(req)
        };

        let intercepted = InterceptedService::new(channel, interceptor);
        let mut client = FlightClient::new(intercepted);

        let ticket = Ticket {
            ticket: Bytes::from("dummy-ticket"),
        };

        let batch = RecordBatch::try_from_iter(vec![(
            "col",
            Arc::new(UInt64Array::from_iter([1, 2, 3, 4])) as _,
        )])
        .unwrap();

        test_server.set_do_get_response(vec![Ok(batch.clone())]);

        let response_stream = client
            .do_get(ticket.clone())
            .await
            .expect("error making do_get request");

        let response: Vec<RecordBatch> = response_stream
            .try_collect()
            .await
            .expect("error streaming data");

        assert_eq!(response, vec![batch]);
        assert_eq!(test_server.take_do_get_request(), Some(ticket));

        let metadata = test_server
            .take_last_request_metadata()
            .expect("server received headers")
            .into_headers();

        let received = metadata
            .get(header_name)
            .expect("interceptor header missing on server")
            .to_str()
            .expect("ascii header value");
        assert_eq!(received, header_value);

        fixture.shutdown_and_wait().await;
    }
}
