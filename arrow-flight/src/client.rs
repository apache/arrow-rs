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
    decode::FlightRecordBatchStream, flight_service_client::FlightServiceClient,
    FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, PutResult, Ticket,
};
use bytes::Bytes;
use futures::{
    future::ready,
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
    ///   // data encoder return Results, but do_put requires FlightData
    ///   .map(|batch|batch.unwrap());
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
    pub async fn do_put<S: Stream<Item = FlightData> + Send + 'static>(
        &mut self,
        request: S,
    ) -> Result<BoxStream<'static, Result<PutResult>>> {
        let request = self.make_request(request);

        let response = self
            .inner
            .do_put(request)
            .await?
            .into_inner()
            .map_err(FlightError::Tonic);

        Ok(response.boxed())
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
    ///   // data encoder return Results, but do_put requires FlightData
    ///   .map(|batch|batch.unwrap());
    ///
    /// // send the stream and get the results as `RecordBatches`
    /// let response: Vec<RecordBatch> = client
    ///   .do_exchange(flight_data_stream)
    ///   .await
    ///   .unwrap()
    ///   .try_collect() // use TryStreamExt to collect stream
    ///   .await
    ///   .expect("error calling do_put");
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

    // TODO other methods
    // get_schema
    // do_action
    // list_actions
    // list_flights

    /// return a Request, adding any configured metadata
    fn make_request<T>(&self, t: T) -> tonic::Request<T> {
        // Pass along metadata
        let mut request = tonic::Request::new(t);
        *request.metadata_mut() = self.metadata.clone();
        request
    }
}
