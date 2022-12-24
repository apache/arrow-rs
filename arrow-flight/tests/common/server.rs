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

use std::sync::{Arc, Mutex};

use arrow_array::RecordBatch;
use futures::{stream::BoxStream, TryStreamExt};
use tonic::{metadata::MetadataMap, Request, Response, Status, Streaming};

use arrow_flight::{
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    flight_service_server::{FlightService, FlightServiceServer},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PutResult, SchemaResult, Ticket,
};

#[derive(Debug, Clone)]
/// Flight server for testing, with configurable responses
pub struct TestFlightServer {
    /// Shared state to configure responses
    state: Arc<Mutex<State>>,
}

impl TestFlightServer {
    /// Create a `TestFlightServer`
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::new())),
        }
    }

    /// Return an [`FlightServiceServer`] that can be used with a
    /// [`Server`](tonic::transport::Server)
    pub fn service(&self) -> FlightServiceServer<TestFlightServer> {
        // wrap up tonic goop
        FlightServiceServer::new(self.clone())
    }

    /// Specify the response returned from the next call to handshake
    pub fn set_handshake_response(&self, response: Result<HandshakeResponse, Status>) {
        let mut state = self.state.lock().expect("mutex not poisoned");

        state.handshake_response.replace(response);
    }

    /// Take and return last handshake request send to the server,
    pub fn take_handshake_request(&self) -> Option<HandshakeRequest> {
        self.state
            .lock()
            .expect("mutex not poisoned")
            .handshake_request
            .take()
    }

    /// Specify the response returned from the next call to handshake
    pub fn set_get_flight_info_response(&self, response: Result<FlightInfo, Status>) {
        let mut state = self.state.lock().expect("mutex not poisoned");

        state.get_flight_info_response.replace(response);
    }

    /// Take and return last get_flight_info request send to the server,
    pub fn take_get_flight_info_request(&self) -> Option<FlightDescriptor> {
        self.state
            .lock()
            .expect("mutex not poisoned")
            .get_flight_info_request
            .take()
    }

    /// Specify the response returned from the next call to `do_get`
    pub fn set_do_get_response(&self, response: Vec<Result<RecordBatch, FlightError>>) {
        let mut state = self.state.lock().expect("mutex not poisoned");
        state.do_get_response.replace(response);
    }

    /// Take and return last do_get request send to the server,
    pub fn take_do_get_request(&self) -> Option<Ticket> {
        self.state
            .lock()
            .expect("mutex not poisoned")
            .do_get_request
            .take()
    }

    /// Returns the last metadata from a request received by the server
    pub fn take_last_request_metadata(&self) -> Option<MetadataMap> {
        self.state
            .lock()
            .expect("mutex not poisoned")
            .last_request_metadata
            .take()
    }

    /// Save the last request's metadatacom
    fn save_metadata<T>(&self, request: &Request<T>) {
        let metadata = request.metadata().clone();
        let mut state = self.state.lock().expect("mutex not poisoned");
        state.last_request_metadata = Some(metadata);
    }
}

/// mutable state for the TestFlightServer, captures requests and provides responses
#[derive(Debug, Default)]
struct State {
    /// The last handshake request that was received
    pub handshake_request: Option<HandshakeRequest>,
    /// The next response to return from `handshake()`
    pub handshake_response: Option<Result<HandshakeResponse, Status>>,
    /// The last `get_flight_info` request received
    pub get_flight_info_request: Option<FlightDescriptor>,
    /// the next response  to return from `get_flight_info`
    pub get_flight_info_response: Option<Result<FlightInfo, Status>>,
    /// The last do_get request received
    pub do_get_request: Option<Ticket>,
    /// The next response returned from `do_get`
    pub do_get_response: Option<Vec<Result<RecordBatch, FlightError>>>,
    /// The last request headers received
    pub last_request_metadata: Option<MetadataMap>,
}

impl State {
    fn new() -> Self {
        Default::default()
    }
}

/// Implement the FlightService trait
#[tonic::async_trait]
impl FlightService for TestFlightServer {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        self.save_metadata(&request);
        let handshake_request = request.into_inner().message().await?.unwrap();

        let mut state = self.state.lock().expect("mutex not poisoned");
        state.handshake_request = Some(handshake_request);

        let response = state.handshake_response.take().unwrap_or_else(|| {
            Err(Status::internal("No handshake response configured"))
        })?;

        // turn into a streaming response
        let output = futures::stream::iter(std::iter::once(Ok(response)));
        Ok(Response::new(Box::pin(output) as Self::HandshakeStream))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Implement list_flights"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.save_metadata(&request);
        let mut state = self.state.lock().expect("mutex not poisoned");
        state.get_flight_info_request = Some(request.into_inner());
        let response = state.get_flight_info_response.take().unwrap_or_else(|| {
            Err(Status::internal("No get_flight_info response configured"))
        })?;
        Ok(Response::new(response))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Implement get_schema"))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        self.save_metadata(&request);
        let mut state = self.state.lock().expect("mutex not poisoned");

        state.do_get_request = Some(request.into_inner());

        let batches: Vec<_> = state
            .do_get_response
            .take()
            .ok_or_else(|| Status::internal("No do_get response configured"))?;

        let batch_stream = futures::stream::iter(batches);

        let stream = FlightDataEncoderBuilder::new()
            .build(batch_stream)
            .map_err(|e| e.into());

        Ok(Response::new(Box::pin(stream) as _))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Implement do_put"))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Implement do_action"))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Implement list_actions"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Implement do_exchange"))
    }
}
