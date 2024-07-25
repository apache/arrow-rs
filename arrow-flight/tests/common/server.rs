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
use arrow_schema::Schema;
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use tonic::{metadata::MetadataMap, Request, Response, Status, Streaming};

use arrow_flight::{
    encode::FlightDataEncoderBuilder,
    flight_service_server::{FlightService, FlightServiceServer},
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};

#[derive(Debug, Clone)]
/// Flight server for testing, with configurable responses
pub struct TestFlightServer {
    /// Shared state to configure responses
    state: Arc<Mutex<State>>,
}

impl TestFlightServer {
    /// Create a `TestFlightServer`
    #[allow(dead_code)]
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::new())),
        }
    }

    /// Return an [`FlightServiceServer`] that can be used with a
    /// [`Server`](tonic::transport::Server)
    #[allow(dead_code)]
    pub fn service(&self) -> FlightServiceServer<TestFlightServer> {
        // wrap up tonic goop
        FlightServiceServer::new(self.clone())
    }

    /// Specify the response returned from the next call to handshake
    #[allow(dead_code)]
    pub fn set_handshake_response(&self, response: Result<HandshakeResponse, Status>) {
        let mut state = self.state.lock().expect("mutex not poisoned");
        state.handshake_response.replace(response);
    }

    /// Take and return last handshake request sent to the server,
    #[allow(dead_code)]
    pub fn take_handshake_request(&self) -> Option<HandshakeRequest> {
        self.state
            .lock()
            .expect("mutex not poisoned")
            .handshake_request
            .take()
    }

    /// Specify the response returned from the next call to get_flight_info
    #[allow(dead_code)]
    pub fn set_get_flight_info_response(&self, response: Result<FlightInfo, Status>) {
        let mut state = self.state.lock().expect("mutex not poisoned");
        state.get_flight_info_response.replace(response);
    }

    /// Take and return last get_flight_info request sent to the server,
    #[allow(dead_code)]
    pub fn take_get_flight_info_request(&self) -> Option<FlightDescriptor> {
        self.state
            .lock()
            .expect("mutex not poisoned")
            .get_flight_info_request
            .take()
    }

    /// Specify the response returned from the next call to poll_flight_info
    #[allow(dead_code)]
    pub fn set_poll_flight_info_response(&self, response: Result<PollInfo, Status>) {
        let mut state = self.state.lock().expect("mutex not poisoned");
        state.poll_flight_info_response.replace(response);
    }

    /// Take and return last poll_flight_info request sent to the server,
    #[allow(dead_code)]
    pub fn take_poll_flight_info_request(&self) -> Option<FlightDescriptor> {
        self.state
            .lock()
            .expect("mutex not poisoned")
            .poll_flight_info_request
            .take()
    }

    /// Specify the response returned from the next call to `do_get`
    #[allow(dead_code)]
    pub fn set_do_get_response(&self, response: Vec<Result<RecordBatch, Status>>) {
        let mut state = self.state.lock().expect("mutex not poisoned");
        state.do_get_response.replace(response);
    }

    /// Take and return last do_get request send to the server,
    #[allow(dead_code)]
    pub fn take_do_get_request(&self) -> Option<Ticket> {
        self.state
            .lock()
            .expect("mutex not poisoned")
            .do_get_request
            .take()
    }

    /// Specify the response returned from the next call to `do_put`
    #[allow(dead_code)]
    pub fn set_do_put_response(&self, response: Vec<Result<PutResult, Status>>) {
        let mut state = self.state.lock().expect("mutex not poisoned");
        state.do_put_response.replace(response);
    }

    /// Take and return last do_put request sent to the server,
    #[allow(dead_code)]
    pub fn take_do_put_request(&self) -> Option<Vec<FlightData>> {
        self.state
            .lock()
            .expect("mutex not poisoned")
            .do_put_request
            .take()
    }

    /// Specify the response returned from the next call to `do_exchange`
    #[allow(dead_code)]
    pub fn set_do_exchange_response(&self, response: Vec<Result<FlightData, Status>>) {
        let mut state = self.state.lock().expect("mutex not poisoned");
        state.do_exchange_response.replace(response);
    }

    /// Take and return last do_exchange request send to the server,
    #[allow(dead_code)]
    pub fn take_do_exchange_request(&self) -> Option<Vec<FlightData>> {
        self.state
            .lock()
            .expect("mutex not poisoned")
            .do_exchange_request
            .take()
    }

    /// Specify the response returned from the next call to `list_flights`
    #[allow(dead_code)]
    pub fn set_list_flights_response(&self, response: Vec<Result<FlightInfo, Status>>) {
        let mut state = self.state.lock().expect("mutex not poisoned");
        state.list_flights_response.replace(response);
    }

    /// Take and return last list_flights request send to the server,
    #[allow(dead_code)]
    pub fn take_list_flights_request(&self) -> Option<Criteria> {
        self.state
            .lock()
            .expect("mutex not poisoned")
            .list_flights_request
            .take()
    }

    /// Specify the response returned from the next call to `get_schema`
    #[allow(dead_code)]
    pub fn set_get_schema_response(&self, response: Result<Schema, Status>) {
        let mut state = self.state.lock().expect("mutex not poisoned");
        state.get_schema_response.replace(response);
    }

    /// Take and return last get_schema request send to the server,
    #[allow(dead_code)]
    pub fn take_get_schema_request(&self) -> Option<FlightDescriptor> {
        self.state
            .lock()
            .expect("mutex not poisoned")
            .get_schema_request
            .take()
    }

    /// Specify the response returned from the next call to `list_actions`
    #[allow(dead_code)]
    pub fn set_list_actions_response(&self, response: Vec<Result<ActionType, Status>>) {
        let mut state = self.state.lock().expect("mutex not poisoned");
        state.list_actions_response.replace(response);
    }

    /// Take and return last list_actions request send to the server,
    #[allow(dead_code)]
    pub fn take_list_actions_request(&self) -> Option<Empty> {
        self.state
            .lock()
            .expect("mutex not poisoned")
            .list_actions_request
            .take()
    }

    /// Specify the response returned from the next call to `do_action`
    #[allow(dead_code)]
    pub fn set_do_action_response(&self, response: Vec<Result<arrow_flight::Result, Status>>) {
        let mut state = self.state.lock().expect("mutex not poisoned");
        state.do_action_response.replace(response);
    }

    /// Take and return last do_action request send to the server,
    #[allow(dead_code)]
    pub fn take_do_action_request(&self) -> Option<Action> {
        self.state
            .lock()
            .expect("mutex not poisoned")
            .do_action_request
            .take()
    }

    /// Returns the last metadata from a request received by the server
    #[allow(dead_code)]
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
    /// The next response to return from `get_flight_info`
    pub get_flight_info_response: Option<Result<FlightInfo, Status>>,
    /// The last `poll_flight_info` request received
    pub poll_flight_info_request: Option<FlightDescriptor>,
    /// The next response to return from `poll_flight_info`
    pub poll_flight_info_response: Option<Result<PollInfo, Status>>,
    /// The last do_get request received
    pub do_get_request: Option<Ticket>,
    /// The next response returned from `do_get`
    pub do_get_response: Option<Vec<Result<RecordBatch, Status>>>,
    /// The last do_put request received
    pub do_put_request: Option<Vec<FlightData>>,
    /// The next response returned from `do_put`
    pub do_put_response: Option<Vec<Result<PutResult, Status>>>,
    /// The last do_exchange request received
    pub do_exchange_request: Option<Vec<FlightData>>,
    /// The next response returned from `do_exchange`
    pub do_exchange_response: Option<Vec<Result<FlightData, Status>>>,
    /// The last list_flights request received
    pub list_flights_request: Option<Criteria>,
    /// The next response returned from `list_flights`
    pub list_flights_response: Option<Vec<Result<FlightInfo, Status>>>,
    /// The last get_schema request received
    pub get_schema_request: Option<FlightDescriptor>,
    /// The next response returned from `get_schema`
    pub get_schema_response: Option<Result<Schema, Status>>,
    /// The last list_actions request received
    pub list_actions_request: Option<Empty>,
    /// The next response returned from `list_actions`
    pub list_actions_response: Option<Vec<Result<ActionType, Status>>>,
    /// The last do_action request received
    pub do_action_request: Option<Action>,
    /// The next response returned from `do_action`
    pub do_action_response: Option<Vec<Result<arrow_flight::Result, Status>>>,
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

        let response = state
            .handshake_response
            .take()
            .unwrap_or_else(|| Err(Status::internal("No handshake response configured")))?;

        // turn into a streaming response
        let output = futures::stream::iter(std::iter::once(Ok(response)));
        Ok(Response::new(output.boxed()))
    }

    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        self.save_metadata(&request);
        let mut state = self.state.lock().expect("mutex not poisoned");

        state.list_flights_request = Some(request.into_inner());

        let flights: Vec<_> = state
            .list_flights_response
            .take()
            .ok_or_else(|| Status::internal("No list_flights response configured"))?;

        let flights_stream = futures::stream::iter(flights);

        Ok(Response::new(flights_stream.boxed()))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.save_metadata(&request);
        let mut state = self.state.lock().expect("mutex not poisoned");
        state.get_flight_info_request = Some(request.into_inner());
        let response = state
            .get_flight_info_response
            .take()
            .unwrap_or_else(|| Err(Status::internal("No get_flight_info response configured")))?;
        Ok(Response::new(response))
    }

    async fn poll_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        self.save_metadata(&request);
        let mut state = self.state.lock().expect("mutex not poisoned");
        state.poll_flight_info_request = Some(request.into_inner());
        let response = state
            .poll_flight_info_response
            .take()
            .unwrap_or_else(|| Err(Status::internal("No poll_flight_info response configured")))?;
        Ok(Response::new(response))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        self.save_metadata(&request);
        let mut state = self.state.lock().expect("mutex not poisoned");
        state.get_schema_request = Some(request.into_inner());
        let schema = state
            .get_schema_response
            .take()
            .unwrap_or_else(|| Err(Status::internal("No get_schema response configured")))?;

        // encode the schema
        let options = arrow_ipc::writer::IpcWriteOptions::default();
        let response: SchemaResult = SchemaAsIpc::new(&schema, &options)
            .try_into()
            .expect("Error encoding schema");

        Ok(Response::new(response))
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

        let batch_stream = futures::stream::iter(batches).map_err(Into::into);

        let stream = FlightDataEncoderBuilder::new()
            .build(batch_stream)
            .map_err(Into::into);

        let mut resp = Response::new(stream.boxed());
        resp.metadata_mut()
            .insert("test-resp-header", "some_val".parse().unwrap());

        Ok(resp)
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        self.save_metadata(&request);
        let do_put_request: Vec<_> = request.into_inner().try_collect().await?;

        let mut state = self.state.lock().expect("mutex not poisoned");

        state.do_put_request = Some(do_put_request);

        let response = state
            .do_put_response
            .take()
            .ok_or_else(|| Status::internal("No do_put response configured"))?;

        let stream = futures::stream::iter(response).map_err(Into::into);

        Ok(Response::new(stream.boxed()))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        self.save_metadata(&request);
        let mut state = self.state.lock().expect("mutex not poisoned");

        state.do_action_request = Some(request.into_inner());

        let results: Vec<_> = state
            .do_action_response
            .take()
            .ok_or_else(|| Status::internal("No do_action response configured"))?;

        let results_stream = futures::stream::iter(results);

        Ok(Response::new(results_stream.boxed()))
    }

    async fn list_actions(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        self.save_metadata(&request);
        let mut state = self.state.lock().expect("mutex not poisoned");

        state.list_actions_request = Some(request.into_inner());

        let actions: Vec<_> = state
            .list_actions_response
            .take()
            .ok_or_else(|| Status::internal("No list_actions response configured"))?;

        let action_stream = futures::stream::iter(actions);

        Ok(Response::new(action_stream.boxed()))
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        self.save_metadata(&request);
        let do_exchange_request: Vec<_> = request.into_inner().try_collect().await?;

        let mut state = self.state.lock().expect("mutex not poisoned");

        state.do_exchange_request = Some(do_exchange_request);

        let response = state
            .do_exchange_response
            .take()
            .ok_or_else(|| Status::internal("No do_exchange response configured"))?;

        let stream = futures::stream::iter(response).map_err(Into::into);

        Ok(Response::new(stream.boxed()))
    }
}
