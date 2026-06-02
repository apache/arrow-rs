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

use std::sync::{Arc, RwLock};

use arrow_array::{
    Array, ArrayRef, DictionaryArray, Int32Array, Int64Array, ListArray, RecordBatch, StringArray,
    types::Int32Type,
};
use arrow_buffer::OffsetBuffer;
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
    flight_service_server::{FlightService, FlightServiceServer},
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use hyper_util::rt::TokioIo;
use tonic::{
    Request, Response, Status, Streaming,
    transport::{Channel, Endpoint, Server},
};

pub type Builder = fn(usize) -> ArrayRef;

pub const TYPES: &[(&str, Builder)] = &[
    ("fixed", fixed),
    ("nested", nested),
    ("variable", variable),
    ("dict", dict),
];

fn fixed(n: usize) -> ArrayRef {
    Arc::new(Int64Array::from_iter_values(0..n as i64))
}

fn variable(n: usize) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(
        (0..n).map(|i| format!("variable_string_{i}{}", "_".repeat(i % 16))),
    ))
}

fn nested(n: usize) -> ArrayRef {
    let values = Int32Array::from_iter_values(0..(n * 4) as i32);
    let offsets = OffsetBuffer::<i32>::from_lengths(std::iter::repeat_n(4usize, n));
    let field = Arc::new(Field::new_list_field(DataType::Int32, false));
    Arc::new(ListArray::new(field, offsets, Arc::new(values), None))
}

fn dict(n: usize) -> ArrayRef {
    let keys = Int32Array::from_iter_values((0..n).map(|i| (i % 32) as i32));
    let values = StringArray::from_iter_values((0..32).map(|i| format!("dictionary_value_{i:03}")));
    Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap())
}

pub fn build_batch(name: &str, rows: usize, cols: usize, build: Builder) -> RecordBatch {
    let arrays: Vec<ArrayRef> = (0..cols).map(|_| build(rows)).collect();
    let fields: Vec<Field> = arrays
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("column_{i}_{name}"), a.data_type().clone(), false))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

#[derive(Clone, Default)]
pub struct BenchServer {
    frames: Arc<RwLock<Vec<FlightData>>>,
}

impl BenchServer {
    #[allow(dead_code)]
    pub fn set_frames(&self, frames: Vec<FlightData>) {
        *self.frames.write().unwrap() = frames;
    }
}

fn unimpl<T>() -> Result<T, Status> {
    Err(Status::unimplemented(""))
}

#[rustfmt::skip]
#[tonic::async_trait]
impl FlightService for BenchServer {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;
    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;
    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_get(&self, _: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        let frames = self.frames.read().unwrap().clone();
        Ok(Response::new(futures::stream::iter(frames.into_iter().map(Ok)).boxed()))
    }

    async fn do_put(&self, req: Request<Streaming<FlightData>>) -> Result<Response<Self::DoPutStream>, Status> {
        let _: Vec<FlightData> = req.into_inner().try_collect().await?;
        let ack = PutResult { app_metadata: Bytes::new() };
        Ok(Response::new(futures::stream::iter([Ok(ack)]).boxed()))
    }

    async fn do_exchange(&self, req: Request<Streaming<FlightData>>) -> Result<Response<Self::DoExchangeStream>, Status> {
        Ok(Response::new(req.into_inner().boxed()))
    }

    async fn handshake(&self, _: Request<Streaming<HandshakeRequest>>) -> Result<Response<Self::HandshakeStream>, Status> { unimpl() }
    async fn list_flights(&self, _: Request<Criteria>) -> Result<Response<Self::ListFlightsStream>, Status> { unimpl() }
    async fn get_flight_info(&self, _: Request<FlightDescriptor>) -> Result<Response<FlightInfo>, Status> { unimpl() }
    async fn poll_flight_info(&self, _: Request<FlightDescriptor>) -> Result<Response<PollInfo>, Status> { unimpl() }
    async fn get_schema(&self, _: Request<FlightDescriptor>) -> Result<Response<SchemaResult>, Status> { unimpl() }
    async fn do_action(&self, _: Request<Action>) -> Result<Response<Self::DoActionStream>, Status> { unimpl() }
    async fn list_actions(&self, _: Request<Empty>) -> Result<Response<Self::ListActionsStream>, Status> { unimpl() }
}
pub async fn start_server() -> (Channel, BenchServer) {
    const DUMMY_URL: &str = "http://localhost:50051";

    let bench_server = BenchServer::default();

    let (client, server) = tokio::io::duplex(1024 * 1024);

    let mut client = Some(client);
    let channel = Endpoint::try_from(DUMMY_URL)
        .expect("Invalid dummy URL for building an endpoint. This should never happen")
        .connect_with_connector_lazy(tower::service_fn(move |_| {
            let client = client
                .take()
                .expect("Client taken twice. This should never happen");
            async move { Ok::<_, std::io::Error>(TokioIo::new(client)) }
        }));
    tokio::spawn(
        Server::builder()
            .add_service(FlightServiceServer::new(bench_server.clone()))
            .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server))),
    );
    (channel, bench_server)
}
