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

//! Integration test for "mid level" Client

mod common {
    pub mod server;
}
use arrow_flight::{FlightClient, HandshakeRequest, HandshakeResponse, error::FlightError, FlightDescriptor, FlightInfo, Ticket};
use common::server::TestFlightServer;
use futures::{Future, TryStreamExt};
use tokio::{net::TcpListener, task::JoinHandle};
use tonic::{
    transport::{Channel, Uri},
    Status,
};

use std::{net::SocketAddr, time::Duration};

const DEFAULT_TIMEOUT_SECONDS: u64 = 30;

#[tokio::test]
async fn test_handshake() {
    do_test(|test_server, mut client| {
        async move {
            let request_payload = b"foo".to_vec();
            let response_payload = b"Bar".to_vec();

            let request = HandshakeRequest {
                payload: request_payload.clone(),
                protocol_version: 0,
            };

            let response = HandshakeResponse {
                payload: response_payload.clone(),
                protocol_version: 0,
            };

            test_server.set_handshake_response(Ok(response));
            let response = client.handshake(request_payload).await.unwrap();
            assert_eq!(response, response_payload);
            assert_eq!(test_server.take_handshake_request(), Some(request));
        }
    }).await;
}


#[tokio::test]
async fn test_handshake_error() {
    do_test(|test_server, mut client| {
        async move {
            let request_payload = "foo".to_string().into_bytes();
            let e = Status::unauthenticated("DENIED");
            test_server.set_handshake_response(Err(e));

            let response = client.handshake(request_payload).await.unwrap_err();
            let e = Status::unauthenticated("DENIED");
            expect_status(response, e);
        }
    }).await;
}


#[tokio::test]
async fn test_handshake_metadata() {
    do_test(|test_server, mut client| {
        async move {
            client.add_header("foo", "bar").unwrap();

            let request_payload = "Blarg".to_string().into_bytes();
            let response_payload = "Bazz".to_string().into_bytes();

            let response = HandshakeResponse {
                payload: response_payload.clone(),
                protocol_version: 0,
            };

            test_server.set_handshake_response(Ok(response));
            client.handshake(request_payload).await.unwrap();
            ensure_metadata(&client, &test_server);
        }
    }).await;
}

/// Verifies that all headers sent from the the client are in the request_metadata
fn ensure_metadata(client: &FlightClient, test_server: &TestFlightServer) {
    let client_metadata = client.metadata().clone().into_headers();
    assert!(client_metadata.len() > 0);
    let metadata = test_server.take_last_request_metadata().expect("No headers in server").into_headers();

    for (k, v) in &client_metadata {
        assert_eq!(metadata.get(k).as_ref(), Some(&v),
                   "Missing / Mismatched metadata {:?} sent {:?} got {:?}",
                   k, client_metadata, metadata);
    }
}

fn test_flight_info(request: &FlightDescriptor) -> FlightInfo {
    FlightInfo {
        schema: vec![],
        endpoint: vec![],
        flight_descriptor: Some(request.clone()),
        total_bytes: 123,
        total_records: 456,
    }
}

#[tokio::test]
async fn test_get_flight_info() {
    do_test(|test_server, mut client| {
        async move {
            let request = FlightDescriptor::new_cmd(b"My Command".to_vec());

            let expected_response = test_flight_info(&request);
            test_server.set_get_flight_info_response(Ok(expected_response.clone()));

            let response = client.get_flight_info(request.clone()).await.unwrap();

            assert_eq!(response, expected_response);
            assert_eq!(test_server.take_get_flight_info_request(), Some(request));
        }
    }).await;
}

#[tokio::test]
async fn test_get_flight_info_error() {
    do_test(|test_server, mut client| {
        async move {
            let request = FlightDescriptor::new_cmd(b"My Command".to_vec());

            let e = Status::unauthenticated("DENIED");
            test_server.set_get_flight_info_response(Err(e));

            let response = client.get_flight_info(request.clone()).await.unwrap_err();
            let e = Status::unauthenticated("DENIED");
            expect_status(response, e);
        }
    }).await;
}


#[tokio::test]
async fn test_get_flight_info_metadata() {
    do_test(|test_server, mut client| {
        async move {
            client.add_header("foo", "bar").unwrap();
            let request = FlightDescriptor::new_cmd(b"My Command".to_vec());

            let expected_response = test_flight_info(&request);
            test_server.set_get_flight_info_response(Ok(expected_response));
            client.get_flight_info(request.clone()).await.unwrap();
            ensure_metadata(&client, &test_server);

        }
    }).await;
}

// TODO more negative  tests (like if there are endpoints defined, etc)


// TODO test for do_get



/// Runs the future returned by the function,  passing it a test server and client
async fn do_test<F, Fut>(f: F)
where
    F: Fn(TestFlightServer, FlightClient) -> Fut,
    Fut: Future<Output=()>
{
    let test_server = TestFlightServer::new();
    let fixture = TestFixture::new(&test_server).await;
    let client = FlightClient::new(fixture.channel().await);

    // run the test function
    f(test_server, client).await;

    // cleanly shutdown the test fixture
    fixture.shutdown_and_wait().await
}





fn expect_status(error: FlightError, expected: Status) {
    let status = if let FlightError::Tonic(status) = error {
        status
    } else {
        panic!("Expected FlightError::Tonic, got: {:?}", error);
    };

    assert_eq!(status.code(), expected.code(), "Got {:?} want {:?}", status, expected);
    assert_eq!(status.message(), expected.message(), "Got {:?} want {:?}", status, expected);
    assert_eq!(status.details(), expected.details(), "Got {:?} want {:?}", status, expected);
}


/// Creates and manages a running TestServer with a background task
struct TestFixture {
    /// channel to send shutdown command
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,

    /// Address the server is listening on
    addr: SocketAddr,

    // handle for the server task
    handle: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl TestFixture {
    /// create a new test fixture from the server
    pub async fn new(test_server: &TestFlightServer) -> Self {
        // let OS choose a a free port
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        println!("Listening on {addr}");

        // prepare the shutdown channel
        let (tx, rx) = tokio::sync::oneshot::channel();

        let server_timeout = Duration::from_secs(DEFAULT_TIMEOUT_SECONDS);

        let shutdown_future = async move {
            rx.await.ok();
        };

        let serve_future = tonic::transport::Server::builder()
            .timeout(server_timeout)
            .add_service(test_server.service())
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                shutdown_future,
            );

        // Run the server in its own background task
        let handle = tokio::task::spawn(serve_future);

        Self {
            shutdown: Some(tx),
            addr,
            handle: Some(handle),
        }
    }

    /// Return a [`Channel`] connected to the TestServer
    pub async fn channel(&self) -> Channel {
        let url = format!("http://{}", self.addr);
        let uri: Uri = url.parse().expect("Valid URI");
        Channel::builder(uri)
            .timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECONDS))
            .connect()
            .await
            .expect("error connecting to server")
    }

    /// Stops the test server and waits for the server to shutdown
    pub async fn shutdown_and_wait(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.send(()).expect("server quit early");
        }
        if let Some(handle) = self.handle.take() {
            println!("Waiting on server to finish");
            handle
                .await
                .expect("task join error (panic?)")
                .expect("Server Error found at shutdown");
        }
    }
}

impl Drop for TestFixture {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.send(()).ok();
        }
        if self.handle.is_some() {
            // tests should properly clean up TestFixture
            println!("TestFixture::Drop called prior to `shutdown_and_wait`");
        }
    }
}

//         // We would just listen on TCP, but it seems impossible to know when tonic is ready to serve
//         let service = FlightSqlServiceImpl {};
//         let serve_future = Server::builder()
//             .add_service(FlightServiceServer::new(service))
//             .serve_with_incoming(stream);

//         let request_future = async {
//             let mut client = client_with_uds(path).await;
//             let token = client.handshake("admin", "password").await.unwrap();
//             println!("Auth succeeded with token: {:?}", token);
//             let mut stmt = client.prepare("select 1;".to_string()).await.unwrap();
//             let flight_info = stmt.execute().await.unwrap();
//             let ticket = flight_info.endpoint[0].ticket.as_ref().unwrap().clone();
//             let flight_data = client.do_get(ticket).await.unwrap();
//             let flight_data: Vec<FlightData> = flight_data.try_collect().await.unwrap();
//             let batches = flight_data_to_batches(&flight_data).unwrap();
//             let res = pretty_format_batches(batches.as_slice()).unwrap();
//             let expected = r#"
// +-------------------+
// | salutation        |
// +-------------------+
// | Hello, FlightSQL! |
// +-------------------+"#
//                 .trim()
//                 .to_string();
//             assert_eq!(res.to_string(), expected);
//         };

//         tokio::select! {
//             _ = serve_future => panic!("server returned first"),
//             _ = request_future => println!("Client finished!"),
//         }
//}
