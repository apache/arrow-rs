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
use arrow_array::{RecordBatch, UInt64Array};
use arrow_flight::{
    error::FlightError, FlightClient, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, Ticket,
};
use bytes::Bytes;
use common::server::TestFlightServer;
use futures::{Future, TryStreamExt};
use tokio::{net::TcpListener, task::JoinHandle};
use tonic::{
    transport::{Channel, Uri},
    Status,
};

use std::{net::SocketAddr, sync::Arc, time::Duration};

const DEFAULT_TIMEOUT_SECONDS: u64 = 30;

#[tokio::test]
async fn test_handshake() {
    do_test(|test_server, mut client| async move {
        let request_payload = Bytes::from("foo");
        let response_payload = Bytes::from("Bar");

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
    })
    .await;
}

#[tokio::test]
async fn test_handshake_error() {
    do_test(|test_server, mut client| async move {
        let request_payload = "foo".to_string().into_bytes();
        let e = Status::unauthenticated("DENIED");
        test_server.set_handshake_response(Err(e));

        let response = client.handshake(request_payload).await.unwrap_err();
        let e = Status::unauthenticated("DENIED");
        expect_status(response, e);
    })
    .await;
}

#[tokio::test]
async fn test_handshake_metadata() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo", "bar").unwrap();

        let request_payload = Bytes::from("Blarg");
        let response_payload = Bytes::from("Bazz");

        let response = HandshakeResponse {
            payload: response_payload.clone(),
            protocol_version: 0,
        };

        test_server.set_handshake_response(Ok(response));
        client.handshake(request_payload).await.unwrap();
        ensure_metadata(&client, &test_server);
    })
    .await;
}

/// Verifies that all headers sent from the the client are in the request_metadata
fn ensure_metadata(client: &FlightClient, test_server: &TestFlightServer) {
    let client_metadata = client.metadata().clone().into_headers();
    assert!(!client_metadata.is_empty());
    let metadata = test_server
        .take_last_request_metadata()
        .expect("No headers in server")
        .into_headers();

    for (k, v) in &client_metadata {
        assert_eq!(
            metadata.get(k).as_ref(),
            Some(&v),
            "Missing / Mismatched metadata {:?} sent {:?} got {:?}",
            k,
            client_metadata,
            metadata
        );
    }
}

fn test_flight_info(request: &FlightDescriptor) -> FlightInfo {
    FlightInfo {
        schema: Bytes::new(),
        endpoint: vec![],
        flight_descriptor: Some(request.clone()),
        total_bytes: 123,
        total_records: 456,
    }
}

#[tokio::test]
async fn test_get_flight_info() {
    do_test(|test_server, mut client| async move {
        let request = FlightDescriptor::new_cmd(b"My Command".to_vec());

        let expected_response = test_flight_info(&request);
        test_server.set_get_flight_info_response(Ok(expected_response.clone()));

        let response = client.get_flight_info(request.clone()).await.unwrap();

        assert_eq!(response, expected_response);
        assert_eq!(test_server.take_get_flight_info_request(), Some(request));
    })
    .await;
}

#[tokio::test]
async fn test_get_flight_info_error() {
    do_test(|test_server, mut client| async move {
        let request = FlightDescriptor::new_cmd(b"My Command".to_vec());

        let e = Status::unauthenticated("DENIED");
        test_server.set_get_flight_info_response(Err(e));

        let response = client.get_flight_info(request.clone()).await.unwrap_err();
        let e = Status::unauthenticated("DENIED");
        expect_status(response, e);
    })
    .await;
}

#[tokio::test]
async fn test_get_flight_info_metadata() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo", "bar").unwrap();
        let request = FlightDescriptor::new_cmd(b"My Command".to_vec());

        let expected_response = test_flight_info(&request);
        test_server.set_get_flight_info_response(Ok(expected_response));
        client.get_flight_info(request.clone()).await.unwrap();
        ensure_metadata(&client, &test_server);
    })
    .await;
}

// TODO more negative  tests (like if there are endpoints defined, etc)

#[tokio::test]
async fn test_do_get() {
    do_test(|test_server, mut client| async move {
        let ticket = Ticket {
            ticket: Bytes::from("my awesome flight ticket"),
        };

        let batch = RecordBatch::try_from_iter(vec![(
            "col",
            Arc::new(UInt64Array::from_iter([1, 2, 3, 4])) as _,
        )])
        .unwrap();

        let response = vec![Ok(batch.clone())];
        test_server.set_do_get_response(response);
        let response_stream = client
            .do_get(ticket.clone())
            .await
            .expect("error making request");

        let expected_response = vec![batch];
        let response: Vec<_> = response_stream
            .try_collect()
            .await
            .expect("Error streaming data");

        assert_eq!(response, expected_response);
        assert_eq!(test_server.take_do_get_request(), Some(ticket));
    })
    .await;
}

#[tokio::test]
async fn test_do_get_error() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo", "bar").unwrap();
        let ticket = Ticket {
            ticket: Bytes::from("my awesome flight ticket"),
        };

        let response = client.do_get(ticket.clone()).await.unwrap_err();

        let e = Status::internal("No do_get response configured");
        expect_status(response, e);
        // server still got the request
        assert_eq!(test_server.take_do_get_request(), Some(ticket));
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_do_get_error_in_record_batch_stream() {
    do_test(|test_server, mut client| async move {
        let ticket = Ticket {
            ticket: Bytes::from("my awesome flight ticket"),
        };

        let batch = RecordBatch::try_from_iter(vec![(
            "col",
            Arc::new(UInt64Array::from_iter([1, 2, 3, 4])) as _,
        )])
        .unwrap();

        let e = Status::data_loss("she's dead jim");

        let expected_response = vec![Ok(batch), Err(FlightError::Tonic(e.clone()))];

        test_server.set_do_get_response(expected_response);

        let response_stream = client
            .do_get(ticket.clone())
            .await
            .expect("error making request");

        let response: Result<Vec<_>, FlightError> = response_stream.try_collect().await;

        let response = response.unwrap_err();
        expect_status(response, e);
        // server still got the request
        assert_eq!(test_server.take_do_get_request(), Some(ticket));
    })
    .await;
}

/// Runs the future returned by the function,  passing it a test server and client
async fn do_test<F, Fut>(f: F)
where
    F: Fn(TestFlightServer, FlightClient) -> Fut,
    Fut: Future<Output = ()>,
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

    assert_eq!(
        status.code(),
        expected.code(),
        "Got {:?} want {:?}",
        status,
        expected
    );
    assert_eq!(
        status.message(),
        expected.message(),
        "Got {:?} want {:?}",
        status,
        expected
    );
    assert_eq!(
        status.details(),
        expected.details(),
        "Got {:?} want {:?}",
        status,
        expected
    );
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
