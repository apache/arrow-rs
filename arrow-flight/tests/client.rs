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

mod common;

use crate::common::fixture::TestFixture;
use arrow_array::{RecordBatch, UInt64Array};
use arrow_flight::{
    decode::FlightRecordBatchStream, encode::FlightDataEncoderBuilder, error::FlightError, Action,
    ActionType, CancelFlightInfoRequest, CancelFlightInfoResult, CancelStatus, Criteria, Empty,
    FlightClient, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, PollInfo, PutResult, RenewFlightEndpointRequest, Ticket,
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use common::server::TestFlightServer;
use futures::{Future, StreamExt, TryStreamExt};
use prost::Message;
use tonic::Status;

use std::sync::Arc;

#[tokio::test]
async fn test_handshake() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();
        let request_payload = Bytes::from("foo-request-payload");
        let response_payload = Bytes::from("bar-response-payload");

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
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_handshake_error() {
    do_test(|test_server, mut client| async move {
        let request_payload = "foo-request-payload".to_string().into_bytes();
        let e = Status::unauthenticated("DENIED");
        test_server.set_handshake_response(Err(e.clone()));

        let response = client.handshake(request_payload).await.unwrap_err();
        expect_status(response, e);
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
            "Missing / Mismatched metadata {k:?} sent {client_metadata:?} got {metadata:?}"
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
        ordered: false,
        app_metadata: Bytes::new(),
    }
}

#[tokio::test]
async fn test_get_flight_info() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();
        let request = FlightDescriptor::new_cmd(b"My Command".to_vec());

        let expected_response = test_flight_info(&request);
        test_server.set_get_flight_info_response(Ok(expected_response.clone()));

        let response = client.get_flight_info(request.clone()).await.unwrap();

        assert_eq!(response, expected_response);
        assert_eq!(test_server.take_get_flight_info_request(), Some(request));
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_get_flight_info_error() {
    do_test(|test_server, mut client| async move {
        let request = FlightDescriptor::new_cmd(b"My Command".to_vec());

        let e = Status::unauthenticated("DENIED");
        test_server.set_get_flight_info_response(Err(e.clone()));

        let response = client.get_flight_info(request.clone()).await.unwrap_err();
        expect_status(response, e);
    })
    .await;
}

fn test_poll_info(request: &FlightDescriptor) -> PollInfo {
    PollInfo {
        info: Some(test_flight_info(request)),
        flight_descriptor: None,
        progress: Some(1.0),
        expiration_time: None,
    }
}

#[tokio::test]
async fn test_poll_flight_info() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();
        let request = FlightDescriptor::new_cmd(b"My Command".to_vec());

        let expected_response = test_poll_info(&request);
        test_server.set_poll_flight_info_response(Ok(expected_response.clone()));

        let response = client.poll_flight_info(request.clone()).await.unwrap();

        assert_eq!(response, expected_response);
        assert_eq!(test_server.take_poll_flight_info_request(), Some(request));
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_poll_flight_info_error() {
    do_test(|test_server, mut client| async move {
        let request = FlightDescriptor::new_cmd(b"My Command".to_vec());

        let e = Status::unauthenticated("DENIED");
        test_server.set_poll_flight_info_response(Err(e.clone()));

        let response = client.poll_flight_info(request.clone()).await.unwrap_err();
        expect_status(response, e);
    })
    .await;
}

// TODO more negative  tests (like if there are endpoints defined, etc)

#[tokio::test]
async fn test_do_get() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();
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
        let mut response_stream = client
            .do_get(ticket.clone())
            .await
            .expect("error making request");

        assert_eq!(
            response_stream
                .headers()
                .get("test-resp-header")
                .expect("header exists")
                .to_str()
                .unwrap(),
            "some_val",
        );

        // trailers are not available before stream exhaustion
        assert!(response_stream.trailers().is_none());

        let expected_response = vec![batch];
        let response: Vec<_> = (&mut response_stream)
            .try_collect()
            .await
            .expect("Error streaming data");
        assert_eq!(response, expected_response);

        assert_eq!(
            response_stream
                .trailers()
                .expect("stream exhausted")
                .get("test-trailer")
                .expect("trailer exists")
                .to_str()
                .unwrap(),
            "trailer_val",
        );

        assert_eq!(test_server.take_do_get_request(), Some(ticket));
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_do_get_error() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();
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

        let expected_response = vec![Ok(batch), Err(e.clone())];

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

#[tokio::test]
async fn test_do_put() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();

        // encode the batch as a stream of FlightData
        let input_flight_data = test_flight_data().await;

        let expected_response = vec![
            PutResult {
                app_metadata: Bytes::from("foo-metadata1"),
            },
            PutResult {
                app_metadata: Bytes::from("bar-metadata2"),
            },
        ];

        test_server.set_do_put_response(expected_response.clone().into_iter().map(Ok).collect());

        let input_stream = futures::stream::iter(input_flight_data.clone()).map(Ok);

        let response_stream = client
            .do_put(input_stream)
            .await
            .expect("error making request");

        let response: Vec<_> = response_stream
            .try_collect()
            .await
            .expect("Error streaming data");

        assert_eq!(response, expected_response);
        assert_eq!(test_server.take_do_put_request(), Some(input_flight_data));
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_do_put_error_server() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();

        let input_flight_data = test_flight_data().await;

        let input_stream = futures::stream::iter(input_flight_data.clone()).map(Ok);

        let response = client.do_put(input_stream).await;
        let response = match response {
            Ok(_) => panic!("unexpected success"),
            Err(e) => e,
        };

        let e = Status::internal("No do_put response configured");
        expect_status(response, e);
        // server still got the request
        assert_eq!(test_server.take_do_put_request(), Some(input_flight_data));
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_do_put_error_stream_server() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();

        let input_flight_data = test_flight_data().await;

        let e = Status::invalid_argument("bad arg");

        let response = vec![
            Ok(PutResult {
                app_metadata: Bytes::from("foo-metadata"),
            }),
            Err(e.clone()),
        ];

        test_server.set_do_put_response(response);

        let input_stream = futures::stream::iter(input_flight_data.clone()).map(Ok);

        let response_stream = client
            .do_put(input_stream)
            .await
            .expect("error making request");

        let response: Result<Vec<_>, _> = response_stream.try_collect().await;
        let response = match response {
            Ok(_) => panic!("unexpected success"),
            Err(e) => e,
        };

        expect_status(response, e);
        // server still got the request
        assert_eq!(test_server.take_do_put_request(), Some(input_flight_data));
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_do_put_error_client() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();

        let e = Status::invalid_argument("bad arg: client");

        // input stream to client sends good FlightData followed by an error
        let input_flight_data = test_flight_data().await;
        let input_stream = futures::stream::iter(input_flight_data.clone())
            .map(Ok)
            .chain(futures::stream::iter(vec![Err(FlightError::from(
                e.clone(),
            ))]));

        // server responds with one good message
        let response = vec![Ok(PutResult {
            app_metadata: Bytes::from("foo-metadata"),
        })];
        test_server.set_do_put_response(response);

        let response_stream = client
            .do_put(input_stream)
            .await
            .expect("error making request");

        let response: Result<Vec<_>, _> = response_stream.try_collect().await;
        let response = match response {
            Ok(_) => panic!("unexpected success"),
            Err(e) => e,
        };

        // expect to the error made from the client
        expect_status(response, e);
        // server still got the request messages until the client sent the error
        assert_eq!(test_server.take_do_put_request(), Some(input_flight_data));
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_do_put_error_client_and_server() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();

        let e_client = Status::invalid_argument("bad arg: client");
        let e_server = Status::invalid_argument("bad arg: server");

        // input stream to client sends good FlightData followed by an error
        let input_flight_data = test_flight_data().await;
        let input_stream = futures::stream::iter(input_flight_data.clone())
            .map(Ok)
            .chain(futures::stream::iter(vec![Err(FlightError::from(
                e_client.clone(),
            ))]));

        // server responds with an error (e.g. because it got truncated data)
        let response = vec![Err(e_server)];
        test_server.set_do_put_response(response);

        let response_stream = client
            .do_put(input_stream)
            .await
            .expect("error making request");

        let response: Result<Vec<_>, _> = response_stream.try_collect().await;
        let response = match response {
            Ok(_) => panic!("unexpected success"),
            Err(e) => e,
        };

        // expect to the error made from the client (not the server)
        expect_status(response, e_client);
        // server still got the request messages until the client sent the error
        assert_eq!(test_server.take_do_put_request(), Some(input_flight_data));
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_do_exchange() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();

        // encode the batch as a stream of FlightData
        let input_flight_data = test_flight_data().await;
        let output_flight_data = test_flight_data2().await;

        test_server
            .set_do_exchange_response(output_flight_data.clone().into_iter().map(Ok).collect());

        let response_stream = client
            .do_exchange(futures::stream::iter(input_flight_data.clone()).map(Ok))
            .await
            .expect("error making request");

        let response: Vec<_> = response_stream
            .try_collect()
            .await
            .expect("Error streaming data");

        let expected_stream = futures::stream::iter(output_flight_data).map(Ok);

        let expected_batches: Vec<_> =
            FlightRecordBatchStream::new_from_flight_data(expected_stream)
                .try_collect()
                .await
                .unwrap();

        assert_eq!(response, expected_batches);
        assert_eq!(
            test_server.take_do_exchange_request(),
            Some(input_flight_data)
        );
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_do_exchange_error() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();

        let input_flight_data = test_flight_data().await;

        let response = client
            .do_exchange(futures::stream::iter(input_flight_data.clone()).map(Ok))
            .await;
        let response = match response {
            Ok(_) => panic!("unexpected success"),
            Err(e) => e,
        };

        let e = Status::internal("No do_exchange response configured");
        expect_status(response, e);
        // server still got the request
        assert_eq!(
            test_server.take_do_exchange_request(),
            Some(input_flight_data)
        );
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_do_exchange_error_stream() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();

        let input_flight_data = test_flight_data().await;

        let e = Status::invalid_argument("the error");
        let response = test_flight_data2()
            .await
            .into_iter()
            .enumerate()
            .map(|(i, m)| {
                if i == 0 {
                    Ok(m)
                } else {
                    // make all messages after the first an error
                    Err(e.clone())
                }
            })
            .collect();

        test_server.set_do_exchange_response(response);

        let response_stream = client
            .do_exchange(futures::stream::iter(input_flight_data.clone()).map(Ok))
            .await
            .expect("error making request");

        let response: Result<Vec<_>, _> = response_stream.try_collect().await;
        let response = match response {
            Ok(_) => panic!("unexpected success"),
            Err(e) => e,
        };

        expect_status(response, e);
        // server still got the request
        assert_eq!(
            test_server.take_do_exchange_request(),
            Some(input_flight_data)
        );
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_do_exchange_error_stream_client() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();

        let e = Status::invalid_argument("bad arg: client");

        // input stream to client sends good FlightData followed by an error
        let input_flight_data = test_flight_data().await;
        let input_stream = futures::stream::iter(input_flight_data.clone())
            .map(Ok)
            .chain(futures::stream::iter(vec![Err(FlightError::from(
                e.clone(),
            ))]));

        let output_flight_data = FlightData::new()
            .with_descriptor(FlightDescriptor::new_cmd("Sample command"))
            .with_data_body("body".as_bytes())
            .with_data_header("header".as_bytes())
            .with_app_metadata("metadata".as_bytes());

        // server responds with one good message
        let response = vec![Ok(output_flight_data)];
        test_server.set_do_exchange_response(response);

        let response_stream = client
            .do_exchange(input_stream)
            .await
            .expect("error making request");

        let response: Result<Vec<_>, _> = response_stream.try_collect().await;
        let response = match response {
            Ok(_) => panic!("unexpected success"),
            Err(e) => e,
        };

        // expect to the error made from the client
        expect_status(response, e);
        // server still got the request messages until the client sent the error
        assert_eq!(
            test_server.take_do_exchange_request(),
            Some(input_flight_data)
        );
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_do_exchange_error_client_and_server() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();

        let e_client = Status::invalid_argument("bad arg: client");
        let e_server = Status::invalid_argument("bad arg: server");

        // input stream to client sends good FlightData followed by an error
        let input_flight_data = test_flight_data().await;
        let input_stream = futures::stream::iter(input_flight_data.clone())
            .map(Ok)
            .chain(futures::stream::iter(vec![Err(FlightError::from(
                e_client.clone(),
            ))]));

        // server responds with an error (e.g. because it got truncated data)
        let response = vec![Err(e_server)];
        test_server.set_do_exchange_response(response);

        let response_stream = client
            .do_exchange(input_stream)
            .await
            .expect("error making request");

        let response: Result<Vec<_>, _> = response_stream.try_collect().await;
        let response = match response {
            Ok(_) => panic!("unexpected success"),
            Err(e) => e,
        };

        // expect to the error made from the client (not the server)
        expect_status(response, e_client);
        // server still got the request messages until the client sent the error
        assert_eq!(
            test_server.take_do_exchange_request(),
            Some(input_flight_data)
        );
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_get_schema() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();

        let schema = Schema::new(vec![Field::new("foo", DataType::Int64, true)]);

        let request = FlightDescriptor::new_cmd("my command");
        test_server.set_get_schema_response(Ok(schema.clone()));

        let response = client
            .get_schema(request.clone())
            .await
            .expect("error making request");

        let expected_schema = schema;
        let expected_request = request;

        assert_eq!(response, expected_schema);
        assert_eq!(
            test_server.take_get_schema_request(),
            Some(expected_request)
        );

        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_get_schema_error() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();
        let request = FlightDescriptor::new_cmd("my command");

        let e = Status::unauthenticated("DENIED");
        test_server.set_get_schema_response(Err(e.clone()));

        let response = client.get_schema(request).await.unwrap_err();
        expect_status(response, e);
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_list_flights() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();

        let infos = vec![
            test_flight_info(&FlightDescriptor::new_cmd("foo")),
            test_flight_info(&FlightDescriptor::new_cmd("bar")),
        ];

        let response = infos.iter().map(|i| Ok(i.clone())).collect();
        test_server.set_list_flights_response(response);

        let response_stream = client
            .list_flights("query")
            .await
            .expect("error making request");

        let expected_response = infos;
        let response: Vec<_> = response_stream
            .try_collect()
            .await
            .expect("Error streaming data");

        let expected_request = Some(Criteria {
            expression: "query".into(),
        });

        assert_eq!(response, expected_response);
        assert_eq!(test_server.take_list_flights_request(), expected_request);
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_list_flights_error() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();

        let response = client.list_flights("query").await;
        let response = match response {
            Ok(_) => panic!("unexpected success"),
            Err(e) => e,
        };

        let e = Status::internal("No list_flights response configured");
        expect_status(response, e);
        // server still got the request
        let expected_request = Some(Criteria {
            expression: "query".into(),
        });
        assert_eq!(test_server.take_list_flights_request(), expected_request);
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_list_flights_error_in_stream() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();

        let e = Status::data_loss("she's dead jim");

        let response = vec![
            Ok(test_flight_info(&FlightDescriptor::new_cmd("foo"))),
            Err(e.clone()),
        ];
        test_server.set_list_flights_response(response);

        let response_stream = client
            .list_flights("other query")
            .await
            .expect("error making request");

        let response: Result<Vec<_>, FlightError> = response_stream.try_collect().await;

        let response = response.unwrap_err();
        expect_status(response, e);
        // server still got the request
        let expected_request = Some(Criteria {
            expression: "other query".into(),
        });
        assert_eq!(test_server.take_list_flights_request(), expected_request);
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_list_actions() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();

        let actions = vec![
            ActionType {
                r#type: "type 1".into(),
                description: "awesomeness".into(),
            },
            ActionType {
                r#type: "type 2".into(),
                description: "more awesomeness".into(),
            },
        ];

        let response = actions.iter().map(|i| Ok(i.clone())).collect();
        test_server.set_list_actions_response(response);

        let response_stream = client.list_actions().await.expect("error making request");

        let expected_response = actions;
        let response: Vec<_> = response_stream
            .try_collect()
            .await
            .expect("Error streaming data");

        assert_eq!(response, expected_response);
        assert_eq!(test_server.take_list_actions_request(), Some(Empty {}));
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_list_actions_error() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();

        let response = client.list_actions().await;
        let response = match response {
            Ok(_) => panic!("unexpected success"),
            Err(e) => e,
        };

        let e = Status::internal("No list_actions response configured");
        expect_status(response, e);
        // server still got the request
        assert_eq!(test_server.take_list_actions_request(), Some(Empty {}));
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_list_actions_error_in_stream() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();

        let e = Status::data_loss("she's dead jim");

        let response = vec![
            Ok(ActionType {
                r#type: "type 1".into(),
                description: "awesomeness".into(),
            }),
            Err(e.clone()),
        ];
        test_server.set_list_actions_response(response);

        let response_stream = client.list_actions().await.expect("error making request");

        let response: Result<Vec<_>, FlightError> = response_stream.try_collect().await;

        let response = response.unwrap_err();
        expect_status(response, e);
        // server still got the request
        assert_eq!(test_server.take_list_actions_request(), Some(Empty {}));
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_do_action() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();

        let bytes = vec![Bytes::from("foo"), Bytes::from("blarg")];

        let response = bytes
            .iter()
            .cloned()
            .map(arrow_flight::Result::new)
            .map(Ok)
            .collect();
        test_server.set_do_action_response(response);

        let request = Action::new("action type", "action body");

        let response_stream = client
            .do_action(request.clone())
            .await
            .expect("error making request");

        let expected_response = bytes;
        let response: Vec<_> = response_stream
            .try_collect()
            .await
            .expect("Error streaming data");

        assert_eq!(response, expected_response);
        assert_eq!(test_server.take_do_action_request(), Some(request));
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_do_action_error() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();

        let request = Action::new("action type", "action body");

        let response = client.do_action(request.clone()).await;
        let response = match response {
            Ok(_) => panic!("unexpected success"),
            Err(e) => e,
        };

        let e = Status::internal("No do_action response configured");
        expect_status(response, e);
        // server still got the request
        assert_eq!(test_server.take_do_action_request(), Some(request));
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_do_action_error_in_stream() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();

        let e = Status::data_loss("she's dead jim");

        let request = Action::new("action type", "action body");

        let response = vec![Ok(arrow_flight::Result::new("foo")), Err(e.clone())];
        test_server.set_do_action_response(response);

        let response_stream = client
            .do_action(request.clone())
            .await
            .expect("error making request");

        let response: Result<Vec<_>, FlightError> = response_stream.try_collect().await;

        let response = response.unwrap_err();
        expect_status(response, e);
        // server still got the request
        assert_eq!(test_server.take_do_action_request(), Some(request));
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_cancel_flight_info() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();

        let expected_response = CancelFlightInfoResult::new(CancelStatus::Cancelled);
        let response = expected_response.encode_to_vec();
        let response = Ok(arrow_flight::Result::new(response));
        test_server.set_do_action_response(vec![response]);

        let request = CancelFlightInfoRequest::new(FlightInfo::new());
        let actual_response = client
            .cancel_flight_info(request.clone())
            .await
            .expect("error making request");

        let expected_request = Action::new("CancelFlightInfo", request.encode_to_vec());
        assert_eq!(actual_response, expected_response);
        assert_eq!(test_server.take_do_action_request(), Some(expected_request));
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_cancel_flight_info_error_no_response() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();

        test_server.set_do_action_response(vec![]);

        let request = CancelFlightInfoRequest::new(FlightInfo::new());
        let err = client
            .cancel_flight_info(request.clone())
            .await
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Protocol error: Received no response for cancel_flight_info call"
        );
        // server still got the request
        let expected_request = Action::new("CancelFlightInfo", request.encode_to_vec());
        assert_eq!(test_server.take_do_action_request(), Some(expected_request));
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_renew_flight_endpoint() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();

        let expected_response = FlightEndpoint::new().with_app_metadata(vec![1]);
        let response = expected_response.encode_to_vec();
        let response = Ok(arrow_flight::Result::new(response));
        test_server.set_do_action_response(vec![response]);

        let request =
            RenewFlightEndpointRequest::new(FlightEndpoint::new().with_app_metadata(vec![0]));
        let actual_response = client
            .renew_flight_endpoint(request.clone())
            .await
            .expect("error making request");

        let expected_request = Action::new("RenewFlightEndpoint", request.encode_to_vec());
        assert_eq!(actual_response, expected_response);
        assert_eq!(test_server.take_do_action_request(), Some(expected_request));
        ensure_metadata(&client, &test_server);
    })
    .await;
}

#[tokio::test]
async fn test_renew_flight_endpoint_error_no_response() {
    do_test(|test_server, mut client| async move {
        client.add_header("foo-header", "bar-header-value").unwrap();

        test_server.set_do_action_response(vec![]);

        let request = RenewFlightEndpointRequest::new(FlightEndpoint::new());
        let err = client
            .renew_flight_endpoint(request.clone())
            .await
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Protocol error: Received no response for renew_flight_endpoint call"
        );
        // server still got the request
        let expected_request = Action::new("RenewFlightEndpoint", request.encode_to_vec());
        assert_eq!(test_server.take_do_action_request(), Some(expected_request));
        ensure_metadata(&client, &test_server);
    })
    .await;
}

async fn test_flight_data() -> Vec<FlightData> {
    let batch = RecordBatch::try_from_iter(vec![(
        "col",
        Arc::new(UInt64Array::from_iter([1, 2, 3, 4])) as _,
    )])
    .unwrap();

    // encode the batch as a stream of FlightData
    FlightDataEncoderBuilder::new()
        .build(futures::stream::iter(vec![Ok(batch)]))
        .try_collect()
        .await
        .unwrap()
}

async fn test_flight_data2() -> Vec<FlightData> {
    let batch = RecordBatch::try_from_iter(vec![(
        "col2",
        Arc::new(UInt64Array::from_iter([10, 23, 33])) as _,
    )])
    .unwrap();

    // encode the batch as a stream of FlightData
    FlightDataEncoderBuilder::new()
        .build(futures::stream::iter(vec![Ok(batch)]))
        .try_collect()
        .await
        .unwrap()
}

/// Runs the future returned by the function,  passing it a test server and client
async fn do_test<F, Fut>(f: F)
where
    F: Fn(TestFlightServer, FlightClient) -> Fut,
    Fut: Future<Output = ()>,
{
    let test_server = TestFlightServer::new();
    let fixture = TestFixture::new(test_server.service()).await;
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
        panic!("Expected FlightError::Tonic, got: {error:?}");
    };

    assert_eq!(
        status.code(),
        expected.code(),
        "Got {status:?} want {expected:?}"
    );
    assert_eq!(
        status.message(),
        expected.message(),
        "Got {status:?} want {expected:?}"
    );
    assert_eq!(
        status.details(),
        expected.details(),
        "Got {status:?} want {expected:?}"
    );
}
