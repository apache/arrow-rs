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

//! Integration test: a tonic [`Channel`] wrapped in an [`InterceptedService`]
//! that injects a custom header is passed to [`FlightClient`], and the server
//! observes the header on the request.

mod common;

use std::sync::Arc;

use arrow_array::{RecordBatch, UInt64Array};
use arrow_flight::{FlightClient, Ticket};
use bytes::Bytes;
use futures::TryStreamExt;
use tonic::Request;
use tonic::Status;
use tonic::service::interceptor::InterceptedService;
use uuid::Uuid;

use crate::common::fixture::TestFixture;
use crate::common::server::TestFlightServer;

#[tokio::test]
async fn test_flight_client_with_intercepted_channel_passes_custom_header() {
    let test_server = TestFlightServer::new();
    let fixture = TestFixture::new(test_server.service()).await;

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

    fixture.shutdown_and_wait().await
}
