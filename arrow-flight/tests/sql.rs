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

//! Integration test for FlightSQL client
mod common {
    // Use common mock server
    pub mod server;
}
use common::server::{
    do_test,
    expect_status,
    TestFlightServer
};


#[tokio::test]
async fn test_handshake() {
    do_test(|test_server, client| async move {
        let sql_client = FlightSQL

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
