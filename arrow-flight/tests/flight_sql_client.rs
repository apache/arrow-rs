// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

mod common;

use crate::common::fixture::TestFixture;
use crate::common::server::TestFlightServer;
use crate::common::utils::make_primitive_batch;

use arrow_array::RecordBatch;
use arrow_flight::close_session_result::Status as CloseSessionStatus;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionBeginTransactionRequest, ActionBeginTransactionResult, ActionEndTransactionRequest,
    CommandStatementIngest, EndTransaction, FallibleRequestStream, ProstMessageExt,
    TableDefinitionOptions, TableExistsOption, TableNotExistOption,
};
use arrow_flight::{
    Action, CloseSessionRequest, CloseSessionResult, Empty, FlightData, FlightDescriptor,
};
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{IntoStreamingRequest, Request, Status};
use uuid::Uuid;

#[tokio::test]
async fn test_close_session_statuses() {
    let test_server = TestFlightServer::new();
    let fixture = TestFixture::new(test_server.service()).await;
    let mut client = FlightSqlServiceClient::new(fixture.channel().await);

    for status in [
        CloseSessionStatus::Unspecified,
        CloseSessionStatus::Closed,
        CloseSessionStatus::Closing,
        CloseSessionStatus::NotCloseable,
    ] {
        let response = CloseSessionResult {
            status: status as i32,
        };
        test_server.set_do_action_response(vec![Ok(arrow_flight::Result::new(
            response.encode_to_vec(),
        ))]);
        assert_eq!(client.close().await.unwrap(), response);
    }

    let unknown_status = 99;
    let response = CloseSessionResult {
        status: unknown_status,
    };
    test_server.set_do_action_response(vec![Ok(arrow_flight::Result::new(
        response.encode_to_vec(),
    ))]);
    assert_eq!(client.close().await.unwrap().status, unknown_status);
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_close_session_errors() {
    let test_server = TestFlightServer::new();
    let fixture = TestFixture::new(test_server.service()).await;
    let mut client = FlightSqlServiceClient::new(fixture.channel().await);

    test_server.set_do_action_response(vec![]);
    assert!(matches!(
        client.close().await,
        Err(FlightError::ProtocolError(_))
    ));

    test_server.set_do_action_response(vec![Ok(arrow_flight::Result::new(vec![0xff]))]);
    assert!(matches!(
        client.close().await,
        Err(FlightError::DecodeError(_))
    ));

    let closed = CloseSessionResult {
        status: CloseSessionStatus::Closed as i32,
    };
    test_server.set_do_action_response(vec![
        Ok(arrow_flight::Result::new(closed.encode_to_vec())),
        Err(Status::not_found("Session not found")),
    ]);
    let err = client.close().await.unwrap_err();
    assert!(matches!(err, FlightError::Tonic(status) if status.code() == tonic::Code::NotFound));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_close_session_sql_server() {
    let test_server = FlightSqlServiceImpl::new();
    let cookie = "arrow_flight_session_id=test-session";
    test_server.sessions.lock().await.insert(cookie.into());
    let fixture = TestFixture::new(test_server.service()).await;
    let mut client = FlightSqlServiceClient::new(fixture.channel().await);
    client.set_header("cookie", cookie);
    client.set_token("test-token".into());

    let actions: Vec<_> = client
        .inner_mut()
        .list_actions(Empty {})
        .await
        .unwrap()
        .into_inner()
        .try_collect()
        .await
        .unwrap();
    assert!(actions.iter().any(|action| action.r#type == "CloseSession"));

    let err = client
        .do_action(Action::new("CloseSession", vec![0xff]))
        .await
        .unwrap_err();
    assert!(
        matches!(err, FlightError::Tonic(status) if status.code() == tonic::Code::InvalidArgument)
    );
    assert!(test_server.sessions.lock().await.contains(cookie));

    assert_eq!(
        client.close().await.unwrap().status(),
        CloseSessionStatus::Closed
    );
    assert!(test_server.sessions.lock().await.is_empty());

    let err = client.close().await.unwrap_err();
    assert!(matches!(err, FlightError::Tonic(status) if status.code() == tonic::Code::NotFound));
    fixture.shutdown_and_wait().await;
}

#[tokio::test]
async fn test_close_session_unimplemented() {
    struct DefaultService;

    #[tonic::async_trait]
    impl FlightSqlService for DefaultService {
        type FlightService = Self;
    }

    let err = arrow_flight::flight_service_server::FlightService::do_action(
        &DefaultService,
        Request::new(Action::new(
            "CloseSession",
            CloseSessionRequest {}.encode_to_vec(),
        )),
    )
    .await
    .err()
    .unwrap();
    assert_eq!(err.code(), tonic::Code::Unimplemented);
}

#[tokio::test]
pub async fn test_begin_end_transaction() {
    let test_server = FlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service()).await;
    let channel = fixture.channel().await;
    let mut flight_sql_client = FlightSqlServiceClient::new(channel);

    // begin commit
    let transaction_id = flight_sql_client.begin_transaction().await.unwrap();
    flight_sql_client
        .end_transaction(transaction_id, EndTransaction::Commit)
        .await
        .unwrap();

    // begin rollback
    let transaction_id = flight_sql_client.begin_transaction().await.unwrap();
    flight_sql_client
        .end_transaction(transaction_id, EndTransaction::Rollback)
        .await
        .unwrap();

    // unknown transaction id
    let transaction_id = "UnknownTransactionId".to_string().into();
    assert!(
        flight_sql_client
            .end_transaction(transaction_id, EndTransaction::Commit)
            .await
            .is_err()
    );
}

#[tokio::test]
pub async fn test_execute_ingest() {
    let test_server = FlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service()).await;
    let channel = fixture.channel().await;
    let mut flight_sql_client = FlightSqlServiceClient::new(channel);
    let cmd = make_ingest_command();
    let expected_rows = 10;
    let batches = vec![
        make_primitive_batch(5),
        make_primitive_batch(3),
        make_primitive_batch(2),
    ];
    let actual_rows = flight_sql_client
        .execute_ingest(cmd, futures::stream::iter(batches.clone()).map(Ok))
        .await
        .expect("ingest should succeed");
    assert_eq!(actual_rows, expected_rows);
    // make sure the batches made it through to the server
    let ingested_batches = test_server.ingested_batches.lock().await.clone();
    assert_eq!(ingested_batches, batches);
}

#[tokio::test]
pub async fn test_execute_ingest_error() {
    let test_server = FlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service()).await;
    let channel = fixture.channel().await;
    let mut flight_sql_client = FlightSqlServiceClient::new(channel);
    let cmd = make_ingest_command();
    // send an error from the client
    let batches = vec![
        Ok(make_primitive_batch(5)),
        Err(FlightError::NotYetImplemented(
            "Client error message".to_string(),
        )),
    ];
    // make sure the client returns the error from the client
    let err = flight_sql_client
        .execute_ingest(cmd, futures::stream::iter(batches))
        .await
        .unwrap_err();
    assert_eq!(
        err.to_string(),
        "External error: Not yet implemented: Client error message"
    );
}

#[tokio::test]
pub async fn test_do_put_empty_stream() {
    // Test for https://github.com/apache/arrow-rs/issues/7329

    let test_server = FlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service()).await;
    let channel = fixture.channel().await;
    let mut flight_sql_client = FlightSqlServiceClient::new(channel);
    let cmd = make_ingest_command();

    // Create an empty request stream
    let input_data = futures::stream::iter(vec![]);
    let flight_descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
    let flight_data_encoder = FlightDataEncoderBuilder::default()
        .with_flight_descriptor(Some(flight_descriptor))
        .build(input_data);
    let flight_data: Vec<FlightData> = Box::pin(flight_data_encoder).try_collect().await.unwrap();
    let request_stream = futures::stream::iter(flight_data);

    // Execute a `do_put` and verify that the server error contains the expected message
    let err = flight_sql_client.do_put(request_stream).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("Unhandled Error: Command is missing."),
    );
}

#[tokio::test]
pub async fn test_do_put_first_element_err() {
    // Test for https://github.com/apache/arrow-rs/issues/7329

    let test_server = FlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service()).await;
    let channel = fixture.channel().await;
    let mut flight_sql_client = FlightSqlServiceClient::new(channel);
    let cmd = make_ingest_command();

    let (sender, _receiver) = futures::channel::oneshot::channel();

    // Create a fallible request stream such that the 1st element is a FlightError
    let input_data = futures::stream::iter(vec![
        Err(FlightError::NotYetImplemented("random error".to_string())),
        Ok(make_primitive_batch(5)),
    ]);
    let flight_descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
    let flight_data_encoder = FlightDataEncoderBuilder::default()
        .with_flight_descriptor(Some(flight_descriptor))
        .build(input_data);
    let flight_data: FallibleRequestStream<FlightData, FlightError> =
        FallibleRequestStream::new(sender, Box::pin(flight_data_encoder));
    let request_stream = flight_data.into_streaming_request();

    // Execute a `do_put` and verify that the server error contains the expected message
    let err = flight_sql_client.do_put(request_stream).await.unwrap_err();

    assert!(
        err.to_string()
            .contains("Unhandled Error: Command is missing."),
    );
}

#[tokio::test]
pub async fn test_do_put_missing_flight_descriptor() {
    // Test for https://github.com/apache/arrow-rs/issues/7329

    let test_server = FlightSqlServiceImpl::new();
    let fixture = TestFixture::new(test_server.service()).await;
    let channel = fixture.channel().await;
    let mut flight_sql_client = FlightSqlServiceClient::new(channel);

    // Create a request stream such that the flight descriptor is missing
    let stream = futures::stream::iter(vec![Ok(make_primitive_batch(5))]);
    let flight_data_encoder = FlightDataEncoderBuilder::default()
        .with_flight_descriptor(None)
        .build(stream);
    let flight_data: Vec<FlightData> = Box::pin(flight_data_encoder).try_collect().await.unwrap();
    let request_stream = futures::stream::iter(flight_data);

    // Execute a `do_put` and verify that the server error contains the expected message
    let err = flight_sql_client.do_put(request_stream).await.unwrap_err();
    assert!(
        err.to_string()
            .contains("Unhandled Error: Flight descriptor is missing."),
    );
}

fn make_ingest_command() -> CommandStatementIngest {
    CommandStatementIngest {
        table_definition_options: Some(TableDefinitionOptions {
            if_not_exist: TableNotExistOption::Create.into(),
            if_exists: TableExistsOption::Fail.into(),
        }),
        table: String::from("test"),
        schema: None,
        catalog: None,
        temporary: true,
        transaction_id: None,
        options: HashMap::default(),
    }
}

#[derive(Clone)]
pub struct FlightSqlServiceImpl {
    sessions: Arc<Mutex<HashSet<String>>>,
    transactions: Arc<Mutex<HashSet<String>>>,
    ingested_batches: Arc<Mutex<Vec<RecordBatch>>>,
}

impl FlightSqlServiceImpl {
    pub fn new() -> Self {
        Self {
            sessions: Arc::new(Mutex::new(HashSet::new())),
            transactions: Arc::new(Mutex::new(HashSet::new())),
            ingested_batches: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Return an [`FlightServiceServer`] that can be used with a
    /// [`Server`](tonic::transport::Server)
    pub fn service(&self) -> FlightServiceServer<Self> {
        // wrap up tonic goop
        FlightServiceServer::new(self.clone())
    }
}

impl Default for FlightSqlServiceImpl {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = FlightSqlServiceImpl;

    async fn do_action_close_session(
        &self,
        _query: CloseSessionRequest,
        request: Request<Action>,
    ) -> Result<CloseSessionResult, Status> {
        assert_eq!(
            request.metadata().get("authorization").unwrap(),
            "Bearer test-token"
        );
        let cookie = request
            .metadata()
            .get("cookie")
            .ok_or_else(|| Status::invalid_argument("Missing session cookie"))?
            .to_str()
            .map_err(|_| Status::invalid_argument("Invalid session cookie"))?;
        if !self.sessions.lock().await.remove(cookie) {
            return Err(Status::not_found("Session not found"));
        }
        Ok(CloseSessionResult {
            status: CloseSessionStatus::Closed as i32,
        })
    }

    async fn do_action_begin_transaction(
        &self,
        _query: ActionBeginTransactionRequest,
        _request: Request<Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        let transaction_id = Uuid::new_v4().to_string();
        self.transactions
            .lock()
            .await
            .insert(transaction_id.clone());
        Ok(ActionBeginTransactionResult {
            transaction_id: transaction_id.as_bytes().to_vec().into(),
        })
    }

    async fn do_action_end_transaction(
        &self,
        query: ActionEndTransactionRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        let transaction_id = String::from_utf8(query.transaction_id.to_vec())
            .map_err(|_| Status::invalid_argument("Invalid transaction id"))?;
        if !self.transactions.lock().await.remove(&transaction_id) {
            return Err(Status::invalid_argument("Transaction id not found"));
        }
        Ok(())
    }

    async fn do_put_statement_ingest(
        &self,
        _ticket: CommandStatementIngest,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(
            request.into_inner().map_err(|e| e.into()),
        )
        .try_collect()
        .await?;
        let affected_rows = batches.iter().map(|batch| batch.num_rows() as i64).sum();
        *self.ingested_batches.lock().await.as_mut() = batches;
        Ok(affected_rows)
    }
}
