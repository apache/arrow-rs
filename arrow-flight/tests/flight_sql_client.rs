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

mod common;

use crate::common::fixture::TestFixture;
use crate::common::utils::make_primitive_batch;

use arrow_array::RecordBatch;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionBeginTransactionRequest, ActionBeginTransactionResult, ActionEndTransactionRequest,
    CommandStatementIngest, EndTransaction, FallibleRequestStream, ProstMessageExt, SqlInfo,
    TableDefinitionOptions, TableExistsOption, TableNotExistOption,
};
use arrow_flight::{Action, FlightData, FlightDescriptor};
use futures::{StreamExt, TryStreamExt};
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{IntoStreamingRequest, Request, Status};
use uuid::Uuid;

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
    transactions: Arc<Mutex<HashMap<String, ()>>>,
    ingested_batches: Arc<Mutex<Vec<RecordBatch>>>,
}

impl FlightSqlServiceImpl {
    pub fn new() -> Self {
        Self {
            transactions: Arc::new(Mutex::new(HashMap::new())),
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

    async fn do_action_begin_transaction(
        &self,
        _query: ActionBeginTransactionRequest,
        _request: Request<Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        let transaction_id = Uuid::new_v4().to_string();
        self.transactions
            .lock()
            .await
            .insert(transaction_id.clone(), ());
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
        if self
            .transactions
            .lock()
            .await
            .remove(&transaction_id)
            .is_none()
        {
            return Err(Status::invalid_argument("Transaction id not found"));
        }
        Ok(())
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}

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
