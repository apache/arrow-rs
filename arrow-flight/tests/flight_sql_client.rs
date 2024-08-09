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
use arrow_flight::flight_service_server::FlightServiceServer;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{
    ActionBeginTransactionRequest, ActionBeginTransactionResult, ActionEndTransactionRequest,
    EndTransaction, SqlInfo,
};
use arrow_flight::Action;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Status};
use uuid::Uuid;

#[tokio::test]
pub async fn test_begin_end_transaction() {
    let test_server = FlightSqlServiceImpl {
        transactions: Arc::new(Mutex::new(HashMap::new())),
    };
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
    assert!(flight_sql_client
        .end_transaction(transaction_id, EndTransaction::Commit)
        .await
        .is_err());
}

#[derive(Clone)]
pub struct FlightSqlServiceImpl {
    transactions: Arc<Mutex<HashMap<String, ()>>>,
}

impl FlightSqlServiceImpl {
    /// Return an [`FlightServiceServer`] that can be used with a
    /// [`Server`](tonic::transport::Server)
    pub fn service(&self) -> FlightServiceServer<Self> {
        // wrap up tonic goop
        FlightServiceServer::new(self.clone())
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
}
