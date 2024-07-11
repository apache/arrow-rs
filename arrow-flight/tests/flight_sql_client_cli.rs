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

use std::{net::SocketAddr, pin::Pin, sync::Arc, time::Duration};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_flight::{
    decode::FlightRecordBatchStream,
    flight_service_server::{FlightService, FlightServiceServer},
    sql::{
        server::{FlightSqlService, PeekableFlightDataStream},
        ActionBeginSavepointRequest, ActionBeginSavepointResult, ActionBeginTransactionRequest,
        ActionBeginTransactionResult, ActionCancelQueryRequest, ActionCancelQueryResult,
        ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
        ActionCreatePreparedStatementResult, ActionCreatePreparedSubstraitPlanRequest,
        ActionEndSavepointRequest, ActionEndTransactionRequest, Any, CommandGetCatalogs,
        CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys,
        CommandGetImportedKeys, CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTableTypes,
        CommandGetTables, CommandGetXdbcTypeInfo, CommandPreparedStatementQuery,
        CommandPreparedStatementUpdate, CommandStatementQuery, CommandStatementSubstraitPlan,
        CommandStatementUpdate, DoPutPreparedStatementResult, ProstMessageExt, SqlInfo,
        TicketStatementQuery,
    },
    utils::batches_to_flight_data,
    Action, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, IpcMessage, SchemaAsIpc, Ticket,
};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::{ArrowError, DataType, Field, Schema};
use assert_cmd::Command;
use bytes::Bytes;
use futures::{Stream, TryStreamExt};
use prost::Message;
use tokio::{net::TcpListener, task::JoinHandle};
use tonic::{Request, Response, Status, Streaming};

const QUERY: &str = "SELECT * FROM table;";

#[tokio::test]
async fn test_simple() {
    let test_server = FlightSqlServiceImpl::default();
    let fixture = TestFixture::new(&test_server).await;
    let addr = fixture.addr;

    let stdout = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("flight_sql_client")
            .unwrap()
            .env_clear()
            .env("RUST_BACKTRACE", "1")
            .env("RUST_LOG", "warn")
            .arg("--host")
            .arg(addr.ip().to_string())
            .arg("--port")
            .arg(addr.port().to_string())
            .arg("statement-query")
            .arg(QUERY)
            .assert()
            .success()
            .get_output()
            .stdout
            .clone()
    })
    .await
    .unwrap();

    fixture.shutdown_and_wait().await;

    assert_eq!(
        std::str::from_utf8(&stdout).unwrap().trim(),
        "+--------------+-----------+\
        \n| field_string | field_int |\
        \n+--------------+-----------+\
        \n| Hello        | 42        |\
        \n| lovely       |           |\
        \n| FlightSQL!   | 1337      |\
        \n+--------------+-----------+",
    );
}

const PREPARED_QUERY: &str = "SELECT * FROM table WHERE field = $1";
const PREPARED_STATEMENT_HANDLE: &str = "prepared_statement_handle";
const UPDATED_PREPARED_STATEMENT_HANDLE: &str = "updated_prepared_statement_handle";

async fn test_do_put_prepared_statement(test_server: FlightSqlServiceImpl) {
    let fixture = TestFixture::new(&test_server).await;
    let addr = fixture.addr;

    let stdout = tokio::task::spawn_blocking(move || {
        Command::cargo_bin("flight_sql_client")
            .unwrap()
            .env_clear()
            .env("RUST_BACKTRACE", "1")
            .env("RUST_LOG", "warn")
            .arg("--host")
            .arg(addr.ip().to_string())
            .arg("--port")
            .arg(addr.port().to_string())
            .arg("prepared-statement-query")
            .arg(PREPARED_QUERY)
            .args(["-p", "$1=string"])
            .args(["-p", "$2=64"])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone()
    })
    .await
    .unwrap();

    fixture.shutdown_and_wait().await;

    assert_eq!(
        std::str::from_utf8(&stdout).unwrap().trim(),
        "+--------------+-----------+\
        \n| field_string | field_int |\
        \n+--------------+-----------+\
        \n| Hello        | 42        |\
        \n| lovely       |           |\
        \n| FlightSQL!   | 1337      |\
        \n+--------------+-----------+",
    );
}

#[tokio::test]
pub async fn test_do_put_prepared_statement_stateless() {
    test_do_put_prepared_statement(FlightSqlServiceImpl {
        stateless_prepared_statements: true,
    })
    .await
}

#[tokio::test]
pub async fn test_do_put_prepared_statement_stateful() {
    test_do_put_prepared_statement(FlightSqlServiceImpl {
        stateless_prepared_statements: false,
    })
    .await
}

/// All tests must complete within this many seconds or else the test server is shutdown
const DEFAULT_TIMEOUT_SECONDS: u64 = 30;

#[derive(Clone)]
pub struct FlightSqlServiceImpl {
    /// Whether to emulate stateless (true) or stateful (false) behavior for
    /// prepared statements. stateful servers will not return an updated
    /// handle after executing `DoPut(CommandPreparedStatementQuery)`
    stateless_prepared_statements: bool,
}

impl Default for FlightSqlServiceImpl {
    fn default() -> Self {
        Self {
            stateless_prepared_statements: true,
        }
    }
}

impl FlightSqlServiceImpl {
    /// Return an [`FlightServiceServer`] that can be used with a
    /// [`Server`](tonic::transport::Server)
    pub fn service(&self) -> FlightServiceServer<Self> {
        // wrap up tonic goop
        FlightServiceServer::new(self.clone())
    }

    fn fake_result() -> Result<RecordBatch, ArrowError> {
        let schema = Schema::new(vec![
            Field::new("field_string", DataType::Utf8, false),
            Field::new("field_int", DataType::Int64, true),
        ]);

        let string_array = StringArray::from(vec!["Hello", "lovely", "FlightSQL!"]);
        let int_array = Int64Array::from(vec![Some(42), None, Some(1337)]);

        let cols = vec![
            Arc::new(string_array) as ArrayRef,
            Arc::new(int_array) as ArrayRef,
        ];
        RecordBatch::try_new(Arc::new(schema), cols)
    }

    fn create_fake_prepared_stmt() -> Result<ActionCreatePreparedStatementResult, ArrowError> {
        let handle = PREPARED_STATEMENT_HANDLE.to_string();
        let schema = Schema::new(vec![
            Field::new("field_string", DataType::Utf8, false),
            Field::new("field_int", DataType::Int64, true),
        ]);

        let parameter_schema = Schema::new(vec![
            Field::new("$1", DataType::Utf8, false),
            Field::new("$2", DataType::Int64, true),
        ]);

        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: handle.into(),
            dataset_schema: serialize_schema(&schema)?,
            parameter_schema: serialize_schema(&parameter_schema)?,
        })
    }

    fn fake_flight_info(&self) -> Result<FlightInfo, ArrowError> {
        let batch = Self::fake_result()?;

        Ok(FlightInfo::new()
            .try_with_schema(batch.schema_ref())
            .expect("encoding schema")
            .with_endpoint(
                FlightEndpoint::new().with_ticket(Ticket::new(
                    FetchResults {
                        handle: String::from("part_1"),
                    }
                    .as_any()
                    .encode_to_vec(),
                )),
            )
            .with_endpoint(
                FlightEndpoint::new().with_ticket(Ticket::new(
                    FetchResults {
                        handle: String::from("part_2"),
                    }
                    .as_any()
                    .encode_to_vec(),
                )),
            )
            .with_total_records(batch.num_rows() as i64)
            .with_total_bytes(batch.get_array_memory_size() as i64)
            .with_ordered(false))
    }
}

fn serialize_schema(schema: &Schema) -> Result<Bytes, ArrowError> {
    Ok(IpcMessage::try_from(SchemaAsIpc::new(schema, &IpcWriteOptions::default()))?.0)
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = FlightSqlServiceImpl;

    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        Err(Status::unimplemented("do_handshake not implemented"))
    }

    async fn do_get_fallback(
        &self,
        _request: Request<Ticket>,
        message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let part = message.unpack::<FetchResults>().unwrap().unwrap().handle;
        let batch = Self::fake_result().unwrap();
        let batch = match part.as_str() {
            "part_1" => batch.slice(0, 2),
            "part_2" => batch.slice(2, 1),
            ticket => panic!("Invalid ticket: {ticket:?}"),
        };
        let schema = batch.schema_ref();
        let batches = vec![batch.clone()];
        let flight_data = batches_to_flight_data(schema, batches)
            .unwrap()
            .into_iter()
            .map(Ok);

        let stream: Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>> =
            Box::pin(futures::stream::iter(flight_data));
        let resp = Response::new(stream);
        Ok(resp)
    }

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        assert_eq!(query.query, QUERY);

        let resp = Response::new(self.fake_flight_info().unwrap());
        Ok(resp)
    }

    async fn get_flight_info_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        if self.stateless_prepared_statements {
            assert_eq!(
                cmd.prepared_statement_handle,
                UPDATED_PREPARED_STATEMENT_HANDLE.as_bytes()
            );
        } else {
            assert_eq!(
                cmd.prepared_statement_handle,
                PREPARED_STATEMENT_HANDLE.as_bytes()
            );
        }
        let resp = Response::new(self.fake_flight_info().unwrap());
        Ok(resp)
    }

    async fn get_flight_info_substrait_plan(
        &self,
        _query: CommandStatementSubstraitPlan,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_substrait_plan not implemented",
        ))
    }

    async fn get_flight_info_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_catalogs not implemented",
        ))
    }

    async fn get_flight_info_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_schemas not implemented",
        ))
    }

    async fn get_flight_info_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_tables not implemented",
        ))
    }

    async fn get_flight_info_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_table_types not implemented",
        ))
    }

    async fn get_flight_info_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_sql_info not implemented",
        ))
    }

    async fn get_flight_info_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_primary_keys not implemented",
        ))
    }

    async fn get_flight_info_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_exported_keys not implemented",
        ))
    }

    async fn get_flight_info_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_imported_keys not implemented",
        ))
    }

    async fn get_flight_info_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_imported_keys not implemented",
        ))
    }

    async fn get_flight_info_xdbc_type_info(
        &self,
        _query: CommandGetXdbcTypeInfo,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_xdbc_type_info not implemented",
        ))
    }

    // do_get
    async fn do_get_statement(
        &self,
        _ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_statement not implemented"))
    }

    async fn do_get_prepared_statement(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_prepared_statement not implemented",
        ))
    }

    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_catalogs not implemented"))
    }

    async fn do_get_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_schemas not implemented"))
    }

    async fn do_get_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_tables not implemented"))
    }

    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_table_types not implemented"))
    }

    async fn do_get_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_sql_info not implemented"))
    }

    async fn do_get_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_primary_keys not implemented"))
    }

    async fn do_get_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_exported_keys not implemented",
        ))
    }

    async fn do_get_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_imported_keys not implemented",
        ))
    }

    async fn do_get_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_cross_reference not implemented",
        ))
    }

    async fn do_get_xdbc_type_info(
        &self,
        _query: CommandGetXdbcTypeInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_xdbc_type_info not implemented",
        ))
    }

    // do_put
    async fn do_put_statement_update(
        &self,
        _ticket: CommandStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented(
            "do_put_statement_update not implemented",
        ))
    }

    async fn do_put_substrait_plan(
        &self,
        _ticket: CommandStatementSubstraitPlan,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented(
            "do_put_substrait_plan not implemented",
        ))
    }

    async fn do_put_prepared_statement_query(
        &self,
        _query: CommandPreparedStatementQuery,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        // just make sure decoding the parameters works
        let parameters = FlightRecordBatchStream::new_from_flight_data(
            request.into_inner().map_err(|e| e.into()),
        )
        .try_collect::<Vec<_>>()
        .await?;

        for (left, right) in parameters[0].schema().flattened_fields().iter().zip(vec![
            Field::new("$1", DataType::Utf8, false),
            Field::new("$2", DataType::Int64, true),
        ]) {
            if left.name() != right.name() || left.data_type() != right.data_type() {
                return Err(Status::invalid_argument(format!(
                    "Parameters did not match parameter schema\ngot {}",
                    parameters[0].schema(),
                )));
            }
        }
        let handle = if self.stateless_prepared_statements {
            UPDATED_PREPARED_STATEMENT_HANDLE.to_string().into()
        } else {
            PREPARED_STATEMENT_HANDLE.to_string().into()
        };
        let result = DoPutPreparedStatementResult {
            prepared_statement_handle: Some(handle),
        };
        Ok(result)
    }

    async fn do_put_prepared_statement_update(
        &self,
        _query: CommandPreparedStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented(
            "do_put_prepared_statement_update not implemented",
        ))
    }

    async fn do_action_create_prepared_statement(
        &self,
        _query: ActionCreatePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        Self::create_fake_prepared_stmt()
            .map_err(|e| Status::internal(format!("Unable to serialize schema: {e}")))
    }

    async fn do_action_close_prepared_statement(
        &self,
        _query: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        unimplemented!("Implement do_action_close_prepared_statement")
    }

    async fn do_action_create_prepared_substrait_plan(
        &self,
        _query: ActionCreatePreparedSubstraitPlanRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        unimplemented!("Implement do_action_create_prepared_substrait_plan")
    }

    async fn do_action_begin_transaction(
        &self,
        _query: ActionBeginTransactionRequest,
        _request: Request<Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        unimplemented!("Implement do_action_begin_transaction")
    }

    async fn do_action_end_transaction(
        &self,
        _query: ActionEndTransactionRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        unimplemented!("Implement do_action_end_transaction")
    }

    async fn do_action_begin_savepoint(
        &self,
        _query: ActionBeginSavepointRequest,
        _request: Request<Action>,
    ) -> Result<ActionBeginSavepointResult, Status> {
        unimplemented!("Implement do_action_begin_savepoint")
    }

    async fn do_action_end_savepoint(
        &self,
        _query: ActionEndSavepointRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        unimplemented!("Implement do_action_end_savepoint")
    }

    async fn do_action_cancel_query(
        &self,
        _query: ActionCancelQueryRequest,
        _request: Request<Action>,
    ) -> Result<ActionCancelQueryResult, Status> {
        unimplemented!("Implement do_action_cancel_query")
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
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
    pub async fn new(test_server: &FlightSqlServiceImpl) -> Self {
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

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct FetchResults {
    #[prost(string, tag = "1")]
    pub handle: ::prost::alloc::string::String,
}

impl ProstMessageExt for FetchResults {
    fn type_url() -> &'static str {
        "type.googleapis.com/arrow.flight.protocol.sql.FetchResults"
    }

    fn as_any(&self) -> Any {
        Any {
            type_url: FetchResults::type_url().to_string(),
            value: ::prost::Message::encode_to_vec(self).into(),
        }
    }
}
