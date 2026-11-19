// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

mod common;

use std::{
    net::SocketAddr,
    pin::Pin,
    process::Output,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::common::fixture::TestFixture;
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray};
use arrow_flight::{
    Action, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, IpcMessage, Location, SchemaAsIpc, Ticket,
    decode::FlightRecordBatchStream,
    encode::FlightDataEncoderBuilder,
    flight_service_server::{FlightService, FlightServiceServer},
    sql::{
        ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult, Any,
        CommandGetCatalogs, CommandGetDbSchemas, CommandGetTableTypes, CommandGetTables,
        CommandPreparedStatementQuery, CommandStatementQuery, DoPutPreparedStatementResult,
        ProstMessageExt,
        server::{FlightSqlService, PeekableFlightDataStream},
    },
    utils::batches_to_flight_data,
};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::{ArrowError, DataType, Field, Schema, TimeUnit};
use assert_cmd::Command;
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};
use prost::Message;
use tokio::{io::AsyncReadExt, net::TcpListener};
use tonic::{Request, Response, Status, Streaming};

const QUERY: &str = "SELECT * FROM table;";

/// Return a Command instance for running the `flight_sql_client` CLI
fn flight_sql_client_cmd() -> Command {
    Command::new(assert_cmd::cargo::cargo_bin!("flight_sql_client"))
}

#[tokio::test]
async fn test_simple() {
    let test_server = FlightSqlServiceImpl::default();
    let fixture = TestFixture::new(test_server.service()).await;
    let addr = fixture.addr;

    let stdout = tokio::task::spawn_blocking(move || {
        flight_sql_client_cmd()
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
        "+--------------+-----------+---------------------------+-----------------------------+\
        \n| field_string | field_int | field_timestamp_nano_notz | field_timestamp_nano_berlin |\
        \n+--------------+-----------+---------------------------+-----------------------------+\
        \n| Hello        | 42        |                           |                             |\
        \n| lovely       |           | 1970-01-01T00:00:00       | 1970-01-01T01:00:00+01:00   |\
        \n| FlightSQL!   | 1337      | 2024-10-30T11:36:57       | 2024-10-30T12:36:57+01:00   |\
        \n+--------------+-----------+---------------------------+-----------------------------+",
    );
}

#[tokio::test]
async fn test_endpoint_token() {
    test_endpoint_location(&["--token", "endpoint-token"], |addr| {
        vec![format!("grpc://{addr}")]
    })
    .await;
}

#[tokio::test]
async fn test_endpoint_handshake_token() {
    test_endpoint_location(&["--username", "user", "--password", "password"], |addr| {
        vec![format!("grpc+tcp://{addr}")]
    })
    .await;
}

#[tokio::test]
async fn test_endpoint_multiple_locations() {
    test_endpoint_location(&["--token", "endpoint-token"], |addr| {
        // Unreachable alternatives make choosing HTTP or the later gRPC location fail.
        vec![
            "http://127.0.0.1:0".into(),
            format!("grpc+tcp://{addr}"),
            "grpc+tcp://127.0.0.1:0".into(),
        ]
    })
    .await;
}

async fn test_endpoint_location(args: &[&str], locations: impl FnOnce(SocketAddr) -> Vec<String>) {
    let data_server = FlightSqlServiceImpl {
        require_token: true,
        ..Default::default()
    };
    let data = TestFixture::new(data_server.service()).await;
    let query_server = FlightSqlServiceImpl {
        locations: locations(data.addr)
            .into_iter()
            .map(|uri| Location { uri })
            .collect(),
        reject_do_get: true,
        handshake_token: Some("endpoint-token"),
        ..Default::default()
    };
    let query = TestFixture::new(query_server.service()).await;
    let output = run_statement_query(query.addr, args).await;
    query.shutdown_and_wait().await;
    data.shutdown_and_wait().await;

    assert_eq!(data_server.handshakes.load(Ordering::SeqCst), 0);
    assert_query_result(output);
}

#[tokio::test]
async fn test_endpoint_reuse_connection() {
    let server = FlightSqlServiceImpl {
        locations: vec![
            Location {
                uri: "grpc+tcp://127.0.0.1:0".into(),
            },
            Location {
                uri: "arrow-flight-reuse-connection://?".into(),
            },
        ],
        ..Default::default()
    };
    let fixture = TestFixture::new(server.service()).await;
    let output = run_statement_query(fixture.addr, &[]).await;
    fixture.shutdown_and_wait().await;
    assert_query_result(output);
}

#[tokio::test]
async fn test_endpoint_unsupported_location() {
    let server = FlightSqlServiceImpl {
        locations: vec![Location {
            uri: "http://127.0.0.1:0".into(),
        }],
        ..Default::default()
    };
    let fixture = TestFixture::new(server.service()).await;
    let output = run_statement_query(fixture.addr, &[]).await;
    fixture.shutdown_and_wait().await;

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("No supported endpoint location"),
        "{stderr}"
    );
}

async fn run_statement_query(addr: SocketAddr, args: &[&str]) -> Output {
    let mut cmd = flight_sql_client_cmd();
    cmd.env_clear()
        .timeout(Duration::from_secs(10))
        .args([
            "--host",
            &addr.ip().to_string(),
            "--port",
            &addr.port().to_string(),
        ])
        .args(args)
        .args(["statement-query", QUERY]);
    tokio::task::spawn_blocking(move || cmd.output().unwrap())
        .await
        .unwrap()
}

fn assert_query_result(output: Output) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let expected =
        arrow_cast::pretty::pretty_format_batches(&[FlightSqlServiceImpl::fake_result().unwrap()])
            .unwrap()
            .to_string();
    assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), expected);
}

#[tokio::test]
async fn test_endpoint_tls() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let query_server = FlightSqlServiceImpl {
        locations: vec![Location {
            uri: format!("grpc+tls://{}", listener.local_addr().unwrap()),
        }],
        reject_do_get: true,
        ..Default::default()
    };
    let query = TestFixture::new(query_server.service()).await;
    let client = tokio::spawn(run_statement_query(query.addr, &[]));
    // A ClientHello proves grpc+tls enabled TLS without --tls or a test CA.
    let hello = tokio::time::timeout(Duration::from_secs(5), async {
        let (mut socket, _) = listener.accept().await?;
        let mut prefix = [0; 6];
        socket.read_exact(&mut prefix).await?;
        Ok::<_, std::io::Error>(prefix)
    })
    .await;
    let output = client.await.unwrap();
    query.shutdown_and_wait().await;

    // The listener closes after the prefix; it does not complete the TLS handshake.
    assert!(!output.status.success());
    let prefix = hello
        .expect("CLI did not connect to the TLS endpoint")
        .unwrap();
    assert_eq!(prefix[0], 0x16, "expected a TLS handshake record");
    assert_eq!(prefix[1], 0x03, "expected the TLS record version");
    assert_eq!(prefix[5], 0x01, "expected a ClientHello");
}

#[tokio::test]
async fn test_get_catalogs() {
    let test_server = FlightSqlServiceImpl::default();
    let fixture = TestFixture::new(test_server.service()).await;
    let addr = fixture.addr;

    let stdout = tokio::task::spawn_blocking(move || {
        flight_sql_client_cmd()
            .env_clear()
            .env("RUST_BACKTRACE", "1")
            .env("RUST_LOG", "warn")
            .arg("--host")
            .arg(addr.ip().to_string())
            .arg("--port")
            .arg(addr.port().to_string())
            .arg("catalogs")
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
        "+--------------+\
        \n| catalog_name |\
        \n+--------------+\
        \n| catalog_a    |\
        \n| catalog_b    |\
        \n+--------------+",
    );
}

#[tokio::test]
async fn test_get_db_schemas() {
    let test_server = FlightSqlServiceImpl::default();
    let fixture = TestFixture::new(test_server.service()).await;
    let addr = fixture.addr;

    let stdout = tokio::task::spawn_blocking(move || {
        flight_sql_client_cmd()
            .env_clear()
            .env("RUST_BACKTRACE", "1")
            .env("RUST_LOG", "warn")
            .arg("--host")
            .arg(addr.ip().to_string())
            .arg("--port")
            .arg(addr.port().to_string())
            .arg("db-schemas")
            .arg("catalog_a")
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
        "+--------------+----------------+\
        \n| catalog_name | db_schema_name |\
        \n+--------------+----------------+\
        \n| catalog_a    | schema_1       |\
        \n| catalog_a    | schema_2       |\
        \n+--------------+----------------+",
    );
}

#[tokio::test]
async fn test_get_tables() {
    let test_server = FlightSqlServiceImpl::default();
    let fixture = TestFixture::new(test_server.service()).await;
    let addr = fixture.addr;

    let stdout = tokio::task::spawn_blocking(move || {
        flight_sql_client_cmd()
            .env_clear()
            .env("RUST_BACKTRACE", "1")
            .env("RUST_LOG", "warn")
            .arg("--host")
            .arg(addr.ip().to_string())
            .arg("--port")
            .arg(addr.port().to_string())
            .arg("tables")
            .arg("catalog_a")
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
        "+--------------+----------------+------------+------------+\
        \n| catalog_name | db_schema_name | table_name | table_type |\
        \n+--------------+----------------+------------+------------+\
        \n| catalog_a    | schema_1       | table_1    | TABLE      |\
        \n| catalog_a    | schema_2       | table_2    | VIEW       |\
        \n+--------------+----------------+------------+------------+",
    );
}
#[tokio::test]
async fn test_get_tables_db_filter() {
    let test_server = FlightSqlServiceImpl::default();
    let fixture = TestFixture::new(test_server.service()).await;
    let addr = fixture.addr;

    let stdout = tokio::task::spawn_blocking(move || {
        flight_sql_client_cmd()
            .env_clear()
            .env("RUST_BACKTRACE", "1")
            .env("RUST_LOG", "warn")
            .arg("--host")
            .arg(addr.ip().to_string())
            .arg("--port")
            .arg(addr.port().to_string())
            .arg("tables")
            .arg("catalog_a")
            .arg("--db-schema-filter")
            .arg("schema_2")
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
        "+--------------+----------------+------------+------------+\
        \n| catalog_name | db_schema_name | table_name | table_type |\
        \n+--------------+----------------+------------+------------+\
        \n| catalog_a    | schema_2       | table_2    | VIEW       |\
        \n+--------------+----------------+------------+------------+",
    );
}

#[tokio::test]
async fn test_get_tables_types() {
    let test_server = FlightSqlServiceImpl::default();
    let fixture = TestFixture::new(test_server.service()).await;
    let addr = fixture.addr;

    let stdout = tokio::task::spawn_blocking(move || {
        flight_sql_client_cmd()
            .env_clear()
            .env("RUST_BACKTRACE", "1")
            .env("RUST_LOG", "warn")
            .arg("--host")
            .arg(addr.ip().to_string())
            .arg("--port")
            .arg(addr.port().to_string())
            .arg("table-types")
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
        "+--------------+\
        \n| table_type   |\
        \n+--------------+\
        \n| SYSTEM_TABLE |\
        \n| TABLE        |\
        \n| VIEW         |\
        \n+--------------+",
    );
}

const PREPARED_QUERY: &str = "SELECT * FROM table WHERE field = $1";
const PREPARED_STATEMENT_HANDLE: &str = "prepared_statement_handle";
const UPDATED_PREPARED_STATEMENT_HANDLE: &str = "updated_prepared_statement_handle";

async fn test_do_put_prepared_statement(test_server: FlightSqlServiceImpl) {
    let fixture = TestFixture::new(test_server.service()).await;
    let addr = fixture.addr;

    let stdout = tokio::task::spawn_blocking(move || {
        flight_sql_client_cmd()
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
        "+--------------+-----------+---------------------------+-----------------------------+\
        \n| field_string | field_int | field_timestamp_nano_notz | field_timestamp_nano_berlin |\
        \n+--------------+-----------+---------------------------+-----------------------------+\
        \n| Hello        | 42        |                           |                             |\
        \n| lovely       |           | 1970-01-01T00:00:00       | 1970-01-01T01:00:00+01:00   |\
        \n| FlightSQL!   | 1337      | 2024-10-30T11:36:57       | 2024-10-30T12:36:57+01:00   |\
        \n+--------------+-----------+---------------------------+-----------------------------+",
    );
}

#[tokio::test]
pub async fn test_do_put_prepared_statement_stateless() {
    test_do_put_prepared_statement(FlightSqlServiceImpl {
        stateless_prepared_statements: true,
        ..Default::default()
    })
    .await
}

#[tokio::test]
pub async fn test_do_put_prepared_statement_stateful() {
    test_do_put_prepared_statement(FlightSqlServiceImpl {
        stateless_prepared_statements: false,
        ..Default::default()
    })
    .await
}

#[derive(Clone)]
pub struct FlightSqlServiceImpl {
    /// Whether to emulate stateless (true) or stateful (false) behavior for
    /// prepared statements. stateful servers will not return an updated
    /// handle after executing `DoPut(CommandPreparedStatementQuery)`
    stateless_prepared_statements: bool,
    locations: Vec<Location>,
    reject_do_get: bool,
    require_token: bool,
    handshake_token: Option<&'static str>,
    handshakes: Arc<AtomicUsize>,
}

impl Default for FlightSqlServiceImpl {
    fn default() -> Self {
        Self {
            stateless_prepared_statements: true,
            locations: Vec::new(),
            reject_do_get: false,
            require_token: false,
            handshake_token: None,
            handshakes: Arc::new(AtomicUsize::new(0)),
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

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("field_string", DataType::Utf8, false),
            Field::new("field_int", DataType::Int64, true),
            Field::new(
                "field_timestamp_nano_notz",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                true,
            ),
            Field::new(
                "field_timestamp_nano_berlin",
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("Europe/Berlin"))),
                true,
            ),
        ])
    }

    fn fake_result() -> Result<RecordBatch, ArrowError> {
        let schema = Self::schema();

        let string_array = StringArray::from(vec!["Hello", "lovely", "FlightSQL!"]);
        let int_array = Int64Array::from(vec![Some(42), None, Some(1337)]);
        let timestamp_array =
            TimestampNanosecondArray::from(vec![None, Some(0), Some(1730288217000000000)]);
        let timestamp_ts_array = timestamp_array
            .clone()
            .with_timezone(Arc::from("Europe/Berlin"));

        let cols = vec![
            Arc::new(string_array) as ArrayRef,
            Arc::new(int_array) as ArrayRef,
            Arc::new(timestamp_array) as ArrayRef,
            Arc::new(timestamp_ts_array) as ArrayRef,
        ];
        RecordBatch::try_new(Arc::new(schema), cols)
    }

    fn create_fake_prepared_stmt() -> Result<ActionCreatePreparedStatementResult, ArrowError> {
        let handle = PREPARED_STATEMENT_HANDLE.to_string();
        let schema = Self::schema();

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

        let mut info = FlightInfo::new()
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
            .with_ordered(false);
        for endpoint in &mut info.endpoint {
            endpoint.location.clone_from(&self.locations);
        }
        Ok(info)
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
        self.handshakes.fetch_add(1, Ordering::SeqCst);
        let Some(token) = self.handshake_token else {
            return Err(Status::unimplemented("do_handshake not implemented"));
        };
        let stream = futures::stream::iter([Ok(HandshakeResponse::default())]);
        let mut response = Response::new(stream.boxed());
        response
            .metadata_mut()
            .insert("authorization", format!("Bearer {token}").parse().unwrap());
        Ok(response)
    }

    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        if self.reject_do_get {
            return Err(Status::unimplemented("DoGet belongs on the data server"));
        }
        if self.require_token
            && request
                .metadata()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                != Some("Bearer endpoint-token")
        {
            return Err(Status::unauthenticated("Missing or incorrect bearer token"));
        }
        let part = message.unpack::<FetchResults>().unwrap().unwrap().handle;
        let batch = Self::fake_result().unwrap();
        let batch = match part.as_str() {
            "part_1" => batch.slice(0, 2),
            "part_2" => batch.slice(2, 1),
            ticket => panic!("Invalid ticket: {ticket:?}"),
        };
        let schema = batch.schema_ref();
        let batches = vec![batch.clone()];
        let flight_data = batches_to_flight_data(schema, &batches)
            .unwrap()
            .into_iter()
            .map(Ok);

        let stream: Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>> =
            Box::pin(futures::stream::iter(flight_data));
        let resp = Response::new(stream);
        Ok(resp)
    }

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .unwrap()
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }
    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .unwrap()
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .unwrap()
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.as_any().encode_to_vec().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(&query.into_builder().schema())
            .unwrap()
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(Response::new(flight_info))
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

    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let mut builder = query.into_builder();
        for catalog_name in ["catalog_a", "catalog_b"] {
            builder.append(catalog_name);
        }
        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let mut builder = query.into_builder();
        for (catalog_name, schema_name) in [
            ("catalog_a", "schema_1"),
            ("catalog_a", "schema_2"),
            ("catalog_b", "schema_3"),
        ] {
            builder.append(catalog_name, schema_name);
        }

        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let mut builder = query.into_builder();
        for (catalog_name, schema_name, table_name, table_type, schema) in [
            (
                "catalog_a",
                "schema_1",
                "table_1",
                "TABLE",
                Arc::new(Schema::empty()),
            ),
            (
                "catalog_a",
                "schema_2",
                "table_2",
                "VIEW",
                Arc::new(Schema::empty()),
            ),
            (
                "catalog_b",
                "schema_3",
                "table_3",
                "TABLE",
                Arc::new(Schema::empty()),
            ),
        ] {
            builder
                .append(catalog_name, schema_name, table_name, table_type, &schema)
                .unwrap();
        }

        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let mut builder = query.into_builder();
        for table_type in ["TABLE", "VIEW", "SYSTEM_TABLE"] {
            builder.append(table_type);
        }

        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
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

    async fn do_action_create_prepared_statement(
        &self,
        _query: ActionCreatePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        Self::create_fake_prepared_stmt()
            .map_err(|e| Status::internal(format!("Unable to serialize schema: {e}")))
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
