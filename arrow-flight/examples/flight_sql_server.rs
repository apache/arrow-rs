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

use arrow_flight::sql::server::PeekableFlightDataStream;
use arrow_flight::sql::DoPutPreparedStatementResult;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use core::str;
use futures::{stream, Stream, TryStreamExt};
use once_cell::sync::Lazy;
use prost::Message;
use std::collections::HashSet;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use tonic::metadata::MetadataValue;
use tonic::transport::Server;
use tonic::transport::{Certificate, Identity, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};

use arrow_array::builder::StringBuilder;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::sql::metadata::{
    SqlInfoData, SqlInfoDataBuilder, XdbcTypeInfo, XdbcTypeInfoData, XdbcTypeInfoDataBuilder,
};
use arrow_flight::sql::{
    server::FlightSqlService, ActionBeginSavepointRequest, ActionBeginSavepointResult,
    ActionBeginTransactionRequest, ActionBeginTransactionResult, ActionCancelQueryRequest,
    ActionCancelQueryResult, ActionClosePreparedStatementRequest,
    ActionCreatePreparedStatementRequest, ActionCreatePreparedStatementResult,
    ActionCreatePreparedSubstraitPlanRequest, ActionEndSavepointRequest,
    ActionEndTransactionRequest, Any, CommandGetCatalogs, CommandGetCrossReference,
    CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys, CommandGetPrimaryKeys,
    CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandGetXdbcTypeInfo,
    CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementIngest,
    CommandStatementQuery, CommandStatementSubstraitPlan, CommandStatementUpdate, Nullable,
    ProstMessageExt, Searchable, SqlInfo, TicketStatementQuery, XdbcDataType,
};
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{
    flight_service_server::FlightService, flight_service_server::FlightServiceServer, Action,
    FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse,
    IpcMessage, SchemaAsIpc, Ticket,
};
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::{ArrowError, DataType, Field, Schema};

macro_rules! status {
    ($desc:expr, $err:expr) => {
        Status::internal(format!("{}: {} at {}:{}", $desc, $err, file!(), line!()))
    };
}

const FAKE_TOKEN: &str = "uuid_token";
const FAKE_HANDLE: &str = "uuid_handle";
const FAKE_UPDATE_RESULT: i64 = 1;

static INSTANCE_SQL_DATA: Lazy<SqlInfoData> = Lazy::new(|| {
    let mut builder = SqlInfoDataBuilder::new();
    // Server information
    builder.append(SqlInfo::FlightSqlServerName, "Example Flight SQL Server");
    builder.append(SqlInfo::FlightSqlServerVersion, "1");
    // 1.3 comes from https://github.com/apache/arrow/blob/f9324b79bf4fc1ec7e97b32e3cce16e75ef0f5e3/format/Schema.fbs#L24
    builder.append(SqlInfo::FlightSqlServerArrowVersion, "1.3");
    builder.build().unwrap()
});

static INSTANCE_XBDC_DATA: Lazy<XdbcTypeInfoData> = Lazy::new(|| {
    let mut builder = XdbcTypeInfoDataBuilder::new();
    builder.append(XdbcTypeInfo {
        type_name: "INTEGER".into(),
        data_type: XdbcDataType::XdbcInteger,
        column_size: Some(32),
        literal_prefix: None,
        literal_suffix: None,
        create_params: None,
        nullable: Nullable::NullabilityNullable,
        case_sensitive: false,
        searchable: Searchable::Full,
        unsigned_attribute: Some(false),
        fixed_prec_scale: false,
        auto_increment: Some(false),
        local_type_name: Some("INTEGER".into()),
        minimum_scale: None,
        maximum_scale: None,
        sql_data_type: XdbcDataType::XdbcInteger,
        datetime_subcode: None,
        num_prec_radix: Some(2),
        interval_precision: None,
    });
    builder.build().unwrap()
});

static TABLES: Lazy<Vec<&'static str>> = Lazy::new(|| vec!["flight_sql.example.table"]);

#[derive(Clone)]
pub struct FlightSqlServiceImpl {}

impl FlightSqlServiceImpl {
    fn check_token<T>(&self, req: &Request<T>) -> Result<(), Status> {
        let metadata = req.metadata();
        let auth = metadata.get("authorization").ok_or_else(|| {
            Status::internal(format!("No authorization header! metadata = {metadata:?}"))
        })?;
        let str = auth
            .to_str()
            .map_err(|e| Status::internal(format!("Error parsing header: {e}")))?;
        let authorization = str.to_string();
        let bearer = "Bearer ";
        if !authorization.starts_with(bearer) {
            Err(Status::internal("Invalid auth header!"))?;
        }
        let token = authorization[bearer.len()..].to_string();
        if token == FAKE_TOKEN {
            Ok(())
        } else {
            Err(Status::unauthenticated("invalid token "))
        }
    }

    fn fake_result() -> Result<RecordBatch, ArrowError> {
        let schema = Schema::new(vec![Field::new("salutation", DataType::Utf8, false)]);
        let mut builder = StringBuilder::new();
        builder.append_value("Hello, FlightSQL!");
        let cols = vec![Arc::new(builder.finish()) as ArrayRef];
        RecordBatch::try_new(Arc::new(schema), cols)
    }
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = FlightSqlServiceImpl;

    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        let basic = "Basic ";
        let authorization = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::invalid_argument("authorization field not present"))?
            .to_str()
            .map_err(|e| status!("authorization not parsable", e))?;
        if !authorization.starts_with(basic) {
            Err(Status::invalid_argument(format!(
                "Auth type not implemented: {authorization}"
            )))?;
        }
        let base64 = &authorization[basic.len()..];
        let bytes = BASE64_STANDARD
            .decode(base64)
            .map_err(|e| status!("authorization not decodable", e))?;
        let str = str::from_utf8(&bytes).map_err(|e| status!("authorization not parsable", e))?;
        let parts: Vec<_> = str.split(':').collect();
        let (user, pass) = match parts.as_slice() {
            [user, pass] => (user, pass),
            _ => Err(Status::invalid_argument(
                "Invalid authorization header".to_string(),
            ))?,
        };
        if user != &"admin" || pass != &"password" {
            Err(Status::unauthenticated("Invalid credentials!"))?
        }

        let result = HandshakeResponse {
            protocol_version: 0,
            payload: FAKE_TOKEN.into(),
        };
        let result = Ok(result);
        let output = futures::stream::iter(vec![result]);

        let token = format!("Bearer {}", FAKE_TOKEN);
        let mut response: Response<Pin<Box<dyn Stream<Item = _> + Send>>> =
            Response::new(Box::pin(output));
        response.metadata_mut().append(
            "authorization",
            MetadataValue::from_str(token.as_str()).unwrap(),
        );
        return Ok(response);
    }

    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        _message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.check_token(&request)?;
        let batch = Self::fake_result().map_err(|e| status!("Could not fake a result", e))?;
        let schema = batch.schema_ref();
        let batches = vec![batch.clone()];
        let flight_data = batches_to_flight_data(schema, batches)
            .map_err(|e| status!("Could not convert batches", e))?
            .into_iter()
            .map(Ok);

        let stream: Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send>> =
            Box::pin(stream::iter(flight_data));
        let resp = Response::new(stream);
        Ok(resp)
    }

    async fn get_flight_info_statement(
        &self,
        _query: CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_statement not implemented",
        ))
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

    async fn get_flight_info_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.check_token(&request)?;
        let handle = std::str::from_utf8(&cmd.prepared_statement_handle)
            .map_err(|e| status!("Unable to parse handle", e))?;

        let batch = Self::fake_result().map_err(|e| status!("Could not fake a result", e))?;
        let schema = (*batch.schema()).clone();
        let num_rows = batch.num_rows();
        let num_bytes = batch.get_array_memory_size();

        let fetch = FetchResults {
            handle: handle.to_string(),
        };
        let buf = fetch.as_any().encode_to_vec().into();
        let ticket = Ticket { ticket: buf };
        let endpoint = FlightEndpoint {
            ticket: Some(ticket),
            location: vec![],
            expiration_time: None,
            app_metadata: vec![].into(),
        };
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| status!("Unable to serialize schema", e))?
            .with_descriptor(FlightDescriptor::new_cmd(vec![]))
            .with_endpoint(endpoint)
            .with_total_records(num_rows as i64)
            .with_total_bytes(num_bytes as i64)
            .with_ordered(false);

        let resp = Response::new(info);
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
            .map_err(|e| status!("Unable to encode schema", e))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(tonic::Response::new(flight_info))
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
            .map_err(|e| status!("Unable to encode schema", e))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(tonic::Response::new(flight_info))
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
            .map_err(|e| status!("Unable to encode schema", e))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(tonic::Response::new(flight_info))
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
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket::new(query.as_any().encode_to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(query.into_builder(&INSTANCE_SQL_DATA).schema().as_ref())
            .map_err(|e| status!("Unable to encode schema", e))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(tonic::Response::new(flight_info))
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
        query: CommandGetXdbcTypeInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket::new(query.as_any().encode_to_vec());
        let endpoint = FlightEndpoint::new().with_ticket(ticket);

        let flight_info = FlightInfo::new()
            .try_with_schema(query.into_builder(&INSTANCE_XBDC_DATA).schema().as_ref())
            .map_err(|e| status!("Unable to encode schema", e))?
            .with_endpoint(endpoint)
            .with_descriptor(flight_descriptor);

        Ok(tonic::Response::new(flight_info))
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
        query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let catalog_names = TABLES
            .iter()
            .map(|full_name| full_name.split('.').collect::<Vec<_>>()[0].to_string())
            .collect::<HashSet<_>>();
        let mut builder = query.into_builder();
        for catalog_name in catalog_names {
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
        let schemas = TABLES
            .iter()
            .map(|full_name| {
                let parts = full_name.split('.').collect::<Vec<_>>();
                (parts[0].to_string(), parts[1].to_string())
            })
            .collect::<HashSet<_>>();

        let mut builder = query.into_builder();
        for (catalog_name, schema_name) in schemas {
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
        let tables = TABLES
            .iter()
            .map(|full_name| {
                let parts = full_name.split('.').collect::<Vec<_>>();
                (
                    parts[0].to_string(),
                    parts[1].to_string(),
                    parts[2].to_string(),
                )
            })
            .collect::<HashSet<_>>();

        let dummy_schema = Schema::empty();
        let mut builder = query.into_builder();
        for (catalog_name, schema_name, table_name) in tables {
            builder
                .append(
                    catalog_name,
                    schema_name,
                    table_name,
                    "TABLE",
                    &dummy_schema,
                )
                .map_err(Status::from)?;
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
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get_table_types not implemented"))
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        let builder = query.into_builder(&INSTANCE_SQL_DATA);
        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
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
        query: CommandGetXdbcTypeInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        // create a builder with pre-defined Xdbc data:
        let builder = query.into_builder(&INSTANCE_XBDC_DATA);
        let schema = builder.schema();
        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(futures::stream::once(async { batch }))
            .map_err(Status::from);
        Ok(Response::new(Box::pin(stream)))
    }

    // do_put
    async fn do_put_statement_update(
        &self,
        _ticket: CommandStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Ok(FAKE_UPDATE_RESULT)
    }

    async fn do_put_statement_ingest(
        &self,
        _ticket: CommandStatementIngest,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Ok(FAKE_UPDATE_RESULT)
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
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        Err(Status::unimplemented(
            "do_put_prepared_statement_query not implemented",
        ))
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
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        self.check_token(&request)?;
        let record_batch =
            Self::fake_result().map_err(|e| status!("Error getting result schema", e))?;
        let schema = record_batch.schema_ref();
        let message = SchemaAsIpc::new(schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e| status!("Unable to serialize schema", e))?;
        let IpcMessage(schema_bytes) = message;
        let res = ActionCreatePreparedStatementResult {
            prepared_statement_handle: FAKE_HANDLE.into(),
            dataset_schema: schema_bytes,
            parameter_schema: Default::default(), // TODO: parameters
        };
        Ok(res)
    }

    async fn do_action_close_prepared_statement(
        &self,
        _query: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Ok(())
    }

    async fn do_action_create_prepared_substrait_plan(
        &self,
        _query: ActionCreatePreparedSubstraitPlanRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        Err(Status::unimplemented(
            "Implement do_action_create_prepared_substrait_plan",
        ))
    }

    async fn do_action_begin_transaction(
        &self,
        _query: ActionBeginTransactionRequest,
        _request: Request<Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        Err(Status::unimplemented(
            "Implement do_action_begin_transaction",
        ))
    }

    async fn do_action_end_transaction(
        &self,
        _query: ActionEndTransactionRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Err(Status::unimplemented("Implement do_action_end_transaction"))
    }

    async fn do_action_begin_savepoint(
        &self,
        _query: ActionBeginSavepointRequest,
        _request: Request<Action>,
    ) -> Result<ActionBeginSavepointResult, Status> {
        Err(Status::unimplemented("Implement do_action_begin_savepoint"))
    }

    async fn do_action_end_savepoint(
        &self,
        _query: ActionEndSavepointRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Err(Status::unimplemented("Implement do_action_end_savepoint"))
    }

    async fn do_action_cancel_query(
        &self,
        _query: ActionCancelQueryRequest,
        _request: Request<Action>,
    ) -> Result<ActionCancelQueryResult, Status> {
        Err(Status::unimplemented("Implement do_action_cancel_query"))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// This example shows how to run a FlightSql server
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr_str = "0.0.0.0:50051";
    let addr = addr_str.parse()?;

    println!("Listening on {:?}", addr);

    if std::env::var("USE_TLS").ok().is_some() {
        let cert = std::fs::read_to_string("arrow-flight/examples/data/server.pem")?;
        let key = std::fs::read_to_string("arrow-flight/examples/data/server.key")?;
        let client_ca = std::fs::read_to_string("arrow-flight/examples/data/client_ca.pem")?;

        let svc = FlightServiceServer::new(FlightSqlServiceImpl {});
        let tls_config = ServerTlsConfig::new()
            .identity(Identity::from_pem(&cert, &key))
            .client_ca_root(Certificate::from_pem(&client_ca));

        Server::builder()
            .tls_config(tls_config)?
            .add_service(svc)
            .serve(addr)
            .await?;
    } else {
        let svc = FlightServiceServer::new(FlightSqlServiceImpl {});

        Server::builder().add_service(svc).serve(addr).await?;
    }

    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::{TryFutureExt, TryStreamExt};
    use hyper_util::rt::TokioIo;
    use std::fs;
    use std::future::Future;
    use std::net::SocketAddr;
    use std::path::PathBuf;
    use std::time::Duration;
    use tempfile::NamedTempFile;
    use tokio::net::{TcpListener, UnixListener, UnixStream};
    use tokio_stream::wrappers::UnixListenerStream;
    use tonic::transport::{Channel, ClientTlsConfig};

    use arrow_cast::pretty::pretty_format_batches;
    use arrow_flight::sql::client::FlightSqlServiceClient;
    use tonic::transport::server::TcpIncoming;
    use tonic::transport::{Certificate, Endpoint};
    use tower::service_fn;

    async fn bind_tcp() -> (TcpIncoming, SocketAddr) {
        let listener = TcpListener::bind("0.0.0.0:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let incoming = TcpIncoming::from_listener(listener, true, None).unwrap();
        (incoming, addr)
    }

    fn endpoint(uri: String) -> Result<Endpoint, ArrowError> {
        let endpoint = Endpoint::new(uri)
            .map_err(|_| ArrowError::IpcError("Cannot create endpoint".to_string()))?
            .connect_timeout(Duration::from_secs(20))
            .timeout(Duration::from_secs(20))
            .tcp_nodelay(true) // Disable Nagle's Algorithm since we don't want packets to wait
            .tcp_keepalive(Option::Some(Duration::from_secs(3600)))
            .http2_keep_alive_interval(Duration::from_secs(300))
            .keep_alive_timeout(Duration::from_secs(20))
            .keep_alive_while_idle(true);

        Ok(endpoint)
    }

    async fn auth_client(client: &mut FlightSqlServiceClient<Channel>) {
        let token = client.handshake("admin", "password").await.unwrap();
        client.set_token(String::from_utf8(token.to_vec()).unwrap());
    }

    async fn test_uds_client<F, C>(f: F)
    where
        F: FnOnce(FlightSqlServiceClient<Channel>) -> C,
        C: Future<Output = ()>,
    {
        let file = NamedTempFile::new().unwrap();
        let path = file.into_temp_path().to_str().unwrap().to_string();
        let _ = fs::remove_file(path.clone());

        let uds = UnixListener::bind(path.clone()).unwrap();
        let stream = UnixListenerStream::new(uds);

        let service = FlightSqlServiceImpl {};
        let serve_future = Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming(stream);

        let request_future = async {
            let connector =
                service_fn(move |_| UnixStream::connect(path.clone()).map_ok(TokioIo::new));
            let channel = Endpoint::try_from("http://example.com")
                .unwrap()
                .connect_with_connector(connector)
                .await
                .unwrap();
            let client = FlightSqlServiceClient::new(channel);
            f(client).await
        };

        tokio::select! {
            _ = serve_future => panic!("server returned first"),
            _ = request_future => println!("Client finished!"),
        }
    }

    async fn test_http_client<F, C>(f: F)
    where
        F: FnOnce(FlightSqlServiceClient<Channel>) -> C,
        C: Future<Output = ()>,
    {
        let (incoming, addr) = bind_tcp().await;
        let uri = format!("http://{}:{}", addr.ip(), addr.port());

        let service = FlightSqlServiceImpl {};
        let serve_future = Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming(incoming);

        let request_future = async {
            let endpoint = endpoint(uri).unwrap();
            let channel = endpoint.connect().await.unwrap();
            let client = FlightSqlServiceClient::new(channel);
            f(client).await
        };

        tokio::select! {
            _ = serve_future => panic!("server returned first"),
            _ = request_future => println!("Client finished!"),
        }
    }

    async fn test_https_client<F, C>(f: F)
    where
        F: FnOnce(FlightSqlServiceClient<Channel>) -> C,
        C: Future<Output = ()>,
    {
        let cert_dir = PathBuf::from("examples/data");

        let cert = std::fs::read_to_string(cert_dir.join("server.pem")).unwrap();
        let key = std::fs::read_to_string(cert_dir.join("server.key")).unwrap();
        let ca_root = std::fs::read_to_string(cert_dir.join("ca_root.pem")).unwrap();

        let tls_config = ServerTlsConfig::new()
            .identity(Identity::from_pem(&cert, &key))
            .client_ca_root(Certificate::from_pem(&ca_root));

        let (incoming, addr) = bind_tcp().await;
        let uri = format!("https://{}:{}", addr.ip(), addr.port());

        let svc = FlightServiceServer::new(FlightSqlServiceImpl {});

        let serve_future = Server::builder()
            .tls_config(tls_config)
            .unwrap()
            .add_service(svc)
            .serve_with_incoming(incoming);

        let request_future = async move {
            let cert = std::fs::read_to_string(cert_dir.join("client.pem")).unwrap();
            let key = std::fs::read_to_string(cert_dir.join("client.key")).unwrap();

            let tls_config = ClientTlsConfig::new()
                .domain_name("localhost")
                .ca_certificate(Certificate::from_pem(&ca_root))
                .identity(Identity::from_pem(cert, key));

            let endpoint = endpoint(uri).unwrap().tls_config(tls_config).unwrap();
            let channel = endpoint.connect().await.unwrap();
            let client = FlightSqlServiceClient::new(channel);
            f(client).await
        };

        tokio::select! {
            _ = serve_future => panic!("server returned first"),
            _ = request_future => println!("Client finished!"),
        }
    }

    async fn test_all_clients<F, C>(task: F)
    where
        F: FnOnce(FlightSqlServiceClient<Channel>) -> C + Copy,
        C: Future<Output = ()>,
    {
        println!("testing uds client");
        test_uds_client(task).await;
        println!("=======");

        println!("testing http client");
        test_http_client(task).await;
        println!("=======");

        println!("testing https client");
        test_https_client(task).await;
        println!("=======");
    }

    #[tokio::test]
    async fn test_select() {
        test_all_clients(|mut client| async move {
            auth_client(&mut client).await;

            let mut stmt = client.prepare("select 1;".to_string(), None).await.unwrap();

            let flight_info = stmt.execute().await.unwrap();

            let ticket = flight_info.endpoint[0].ticket.as_ref().unwrap().clone();
            let flight_data = client.do_get(ticket).await.unwrap();
            let batches: Vec<_> = flight_data.try_collect().await.unwrap();

            let res = pretty_format_batches(batches.as_slice()).unwrap();
            let expected = r#"
+-------------------+
| salutation        |
+-------------------+
| Hello, FlightSQL! |
+-------------------+"#
                .trim()
                .to_string();
            assert_eq!(res.to_string(), expected);
        })
        .await
    }

    #[tokio::test]
    async fn test_execute_update() {
        test_all_clients(|mut client| async move {
            auth_client(&mut client).await;
            let res = client
                .execute_update("creat table test(a int);".to_string(), None)
                .await
                .unwrap();
            assert_eq!(res, FAKE_UPDATE_RESULT);
        })
        .await
    }

    #[tokio::test]
    async fn test_auth() {
        test_all_clients(|mut client| async move {
            // no handshake
            assert!(client
                .prepare("select 1;".to_string(), None)
                .await
                .unwrap_err()
                .to_string()
                .contains("No authorization header"));

            // Invalid credentials
            assert!(client
                .handshake("admin", "password2")
                .await
                .unwrap_err()
                .to_string()
                .contains("Invalid credentials"));

            // Invalid Tokens
            client.handshake("admin", "password").await.unwrap();
            client.set_token("wrong token".to_string());
            assert!(client
                .prepare("select 1;".to_string(), None)
                .await
                .unwrap_err()
                .to_string()
                .contains("invalid token"));

            client.clear_token();

            // Successful call (token is automatically set by handshake)
            client.handshake("admin", "password").await.unwrap();
            client.prepare("select 1;".to_string(), None).await.unwrap();
        })
        .await
    }
}
