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

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use futures::{stream, Stream, TryStreamExt};
use once_cell::sync::Lazy;
use prost::Message;
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use tonic::transport::Server;
use tonic::transport::{Certificate, Identity, ServerTlsConfig};
use tonic::{Request, Response, Status, Streaming};

use arrow_array::builder::StringBuilder;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::sql::catalogs::{
    get_catalogs_batch, get_catalogs_schema, get_db_schemas_schema, get_tables_schema,
    GetSchemasBuilder, GetTablesBuilder,
};
use arrow_flight::sql::sql_info::SqlInfoList;
use arrow_flight::sql::{
    server::FlightSqlService, ActionBeginSavepointRequest, ActionBeginSavepointResult,
    ActionBeginTransactionRequest, ActionBeginTransactionResult,
    ActionCancelQueryRequest, ActionCancelQueryResult,
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, ActionCreatePreparedSubstraitPlanRequest,
    ActionEndSavepointRequest, ActionEndTransactionRequest, Any, CommandGetCatalogs,
    CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys,
    CommandGetImportedKeys, CommandGetPrimaryKeys, CommandGetSqlInfo,
    CommandGetTableTypes, CommandGetTables, CommandGetXdbcTypeInfo,
    CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementQuery,
    CommandStatementSubstraitPlan, CommandStatementUpdate, ProstMessageExt, SqlInfo,
    TicketStatementQuery,
};
use arrow_flight::utils::batches_to_flight_data;
use arrow_flight::{
    flight_service_server::FlightService, flight_service_server::FlightServiceServer,
    Action, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest,
    HandshakeResponse, IpcMessage, Location, SchemaAsIpc, Ticket,
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

static INSTANCE_SQL_INFO: Lazy<SqlInfoList> = Lazy::new(|| {
    SqlInfoList::new()
        // Server information
        .with_sql_info(SqlInfo::FlightSqlServerName, "Example Flight SQL Server")
        .with_sql_info(SqlInfo::FlightSqlServerVersion, "1")
        // 1.3 comes from https://github.com/apache/arrow/blob/f9324b79bf4fc1ec7e97b32e3cce16e75ef0f5e3/format/Schema.fbs#L24
        .with_sql_info(SqlInfo::FlightSqlServerArrowVersion, "1.3")
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
        let str = String::from_utf8(bytes)
            .map_err(|e| status!("authorization not parsable", e))?;
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
        return Ok(Response::new(Box::pin(output)));
    }

    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        _message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        self.check_token(&request)?;
        let batch =
            Self::fake_result().map_err(|e| status!("Could not fake a result", e))?;
        let schema = (*batch.schema()).clone();
        let batches = vec![batch];
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
        let batch =
            Self::fake_result().map_err(|e| status!("Could not fake a result", e))?;
        let schema = (*batch.schema()).clone();
        let num_rows = batch.num_rows();
        let num_bytes = batch.get_array_memory_size();
        let loc = Location {
            uri: "grpc+tcp://127.0.0.1".to_string(),
        };
        let fetch = FetchResults {
            handle: handle.to_string(),
        };
        let buf = fetch.as_any().encode_to_vec().into();
        let ticket = Ticket { ticket: buf };
        let endpoint = FlightEndpoint {
            ticket: Some(ticket),
            location: vec![loc],
        };
        let endpoints = vec![endpoint];

        let message = SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e| status!("Unable to serialize schema", e))?;
        let IpcMessage(schema_bytes) = message;

        let flight_desc = FlightDescriptor {
            r#type: DescriptorType::Cmd.into(),
            cmd: Default::default(),
            path: vec![],
        };
        let info = FlightInfo {
            schema: schema_bytes,
            flight_descriptor: Some(flight_desc),
            endpoint: endpoints,
            total_records: num_rows as i64,
            total_bytes: num_bytes as i64,
            ordered: false,
        };
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
            ticket: query.encode_to_vec().into(),
        };

        let options = IpcWriteOptions::default();

        // encode the schema into the correct form
        let IpcMessage(schema) = SchemaAsIpc::new(get_catalogs_schema(), &options)
            .try_into()
            .expect("valid catalogs schema");

        let endpoint = vec![FlightEndpoint {
            ticket: Some(ticket),
            location: vec![],
        }];

        let flight_info = FlightInfo {
            schema,
            flight_descriptor: Some(flight_descriptor),
            endpoint,
            total_records: -1,
            total_bytes: -1,
            ordered: false,
        };

        Ok(tonic::Response::new(flight_info))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.encode_to_vec().into(),
        };

        let options = IpcWriteOptions::default();

        // encode the schema into the correct form
        let IpcMessage(schema) =
            SchemaAsIpc::new(get_db_schemas_schema().as_ref(), &options)
                .try_into()
                .expect("valid schemas schema");

        let endpoint = vec![FlightEndpoint {
            ticket: Some(ticket),
            location: vec![],
        }];

        let flight_info = FlightInfo {
            schema,
            flight_descriptor: Some(flight_descriptor),
            endpoint,
            total_records: -1,
            total_bytes: -1,
            ordered: false,
        };

        Ok(tonic::Response::new(flight_info))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let flight_descriptor = request.into_inner();
        let ticket = Ticket {
            ticket: query.encode_to_vec().into(),
        };

        let options = IpcWriteOptions::default();

        // encode the schema into the correct form
        let IpcMessage(schema) =
            SchemaAsIpc::new(get_tables_schema(query.include_schema).as_ref(), &options)
                .try_into()
                .expect("valid tables schema");

        let endpoint = vec![FlightEndpoint {
            ticket: Some(ticket),
            location: vec![],
        }];

        let flight_info = FlightInfo {
            schema,
            flight_descriptor: Some(flight_descriptor),
            endpoint,
            total_records: -1,
            total_bytes: -1,
            ordered: false,
        };

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
        let ticket = Ticket {
            ticket: query.encode_to_vec().into(),
        };

        let options = IpcWriteOptions::default();

        // encode the schema into the correct form
        let IpcMessage(schema) = SchemaAsIpc::new(SqlInfoList::schema(), &options)
            .try_into()
            .expect("valid sql_info schema");

        let endpoint = vec![FlightEndpoint {
            ticket: Some(ticket),
            // we assume users wnating to use this helper would reasonably
            // never need to be distributed across multile endpoints?
            location: vec![],
        }];

        let flight_info = FlightInfo {
            schema,
            flight_descriptor: Some(flight_descriptor),
            endpoint,
            total_records: -1,
            total_bytes: -1,
            ordered: false,
        };

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
        let catalog_names = TABLES
            .iter()
            .map(|full_name| full_name.split('.').collect::<Vec<_>>()[0].to_string())
            .collect::<HashSet<_>>();
        let batch = get_catalogs_batch(catalog_names.into_iter().collect());
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(Arc::new(get_catalogs_schema().clone()))
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

        let mut builder = GetSchemasBuilder::new(query.db_schema_filter_pattern);
        if let Some(catalog) = query.catalog {
            for (catalog_name, schema_name) in schemas {
                if catalog == catalog_name {
                    builder
                        .append(catalog_name, schema_name)
                        .map_err(Status::from)?;
                }
            }
        } else {
            for (catalog_name, schema_name) in schemas {
                builder
                    .append(catalog_name, schema_name)
                    .map_err(Status::from)?;
            }
        };

        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(get_db_schemas_schema())
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

        let mut builder = GetTablesBuilder::new(
            query.db_schema_filter_pattern,
            query.table_name_filter_pattern,
            query.include_schema,
        );
        let dummy_schema = Schema::empty();
        if let Some(catalog) = query.catalog {
            for (catalog_name, schema_name, table_name) in tables {
                if catalog == catalog_name {
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
            }
        } else {
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
        };

        let batch = builder.build();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(get_db_schemas_schema())
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
        let batch = INSTANCE_SQL_INFO.filter(&query.info).encode();
        let stream = FlightDataEncoderBuilder::new()
            .with_schema(Arc::new(SqlInfoList::schema().clone()))
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
        _request: Request<Streaming<FlightData>>,
    ) -> Result<i64, Status> {
        Ok(FAKE_UPDATE_RESULT)
    }

    async fn do_put_substrait_plan(
        &self,
        _ticket: CommandStatementSubstraitPlan,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented(
            "do_put_substrait_plan not implemented",
        ))
    }

    async fn do_put_prepared_statement_query(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        Err(Status::unimplemented(
            "do_put_prepared_statement_query not implemented",
        ))
    }

    async fn do_put_prepared_statement_update(
        &self,
        _query: CommandPreparedStatementUpdate,
        _request: Request<Streaming<FlightData>>,
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
        let schema = Self::fake_result()
            .map_err(|e| status!("Error getting result schema", e))?
            .schema();
        let message = SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
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
        Err(Status::unimplemented(
            "Implement do_action_close_prepared_statement",
        ))
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
    let addr = "0.0.0.0:50051".parse()?;

    let svc = FlightServiceServer::new(FlightSqlServiceImpl {});

    println!("Listening on {:?}", addr);

    if std::env::var("USE_TLS").ok().is_some() {
        let cert = std::fs::read_to_string("arrow-flight/examples/data/server.pem")?;
        let key = std::fs::read_to_string("arrow-flight/examples/data/server.key")?;
        let client_ca =
            std::fs::read_to_string("arrow-flight/examples/data/client_ca.pem")?;

        let tls_config = ServerTlsConfig::new()
            .identity(Identity::from_pem(&cert, &key))
            .client_ca_root(Certificate::from_pem(&client_ca));

        Server::builder()
            .tls_config(tls_config)?
            .add_service(svc)
            .serve(addr)
            .await?;
    } else {
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
    use futures::TryStreamExt;
    use std::fs;
    use std::future::Future;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tempfile::NamedTempFile;
    use tokio::net::{TcpListener, UnixListener, UnixStream};
    use tokio_stream::wrappers::UnixListenerStream;
    use tonic::transport::{Channel, ClientTlsConfig};

    use arrow_cast::pretty::pretty_format_batches;
    use arrow_flight::sql::client::FlightSqlServiceClient;
    use arrow_flight::utils::flight_data_to_batches;
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
            .map_err(|_| ArrowError::IoError("Cannot create endpoint".to_string()))?
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
            let connector = service_fn(move |_| UnixStream::connect(path.clone()));
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
        let cert = std::fs::read_to_string("examples/data/server.pem").unwrap();
        let key = std::fs::read_to_string("examples/data/server.key").unwrap();
        let client_ca = std::fs::read_to_string("examples/data/client_ca.pem").unwrap();

        let tls_config = ServerTlsConfig::new()
            .identity(Identity::from_pem(&cert, &key))
            .client_ca_root(Certificate::from_pem(&client_ca));

        let (incoming, addr) = bind_tcp().await;
        let uri = format!("https://{}:{}", addr.ip(), addr.port());

        let svc = FlightServiceServer::new(FlightSqlServiceImpl {});

        let serve_future = Server::builder()
            .tls_config(tls_config)
            .unwrap()
            .add_service(svc)
            .serve_with_incoming(incoming);

        let request_future = async {
            let cert = std::fs::read_to_string("examples/data/client1.pem").unwrap();
            let key = std::fs::read_to_string("examples/data/client1.key").unwrap();
            let server_ca = std::fs::read_to_string("examples/data/ca.pem").unwrap();

            let tls_config = ClientTlsConfig::new()
                .domain_name("localhost")
                .ca_certificate(Certificate::from_pem(&server_ca))
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
            let flight_data: Vec<FlightData> = flight_data.try_collect().await.unwrap();
            let batches = flight_data_to_batches(&flight_data).unwrap();

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

            // forget to set_token
            client.handshake("admin", "password").await.unwrap();
            assert!(client
                .prepare("select 1;".to_string(), None)
                .await
                .unwrap_err()
                .to_string()
                .contains("No authorization header"));

            // Invalid Tokens
            client.handshake("admin", "password").await.unwrap();
            client.set_token("wrong token".to_string());
            assert!(client
                .prepare("select 1;".to_string(), None)
                .await
                .unwrap_err()
                .to_string()
                .contains("invalid token"));
        })
        .await
    }
}
