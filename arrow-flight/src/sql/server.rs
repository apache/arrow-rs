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

use std::pin::Pin;

use futures::Stream;
use prost::Message;
use tonic::{Request, Response, Status, Streaming};

use super::{
    super::{
        flight_service_server::FlightService, Action, ActionType, Criteria, Empty,
        FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
        PutResult, SchemaResult, Ticket,
    },
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    CommandGetCatalogs, CommandGetCrossReference, CommandGetDbSchemas,
    CommandGetExportedKeys, CommandGetImportedKeys, CommandGetPrimaryKeys,
    CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables,
    CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementQuery,
    CommandStatementUpdate, ProstAnyExt, SqlInfo, TicketStatementQuery,
};

static CREATE_PREPARED_STATEMENT: &str = "CreatePreparedStatement";
static CLOSE_PREPARED_STATEMENT: &str = "ClosePreparedStatement";

/// Implements FlightSqlService to handle the flight sql protocol
#[tonic::async_trait]
pub trait FlightSqlService:
    std::marker::Sync + std::marker::Send + std::marker::Sized + 'static
{
    /// When impl FlightSqlService, you can always set FlightService to Self
    type FlightService: FlightService;

    /// Get a FlightInfo for executing a SQL query.
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status>;

    /// Get a FlightInfo for executing an already created prepared statement.
    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status>;

    /// Get a FlightInfo for listing catalogs.
    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status>;

    /// Get a FlightInfo for listing schemas.
    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status>;

    /// Get a FlightInfo for listing tables.
    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status>;

    /// Get a FlightInfo to extract information about the table types.
    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status>;

    /// Get a FlightInfo for retrieving other information (See SqlInfo).
    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status>;

    /// Get a FlightInfo to extract information about primary and foreign keys.
    async fn get_flight_info_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status>;

    /// Get a FlightInfo to extract information about exported keys.
    async fn get_flight_info_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status>;

    /// Get a FlightInfo to extract information about imported keys.
    async fn get_flight_info_imported_keys(
        &self,
        query: CommandGetImportedKeys,
        request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status>;

    /// Get a FlightInfo to extract information about cross reference.
    async fn get_flight_info_cross_reference(
        &self,
        query: CommandGetCrossReference,
        request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status>;

    // do_get

    /// Get a FlightInfo for executing an already created prepared statement.
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;

    /// Get a FlightDataStream containing the prepared statement query results.
    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;

    /// Get a FlightDataStream containing the list of catalogs.
    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;

    /// Get a FlightDataStream containing the list of schemas.
    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;

    /// Get a FlightDataStream containing the list of tables.
    async fn do_get_tables(
        &self,
        query: CommandGetTables,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;

    /// Get a FlightDataStream containing the data related to the table types.
    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;

    /// Get a FlightDataStream containing the list of SqlInfo results.
    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;

    /// Get a FlightDataStream containing the data related to the primary and foreign keys.
    async fn do_get_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;

    /// Get a FlightDataStream containing the data related to the exported keys.
    async fn do_get_exported_keys(
        &self,
        query: CommandGetExportedKeys,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;

    /// Get a FlightDataStream containing the data related to the imported keys.
    async fn do_get_imported_keys(
        &self,
        query: CommandGetImportedKeys,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;

    /// Get a FlightDataStream containing the data related to the cross reference.
    async fn do_get_cross_reference(
        &self,
        query: CommandGetCrossReference,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;

    // do_put

    /// Execute an update SQL statement.
    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status>;

    /// Bind parameters to given prepared statement.
    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status>;

    /// Execute an update SQL prepared statement.
    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status>;

    // do_action

    /// Create a prepared statement from given SQL statement.
    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status>;

    /// Close a prepared statement.
    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status>;

    /// Register a new SqlInfo result, making it available when calling GetSqlInfo.
    async fn register_sql_info(&self, id: i32, result: &SqlInfo);
}

/// Implements the lower level interface to handle FlightSQL
#[tonic::async_trait]
impl<T: 'static> FlightService for T
where
    T: FlightSqlService + std::marker::Send,
{
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + 'static>>;
    type DoGetStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
    type DoPutStream =
        Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + 'static>>;
    type DoActionStream = Pin<
        Box<dyn Stream<Item = Result<super::super::Result, Status>> + Send + 'static>,
    >;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let request = request.into_inner();
        let any: prost_types::Any =
            prost::Message::decode(&*request.cmd).map_err(decode_error_to_status)?;

        if any.is::<CommandStatementQuery>() {
            return self
                .get_flight_info_statement(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                    request,
                )
                .await;
        }
        if any.is::<CommandPreparedStatementQuery>() {
            return self
                .get_flight_info_prepared_statement(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                    request,
                )
                .await;
        }
        if any.is::<CommandGetCatalogs>() {
            return self
                .get_flight_info_catalogs(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                    request,
                )
                .await;
        }
        if any.is::<CommandGetDbSchemas>() {
            return self
                .get_flight_info_schemas(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                    request,
                )
                .await;
        }
        if any.is::<CommandGetTables>() {
            return self
                .get_flight_info_tables(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                    request,
                )
                .await;
        }
        if any.is::<CommandGetTableTypes>() {
            return self
                .get_flight_info_table_types(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                    request,
                )
                .await;
        }
        if any.is::<CommandGetSqlInfo>() {
            return self
                .get_flight_info_sql_info(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                    request,
                )
                .await;
        }
        if any.is::<CommandGetPrimaryKeys>() {
            return self
                .get_flight_info_primary_keys(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                    request,
                )
                .await;
        }
        if any.is::<CommandGetExportedKeys>() {
            return self
                .get_flight_info_exported_keys(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                    request,
                )
                .await;
        }
        if any.is::<CommandGetImportedKeys>() {
            return self
                .get_flight_info_imported_keys(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                    request,
                )
                .await;
        }
        if any.is::<CommandGetCrossReference>() {
            return self
                .get_flight_info_cross_reference(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                    request,
                )
                .await;
        }

        Err(Status::unimplemented(format!(
            "get_flight_info: The defined request is invalid: {:?}",
            String::from_utf8(any.encode_to_vec()).unwrap()
        )))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let request = _request.into_inner();
        let any: prost_types::Any =
            prost::Message::decode(&*request.ticket).map_err(decode_error_to_status)?;

        if any.is::<TicketStatementQuery>() {
            return self
                .do_get_statement(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                )
                .await;
        }
        if any.is::<CommandPreparedStatementQuery>() {
            return self
                .do_get_prepared_statement(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                )
                .await;
        }
        if any.is::<CommandGetCatalogs>() {
            return self
                .do_get_catalogs(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                )
                .await;
        }
        if any.is::<CommandGetDbSchemas>() {
            return self
                .do_get_schemas(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                )
                .await;
        }
        if any.is::<CommandGetTables>() {
            return self
                .do_get_tables(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                )
                .await;
        }
        if any.is::<CommandGetTableTypes>() {
            return self
                .do_get_table_types(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                )
                .await;
        }
        if any.is::<CommandGetSqlInfo>() {
            return self
                .do_get_sql_info(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                )
                .await;
        }
        if any.is::<CommandGetPrimaryKeys>() {
            return self
                .do_get_primary_keys(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                )
                .await;
        }
        if any.is::<CommandGetExportedKeys>() {
            return self
                .do_get_exported_keys(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                )
                .await;
        }
        if any.is::<CommandGetImportedKeys>() {
            return self
                .do_get_imported_keys(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                )
                .await;
        }
        if any.is::<CommandGetCrossReference>() {
            return self
                .do_get_cross_reference(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                )
                .await;
        }

        Err(Status::unimplemented(format!(
            "do_get: The defined request is invalid: {:?}",
            String::from_utf8(request.ticket).unwrap()
        )))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let request = _request.into_inner().message().await?.unwrap();
        let any: prost_types::Any =
            prost::Message::decode(&*request.flight_descriptor.unwrap().cmd)
                .map_err(decode_error_to_status)?;
        if any.is::<CommandStatementUpdate>() {
            return self
                .do_put_statement_update(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                )
                .await;
        }
        if any.is::<CommandPreparedStatementQuery>() {
            return self
                .do_put_prepared_statement_query(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                )
                .await;
        }
        if any.is::<CommandPreparedStatementUpdate>() {
            return self
                .do_put_prepared_statement_update(
                    any.unpack()
                        .map_err(arrow_error_to_status)?
                        .expect("unreachable"),
                )
                .await;
        }

        Err(Status::unimplemented(format!(
            "do_put: The defined request is invalid: {:?}",
            String::from_utf8(any.encode_to_vec()).unwrap()
        )))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let create_prepared_statement_action_type = ActionType {
            r#type: CREATE_PREPARED_STATEMENT.to_string(),
            description: "Creates a reusable prepared statement resource on the server.\n
                Request Message: ActionCreatePreparedStatementRequest\n
                Response Message: ActionCreatePreparedStatementResult"
                .into(),
        };
        let close_prepared_statement_action_type = ActionType {
            r#type: CLOSE_PREPARED_STATEMENT.to_string(),
            description: "Closes a reusable prepared statement resource on the server.\n
                Request Message: ActionClosePreparedStatementRequest\n
                Response Message: N/A"
                .into(),
        };
        let actions: Vec<Result<ActionType, Status>> = vec![
            Ok(create_prepared_statement_action_type),
            Ok(close_prepared_statement_action_type),
        ];
        let output = futures::stream::iter(actions);
        Ok(Response::new(Box::pin(output) as Self::ListActionsStream))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let request = _request.into_inner();

        if request.r#type == CREATE_PREPARED_STATEMENT {
            let any: prost_types::Any =
                prost::Message::decode(&*request.body).map_err(decode_error_to_status)?;

            let cmd: ActionCreatePreparedStatementRequest = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .ok_or_else(|| {
                    Status::invalid_argument(
                        "Unable to unpack ActionCreatePreparedStatementRequest.",
                    )
                })?;
            return self.do_action_create_prepared_statement(cmd).await;
        }
        if request.r#type == CLOSE_PREPARED_STATEMENT {
            let any: prost_types::Any =
                prost::Message::decode(&*request.body).map_err(decode_error_to_status)?;

            let cmd: ActionClosePreparedStatementRequest = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .ok_or_else(|| {
                    Status::invalid_argument(
                        "Unable to unpack CloseCreatePreparedStatementRequest.",
                    )
                })?;
            return self.do_action_close_prepared_statement(cmd).await;
        }

        Err(Status::unimplemented(format!(
            "do_action: The defined request is invalid: {:?}",
            request.r#type
        )))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
}

fn decode_error_to_status(err: prost::DecodeError) -> tonic::Status {
    tonic::Status::invalid_argument(format!("{:?}", err))
}

fn arrow_error_to_status(err: arrow::error::ArrowError) -> tonic::Status {
    tonic::Status::internal(format!("{:?}", err))
}
