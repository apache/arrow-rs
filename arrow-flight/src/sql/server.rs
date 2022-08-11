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
    ActionCreatePreparedStatementResult, CommandGetCatalogs, CommandGetCrossReference,
    CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys,
    CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables,
    CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementQuery,
    CommandStatementUpdate, DoPutUpdateResult, ProstAnyExt, ProstMessageExt, SqlInfo,
    TicketStatementQuery,
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

    /// Accept authentication and return a token
    /// <https://arrow.apache.org/docs/format/Flight.html#authentication>
    async fn do_handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<
        Response<Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>>,
        Status,
    > {
        Err(Status::unimplemented(
            "Handshake has no default implementation",
        ))
    }

    /// Get a FlightInfo for executing a SQL query.
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status>;

    /// Get a FlightInfo for executing an already created prepared statement.
    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status>;

    /// Get a FlightInfo for listing catalogs.
    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status>;

    /// Get a FlightInfo for listing schemas.
    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status>;

    /// Get a FlightInfo for listing tables.
    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status>;

    /// Get a FlightInfo to extract information about the table types.
    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status>;

    /// Get a FlightInfo for retrieving other information (See SqlInfo).
    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status>;

    /// Get a FlightInfo to extract information about primary and foreign keys.
    async fn get_flight_info_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status>;

    /// Get a FlightInfo to extract information about exported keys.
    async fn get_flight_info_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status>;

    /// Get a FlightInfo to extract information about imported keys.
    async fn get_flight_info_imported_keys(
        &self,
        query: CommandGetImportedKeys,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status>;

    /// Get a FlightInfo to extract information about cross reference.
    async fn get_flight_info_cross_reference(
        &self,
        query: CommandGetCrossReference,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status>;

    // do_get

    /// Get a FlightDataStream containing the query results.
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;

    /// Get a FlightDataStream containing the prepared statement query results.
    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;

    /// Get a FlightDataStream containing the list of catalogs.
    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;

    /// Get a FlightDataStream containing the list of schemas.
    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;

    /// Get a FlightDataStream containing the list of tables.
    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;

    /// Get a FlightDataStream containing the data related to the table types.
    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;

    /// Get a FlightDataStream containing the list of SqlInfo results.
    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;

    /// Get a FlightDataStream containing the data related to the primary and foreign keys.
    async fn do_get_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;

    /// Get a FlightDataStream containing the data related to the exported keys.
    async fn do_get_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;

    /// Get a FlightDataStream containing the data related to the imported keys.
    async fn do_get_imported_keys(
        &self,
        query: CommandGetImportedKeys,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;

    /// Get a FlightDataStream containing the data related to the cross reference.
    async fn do_get_cross_reference(
        &self,
        query: CommandGetCrossReference,
        request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;

    // do_put

    /// Execute an update SQL statement.
    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        request: Request<Streaming<FlightData>>,
    ) -> Result<i64, Status>;

    /// Bind parameters to given prepared statement.
    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status>;

    /// Execute an update SQL prepared statement.
    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
        request: Request<Streaming<FlightData>>,
    ) -> Result<i64, Status>;

    // do_action

    /// Create a prepared statement from given SQL statement.
    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status>;

    /// Close a prepared statement.
    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        request: Request<Action>,
    );

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
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let res = self.do_handshake(request).await?;
        Ok(res)
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
        let any: prost_types::Any =
            Message::decode(&*request.get_ref().cmd).map_err(decode_error_to_status)?;

        if any.is::<CommandStatementQuery>() {
            let token = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            return self.get_flight_info_statement(token, request).await;
        }
        if any.is::<CommandPreparedStatementQuery>() {
            let handle = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            return self
                .get_flight_info_prepared_statement(handle, request)
                .await;
        }
        if any.is::<CommandGetCatalogs>() {
            let token = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            return self.get_flight_info_catalogs(token, request).await;
        }
        if any.is::<CommandGetDbSchemas>() {
            let token = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            return self.get_flight_info_schemas(token, request).await;
        }
        if any.is::<CommandGetTables>() {
            let token = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            return self.get_flight_info_tables(token, request).await;
        }
        if any.is::<CommandGetTableTypes>() {
            let token = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            return self.get_flight_info_table_types(token, request).await;
        }
        if any.is::<CommandGetSqlInfo>() {
            let token = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            return self.get_flight_info_sql_info(token, request).await;
        }
        if any.is::<CommandGetPrimaryKeys>() {
            let token = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            return self.get_flight_info_primary_keys(token, request).await;
        }
        if any.is::<CommandGetExportedKeys>() {
            let token = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            return self.get_flight_info_exported_keys(token, request).await;
        }
        if any.is::<CommandGetImportedKeys>() {
            let token = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            return self.get_flight_info_imported_keys(token, request).await;
        }
        if any.is::<CommandGetCrossReference>() {
            let token = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            return self.get_flight_info_cross_reference(token, request).await;
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
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let any: prost_types::Any = prost::Message::decode(&*request.get_ref().ticket)
            .map_err(decode_error_to_status)?;

        if any.is::<TicketStatementQuery>() {
            let token = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            return self.do_get_statement(token, request).await;
        }
        if any.is::<CommandPreparedStatementQuery>() {
            let token = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            return self.do_get_prepared_statement(token, request).await;
        }
        if any.is::<CommandGetCatalogs>() {
            let token = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            return self.do_get_catalogs(token, request).await;
        }
        if any.is::<CommandGetDbSchemas>() {
            let token = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            return self.do_get_schemas(token, request).await;
        }
        if any.is::<CommandGetTables>() {
            let token = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            return self.do_get_tables(token, request).await;
        }
        if any.is::<CommandGetTableTypes>() {
            let token = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            return self.do_get_table_types(token, request).await;
        }
        if any.is::<CommandGetSqlInfo>() {
            let token = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            return self.do_get_sql_info(token, request).await;
        }
        if any.is::<CommandGetPrimaryKeys>() {
            let token = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            return self.do_get_primary_keys(token, request).await;
        }
        if any.is::<CommandGetExportedKeys>() {
            let token = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            return self.do_get_exported_keys(token, request).await;
        }
        if any.is::<CommandGetImportedKeys>() {
            let token = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            return self.do_get_imported_keys(token, request).await;
        }
        if any.is::<CommandGetCrossReference>() {
            let token = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            return self.do_get_cross_reference(token, request).await;
        }

        Err(Status::unimplemented(format!(
            "do_get: The defined request is invalid: {:?}",
            String::from_utf8(request.get_ref().ticket.clone()).unwrap()
        )))
    }

    async fn do_put(
        &self,
        mut request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let cmd = request.get_mut().message().await?.unwrap();
        let any: prost_types::Any =
            prost::Message::decode(&*cmd.flight_descriptor.unwrap().cmd)
                .map_err(decode_error_to_status)?;
        if any.is::<CommandStatementUpdate>() {
            let token = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            let record_count = self.do_put_statement_update(token, request).await?;
            let result = DoPutUpdateResult { record_count };
            let output = futures::stream::iter(vec![Ok(super::super::gen::PutResult {
                app_metadata: result.encode_to_vec(),
            })]);
            return Ok(Response::new(Box::pin(output)));
        }
        if any.is::<CommandPreparedStatementQuery>() {
            let token = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            return self.do_put_prepared_statement_query(token, request).await;
        }
        if any.is::<CommandPreparedStatementUpdate>() {
            let handle = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .expect("unreachable");
            let record_count = self
                .do_put_prepared_statement_update(handle, request)
                .await?;
            let result = DoPutUpdateResult { record_count };
            let output = futures::stream::iter(vec![Ok(super::super::gen::PutResult {
                app_metadata: result.encode_to_vec(),
            })]);
            return Ok(Response::new(Box::pin(output)));
        }

        Err(Status::invalid_argument(format!(
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
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        if request.get_ref().r#type == CREATE_PREPARED_STATEMENT {
            let any: prost_types::Any = Message::decode(&*request.get_ref().body)
                .map_err(decode_error_to_status)?;

            let cmd: ActionCreatePreparedStatementRequest = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .ok_or_else(|| {
                    Status::invalid_argument(
                        "Unable to unpack ActionCreatePreparedStatementRequest.",
                    )
                })?;
            let stmt = self
                .do_action_create_prepared_statement(cmd, request)
                .await?;
            let output = futures::stream::iter(vec![Ok(super::super::gen::Result {
                body: stmt.as_any().encode_to_vec(),
            })]);
            return Ok(Response::new(Box::pin(output)));
        }
        if request.get_ref().r#type == CLOSE_PREPARED_STATEMENT {
            let any: prost_types::Any = Message::decode(&*request.get_ref().body)
                .map_err(decode_error_to_status)?;

            let cmd: ActionClosePreparedStatementRequest = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .ok_or_else(|| {
                    Status::invalid_argument(
                        "Unable to unpack ActionClosePreparedStatementRequest.",
                    )
                })?;
            self.do_action_close_prepared_statement(cmd, request).await;
            return Ok(Response::new(Box::pin(futures::stream::empty())));
        }

        Err(Status::invalid_argument(format!(
            "do_action: The defined request is invalid: {:?}",
            request.get_ref().r#type
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
