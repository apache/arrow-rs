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

//! Helper trait [`FlightSqlService`] for implementing a [`FlightService`] that implements FlightSQL.

use std::pin::Pin;

use futures::{stream::Peekable, Stream, StreamExt};
use prost::Message;
use tonic::{Request, Response, Status, Streaming};

use super::{
    ActionBeginSavepointRequest, ActionBeginSavepointResult, ActionBeginTransactionRequest,
    ActionBeginTransactionResult, ActionCancelQueryRequest, ActionCancelQueryResult,
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, ActionCreatePreparedSubstraitPlanRequest,
    ActionEndSavepointRequest, ActionEndTransactionRequest, Any, Command, CommandGetCatalogs,
    CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys,
    CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables,
    CommandGetXdbcTypeInfo, CommandPreparedStatementQuery, CommandPreparedStatementUpdate,
    CommandStatementIngest, CommandStatementQuery, CommandStatementSubstraitPlan,
    CommandStatementUpdate, DoPutPreparedStatementResult, DoPutUpdateResult, ProstMessageExt,
    SqlInfo, TicketStatementQuery,
};
use crate::{
    flight_service_server::FlightService, gen::PollInfo, Action, ActionType, Criteria, Empty,
    FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse, PutResult,
    SchemaResult, Ticket,
};

pub(crate) static CREATE_PREPARED_STATEMENT: &str = "CreatePreparedStatement";
pub(crate) static CLOSE_PREPARED_STATEMENT: &str = "ClosePreparedStatement";
pub(crate) static CREATE_PREPARED_SUBSTRAIT_PLAN: &str = "CreatePreparedSubstraitPlan";
pub(crate) static BEGIN_TRANSACTION: &str = "BeginTransaction";
pub(crate) static END_TRANSACTION: &str = "EndTransaction";
pub(crate) static BEGIN_SAVEPOINT: &str = "BeginSavepoint";
pub(crate) static END_SAVEPOINT: &str = "EndSavepoint";
pub(crate) static CANCEL_QUERY: &str = "CancelQuery";

/// Implements FlightSqlService to handle the flight sql protocol
#[tonic::async_trait]
pub trait FlightSqlService: Sync + Send + Sized + 'static {
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

    /// Implementors may override to handle additional calls to do_get()
    async fn do_get_fallback(
        &self,
        _request: Request<Ticket>,
        message: Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(format!(
            "do_get: The defined request is invalid: {}",
            message.type_url
        )))
    }

    /// Get a FlightInfo for executing a SQL query.
    async fn get_flight_info_statement(
        &self,
        _query: CommandStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_statement has no default implementation",
        ))
    }

    /// Get a FlightInfo for executing a substrait plan.
    async fn get_flight_info_substrait_plan(
        &self,
        _query: CommandStatementSubstraitPlan,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_substrait_plan has no default implementation",
        ))
    }

    /// Get a FlightInfo for executing an already created prepared statement.
    async fn get_flight_info_prepared_statement(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_prepared_statement has no default implementation",
        ))
    }

    /// Get a FlightInfo for listing catalogs.
    async fn get_flight_info_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_catalogs has no default implementation",
        ))
    }

    /// Get a FlightInfo for listing schemas.
    async fn get_flight_info_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_schemas has no default implementation",
        ))
    }

    /// Get a FlightInfo for listing tables.
    async fn get_flight_info_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_tables has no default implementation",
        ))
    }

    /// Get a FlightInfo to extract information about the table types.
    async fn get_flight_info_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_table_types has no default implementation",
        ))
    }

    /// Get a FlightInfo for retrieving other information (See SqlInfo).
    async fn get_flight_info_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_sql_info has no default implementation",
        ))
    }

    /// Get a FlightInfo to extract information about primary and foreign keys.
    async fn get_flight_info_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_primary_keys has no default implementation",
        ))
    }

    /// Get a FlightInfo to extract information about exported keys.
    async fn get_flight_info_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_exported_keys has no default implementation",
        ))
    }

    /// Get a FlightInfo to extract information about imported keys.
    async fn get_flight_info_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_imported_keys has no default implementation",
        ))
    }

    /// Get a FlightInfo to extract information about cross reference.
    async fn get_flight_info_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_cross_reference has no default implementation",
        ))
    }

    /// Get a FlightInfo to extract information about the supported XDBC types.
    async fn get_flight_info_xdbc_type_info(
        &self,
        _query: CommandGetXdbcTypeInfo,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(
            "get_flight_info_xdbc_type_info has no default implementation",
        ))
    }

    /// Implementors may override to handle additional calls to get_flight_info()
    async fn get_flight_info_fallback(
        &self,
        cmd: Command,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented(format!(
            "get_flight_info: The defined request is invalid: {}",
            cmd.type_url()
        )))
    }

    // do_get

    /// Get a FlightDataStream containing the query results.
    async fn do_get_statement(
        &self,
        _ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_statement has no default implementation",
        ))
    }

    /// Get a FlightDataStream containing the prepared statement query results.
    async fn do_get_prepared_statement(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_prepared_statement has no default implementation",
        ))
    }

    /// Get a FlightDataStream containing the list of catalogs.
    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_catalogs has no default implementation",
        ))
    }

    /// Get a FlightDataStream containing the list of schemas.
    async fn do_get_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_schemas has no default implementation",
        ))
    }

    /// Get a FlightDataStream containing the list of tables.
    async fn do_get_tables(
        &self,
        _query: CommandGetTables,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_tables has no default implementation",
        ))
    }

    /// Get a FlightDataStream containing the data related to the table types.
    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_table_types has no default implementation",
        ))
    }

    /// Get a FlightDataStream containing the list of SqlInfo results.
    async fn do_get_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_sql_info has no default implementation",
        ))
    }

    /// Get a FlightDataStream containing the data related to the primary and foreign keys.
    async fn do_get_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_primary_keys has no default implementation",
        ))
    }

    /// Get a FlightDataStream containing the data related to the exported keys.
    async fn do_get_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_exported_keys has no default implementation",
        ))
    }

    /// Get a FlightDataStream containing the data related to the imported keys.
    async fn do_get_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_imported_keys has no default implementation",
        ))
    }

    /// Get a FlightDataStream containing the data related to the cross reference.
    async fn do_get_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_cross_reference has no default implementation",
        ))
    }

    /// Get a FlightDataStream containing the data related to the supported XDBC types.
    async fn do_get_xdbc_type_info(
        &self,
        _query: CommandGetXdbcTypeInfo,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented(
            "do_get_xdbc_type_info has no default implementation",
        ))
    }

    // do_put

    /// Implementors may override to handle additional calls to do_put()
    async fn do_put_fallback(
        &self,
        _request: Request<PeekableFlightDataStream>,
        message: Any,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        Err(Status::unimplemented(format!(
            "do_put: The defined request is invalid: {}",
            message.type_url
        )))
    }

    /// Execute an update SQL statement.
    async fn do_put_statement_update(
        &self,
        _ticket: CommandStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented(
            "do_put_statement_update has no default implementation",
        ))
    }

    /// Execute a bulk ingestion.
    async fn do_put_statement_ingest(
        &self,
        _ticket: CommandStatementIngest,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented(
            "do_put_statement_ingest has no default implementation",
        ))
    }

    /// Bind parameters to given prepared statement.
    ///
    /// Returns an opaque handle that the client should pass
    /// back to the server during subsequent requests with this
    /// prepared statement.
    async fn do_put_prepared_statement_query(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        Err(Status::unimplemented(
            "do_put_prepared_statement_query has no default implementation",
        ))
    }

    /// Execute an update SQL prepared statement.
    async fn do_put_prepared_statement_update(
        &self,
        _query: CommandPreparedStatementUpdate,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented(
            "do_put_prepared_statement_update has no default implementation",
        ))
    }

    /// Execute a substrait plan
    async fn do_put_substrait_plan(
        &self,
        _query: CommandStatementSubstraitPlan,
        _request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented(
            "do_put_substrait_plan has no default implementation",
        ))
    }

    // do_action

    /// Implementors may override to handle additional calls to do_action()
    async fn do_action_fallback(
        &self,
        request: Request<Action>,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        Err(Status::invalid_argument(format!(
            "do_action: The defined request is invalid: {:?}",
            request.get_ref().r#type
        )))
    }

    /// Add custom actions to list_actions() result
    async fn list_custom_actions(&self) -> Option<Vec<Result<ActionType, Status>>> {
        None
    }

    /// Create a prepared statement from given SQL statement.
    async fn do_action_create_prepared_statement(
        &self,
        _query: ActionCreatePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        Err(Status::unimplemented(
            "do_action_create_prepared_statement has no default implementation",
        ))
    }

    /// Close a prepared statement.
    async fn do_action_close_prepared_statement(
        &self,
        _query: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Err(Status::unimplemented(
            "do_action_close_prepared_statement has no default implementation",
        ))
    }

    /// Create a prepared substrait plan.
    async fn do_action_create_prepared_substrait_plan(
        &self,
        _query: ActionCreatePreparedSubstraitPlanRequest,
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        Err(Status::unimplemented(
            "do_action_create_prepared_substrait_plan has no default implementation",
        ))
    }

    /// Begin a transaction
    async fn do_action_begin_transaction(
        &self,
        _query: ActionBeginTransactionRequest,
        _request: Request<Action>,
    ) -> Result<ActionBeginTransactionResult, Status> {
        Err(Status::unimplemented(
            "do_action_begin_transaction has no default implementation",
        ))
    }

    /// End a transaction
    async fn do_action_end_transaction(
        &self,
        _query: ActionEndTransactionRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Err(Status::unimplemented(
            "do_action_end_transaction has no default implementation",
        ))
    }

    /// Begin a savepoint
    async fn do_action_begin_savepoint(
        &self,
        _query: ActionBeginSavepointRequest,
        _request: Request<Action>,
    ) -> Result<ActionBeginSavepointResult, Status> {
        Err(Status::unimplemented(
            "do_action_begin_savepoint has no default implementation",
        ))
    }

    /// End a savepoint
    async fn do_action_end_savepoint(
        &self,
        _query: ActionEndSavepointRequest,
        _request: Request<Action>,
    ) -> Result<(), Status> {
        Err(Status::unimplemented(
            "do_action_end_savepoint has no default implementation",
        ))
    }

    /// Cancel a query
    async fn do_action_cancel_query(
        &self,
        _query: ActionCancelQueryRequest,
        _request: Request<Action>,
    ) -> Result<ActionCancelQueryResult, Status> {
        Err(Status::unimplemented(
            "do_action_cancel_query has no default implementation",
        ))
    }

    /// do_exchange

    /// Implementors may override to handle additional calls to do_exchange()
    async fn do_exchange_fallback(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<<Self as FlightService>::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    /// Register a new SqlInfo result, making it available when calling GetSqlInfo.
    async fn register_sql_info(&self, id: i32, result: &SqlInfo);
}

/// Implements the lower level interface to handle FlightSQL
#[tonic::async_trait]
impl<T: 'static> FlightService for T
where
    T: FlightSqlService + Send,
{
    type HandshakeStream =
        Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + 'static>>;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + 'static>>;
    type DoGetStream = Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + 'static>>;
    type DoPutStream = Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + 'static>>;
    type DoActionStream =
        Pin<Box<dyn Stream<Item = Result<super::super::Result, Status>> + Send + 'static>>;
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
        let message = Any::decode(&*request.get_ref().cmd).map_err(decode_error_to_status)?;

        match Command::try_from(message).map_err(arrow_error_to_status)? {
            Command::CommandStatementQuery(token) => {
                self.get_flight_info_statement(token, request).await
            }
            Command::CommandPreparedStatementQuery(handle) => {
                self.get_flight_info_prepared_statement(handle, request)
                    .await
            }
            Command::CommandStatementSubstraitPlan(handle) => {
                self.get_flight_info_substrait_plan(handle, request).await
            }
            Command::CommandGetCatalogs(token) => {
                self.get_flight_info_catalogs(token, request).await
            }
            Command::CommandGetDbSchemas(token) => {
                return self.get_flight_info_schemas(token, request).await
            }
            Command::CommandGetTables(token) => self.get_flight_info_tables(token, request).await,
            Command::CommandGetTableTypes(token) => {
                self.get_flight_info_table_types(token, request).await
            }
            Command::CommandGetSqlInfo(token) => {
                self.get_flight_info_sql_info(token, request).await
            }
            Command::CommandGetPrimaryKeys(token) => {
                self.get_flight_info_primary_keys(token, request).await
            }
            Command::CommandGetExportedKeys(token) => {
                self.get_flight_info_exported_keys(token, request).await
            }
            Command::CommandGetImportedKeys(token) => {
                self.get_flight_info_imported_keys(token, request).await
            }
            Command::CommandGetCrossReference(token) => {
                self.get_flight_info_cross_reference(token, request).await
            }
            Command::CommandGetXdbcTypeInfo(token) => {
                self.get_flight_info_xdbc_type_info(token, request).await
            }
            cmd => self.get_flight_info_fallback(cmd, request).await,
        }
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
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
        let msg: Any =
            Message::decode(&*request.get_ref().ticket).map_err(decode_error_to_status)?;

        match Command::try_from(msg).map_err(arrow_error_to_status)? {
            Command::TicketStatementQuery(command) => self.do_get_statement(command, request).await,
            Command::CommandPreparedStatementQuery(command) => {
                self.do_get_prepared_statement(command, request).await
            }
            Command::CommandGetCatalogs(command) => self.do_get_catalogs(command, request).await,
            Command::CommandGetDbSchemas(command) => self.do_get_schemas(command, request).await,
            Command::CommandGetTables(command) => self.do_get_tables(command, request).await,
            Command::CommandGetTableTypes(command) => {
                self.do_get_table_types(command, request).await
            }
            Command::CommandGetSqlInfo(command) => self.do_get_sql_info(command, request).await,
            Command::CommandGetPrimaryKeys(command) => {
                self.do_get_primary_keys(command, request).await
            }
            Command::CommandGetExportedKeys(command) => {
                self.do_get_exported_keys(command, request).await
            }
            Command::CommandGetImportedKeys(command) => {
                self.do_get_imported_keys(command, request).await
            }
            Command::CommandGetCrossReference(command) => {
                self.do_get_cross_reference(command, request).await
            }
            Command::CommandGetXdbcTypeInfo(command) => {
                self.do_get_xdbc_type_info(command, request).await
            }
            cmd => self.do_get_fallback(request, cmd.into_any()).await,
        }
    }

    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        // See issue #4658: https://github.com/apache/arrow-rs/issues/4658
        // To dispatch to the correct `do_put` method, we cannot discard the first message,
        // as it may contain the Arrow schema, which the `do_put` handler may need.
        // To allow the first message to be reused by the `do_put` handler,
        // we wrap this stream in a `Peekable` one, which allows us to peek at
        // the first message without discarding it.
        let mut request = request.map(PeekableFlightDataStream::new);
        let cmd = Pin::new(request.get_mut()).peek().await.unwrap().clone()?;

        let message =
            Any::decode(&*cmd.flight_descriptor.unwrap().cmd).map_err(decode_error_to_status)?;
        match Command::try_from(message).map_err(arrow_error_to_status)? {
            Command::CommandStatementUpdate(command) => {
                let record_count = self.do_put_statement_update(command, request).await?;
                let result = DoPutUpdateResult { record_count };
                let output = futures::stream::iter(vec![Ok(PutResult {
                    app_metadata: result.as_any().encode_to_vec().into(),
                })]);
                Ok(Response::new(Box::pin(output)))
            }
            Command::CommandStatementIngest(command) => {
                let record_count = self.do_put_statement_ingest(command, request).await?;
                let result = DoPutUpdateResult { record_count };
                let output = futures::stream::iter(vec![Ok(PutResult {
                    app_metadata: result.as_any().encode_to_vec().into(),
                })]);
                Ok(Response::new(Box::pin(output)))
            }
            Command::CommandPreparedStatementQuery(command) => {
                let result = self
                    .do_put_prepared_statement_query(command, request)
                    .await?;
                let output = futures::stream::iter(vec![Ok(PutResult {
                    app_metadata: result.encode_to_vec().into(),
                })]);
                Ok(Response::new(Box::pin(output)))
            }
            Command::CommandStatementSubstraitPlan(command) => {
                let record_count = self.do_put_substrait_plan(command, request).await?;
                let result = DoPutUpdateResult { record_count };
                let output = futures::stream::iter(vec![Ok(PutResult {
                    app_metadata: result.as_any().encode_to_vec().into(),
                })]);
                Ok(Response::new(Box::pin(output)))
            }
            Command::CommandPreparedStatementUpdate(command) => {
                let record_count = self
                    .do_put_prepared_statement_update(command, request)
                    .await?;
                let result = DoPutUpdateResult { record_count };
                let output = futures::stream::iter(vec![Ok(PutResult {
                    app_metadata: result.as_any().encode_to_vec().into(),
                })]);
                Ok(Response::new(Box::pin(output)))
            }
            cmd => self.do_put_fallback(request, cmd.into_any()).await,
        }
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
        let create_prepared_substrait_plan_action_type = ActionType {
            r#type: CREATE_PREPARED_SUBSTRAIT_PLAN.to_string(),
            description: "Creates a reusable prepared substrait plan resource on the server.\n
                Request Message: ActionCreatePreparedSubstraitPlanRequest\n
                Response Message: ActionCreatePreparedStatementResult"
                .into(),
        };
        let begin_transaction_action_type = ActionType {
            r#type: BEGIN_TRANSACTION.to_string(),
            description: "Begins a transaction.\n
                Request Message: ActionBeginTransactionRequest\n
                Response Message: ActionBeginTransactionResult"
                .into(),
        };
        let end_transaction_action_type = ActionType {
            r#type: END_TRANSACTION.to_string(),
            description: "Ends a transaction\n
                Request Message: ActionEndTransactionRequest\n
                Response Message: N/A"
                .into(),
        };
        let begin_savepoint_action_type = ActionType {
            r#type: BEGIN_SAVEPOINT.to_string(),
            description: "Begins a savepoint.\n
                Request Message: ActionBeginSavepointRequest\n
                Response Message: ActionBeginSavepointResult"
                .into(),
        };
        let end_savepoint_action_type = ActionType {
            r#type: END_SAVEPOINT.to_string(),
            description: "Ends a savepoint\n
                Request Message: ActionEndSavepointRequest\n
                Response Message: N/A"
                .into(),
        };
        let cancel_query_action_type = ActionType {
            r#type: CANCEL_QUERY.to_string(),
            description: "Cancels a query\n
                Request Message: ActionCancelQueryRequest\n
                Response Message: ActionCancelQueryResult"
                .into(),
        };
        let mut actions: Vec<Result<ActionType, Status>> = vec![
            Ok(create_prepared_statement_action_type),
            Ok(close_prepared_statement_action_type),
            Ok(create_prepared_substrait_plan_action_type),
            Ok(begin_transaction_action_type),
            Ok(end_transaction_action_type),
            Ok(begin_savepoint_action_type),
            Ok(end_savepoint_action_type),
            Ok(cancel_query_action_type),
        ];

        if let Some(mut custom_actions) = self.list_custom_actions().await {
            actions.append(&mut custom_actions);
        }

        let output = futures::stream::iter(actions);
        Ok(Response::new(Box::pin(output) as Self::ListActionsStream))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        if request.get_ref().r#type == CREATE_PREPARED_STATEMENT {
            let any = Any::decode(&*request.get_ref().body).map_err(decode_error_to_status)?;

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
                body: stmt.as_any().encode_to_vec().into(),
            })]);
            return Ok(Response::new(Box::pin(output)));
        } else if request.get_ref().r#type == CLOSE_PREPARED_STATEMENT {
            let any = Any::decode(&*request.get_ref().body).map_err(decode_error_to_status)?;

            let cmd: ActionClosePreparedStatementRequest = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .ok_or_else(|| {
                    Status::invalid_argument(
                        "Unable to unpack ActionClosePreparedStatementRequest.",
                    )
                })?;
            self.do_action_close_prepared_statement(cmd, request)
                .await?;
            return Ok(Response::new(Box::pin(futures::stream::empty())));
        } else if request.get_ref().r#type == CREATE_PREPARED_SUBSTRAIT_PLAN {
            let any = Any::decode(&*request.get_ref().body).map_err(decode_error_to_status)?;

            let cmd: ActionCreatePreparedSubstraitPlanRequest = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .ok_or_else(|| {
                    Status::invalid_argument(
                        "Unable to unpack ActionCreatePreparedSubstraitPlanRequest.",
                    )
                })?;
            self.do_action_create_prepared_substrait_plan(cmd, request)
                .await?;
            return Ok(Response::new(Box::pin(futures::stream::empty())));
        } else if request.get_ref().r#type == BEGIN_TRANSACTION {
            let any = Any::decode(&*request.get_ref().body).map_err(decode_error_to_status)?;

            let cmd: ActionBeginTransactionRequest = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .ok_or_else(|| {
                Status::invalid_argument("Unable to unpack ActionBeginTransactionRequest.")
            })?;
            let stmt = self.do_action_begin_transaction(cmd, request).await?;
            let output = futures::stream::iter(vec![Ok(super::super::gen::Result {
                body: stmt.as_any().encode_to_vec().into(),
            })]);
            return Ok(Response::new(Box::pin(output)));
        } else if request.get_ref().r#type == END_TRANSACTION {
            let any = Any::decode(&*request.get_ref().body).map_err(decode_error_to_status)?;

            let cmd: ActionEndTransactionRequest = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .ok_or_else(|| {
                    Status::invalid_argument("Unable to unpack ActionEndTransactionRequest.")
                })?;
            self.do_action_end_transaction(cmd, request).await?;
            return Ok(Response::new(Box::pin(futures::stream::empty())));
        } else if request.get_ref().r#type == BEGIN_SAVEPOINT {
            let any = Any::decode(&*request.get_ref().body).map_err(decode_error_to_status)?;

            let cmd: ActionBeginSavepointRequest = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .ok_or_else(|| {
                    Status::invalid_argument("Unable to unpack ActionBeginSavepointRequest.")
                })?;
            let stmt = self.do_action_begin_savepoint(cmd, request).await?;
            let output = futures::stream::iter(vec![Ok(super::super::gen::Result {
                body: stmt.as_any().encode_to_vec().into(),
            })]);
            return Ok(Response::new(Box::pin(output)));
        } else if request.get_ref().r#type == END_SAVEPOINT {
            let any = Any::decode(&*request.get_ref().body).map_err(decode_error_to_status)?;

            let cmd: ActionEndSavepointRequest = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .ok_or_else(|| {
                    Status::invalid_argument("Unable to unpack ActionEndSavepointRequest.")
                })?;
            self.do_action_end_savepoint(cmd, request).await?;
            return Ok(Response::new(Box::pin(futures::stream::empty())));
        } else if request.get_ref().r#type == CANCEL_QUERY {
            let any = Any::decode(&*request.get_ref().body).map_err(decode_error_to_status)?;

            let cmd: ActionCancelQueryRequest = any
                .unpack()
                .map_err(arrow_error_to_status)?
                .ok_or_else(|| {
                    Status::invalid_argument("Unable to unpack ActionCancelQueryRequest.")
                })?;
            let stmt = self.do_action_cancel_query(cmd, request).await?;
            let output = futures::stream::iter(vec![Ok(super::super::gen::Result {
                body: stmt.as_any().encode_to_vec().into(),
            })]);
            return Ok(Response::new(Box::pin(output)));
        }

        self.do_action_fallback(request).await
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        self.do_exchange_fallback(request).await
    }
}

fn decode_error_to_status(err: prost::DecodeError) -> Status {
    Status::invalid_argument(format!("{err:?}"))
}

fn arrow_error_to_status(err: arrow_schema::ArrowError) -> Status {
    Status::internal(format!("{err:?}"))
}

/// A wrapper around [`Streaming<FlightData>`] that allows "peeking" at the
/// message at the front of the stream without consuming it.
///
/// This is needed because sometimes the first message in the stream will contain
/// a [`FlightDescriptor`] in addition to potentially any data, and the dispatch logic
/// must inspect this information.
///
/// # Example
///
/// [`PeekableFlightDataStream::peek`] can be used to peek at the first message without
/// discarding it; otherwise, `PeekableFlightDataStream` can be used as a regular stream.
/// See the following example:
///
/// ```no_run
/// use arrow_array::RecordBatch;
/// use arrow_flight::decode::FlightRecordBatchStream;
/// use arrow_flight::FlightDescriptor;
/// use arrow_flight::error::FlightError;
/// use arrow_flight::sql::server::PeekableFlightDataStream;
/// use tonic::{Request, Status};
/// use futures::TryStreamExt;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Status> {
///     let request: Request<PeekableFlightDataStream> = todo!();
///     let stream: PeekableFlightDataStream = request.into_inner();
///
///     // The first message contains the flight descriptor and the schema.
///     // Read the flight descriptor without discarding the schema:
///     let flight_descriptor: FlightDescriptor = stream
///         .peek()
///         .await
///         .cloned()
///         .transpose()?
///         .and_then(|data| data.flight_descriptor)
///         .expect("first message should contain flight descriptor");
///
///     // Pass the stream through a decoder
///     let batches: Vec<RecordBatch> = FlightRecordBatchStream::new_from_flight_data(
///         request.into_inner().map_err(|e| e.into()),
///     )
///     .try_collect()
///     .await?;
/// }
/// ```
pub struct PeekableFlightDataStream {
    inner: Peekable<Streaming<FlightData>>,
}

impl PeekableFlightDataStream {
    fn new(stream: Streaming<FlightData>) -> Self {
        Self {
            inner: stream.peekable(),
        }
    }

    /// Convert this stream into a `Streaming<FlightData>`.
    /// Any messages observed through [`Self::peek`] will be lost
    /// after the conversion.
    pub fn into_inner(self) -> Streaming<FlightData> {
        self.inner.into_inner()
    }

    /// Convert this stream into a `Peekable<Streaming<FlightData>>`.
    /// Preserves the state of the stream, so that calls to [`Self::peek`]
    /// and [`Self::poll_next`] are the same.
    pub fn into_peekable(self) -> Peekable<Streaming<FlightData>> {
        self.inner
    }

    /// Peek at the head of this stream without advancing it.
    pub async fn peek(&mut self) -> Option<&Result<FlightData, Status>> {
        Pin::new(&mut self.inner).peek().await
    }
}

impl Stream for PeekableFlightDataStream {
    type Item = Result<FlightData, Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}
