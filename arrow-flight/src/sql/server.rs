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
use protobuf::Message;
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
    CommandStatementUpdate, TicketStatementQuery,
};

use lazy_static::lazy_static;
lazy_static! {
    static ref CREATE_PREPARED_STATEMENT_ACTION_TYPE: ActionType = ActionType {
        r#type: "CreatePreparedStatement".into(),
        description: "Creates a reusable prepared statement resource on the server.\n
                Request Message: ActionCreatePreparedStatementRequest\n
                Response Message: ActionCreatePreparedStatementResult"
            .into(),
    };
    static ref CLOSE_PREPARED_STATEMENT_ACTION_TYPE: ActionType = ActionType {
        r#type: "ClosePreparedStatement".into(),
        description: "Closes a reusable prepared statement resource on the server.\n
                Request Message: ActionClosePreparedStatementRequest\n
                Response Message: N/A"
            .into(),
    };
}

#[tonic::async_trait]
pub trait FlightSqlService:
    std::marker::Sync + std::marker::Send + std::marker::Sized + FlightService + 'static
{
    type FlightService: FlightService;

    // get_flight_info
    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status>;
    async fn get_flight_info_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
        request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status>;
    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status>;
    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status>;
    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status>;
    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status>;
    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status>;
    async fn get_flight_info_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
        request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status>;
    async fn get_flight_info_exported_keys(
        &self,
        query: CommandGetExportedKeys,
        request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status>;
    async fn get_flight_info_imported_keys(
        &self,
        query: CommandGetImportedKeys,
        request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status>;
    async fn get_flight_info_cross_reference(
        &self,
        query: CommandGetCrossReference,
        request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status>;
    // do_get
    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;
    async fn do_get_prepared_statement(
        &self,
        query: CommandPreparedStatementQuery,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;
    async fn do_get_catalogs(
        &self,
        query: CommandGetCatalogs,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;
    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;
    async fn do_get_tables(
        &self,
        query: CommandGetTables,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;
    async fn do_get_table_types(
        &self,
        query: CommandGetTableTypes,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;
    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;
    async fn do_get_primary_keys(
        &self,
        query: CommandGetPrimaryKeys,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;
    async fn do_get_exported_keys(
        &self,
        query: CommandGetExportedKeys,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;
    async fn do_get_imported_keys(
        &self,
        query: CommandGetImportedKeys,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;
    async fn do_get_cross_reference(
        &self,
        query: CommandGetCrossReference,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status>;
    // do_put
    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status>;
    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status>;
    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status>;
    // do_action
    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status>;
    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status>;
}

#[tonic::async_trait]
impl<T: 'static> FlightService for T
where
    T: FlightSqlService + std::marker::Sync + std::marker::Send,
{
    type HandshakeStream = Pin<
        Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send + Sync + 'static>,
    >;
    type ListFlightsStream =
        Pin<Box<dyn Stream<Item = Result<FlightInfo, Status>> + Send + Sync + 'static>>;
    type DoGetStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;
    type DoPutStream =
        Pin<Box<dyn Stream<Item = Result<PutResult, Status>> + Send + Sync + 'static>>;
    type DoActionStream = Pin<
        Box<
            dyn Stream<Item = Result<super::super::Result, Status>>
                + Send
                + Sync
                + 'static,
        >,
    >;
    type ListActionsStream =
        Pin<Box<dyn Stream<Item = Result<ActionType, Status>> + Send + Sync + 'static>>;
    type DoExchangeStream =
        Pin<Box<dyn Stream<Item = Result<FlightData, Status>> + Send + Sync + 'static>>;

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
        let any: protobuf::well_known_types::Any =
            protobuf::Message::parse_from_bytes(&request.cmd)
                .map_err(|_| Status::invalid_argument("Unable to parse command"))?;

        if any.is::<CommandStatementQuery>() {
            return self
                .get_flight_info_statement(any.unpack().unwrap().unwrap(), request)
                .await;
        }
        if any.is::<CommandPreparedStatementQuery>() {
            return self
                .get_flight_info_prepared_statement(
                    any.unpack().unwrap().unwrap(),
                    request,
                )
                .await;
        }
        if any.is::<CommandGetCatalogs>() {
            return self
                .get_flight_info_catalogs(any.unpack().unwrap().unwrap(), request)
                .await;
        }
        if any.is::<CommandGetDbSchemas>() {
            return self
                .get_flight_info_schemas(any.unpack().unwrap().unwrap(), request)
                .await;
        }
        if any.is::<CommandGetTables>() {
            return self
                .get_flight_info_tables(any.unpack().unwrap().unwrap(), request)
                .await;
        }
        if any.is::<CommandGetTableTypes>() {
            return self
                .get_flight_info_table_types(any.unpack().unwrap().unwrap(), request)
                .await;
        }
        if any.is::<CommandGetSqlInfo>() {
            return self
                .get_flight_info_sql_info(any.unpack().unwrap().unwrap(), request)
                .await;
        }
        if any.is::<CommandGetPrimaryKeys>() {
            return self
                .get_flight_info_primary_keys(any.unpack().unwrap().unwrap(), request)
                .await;
        }
        if any.is::<CommandGetExportedKeys>() {
            return self
                .get_flight_info_exported_keys(any.unpack().unwrap().unwrap(), request)
                .await;
        }
        if any.is::<CommandGetImportedKeys>() {
            return self
                .get_flight_info_imported_keys(any.unpack().unwrap().unwrap(), request)
                .await;
        }
        if any.is::<CommandGetCrossReference>() {
            return self
                .get_flight_info_cross_reference(any.unpack().unwrap().unwrap(), request)
                .await;
        }

        Err(Status::unimplemented(format!(
            "get_flight_info: The defined request is invalid: {:?}",
            String::from_utf8(any.write_to_bytes().unwrap()).unwrap()
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
        let any: protobuf::well_known_types::Any =
            protobuf::Message::parse_from_bytes(&request.ticket)
                .map_err(|_| Status::invalid_argument("Unable to parse ticket."))?;

        if any.is::<TicketStatementQuery>() {
            return self.do_get_statement(any.unpack().unwrap().unwrap()).await;
        }
        if any.is::<CommandPreparedStatementQuery>() {
            return self
                .do_get_prepared_statement(any.unpack().unwrap().unwrap())
                .await;
        }
        if any.is::<CommandGetCatalogs>() {
            return self.do_get_catalogs(any.unpack().unwrap().unwrap()).await;
        }
        if any.is::<CommandGetDbSchemas>() {
            return self.do_get_schemas(any.unpack().unwrap().unwrap()).await;
        }
        if any.is::<CommandGetTables>() {
            return self.do_get_tables(any.unpack().unwrap().unwrap()).await;
        }
        if any.is::<CommandGetTableTypes>() {
            return self
                .do_get_table_types(any.unpack().unwrap().unwrap())
                .await;
        }
        if any.is::<CommandGetSqlInfo>() {
            return self.do_get_sql_info(any.unpack().unwrap().unwrap()).await;
        }
        if any.is::<CommandGetPrimaryKeys>() {
            return self
                .do_get_primary_keys(any.unpack().unwrap().unwrap())
                .await;
        }
        if any.is::<CommandGetExportedKeys>() {
            return self
                .do_get_exported_keys(any.unpack().unwrap().unwrap())
                .await;
        }
        if any.is::<CommandGetImportedKeys>() {
            return self
                .do_get_imported_keys(any.unpack().unwrap().unwrap())
                .await;
        }
        if any.is::<CommandGetCrossReference>() {
            return self
                .do_get_cross_reference(any.unpack().unwrap().unwrap())
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
        let any: protobuf::well_known_types::Any =
            protobuf::Message::parse_from_bytes(&request.flight_descriptor.unwrap().cmd)
                .map_err(|_| Status::invalid_argument("Unable to parse command."))?;

        if any.is::<CommandStatementUpdate>() {
            return self
                .do_put_statement_update(any.unpack().unwrap().unwrap())
                .await;
        }
        if any.is::<CommandPreparedStatementQuery>() {
            return self
                .do_put_prepared_statement_query(any.unpack().unwrap().unwrap())
                .await;
        }
        if any.is::<CommandPreparedStatementUpdate>() {
            return self
                .do_put_prepared_statement_update(any.unpack().unwrap().unwrap())
                .await;
        }

        Err(Status::unimplemented(format!(
            "do_put: The defined request is invalid: {:?}",
            String::from_utf8(any.write_to_bytes().unwrap()).unwrap()
        )))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let _actions = [
            CREATE_PREPARED_STATEMENT_ACTION_TYPE.clone(),
            CLOSE_PREPARED_STATEMENT_ACTION_TYPE.clone(),
        ];
        // TODO: return
        Err(Status::ok(""))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let request = _request.into_inner();

        if request.r#type == CREATE_PREPARED_STATEMENT_ACTION_TYPE.r#type {
            let any: protobuf::well_known_types::Any =
                protobuf::Message::parse_from_bytes(&request.body)
                    .map_err(|_| Status::invalid_argument("Unable to parse action."))?;

            let cmd: ActionCreatePreparedStatementRequest = any
                .unpack()
                .map_err(|err| Status::invalid_argument(err.to_string()))?
                .ok_or(Status::invalid_argument(
                    "Unable to unpack ActionCreatePreparedStatementRequest.",
                ))?;
            return self.do_action_create_prepared_statement(cmd).await;
        }
        if request.r#type == CLOSE_PREPARED_STATEMENT_ACTION_TYPE.r#type {
            let any: protobuf::well_known_types::Any =
                protobuf::Message::parse_from_bytes(&request.body)
                    .map_err(|_| Status::invalid_argument("Unable to parse action."))?;

            let cmd: ActionClosePreparedStatementRequest = any
                .unpack()
                .map_err(|err| Status::invalid_argument(err.to_string()))?
                .ok_or(Status::invalid_argument(
                    "Unable to unpack CloseCreatePreparedStatementRequest.",
                ))?;
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
