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

use arrow_flight::sql::SqlInfo;
use tonic::transport::Server;
use tonic::{Response, Status};

use arrow_flight::{
    flight_service_server::FlightService,
    flight_service_server::FlightServiceServer,
    sql::{
        server::FlightSqlService, ActionClosePreparedStatementRequest,
        ActionCreatePreparedStatementRequest, CommandGetCatalogs,
        CommandGetCrossReference, CommandGetDbSchemas, CommandGetExportedKeys,
        CommandGetImportedKeys, CommandGetPrimaryKeys, CommandGetSqlInfo,
        CommandGetTableTypes, CommandGetTables, CommandPreparedStatementQuery,
        CommandPreparedStatementUpdate, CommandStatementQuery, CommandStatementUpdate,
        TicketStatementQuery,
    },
    FlightDescriptor, FlightInfo,
};

#[derive(Clone)]
pub struct FlightSqlServiceImpl {}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = FlightSqlServiceImpl;
    // get_flight_info
    async fn get_flight_info_statement(
        &self,
        _query: CommandStatementQuery,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn get_flight_info_prepared_statement(
        &self,
        _query: CommandPreparedStatementQuery,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn get_flight_info_catalogs(
        &self,
        _query: CommandGetCatalogs,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn get_flight_info_schemas(
        &self,
        _query: CommandGetDbSchemas,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn get_flight_info_tables(
        &self,
        _query: CommandGetTables,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn get_flight_info_table_types(
        &self,
        _query: CommandGetTableTypes,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn get_flight_info_sql_info(
        &self,
        _query: CommandGetSqlInfo,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn get_flight_info_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn get_flight_info_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn get_flight_info_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn get_flight_info_cross_reference(
        &self,
        _query: CommandGetCrossReference,
        _request: FlightDescriptor,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    // do_get
    async fn do_get_statement(
        &self,
        _ticket: TicketStatementQuery,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn do_get_prepared_statement(
        &self,
        _query: CommandPreparedStatementQuery,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_get_schemas(
        &self,
        _query: CommandGetDbSchemas,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_get_tables(
        &self,
        _query: CommandGetTables,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_get_sql_info(
        &self,
        _query: CommandGetSqlInfo,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_get_primary_keys(
        &self,
        _query: CommandGetPrimaryKeys,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_get_exported_keys(
        &self,
        _query: CommandGetExportedKeys,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_get_imported_keys(
        &self,
        _query: CommandGetImportedKeys,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_get_cross_reference(
        &self,
        _query: CommandGetCrossReference,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    // do_put
    async fn do_put_statement_update(
        &self,
        _ticket: CommandStatementUpdate,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_put_prepared_statement_query(
        &self,
        _query: CommandPreparedStatementQuery,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_put_prepared_statement_update(
        &self,
        _query: CommandPreparedStatementUpdate,
    ) -> Result<Response<<Self as FlightService>::DoPutStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    // do_action
    async fn do_action_create_prepared_statement(
        &self,
        _query: ActionCreatePreparedStatementRequest,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
    async fn do_action_close_prepared_statement(
        &self,
        _query: ActionClosePreparedStatementRequest,
    ) -> Result<Response<<Self as FlightService>::DoActionStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// This example shows how to run a FlightSql server
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "0.0.0.0:50051".parse()?;

    let svc = FlightServiceServer::new(FlightSqlServiceImpl {});

    println!("Listening on {:?}", addr);

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}
