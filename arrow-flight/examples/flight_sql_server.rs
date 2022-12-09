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

use arrow_array::builder::StringBuilder;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_flight::sql::{ActionCreatePreparedStatementResult, ProstMessageExt, SqlInfo};
use arrow_flight::{
    Action, FlightData, FlightEndpoint, HandshakeRequest, HandshakeResponse, IpcMessage,
    Location, SchemaAsIpc, Ticket,
};
use futures::{stream, Stream};
use prost_types::Any;
use std::fs;
use std::pin::Pin;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::net::{UnixListener, UnixStream};
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::{Endpoint, Server};
use tonic::{Request, Response, Status, Streaming};

use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::sql::client::FlightSqlServiceClient;
use arrow_flight::utils::batches_to_flight_data;
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
use arrow_ipc::writer::IpcWriteOptions;
use arrow_schema::{ArrowError, DataType, Field, Schema};

macro_rules! status {
    ($desc:expr, $err:expr) => {
        Status::internal(format!("{}: {} at {}:{}", $desc, $err, file!(), line!()))
    };
}

#[derive(Clone)]
pub struct FlightSqlServiceImpl {}

impl FlightSqlServiceImpl {
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
            .ok_or(Status::invalid_argument("authorization field not present"))?
            .to_str()
            .map_err(|e| status!("authorization not parsable", e))?;
        if !authorization.starts_with(basic) {
            Err(Status::invalid_argument(format!(
                "Auth type not implemented: {}",
                authorization
            )))?;
        }
        let base64 = &authorization[basic.len()..];
        let bytes = base64::decode(base64)
            .map_err(|e| status!("authorization not decodable", e))?;
        let str = String::from_utf8(bytes)
            .map_err(|e| status!("authorization not parsable", e))?;
        let parts: Vec<_> = str.split(":").collect();
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
            payload: "random_uuid_token".as_bytes().to_vec(),
        };
        let result = Ok(result);
        let output = futures::stream::iter(vec![result]);
        return Ok(Response::new(Box::pin(output)));
    }

    async fn do_get_fallback(
        &self,
        _request: Request<Ticket>,
        _message: prost_types::Any,
    ) -> Result<Response<<Self as FlightService>::DoGetStream>, Status> {
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

    async fn get_flight_info_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let handle = String::from_utf8(cmd.prepared_statement_handle)
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
        let buf = ::prost::Message::encode_to_vec(&fetch.as_any());
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
            cmd: vec![],
            path: vec![],
        };
        let info = FlightInfo {
            schema: schema_bytes,
            flight_descriptor: Some(flight_desc),
            endpoint: endpoints,
            total_records: num_rows as i64,
            total_bytes: num_bytes as i64,
        };
        let resp = Response::new(info);
        Ok(resp)
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

    // do_put
    async fn do_put_statement_update(
        &self,
        _ticket: CommandStatementUpdate,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<i64, Status> {
        Err(Status::unimplemented(
            "do_put_statement_update not implemented",
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
        _request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let handle = "some_uuid";
        let schema = Self::fake_result()
            .map_err(|e| status!("Error getting result schema", e))?
            .schema();
        let message = SchemaAsIpc::new(&schema, &IpcWriteOptions::default())
            .try_into()
            .map_err(|e| status!("Unable to serialize schema", e))?;
        let IpcMessage(schema_bytes) = message;
        let res = ActionCreatePreparedStatementResult {
            prepared_statement_handle: handle.as_bytes().to_vec(),
            dataset_schema: schema_bytes,
            parameter_schema: vec![], // TODO: parameters
        };
        Ok(res)
    }

    async fn do_action_close_prepared_statement(
        &self,
        _query: ActionClosePreparedStatementRequest,
        _request: Request<Action>,
    ) {
        unimplemented!("Implement do_action_close_prepared_statement")
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
        prost_types::Any {
            type_url: FetchResults::type_url().to_string(),
            value: ::prost::Message::encode_to_vec(self),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::TryStreamExt;

    use arrow::util::pretty::pretty_format_batches;
    use arrow_flight::utils::flight_data_to_batches;
    use tower::service_fn;

    async fn client_with_uds(path: String) -> FlightSqlServiceClient {
        let connector = service_fn(move |_| UnixStream::connect(path.clone()));
        let channel = Endpoint::try_from("https://example.com")
            .unwrap()
            .connect_with_connector(connector)
            .await
            .unwrap();
        FlightSqlServiceClient::new(channel)
    }

    #[tokio::test]
    async fn test_select_1() {
        let file = NamedTempFile::new().unwrap();
        let path = file.into_temp_path().to_str().unwrap().to_string();
        let _ = fs::remove_file(path.clone());

        let uds = UnixListener::bind(path.clone()).unwrap();
        let stream = UnixListenerStream::new(uds);

        // We would just listen on TCP, but it seems impossible to know when tonic is ready to serve
        let service = FlightSqlServiceImpl {};
        let serve_future = Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming(stream);

        let request_future = async {
            let mut client = client_with_uds(path).await;
            let token = client.handshake("admin", "password").await.unwrap();
            println!("Auth succeeded with token: {:?}", token);
            let mut stmt = client.prepare("select 1;".to_string()).await.unwrap();
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
        };

        tokio::select! {
            _ = serve_future => panic!("server returned first"),
            _ = request_future => println!("Client finished!"),
        }
    }
}
