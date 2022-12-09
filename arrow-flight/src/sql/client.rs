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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::flight_service_client::FlightServiceClient;
use crate::sql::server::{CLOSE_PREPARED_STATEMENT, CREATE_PREPARED_STATEMENT};
use crate::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, CommandGetCatalogs, CommandGetCrossReference,
    CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys,
    CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables,
    CommandPreparedStatementQuery, CommandStatementQuery, CommandStatementUpdate,
    DoPutUpdateResult, ProstAnyExt, ProstMessageExt, SqlInfo,
};
use crate::{
    Action, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest,
    HandshakeResponse, IpcMessage, Ticket,
};
use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::convert::fb_to_schema;
use arrow_ipc::reader::read_record_batch;
use arrow_ipc::{root_as_message, MessageHeader};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use futures::{stream, TryStreamExt};
use prost::Message;
use tokio::sync::{Mutex, MutexGuard};
use tonic::transport::{Channel, Endpoint};
use tonic::Streaming;

/// A FlightSQLServiceClient is an endpoint for retrieving or storing Arrow data
/// by FlightSQL protocol.
#[derive(Debug, Clone)]
pub struct FlightSqlServiceClient {
    token: Option<String>,
    flight_client: Arc<Mutex<FlightServiceClient<Channel>>>,
}

/// A FlightSql protocol client that can run queries against FlightSql servers
/// This client is in the "experimental" stage. It is not guaranteed to follow the spec in all instances.
/// Github issues are welcomed.
impl FlightSqlServiceClient {
    /// Creates a new FlightSql Client that connects via TCP to a server
    pub async fn new_with_endpoint(host: &str, port: u16) -> Result<Self, ArrowError> {
        let addr = format!("http://{}:{}", host, port);
        let endpoint = Endpoint::new(addr)
            .map_err(|_| ArrowError::IoError("Cannot create endpoint".to_string()))?
            .connect_timeout(Duration::from_secs(20))
            .timeout(Duration::from_secs(20))
            .tcp_nodelay(true) // Disable Nagle's Algorithm since we don't want packets to wait
            .tcp_keepalive(Option::Some(Duration::from_secs(3600)))
            .http2_keep_alive_interval(Duration::from_secs(300))
            .keep_alive_timeout(Duration::from_secs(20))
            .keep_alive_while_idle(true);
        let channel = endpoint.connect().await.map_err(|e| {
            ArrowError::IoError(format!("Cannot connect to endpoint: {}", e))
        })?;
        Ok(Self::new(channel))
    }

    /// Creates a new FlightSql client that connects to a server over an arbitrary tonic `Channel`
    pub fn new(channel: Channel) -> Self {
        let flight_client = FlightServiceClient::new(channel);
        FlightSqlServiceClient {
            token: None,
            flight_client: Arc::new(Mutex::new(flight_client)),
        }
    }

    fn mut_client(
        &mut self,
    ) -> Result<MutexGuard<FlightServiceClient<Channel>>, ArrowError> {
        self.flight_client
            .try_lock()
            .map_err(|_| ArrowError::IoError("Unable to lock client".to_string()))
    }

    async fn get_flight_info_for_command<M: ProstMessageExt>(
        &mut self,
        cmd: M,
    ) -> Result<FlightInfo, ArrowError> {
        let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
        let fi = self
            .mut_client()?
            .get_flight_info(descriptor)
            .await
            .map_err(status_to_arrow_error)?
            .into_inner();
        Ok(fi)
    }

    /// Execute a query on the server.
    pub async fn execute(&mut self, query: String) -> Result<FlightInfo, ArrowError> {
        let cmd = CommandStatementQuery { query };
        self.get_flight_info_for_command(cmd).await
    }

    /// Perform a `handshake` with the server, passing credentials and establishing a session
    /// Returns arbitrary auth/handshake info binary blob
    pub async fn handshake(
        &mut self,
        username: &str,
        password: &str,
    ) -> Result<Vec<u8>, ArrowError> {
        let cmd = HandshakeRequest {
            protocol_version: 0,
            payload: vec![],
        };
        let mut req = tonic::Request::new(stream::iter(vec![cmd]));
        let val = base64::encode(format!("{}:{}", username, password));
        let val = format!("Basic {}", val)
            .parse()
            .map_err(|_| ArrowError::ParseError("Cannot parse header".to_string()))?;
        req.metadata_mut().insert("authorization", val);
        let resp = self
            .mut_client()?
            .handshake(req)
            .await
            .map_err(|e| ArrowError::IoError(format!("Can't handshake {}", e)))?;
        if let Some(auth) = resp.metadata().get("authorization") {
            let auth = auth.to_str().map_err(|_| {
                ArrowError::ParseError("Can't read auth header".to_string())
            })?;
            let bearer = "Bearer ";
            if !auth.starts_with(bearer) {
                Err(ArrowError::ParseError("Invalid auth header!".to_string()))?;
            }
            let auth = auth[bearer.len()..].to_string();
            self.token = Some(auth);
        }
        let responses: Vec<HandshakeResponse> =
            resp.into_inner().try_collect().await.map_err(|_| {
                ArrowError::ParseError("Can't collect responses".to_string())
            })?;
        let resp = match responses.as_slice() {
            [resp] => resp,
            [] => Err(ArrowError::ParseError("No handshake response".to_string()))?,
            _ => Err(ArrowError::ParseError(
                "Multiple handshake responses".to_string(),
            ))?,
        };
        Ok(resp.payload.clone())
    }

    /// Execute a update query on the server, and return the number of records affected
    pub async fn execute_update(&mut self, query: String) -> Result<i64, ArrowError> {
        let cmd = CommandStatementUpdate { query };
        let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
        let mut result = self
            .mut_client()?
            .do_put(stream::iter(vec![FlightData {
                flight_descriptor: Some(descriptor),
                ..Default::default()
            }]))
            .await
            .map_err(status_to_arrow_error)?
            .into_inner();
        let result = result
            .message()
            .await
            .map_err(status_to_arrow_error)?
            .unwrap();
        let any: prost_types::Any = prost::Message::decode(&*result.app_metadata)
            .map_err(decode_error_to_arrow_error)?;
        let result: DoPutUpdateResult = any.unpack()?.unwrap();
        Ok(result.record_count)
    }

    /// Request a list of catalogs as tabular FlightInfo results
    pub async fn get_catalogs(&mut self) -> Result<FlightInfo, ArrowError> {
        self.get_flight_info_for_command(CommandGetCatalogs {})
            .await
    }

    /// Request a list of database schemas as tabular FlightInfo results
    pub async fn get_db_schemas(
        &mut self,
        request: CommandGetDbSchemas,
    ) -> Result<FlightInfo, ArrowError> {
        self.get_flight_info_for_command(request).await
    }

    /// Given a flight ticket, request to be sent the stream. Returns record batch stream reader
    pub async fn do_get(
        &mut self,
        ticket: Ticket,
    ) -> Result<Streaming<FlightData>, ArrowError> {
        Ok(self
            .mut_client()?
            .do_get(ticket)
            .await
            .map_err(status_to_arrow_error)?
            .into_inner())
    }

    /// Request a list of tables.
    pub async fn get_tables(
        &mut self,
        request: CommandGetTables,
    ) -> Result<FlightInfo, ArrowError> {
        self.get_flight_info_for_command(request).await
    }

    /// Request the primary keys for a table.
    pub async fn get_primary_keys(
        &mut self,
        request: CommandGetPrimaryKeys,
    ) -> Result<FlightInfo, ArrowError> {
        self.get_flight_info_for_command(request).await
    }

    /// Retrieves a description about the foreign key columns that reference the
    /// primary key columns of the given table.
    pub async fn get_exported_keys(
        &mut self,
        request: CommandGetExportedKeys,
    ) -> Result<FlightInfo, ArrowError> {
        self.get_flight_info_for_command(request).await
    }

    /// Retrieves the foreign key columns for the given table.
    pub async fn get_imported_keys(
        &mut self,
        request: CommandGetImportedKeys,
    ) -> Result<FlightInfo, ArrowError> {
        self.get_flight_info_for_command(request).await
    }

    /// Retrieves a description of the foreign key columns in the given foreign key
    /// table that reference the primary key or the columns representing a unique
    /// constraint of the parent table (could be the same or a different table).
    pub async fn get_cross_reference(
        &mut self,
        request: CommandGetCrossReference,
    ) -> Result<FlightInfo, ArrowError> {
        self.get_flight_info_for_command(request).await
    }

    /// Request a list of table types.
    pub async fn get_table_types(&mut self) -> Result<FlightInfo, ArrowError> {
        self.get_flight_info_for_command(CommandGetTableTypes {})
            .await
    }

    /// Request a list of SQL information.
    pub async fn get_sql_info(
        &mut self,
        sql_infos: Vec<SqlInfo>,
    ) -> Result<FlightInfo, ArrowError> {
        let request = CommandGetSqlInfo {
            info: sql_infos.iter().map(|sql_info| *sql_info as u32).collect(),
        };
        self.get_flight_info_for_command(request).await
    }

    /// Create a prepared statement object.
    pub async fn prepare(
        &mut self,
        query: String,
    ) -> Result<PreparedStatement<Channel>, ArrowError> {
        let cmd = ActionCreatePreparedStatementRequest { query };
        let action = Action {
            r#type: CREATE_PREPARED_STATEMENT.to_string(),
            body: cmd.as_any().encode_to_vec(),
        };
        let mut req = tonic::Request::new(action);
        if let Some(token) = &self.token {
            let val = format!("Bearer {}", token).parse().map_err(|_| {
                ArrowError::IoError("Statement already closed.".to_string())
            })?;
            req.metadata_mut().insert("authorization", val);
        }
        let mut result = self
            .mut_client()?
            .do_action(req)
            .await
            .map_err(status_to_arrow_error)?
            .into_inner();
        let result = result
            .message()
            .await
            .map_err(status_to_arrow_error)?
            .unwrap();
        let any: prost_types::Any =
            prost::Message::decode(&*result.body).map_err(decode_error_to_arrow_error)?;
        let prepared_result: ActionCreatePreparedStatementResult = any.unpack()?.unwrap();
        let dataset_schema = match prepared_result.dataset_schema.len() {
            0 => Schema::empty(),
            _ => Schema::try_from(IpcMessage(prepared_result.dataset_schema))?,
        };
        let parameter_schema = match prepared_result.parameter_schema.len() {
            0 => Schema::empty(),
            _ => Schema::try_from(IpcMessage(prepared_result.parameter_schema))?,
        };
        Ok(PreparedStatement::new(
            self.flight_client.clone(),
            prepared_result.prepared_statement_handle,
            dataset_schema,
            parameter_schema,
        ))
    }

    /// Explicitly shut down and clean up the client.
    pub async fn close(&mut self) -> Result<(), ArrowError> {
        Ok(())
    }
}

/// A PreparedStatement
#[derive(Debug, Clone)]
pub struct PreparedStatement<T> {
    flight_client: Arc<Mutex<FlightServiceClient<T>>>,
    parameter_binding: Option<RecordBatch>,
    handle: Vec<u8>,
    dataset_schema: Schema,
    parameter_schema: Schema,
}

impl PreparedStatement<Channel> {
    pub(crate) fn new(
        client: Arc<Mutex<FlightServiceClient<Channel>>>,
        handle: Vec<u8>,
        dataset_schema: Schema,
        parameter_schema: Schema,
    ) -> Self {
        PreparedStatement {
            flight_client: client,
            parameter_binding: None,
            handle,
            dataset_schema,
            parameter_schema,
        }
    }

    /// Executes the prepared statement query on the server.
    pub async fn execute(&mut self) -> Result<FlightInfo, ArrowError> {
        let cmd = CommandPreparedStatementQuery {
            prepared_statement_handle: self.handle.clone(),
        };
        let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
        let result = self
            .mut_client()?
            .get_flight_info(descriptor)
            .await
            .map_err(status_to_arrow_error)?
            .into_inner();
        Ok(result)
    }

    /// Executes the prepared statement update query on the server.
    pub async fn execute_update(&mut self) -> Result<i64, ArrowError> {
        let cmd = CommandPreparedStatementQuery {
            prepared_statement_handle: self.handle.clone(),
        };
        let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
        let mut result = self
            .mut_client()?
            .do_put(stream::iter(vec![FlightData {
                flight_descriptor: Some(descriptor),
                ..Default::default()
            }]))
            .await
            .map_err(status_to_arrow_error)?
            .into_inner();
        let result = result
            .message()
            .await
            .map_err(status_to_arrow_error)?
            .unwrap();
        let any: prost_types::Any = Message::decode(&*result.app_metadata)
            .map_err(decode_error_to_arrow_error)?;
        let result: DoPutUpdateResult = any.unpack()?.unwrap();
        Ok(result.record_count)
    }

    /// Retrieve the parameter schema from the query.
    pub fn parameter_schema(&self) -> Result<&Schema, ArrowError> {
        Ok(&self.parameter_schema)
    }

    /// Retrieve the ResultSet schema from the query.
    pub fn dataset_schema(&self) -> Result<&Schema, ArrowError> {
        Ok(&self.dataset_schema)
    }

    /// Set a RecordBatch that contains the parameters that will be bind.
    pub fn set_parameters(
        &mut self,
        parameter_binding: RecordBatch,
    ) -> Result<(), ArrowError> {
        self.parameter_binding = Some(parameter_binding);
        Ok(())
    }

    /// Close the prepared statement, so that this PreparedStatement can not used
    /// anymore and server can free up any resources.
    pub async fn close(mut self) -> Result<(), ArrowError> {
        let cmd = ActionClosePreparedStatementRequest {
            prepared_statement_handle: self.handle.clone(),
        };
        let action = Action {
            r#type: CLOSE_PREPARED_STATEMENT.to_string(),
            body: cmd.as_any().encode_to_vec(),
        };
        let _ = self
            .mut_client()?
            .do_action(action)
            .await
            .map_err(status_to_arrow_error)?;
        Ok(())
    }

    fn mut_client(
        &mut self,
    ) -> Result<MutexGuard<FlightServiceClient<Channel>>, ArrowError> {
        self.flight_client
            .try_lock()
            .map_err(|_| ArrowError::IoError("Unable to lock client".to_string()))
    }
}

fn decode_error_to_arrow_error(err: prost::DecodeError) -> ArrowError {
    ArrowError::IoError(err.to_string())
}

fn status_to_arrow_error(status: tonic::Status) -> ArrowError {
    ArrowError::IoError(format!("{:?}", status))
}

// A polymorphic structure to natively represent different types of data contained in `FlightData`
pub enum ArrowFlightData {
    RecordBatch(RecordBatch),
    Schema(Schema),
}

/// Extract `Schema` or `RecordBatch`es from the `FlightData` wire representation
pub fn arrow_data_from_flight_data(
    flight_data: FlightData,
    arrow_schema_ref: &SchemaRef,
) -> Result<ArrowFlightData, ArrowError> {
    let ipc_message = root_as_message(&flight_data.data_header[..]).map_err(|err| {
        ArrowError::ParseError(format!("Unable to get root as message: {:?}", err))
    })?;

    match ipc_message.header_type() {
        MessageHeader::RecordBatch => {
            let ipc_record_batch =
                ipc_message.header_as_record_batch().ok_or_else(|| {
                    ArrowError::ComputeError(
                        "Unable to convert flight data header to a record batch"
                            .to_string(),
                    )
                })?;

            let dictionaries_by_field = HashMap::new();
            let record_batch = read_record_batch(
                &Buffer::from(&flight_data.data_body),
                ipc_record_batch,
                arrow_schema_ref.clone(),
                &dictionaries_by_field,
                None,
                &ipc_message.version(),
            )?;
            Ok(ArrowFlightData::RecordBatch(record_batch))
        }
        MessageHeader::Schema => {
            let ipc_schema = ipc_message.header_as_schema().ok_or_else(|| {
                ArrowError::ComputeError(
                    "Unable to convert flight data header to a schema".to_string(),
                )
            })?;

            let arrow_schema = fb_to_schema(ipc_schema);
            Ok(ArrowFlightData::Schema(arrow_schema))
        }
        MessageHeader::DictionaryBatch => {
            let _ = ipc_message.header_as_dictionary_batch().ok_or_else(|| {
                ArrowError::ComputeError(
                    "Unable to convert flight data header to a dictionary batch"
                        .to_string(),
                )
            })?;
            Err(ArrowError::NotYetImplemented(
                "no idea on how to convert an ipc dictionary batch to an arrow type"
                    .to_string(),
            ))
        }
        MessageHeader::Tensor => {
            let _ = ipc_message.header_as_tensor().ok_or_else(|| {
                ArrowError::ComputeError(
                    "Unable to convert flight data header to a tensor".to_string(),
                )
            })?;
            Err(ArrowError::NotYetImplemented(
                "no idea on how to convert an ipc tensor to an arrow type".to_string(),
            ))
        }
        MessageHeader::SparseTensor => {
            let _ = ipc_message.header_as_sparse_tensor().ok_or_else(|| {
                ArrowError::ComputeError(
                    "Unable to convert flight data header to a sparse tensor".to_string(),
                )
            })?;
            Err(ArrowError::NotYetImplemented(
                "no idea on how to convert an ipc sparse tensor to an arrow type"
                    .to_string(),
            ))
        }
        _ => Err(ArrowError::ComputeError(format!(
            "Unable to convert message with header_type: '{:?}' to arrow data",
            ipc_message.header_type()
        ))),
    }
}
