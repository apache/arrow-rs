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

//! A FlightSQL Client [`FlightSqlServiceClient`]

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use bytes::Bytes;
use std::collections::HashMap;
use std::str::FromStr;
use tonic::metadata::AsciiMetadataKey;

use crate::decode::FlightRecordBatchStream;
use crate::encode::FlightDataEncoderBuilder;
use crate::error::FlightError;
use crate::flight_service_client::FlightServiceClient;
use crate::sql::server::{CLOSE_PREPARED_STATEMENT, CREATE_PREPARED_STATEMENT};
use crate::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, Any, CommandGetCatalogs, CommandGetCrossReference,
    CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys, CommandGetPrimaryKeys,
    CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandGetXdbcTypeInfo,
    CommandPreparedStatementQuery, CommandPreparedStatementUpdate, CommandStatementQuery,
    CommandStatementUpdate, DoPutPreparedStatementResult, DoPutUpdateResult, ProstMessageExt,
    SqlInfo,
};
use crate::trailers::extract_lazy_trailers;
use crate::{
    Action, FlightData, FlightDescriptor, FlightInfo, HandshakeRequest, HandshakeResponse,
    IpcMessage, PutResult, Ticket,
};
use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::convert::fb_to_schema;
use arrow_ipc::reader::read_record_batch;
use arrow_ipc::{root_as_message, MessageHeader};
use arrow_schema::{ArrowError, Schema, SchemaRef};
use futures::{stream, TryStreamExt};
use prost::Message;
use tonic::transport::Channel;
use tonic::{IntoRequest, Streaming};

/// A FlightSQLServiceClient is an endpoint for retrieving or storing Arrow data
/// by FlightSQL protocol.
#[derive(Debug, Clone)]
pub struct FlightSqlServiceClient<T> {
    token: Option<String>,
    headers: HashMap<String, String>,
    flight_client: FlightServiceClient<T>,
}

/// A FlightSql protocol client that can run queries against FlightSql servers
/// This client is in the "experimental" stage. It is not guaranteed to follow the spec in all instances.
/// Github issues are welcomed.
impl FlightSqlServiceClient<Channel> {
    /// Creates a new FlightSql client that connects to a server over an arbitrary tonic `Channel`
    pub fn new(channel: Channel) -> Self {
        Self::new_from_inner(FlightServiceClient::new(channel))
    }

    /// Creates a new higher level client with the provided lower level client
    pub fn new_from_inner(inner: FlightServiceClient<Channel>) -> Self {
        Self {
            token: None,
            flight_client: inner,
            headers: HashMap::default(),
        }
    }

    /// Return a reference to the underlying [`FlightServiceClient`]
    pub fn inner(&self) -> &FlightServiceClient<Channel> {
        &self.flight_client
    }

    /// Return a mutable reference to the underlying [`FlightServiceClient`]
    pub fn inner_mut(&mut self) -> &mut FlightServiceClient<Channel> {
        &mut self.flight_client
    }

    /// Consume this client and return the underlying [`FlightServiceClient`]
    pub fn into_inner(self) -> FlightServiceClient<Channel> {
        self.flight_client
    }

    /// Set auth token to the given value.
    pub fn set_token(&mut self, token: String) {
        self.token = Some(token);
    }

    /// Clear the auth token.
    pub fn clear_token(&mut self) {
        self.token = None;
    }

    /// Set header value.
    pub fn set_header(&mut self, key: impl Into<String>, value: impl Into<String>) {
        let key: String = key.into();
        let value: String = value.into();
        self.headers.insert(key, value);
    }

    async fn get_flight_info_for_command<M: ProstMessageExt>(
        &mut self,
        cmd: M,
    ) -> Result<FlightInfo, ArrowError> {
        let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
        let req = self.set_request_headers(descriptor.into_request())?;
        let fi = self
            .flight_client
            .get_flight_info(req)
            .await
            .map_err(status_to_arrow_error)?
            .into_inner();
        Ok(fi)
    }

    /// Execute a query on the server.
    pub async fn execute(
        &mut self,
        query: String,
        transaction_id: Option<Bytes>,
    ) -> Result<FlightInfo, ArrowError> {
        let cmd = CommandStatementQuery {
            query,
            transaction_id,
        };
        self.get_flight_info_for_command(cmd).await
    }

    /// Perform a `handshake` with the server, passing credentials and establishing a session.
    ///
    /// If the server returns an "authorization" header, it is automatically parsed and set as
    /// a token for future requests. Any other data returned by the server in the handshake
    /// response is returned as a binary blob.
    pub async fn handshake(&mut self, username: &str, password: &str) -> Result<Bytes, ArrowError> {
        let cmd = HandshakeRequest {
            protocol_version: 0,
            payload: Default::default(),
        };
        let mut req = tonic::Request::new(stream::iter(vec![cmd]));
        let val = BASE64_STANDARD.encode(format!("{username}:{password}"));
        let val = format!("Basic {val}")
            .parse()
            .map_err(|_| ArrowError::ParseError("Cannot parse header".to_string()))?;
        req.metadata_mut().insert("authorization", val);
        let req = self.set_request_headers(req)?;
        let resp = self
            .flight_client
            .handshake(req)
            .await
            .map_err(|e| ArrowError::IpcError(format!("Can't handshake {e}")))?;
        if let Some(auth) = resp.metadata().get("authorization") {
            let auth = auth
                .to_str()
                .map_err(|_| ArrowError::ParseError("Can't read auth header".to_string()))?;
            let bearer = "Bearer ";
            if !auth.starts_with(bearer) {
                Err(ArrowError::ParseError("Invalid auth header!".to_string()))?;
            }
            let auth = auth[bearer.len()..].to_string();
            self.token = Some(auth);
        }
        let responses: Vec<HandshakeResponse> = resp
            .into_inner()
            .try_collect()
            .await
            .map_err(|_| ArrowError::ParseError("Can't collect responses".to_string()))?;
        let resp = match responses.as_slice() {
            [resp] => resp.payload.clone(),
            [] => Bytes::new(),
            _ => Err(ArrowError::ParseError(
                "Multiple handshake responses".to_string(),
            ))?,
        };
        Ok(resp)
    }

    /// Execute a update query on the server, and return the number of records affected
    pub async fn execute_update(
        &mut self,
        query: String,
        transaction_id: Option<Bytes>,
    ) -> Result<i64, ArrowError> {
        let cmd = CommandStatementUpdate {
            query,
            transaction_id,
        };
        let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
        let req = self.set_request_headers(
            stream::iter(vec![FlightData {
                flight_descriptor: Some(descriptor),
                ..Default::default()
            }])
            .into_request(),
        )?;
        let mut result = self
            .flight_client
            .do_put(req)
            .await
            .map_err(status_to_arrow_error)?
            .into_inner();
        let result = result
            .message()
            .await
            .map_err(status_to_arrow_error)?
            .unwrap();
        let any = Any::decode(&*result.app_metadata).map_err(decode_error_to_arrow_error)?;
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
        ticket: impl IntoRequest<Ticket>,
    ) -> Result<FlightRecordBatchStream, ArrowError> {
        let req = self.set_request_headers(ticket.into_request())?;

        let (md, response_stream, _ext) = self
            .flight_client
            .do_get(req)
            .await
            .map_err(status_to_arrow_error)?
            .into_parts();
        let (response_stream, trailers) = extract_lazy_trailers(response_stream);

        Ok(FlightRecordBatchStream::new_from_flight_data(
            response_stream.map_err(FlightError::Tonic),
        )
        .with_headers(md)
        .with_trailers(trailers))
    }

    /// Push a stream to the flight service associated with a particular flight stream.
    pub async fn do_put(
        &mut self,
        request: impl tonic::IntoStreamingRequest<Message = FlightData>,
    ) -> Result<Streaming<PutResult>, ArrowError> {
        let req = self.set_request_headers(request.into_streaming_request())?;
        Ok(self
            .flight_client
            .do_put(req)
            .await
            .map_err(status_to_arrow_error)?
            .into_inner())
    }

    /// DoAction allows a flight client to do a specific action against a flight service
    pub async fn do_action(
        &mut self,
        request: impl IntoRequest<Action>,
    ) -> Result<Streaming<crate::Result>, ArrowError> {
        let req = self.set_request_headers(request.into_request())?;
        Ok(self
            .flight_client
            .do_action(req)
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

    /// Request XDBC SQL information.
    pub async fn get_xdbc_type_info(
        &mut self,
        request: CommandGetXdbcTypeInfo,
    ) -> Result<FlightInfo, ArrowError> {
        self.get_flight_info_for_command(request).await
    }

    /// Create a prepared statement object.
    pub async fn prepare(
        &mut self,
        query: String,
        transaction_id: Option<Bytes>,
    ) -> Result<PreparedStatement<Channel>, ArrowError> {
        let cmd = ActionCreatePreparedStatementRequest {
            query,
            transaction_id,
        };
        let action = Action {
            r#type: CREATE_PREPARED_STATEMENT.to_string(),
            body: cmd.as_any().encode_to_vec().into(),
        };
        let req = self.set_request_headers(action.into_request())?;
        let mut result = self
            .flight_client
            .do_action(req)
            .await
            .map_err(status_to_arrow_error)?
            .into_inner();
        let result = result
            .message()
            .await
            .map_err(status_to_arrow_error)?
            .unwrap();
        let any = Any::decode(&*result.body).map_err(decode_error_to_arrow_error)?;
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
            self.clone(),
            prepared_result.prepared_statement_handle,
            dataset_schema,
            parameter_schema,
        ))
    }

    /// Explicitly shut down and clean up the client.
    pub async fn close(&mut self) -> Result<(), ArrowError> {
        // TODO: consume self instead of &mut self to explicitly prevent reuse?
        Ok(())
    }

    fn set_request_headers<T>(
        &self,
        mut req: tonic::Request<T>,
    ) -> Result<tonic::Request<T>, ArrowError> {
        for (k, v) in &self.headers {
            let k = AsciiMetadataKey::from_str(k.as_str()).map_err(|e| {
                ArrowError::ParseError(format!("Cannot convert header key \"{k}\": {e}"))
            })?;
            let v = v.parse().map_err(|e| {
                ArrowError::ParseError(format!("Cannot convert header value \"{v}\": {e}"))
            })?;
            req.metadata_mut().insert(k, v);
        }
        if let Some(token) = &self.token {
            let val = format!("Bearer {token}").parse().map_err(|e| {
                ArrowError::ParseError(format!("Cannot convert token to header value: {e}"))
            })?;
            req.metadata_mut().insert("authorization", val);
        }
        Ok(req)
    }
}

/// A PreparedStatement
#[derive(Debug, Clone)]
pub struct PreparedStatement<T> {
    flight_sql_client: FlightSqlServiceClient<T>,
    parameter_binding: Option<RecordBatch>,
    handle: Bytes,
    dataset_schema: Schema,
    parameter_schema: Schema,
}

impl PreparedStatement<Channel> {
    pub(crate) fn new(
        flight_client: FlightSqlServiceClient<Channel>,
        handle: impl Into<Bytes>,
        dataset_schema: Schema,
        parameter_schema: Schema,
    ) -> Self {
        PreparedStatement {
            flight_sql_client: flight_client,
            parameter_binding: None,
            handle: handle.into(),
            dataset_schema,
            parameter_schema,
        }
    }

    /// Executes the prepared statement query on the server.
    pub async fn execute(&mut self) -> Result<FlightInfo, ArrowError> {
        self.write_bind_params().await?;

        let cmd = CommandPreparedStatementQuery {
            prepared_statement_handle: self.handle.clone(),
        };

        let result = self
            .flight_sql_client
            .get_flight_info_for_command(cmd)
            .await?;
        Ok(result)
    }

    /// Executes the prepared statement update query on the server.
    pub async fn execute_update(&mut self) -> Result<i64, ArrowError> {
        self.write_bind_params().await?;

        let cmd = CommandPreparedStatementUpdate {
            prepared_statement_handle: self.handle.clone(),
        };
        let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
        let mut result = self
            .flight_sql_client
            .do_put(stream::iter(vec![FlightData {
                flight_descriptor: Some(descriptor),
                ..Default::default()
            }]))
            .await?;
        let result = result
            .message()
            .await
            .map_err(status_to_arrow_error)?
            .unwrap();
        let any = Any::decode(&*result.app_metadata).map_err(decode_error_to_arrow_error)?;
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
    pub fn set_parameters(&mut self, parameter_binding: RecordBatch) -> Result<(), ArrowError> {
        self.parameter_binding = Some(parameter_binding);
        Ok(())
    }

    /// Submit parameters to the server, if any have been set on this prepared statement instance
    /// Updates our stored prepared statement handle with the handle given by the server response.
    async fn write_bind_params(&mut self) -> Result<(), ArrowError> {
        if let Some(ref params_batch) = self.parameter_binding {
            let cmd = CommandPreparedStatementQuery {
                prepared_statement_handle: self.handle.clone(),
            };

            let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
            let flight_stream_builder = FlightDataEncoderBuilder::new()
                .with_flight_descriptor(Some(descriptor))
                .with_schema(params_batch.schema());
            let flight_data = flight_stream_builder
                .build(futures::stream::iter(
                    self.parameter_binding.clone().map(Ok),
                ))
                .try_collect::<Vec<_>>()
                .await
                .map_err(flight_error_to_arrow_error)?;

            // Attempt to update the stored handle with any updated handle in the DoPut result.
            // Older servers do not respond with a result for DoPut, so skip this step when
            // the stream closes with no response.
            if let Some(result) = self
                .flight_sql_client
                .do_put(stream::iter(flight_data))
                .await?
                .message()
                .await
                .map_err(status_to_arrow_error)?
            {
                if let Some(handle) = self.unpack_prepared_statement_handle(&result)? {
                    self.handle = handle;
                }
            }
        }
        Ok(())
    }

    /// Decodes the app_metadata stored in a [`PutResult`] as a
    /// [`DoPutPreparedStatementResult`] and then returns
    /// the inner prepared statement handle as [`Bytes`]
    fn unpack_prepared_statement_handle(
        &self,
        put_result: &PutResult,
    ) -> Result<Option<Bytes>, ArrowError> {
        let result: DoPutPreparedStatementResult =
            Message::decode(&*put_result.app_metadata).map_err(decode_error_to_arrow_error)?;
        Ok(result.prepared_statement_handle)
    }

    /// Close the prepared statement, so that this PreparedStatement can not used
    /// anymore and server can free up any resources.
    pub async fn close(mut self) -> Result<(), ArrowError> {
        let cmd = ActionClosePreparedStatementRequest {
            prepared_statement_handle: self.handle.clone(),
        };
        let action = Action {
            r#type: CLOSE_PREPARED_STATEMENT.to_string(),
            body: cmd.as_any().encode_to_vec().into(),
        };
        let _ = self.flight_sql_client.do_action(action).await?;
        Ok(())
    }
}

fn decode_error_to_arrow_error(err: prost::DecodeError) -> ArrowError {
    ArrowError::IpcError(err.to_string())
}

fn status_to_arrow_error(status: tonic::Status) -> ArrowError {
    ArrowError::IpcError(format!("{status:?}"))
}

fn flight_error_to_arrow_error(err: FlightError) -> ArrowError {
    match err {
        FlightError::Arrow(e) => e,
        e => ArrowError::ExternalError(Box::new(e)),
    }
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
    let ipc_message = root_as_message(&flight_data.data_header[..])
        .map_err(|err| ArrowError::ParseError(format!("Unable to get root as message: {err:?}")))?;

    match ipc_message.header_type() {
        MessageHeader::RecordBatch => {
            let ipc_record_batch = ipc_message.header_as_record_batch().ok_or_else(|| {
                ArrowError::ComputeError(
                    "Unable to convert flight data header to a record batch".to_string(),
                )
            })?;

            let dictionaries_by_field = HashMap::new();
            let record_batch = read_record_batch(
                &Buffer::from_bytes(flight_data.data_body.into()),
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
                    "Unable to convert flight data header to a dictionary batch".to_string(),
                )
            })?;
            Err(ArrowError::NotYetImplemented(
                "no idea on how to convert an ipc dictionary batch to an arrow type".to_string(),
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
                "no idea on how to convert an ipc sparse tensor to an arrow type".to_string(),
            ))
        }
        _ => Err(ArrowError::ComputeError(format!(
            "Unable to convert message with header_type: '{:?}' to arrow data",
            ipc_message.header_type()
        ))),
    }
}
