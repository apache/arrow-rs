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

use std::cell::{RefCell, RefMut};

use arrow::{
    datatypes::{Schema, SchemaRef},
    error::{ArrowError, Result},
    ipc::{MessageHeader, RecordBatch},
};
use futures::stream;
use prost::Message;
use tonic::{
    codegen::{Body, StdError},
    Streaming,
};

use crate::{
    flight_service_client::FlightServiceClient, Action, FlightData, FlightDescriptor,
    FlightInfo, IpcMessage, Ticket,
};

use super::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, CommandGetCatalogs, CommandGetCrossReference,
    CommandGetDbSchemas, CommandGetExportedKeys, CommandGetImportedKeys,
    CommandGetPrimaryKeys, CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables,
    CommandPreparedStatementQuery, CommandStatementQuery, CommandStatementUpdate,
    DoPutUpdateResult, ProstAnyExt, ProstMessageExt, SqlInfo,
    ACTION_TYPE_CLOSE_PREPARED_STATEMENT, ACTION_TYPE_CREATE_PREPARED_STATEMENT,
};

/// A FlightSQLServiceClient is an endpoint for retrieving or storing Arrow data
/// by FlightSQL protocol.
#[derive(Debug, Clone)]
pub struct FlightSqlServiceClient<T> {
    inner: RefCell<FlightServiceClient<T>>,
}

impl<T> FlightSqlServiceClient<T>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Default + Body<Data = bytes::Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    /// create FlightSqlServiceClient using FlightServiceClient
    pub fn new(client: RefCell<FlightServiceClient<T>>) -> Self {
        FlightSqlServiceClient { inner: client }
    }

    /// borrow mut FlightServiceClient
    fn mut_client(&self) -> RefMut<'_, FlightServiceClient<T>> {
        self.inner.borrow_mut()
    }

    async fn get_flight_info_for_command<M: ProstMessageExt>(
        &mut self,
        cmd: M,
    ) -> Result<FlightInfo> {
        let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
        Ok(self
            .mut_client()
            .get_flight_info(descriptor)
            .await
            .map_err(status_to_arrow_error)?
            .into_inner())
    }

    /// Execute a query on the server.
    pub async fn execute(&mut self, query: String) -> Result<FlightInfo> {
        let cmd = CommandStatementQuery { query };
        self.get_flight_info_for_command(cmd).await
    }

    /// Execute a update query on the server.
    pub async fn execute_update(&mut self, query: String) -> Result<i64> {
        let cmd = CommandStatementUpdate { query };
        let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
        let mut result = self
            .mut_client()
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

    /// Request a list of catalogs.
    pub async fn get_catalogs(&mut self) -> Result<FlightInfo> {
        self.get_flight_info_for_command(CommandGetCatalogs {})
            .await
    }

    /// Request a list of database schemas.
    pub async fn get_db_schemas(
        &mut self,
        request: CommandGetDbSchemas,
    ) -> Result<FlightInfo> {
        self.get_flight_info_for_command(request).await
    }

    /// Given a flight ticket and schema, request to be sent the
    /// stream. Returns record batch stream reader
    pub async fn do_get(&mut self, ticket: Ticket) -> Result<Streaming<FlightData>> {
        Ok(self
            .mut_client()
            .do_get(ticket)
            .await
            .map_err(status_to_arrow_error)?
            .into_inner())
    }

    /// Request a list of tables.
    pub async fn get_tables(&mut self, request: CommandGetTables) -> Result<FlightInfo> {
        self.get_flight_info_for_command(request).await
    }

    /// Request the primary keys for a table.
    pub async fn get_primary_keys(
        &mut self,
        request: CommandGetPrimaryKeys,
    ) -> Result<FlightInfo> {
        self.get_flight_info_for_command(request).await
    }

    /// Retrieves a description about the foreign key columns that reference the
    /// primary key columns of the given table.
    pub async fn get_exported_keys(
        &mut self,
        request: CommandGetExportedKeys,
    ) -> Result<FlightInfo> {
        self.get_flight_info_for_command(request).await
    }

    /// Retrieves the foreign key columns for the given table.
    pub async fn get_imported_keys(
        &mut self,
        request: CommandGetImportedKeys,
    ) -> Result<FlightInfo> {
        self.get_flight_info_for_command(request).await
    }

    /// Retrieves a description of the foreign key columns in the given foreign key
    /// table that reference the primary key or the columns representing a unique
    /// constraint of the parent table (could be the same or a different table).
    pub async fn get_cross_reference(
        &mut self,
        request: CommandGetCrossReference,
    ) -> Result<FlightInfo> {
        self.get_flight_info_for_command(request).await
    }

    /// Request a list of table types.
    pub async fn get_table_types(&mut self) -> Result<FlightInfo> {
        self.get_flight_info_for_command(CommandGetTableTypes {})
            .await
    }

    /// Request a list of SQL information.
    pub async fn get_sql_info(&mut self, sql_infos: Vec<SqlInfo>) -> Result<FlightInfo> {
        let request = CommandGetSqlInfo {
            info: sql_infos.iter().map(|sql_info| *sql_info as u32).collect(),
        };
        self.get_flight_info_for_command(request).await
    }

    /// Create a prepared statement object.
    pub async fn prepare(&mut self, query: String) -> Result<PreparedStatement<'_, T>> {
        let cmd = ActionCreatePreparedStatementRequest { query };
        let action = Action {
            r#type: ACTION_TYPE_CREATE_PREPARED_STATEMENT.to_string(),
            body: cmd.as_any().encode_to_vec(),
        };
        let mut result = self
            .mut_client()
            .do_action(tonic::Request::new(action))
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
        let dataset_schema =
            Schema::try_from(IpcMessage(prepared_result.dataset_schema))?;
        let parameter_schema =
            Schema::try_from(IpcMessage(prepared_result.parameter_schema))?;
        Ok(PreparedStatement::new(
            &self.inner,
            prepared_result.prepared_statement_handle,
            dataset_schema,
            parameter_schema,
        ))
    }

    /// Explicitly shut down and clean up the client.
    pub async fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

/// A PreparedStatement
#[derive(Debug, Clone)]
pub struct PreparedStatement<'a, T> {
    inner: &'a RefCell<FlightServiceClient<T>>,
    is_closed: bool,
    parameter_binding: Option<RecordBatch<'a>>,
    handle: Vec<u8>,
    dataset_schema: Schema,
    parameter_schema: Schema,
}

impl<'a, T> PreparedStatement<'a, T>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody>,
    T::Error: Into<StdError>,
    T::ResponseBody: Default + Body<Data = bytes::Bytes> + Send + 'static,
    <T::ResponseBody as Body>::Error: Into<StdError> + Send,
{
    pub(crate) fn new(
        client: &'a RefCell<FlightServiceClient<T>>,
        handle: Vec<u8>,
        dataset_schema: Schema,
        parameter_schema: Schema,
    ) -> Self {
        PreparedStatement {
            inner: client,
            is_closed: false,
            parameter_binding: None,
            handle,
            dataset_schema,
            parameter_schema,
        }
    }
    /// Executes the prepared statement query on the server.
    pub async fn execute(&mut self) -> Result<FlightInfo> {
        if self.is_closed() {
            return Err(ArrowError::TonicRequestError(
                "Statement already closed.".to_string(),
            ));
        }
        let cmd = CommandPreparedStatementQuery {
            prepared_statement_handle: self.handle.clone(),
        };
        let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
        let mut result = self
            .mut_client()
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
        let _: prost_types::Any = prost::Message::decode(&*result.app_metadata)
            .map_err(decode_error_to_arrow_error)?;
        Err(ArrowError::NotYetImplemented(
            "Not yet implemented".to_string(),
        ))
    }

    /// Executes the prepared statement update query on the server.
    pub async fn execute_update(&self) -> Result<i64> {
        if self.is_closed() {
            return Err(ArrowError::TonicRequestError(
                "Statement already closed.".to_string(),
            ));
        }
        let cmd = CommandPreparedStatementQuery {
            prepared_statement_handle: self.handle.clone(),
        };
        let descriptor = FlightDescriptor::new_cmd(cmd.as_any().encode_to_vec());
        let mut result = self
            .mut_client()
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

    /// Retrieve the parameter schema from the query.
    pub async fn parameter_schema(&self) -> Result<&Schema> {
        Ok(&self.parameter_schema)
    }

    /// Retrieve the ResultSet schema from the query.
    pub async fn dataset_schema(&self) -> Result<&Schema> {
        Ok(&self.dataset_schema)
    }

    /// Set a RecordBatch that contains the parameters that will be bind.
    pub async fn set_parameters(
        &mut self,
        parameter_binding: RecordBatch<'a>,
    ) -> Result<()> {
        self.parameter_binding = Some(parameter_binding);
        Ok(())
    }

    /// Close the prepared statement, so that this PreparedStatement can not used
    /// anymore and server can free up any resources.
    pub async fn close(&mut self) -> Result<()> {
        if self.is_closed() {
            return Err(ArrowError::TonicRequestError(
                "Statement already closed.".to_string(),
            ));
        }
        let cmd = ActionClosePreparedStatementRequest {
            prepared_statement_handle: self.handle.clone(),
        };
        let action = Action {
            r#type: ACTION_TYPE_CLOSE_PREPARED_STATEMENT.to_string(),
            body: cmd.as_any().encode_to_vec(),
        };
        let _ = self
            .mut_client()
            .do_action(action)
            .await
            .map_err(status_to_arrow_error)?;
        self.is_closed = true;
        Ok(())
    }

    /// Check if the prepared statement is closed.
    pub fn is_closed(&self) -> bool {
        self.is_closed
    }

    /// borrow mut FlightServiceClient
    fn mut_client(&self) -> RefMut<'_, FlightServiceClient<T>> {
        self.inner.borrow_mut()
    }
}

pub fn decode_error_to_arrow_error(err: prost::DecodeError) -> ArrowError {
    ArrowError::IoError(err.to_string())
}

pub fn arrow_error_to_status(err: arrow::error::ArrowError) -> tonic::Status {
    tonic::Status::internal(format!("{:?}", err))
}

pub fn status_to_arrow_error(status: tonic::Status) -> ArrowError {
    ArrowError::TonicRequestError(format!("{:?}", status))
}

pub fn transport_error_to_arrow_erorr(error: tonic::transport::Error) -> ArrowError {
    ArrowError::TonicRequestError(format!("{}", error))
}

pub fn arrow_schema_from_flight_info(fi: &FlightInfo) -> Result<Schema> {
    let ipc_message = arrow::ipc::size_prefixed_root_as_message(&fi.schema[4..])
        .map_err(|e| ArrowError::ComputeError(format!("{:?}", e)))?;

    let ipc_schema = ipc_message
        .header_as_schema()
        .ok_or(ArrowError::ComputeError(
            "failed to get schema...".to_string(),
        ))?;

    let arrow_schema = arrow::ipc::convert::fb_to_schema(ipc_schema);

    Ok(arrow_schema)
}

pub enum ArrowFlightData {
    RecordBatch(arrow::record_batch::RecordBatch),
    Schema(arrow::datatypes::Schema),
}

pub fn arrow_data_from_flight_data(
    flight_data: FlightData,
    arrow_schema_ref: &SchemaRef,
) -> Result<ArrowFlightData> {
    let ipc_message =
        arrow::ipc::root_as_message(&flight_data.data_header[..]).map_err(|err| {
            ArrowError::ParseError(format!("Unable to get root as message: {:?}", err))
        })?;

    match ipc_message.header_type() {
        MessageHeader::RecordBatch => {
            let ipc_record_batch =
                ipc_message
                    .header_as_record_batch()
                    .ok_or(ArrowError::ComputeError(
                        "Unable to convert flight data header to a record batch"
                            .to_string(),
                    ))?;

            let dictionaries_by_field = &[];
            let record_batch = arrow::ipc::reader::read_record_batch(
                &flight_data.data_body,
                ipc_record_batch,
                arrow_schema_ref.clone(),
                dictionaries_by_field,
                None,
            )?;
            Ok(ArrowFlightData::RecordBatch(record_batch))
        }
        MessageHeader::Schema => {
            let ipc_schema =
                ipc_message
                    .header_as_schema()
                    .ok_or(ArrowError::ComputeError(
                        "Unable to convert flight data header to a schema".to_string(),
                    ))?;

            let arrow_schema = arrow::ipc::convert::fb_to_schema(ipc_schema);
            Ok(ArrowFlightData::Schema(arrow_schema))
        }
        MessageHeader::DictionaryBatch => {
            let _ = ipc_message.header_as_dictionary_batch().ok_or(
                ArrowError::ComputeError(
                    "Unable to convert flight data header to a dictionary batch"
                        .to_string(),
                ),
            )?;
            Err(ArrowError::NotYetImplemented(
                "no idea on how to convert an ipc dictionary batch to an arrow type"
                    .to_string(),
            ))
        }
        MessageHeader::Tensor => {
            let _ = ipc_message
                .header_as_tensor()
                .ok_or(ArrowError::ComputeError(
                    "Unable to convert flight data header to a tensor".to_string(),
                ))?;
            Err(ArrowError::NotYetImplemented(
                "no idea on how to convert an ipc tensor to an arrow type".to_string(),
            ))
        }
        MessageHeader::SparseTensor => {
            let _ =
                ipc_message
                    .header_as_sparse_tensor()
                    .ok_or(ArrowError::ComputeError(
                        "Unable to convert flight data header to a sparse tensor"
                            .to_string(),
                    ))?;
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
