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

//! Support for execute SQL queries using [Apache Arrow] [Flight SQL].
//!
//! [Flight SQL] is built on top of Arrow Flight RPC framework, by
//! defining specific messages, encoded using the protobuf format,
//! sent in the[`FlightDescriptor::cmd`] field to [`FlightService`]
//! endpoints such as[`get_flight_info`] and [`do_get`].
//!
//! This module contains:
//! 1. [prost] generated structs for FlightSQL messages such as [`CommandStatementQuery`]
//! 2. Helpers for encoding and decoding FlightSQL messages: [`Any`] and [`Command`]
//! 3. A [`FlightSqlServiceClient`] for interacting with FlightSQL servers.
//! 4. A [`FlightSqlService`] to help building FlightSQL servers from [`FlightService`].
//! 5. Helpers to build responses for FlightSQL metadata APIs: [`metadata`]
//!
//! [Flight SQL]: https://arrow.apache.org/docs/format/FlightSql.html
//! [Apache Arrow]: https://arrow.apache.org
//! [`FlightDescriptor::cmd`]: crate::FlightDescriptor::cmd
//! [`FlightService`]: crate::flight_service_server::FlightService
//! [`get_flight_info`]: crate::flight_service_server::FlightService::get_flight_info
//! [`do_get`]: crate::flight_service_server::FlightService::do_get
//! [`FlightSqlServiceClient`]: client::FlightSqlServiceClient
//! [`FlightSqlService`]: server::FlightSqlService
//! [`metadata`]: crate::sql::metadata
use arrow_schema::ArrowError;
use bytes::Bytes;
use paste::paste;
use prost::Message;

#[allow(clippy::all)]
mod gen {
    #![allow(rustdoc::unportable_markdown)]
    // Since this file is auto-generated, we suppress all warnings
    #![allow(missing_docs)]
    include!("arrow.flight.protocol.sql.rs");
}

pub use gen::action_end_transaction_request::EndTransaction;
pub use gen::command_statement_ingest::table_definition_options::{
    TableExistsOption, TableNotExistOption,
};
pub use gen::command_statement_ingest::TableDefinitionOptions;
pub use gen::ActionBeginSavepointRequest;
pub use gen::ActionBeginSavepointResult;
pub use gen::ActionBeginTransactionRequest;
pub use gen::ActionBeginTransactionResult;
pub use gen::ActionCancelQueryRequest;
pub use gen::ActionCancelQueryResult;
pub use gen::ActionClosePreparedStatementRequest;
pub use gen::ActionCreatePreparedStatementRequest;
pub use gen::ActionCreatePreparedStatementResult;
pub use gen::ActionCreatePreparedSubstraitPlanRequest;
pub use gen::ActionEndSavepointRequest;
pub use gen::ActionEndTransactionRequest;
pub use gen::CommandGetCatalogs;
pub use gen::CommandGetCrossReference;
pub use gen::CommandGetDbSchemas;
pub use gen::CommandGetExportedKeys;
pub use gen::CommandGetImportedKeys;
pub use gen::CommandGetPrimaryKeys;
pub use gen::CommandGetSqlInfo;
pub use gen::CommandGetTableTypes;
pub use gen::CommandGetTables;
pub use gen::CommandGetXdbcTypeInfo;
pub use gen::CommandPreparedStatementQuery;
pub use gen::CommandPreparedStatementUpdate;
pub use gen::CommandStatementIngest;
pub use gen::CommandStatementQuery;
pub use gen::CommandStatementSubstraitPlan;
pub use gen::CommandStatementUpdate;
pub use gen::DoPutPreparedStatementResult;
pub use gen::DoPutUpdateResult;
pub use gen::Nullable;
pub use gen::Searchable;
pub use gen::SqlInfo;
pub use gen::SqlNullOrdering;
pub use gen::SqlOuterJoinsSupportLevel;
pub use gen::SqlSupportedCaseSensitivity;
pub use gen::SqlSupportedElementActions;
pub use gen::SqlSupportedGroupBy;
pub use gen::SqlSupportedPositionedCommands;
pub use gen::SqlSupportedResultSetConcurrency;
pub use gen::SqlSupportedResultSetType;
pub use gen::SqlSupportedSubqueries;
pub use gen::SqlSupportedTransaction;
pub use gen::SqlSupportedTransactions;
pub use gen::SqlSupportedUnions;
pub use gen::SqlSupportsConvert;
pub use gen::SqlTransactionIsolationLevel;
pub use gen::SubstraitPlan;
pub use gen::SupportedSqlGrammar;
pub use gen::TicketStatementQuery;
pub use gen::UpdateDeleteRules;
pub use gen::XdbcDataType;
pub use gen::XdbcDatetimeSubcode;

pub mod client;
pub mod metadata;
pub mod server;

/// ProstMessageExt are useful utility methods for prost::Message types
pub trait ProstMessageExt: prost::Message + Default {
    /// type_url for this Message
    fn type_url() -> &'static str;

    /// Convert this Message to [`Any`]
    fn as_any(&self) -> Any;
}

/// Macro to coerce a token to an item, specifically
/// to build the `Commands` enum.
///
/// See: <https://danielkeep.github.io/tlborm/book/blk-ast-coercion.html>
macro_rules! as_item {
    ($i:item) => {
        $i
    };
}

macro_rules! prost_message_ext {
    ($($name:tt,)*) => {
        paste! {
            $(
            const [<$name:snake:upper _TYPE_URL>]: &'static str = concat!("type.googleapis.com/arrow.flight.protocol.sql.", stringify!($name));
            )*

                as_item! {
                /// Helper to convert to/from protobuf [`Any`] message
                /// to a specific FlightSQL command message.
                ///
                /// # Example
                /// ```rust
                /// # use arrow_flight::sql::{Any, CommandStatementQuery, Command};
                /// let flightsql_message = CommandStatementQuery {
                ///   query: "SELECT * FROM foo".to_string(),
                ///   transaction_id: None,
                /// };
                ///
                /// // Given a packed FlightSQL Any message
                /// let any_message = Any::pack(&flightsql_message).unwrap();
                ///
                /// // decode it to Command:
                /// match Command::try_from(any_message).unwrap() {
                ///   Command::CommandStatementQuery(decoded) => {
                ///    assert_eq!(flightsql_message, decoded);
                ///   }
                ///   _ => panic!("Unexpected decoded message"),
                /// }
                /// ```
                #[derive(Clone, Debug, PartialEq)]
                pub enum Command {
                    $(
                        #[doc = concat!(stringify!($name), "variant")]
                        $name($name),)*

                    /// Any message that is not any FlightSQL command.
                    Unknown(Any),
                }
            }

            impl Command {
                /// Convert the command to [`Any`].
                pub fn into_any(self) -> Any {
                    match self {
                        $(
                        Self::$name(cmd) => cmd.as_any(),
                        )*
                        Self::Unknown(any) => any,
                    }
                }

                /// Get the URL for the command.
                pub fn type_url(&self) -> &str {
                    match self {
                        $(
                        Self::$name(_) => [<$name:snake:upper _TYPE_URL>],
                        )*
                        Self::Unknown(any) => any.type_url.as_str(),
                    }
                }
            }

            impl TryFrom<Any> for Command {
                type Error = ArrowError;

                fn try_from(any: Any) -> Result<Self, Self::Error> {
                    match any.type_url.as_str() {
                        $(
                        [<$name:snake:upper _TYPE_URL>]
                            => {
                                let m: $name = Message::decode(&*any.value).map_err(|err| {
                                    ArrowError::ParseError(format!("Unable to decode Any value: {err}"))
                                })?;
                                Ok(Self::$name(m))
                            }
                        )*
                        _ => Ok(Self::Unknown(any)),
                    }
                }
            }

            $(
                impl ProstMessageExt for $name {
                    fn type_url() -> &'static str {
                        [<$name:snake:upper _TYPE_URL>]
                    }

                    fn as_any(&self) -> Any {
                        Any {
                            type_url: <$name>::type_url().to_string(),
                            value: self.encode_to_vec().into(),
                        }
                    }
                }
            )*
        }
    };
}

// Implement ProstMessageExt for all structs defined in FlightSql.proto
prost_message_ext!(
    ActionBeginSavepointRequest,
    ActionBeginSavepointResult,
    ActionBeginTransactionRequest,
    ActionBeginTransactionResult,
    ActionCancelQueryRequest,
    ActionCancelQueryResult,
    ActionClosePreparedStatementRequest,
    ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult,
    ActionCreatePreparedSubstraitPlanRequest,
    ActionEndSavepointRequest,
    ActionEndTransactionRequest,
    CommandGetCatalogs,
    CommandGetCrossReference,
    CommandGetDbSchemas,
    CommandGetExportedKeys,
    CommandGetImportedKeys,
    CommandGetPrimaryKeys,
    CommandGetSqlInfo,
    CommandGetTableTypes,
    CommandGetTables,
    CommandGetXdbcTypeInfo,
    CommandPreparedStatementQuery,
    CommandPreparedStatementUpdate,
    CommandStatementIngest,
    CommandStatementQuery,
    CommandStatementSubstraitPlan,
    CommandStatementUpdate,
    DoPutPreparedStatementResult,
    DoPutUpdateResult,
    TicketStatementQuery,
);

/// An implementation of the protobuf [`Any`] message type
///
/// Encoded protobuf messages are not self-describing, nor contain any information
/// on the schema of the encoded payload. Consequently to decode a protobuf a client
/// must know the exact schema of the message.
///
/// This presents a problem for loosely typed APIs, where the exact message payloads
/// are not enumerable, and therefore cannot be enumerated as variants in a [oneof].
///
/// One solution is [`Any`] where the encoded payload is paired with a `type_url`
/// identifying the type of encoded message, and the resulting combination encoded.
///
/// Clients can then decode the outer [`Any`], inspect the `type_url` and if it is
/// a type they recognise, proceed to decode the embedded message `value`
///
/// [`Any`]: https://developers.google.com/protocol-buffers/docs/proto3#any
/// [oneof]: https://developers.google.com/protocol-buffers/docs/proto3#oneof
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Any {
    /// A URL/resource name that uniquely identifies the type of the serialized
    /// protocol buffer message. This string must contain at least
    /// one "/" character. The last segment of the URL's path must represent
    /// the fully qualified name of the type (as in
    /// `path/google.protobuf.Duration`). The name should be in a canonical form
    /// (e.g., leading "." is not accepted).
    #[prost(string, tag = "1")]
    pub type_url: String,
    /// Must be a valid serialized protocol buffer of the above specified type.
    #[prost(bytes = "bytes", tag = "2")]
    pub value: Bytes,
}

impl Any {
    /// Checks whether the message is of type `M`
    pub fn is<M: ProstMessageExt>(&self) -> bool {
        M::type_url() == self.type_url
    }

    /// Unpacks the contents of the message if it is of type `M`
    pub fn unpack<M: ProstMessageExt>(&self) -> Result<Option<M>, ArrowError> {
        if !self.is::<M>() {
            return Ok(None);
        }
        let m = Message::decode(&*self.value)
            .map_err(|err| ArrowError::ParseError(format!("Unable to decode Any value: {err}")))?;
        Ok(Some(m))
    }

    /// Packs a message into an [`Any`] message
    pub fn pack<M: ProstMessageExt>(message: &M) -> Result<Any, ArrowError> {
        Ok(message.as_any())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_url() {
        assert_eq!(
            TicketStatementQuery::type_url(),
            "type.googleapis.com/arrow.flight.protocol.sql.TicketStatementQuery"
        );
        assert_eq!(
            CommandStatementQuery::type_url(),
            "type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"
        );
    }

    #[test]
    fn test_prost_any_pack_unpack() {
        let query = CommandStatementQuery {
            query: "select 1".to_string(),
            transaction_id: None,
        };
        let any = Any::pack(&query).unwrap();
        assert!(any.is::<CommandStatementQuery>());
        let unpack_query: CommandStatementQuery = any.unpack().unwrap().unwrap();
        assert_eq!(query, unpack_query);
    }

    #[test]
    fn test_command() {
        let query = CommandStatementQuery {
            query: "select 1".to_string(),
            transaction_id: None,
        };
        let any = Any::pack(&query).unwrap();
        let cmd: Command = any.try_into().unwrap();

        assert!(matches!(cmd, Command::CommandStatementQuery(_)));
        assert_eq!(cmd.type_url(), COMMAND_STATEMENT_QUERY_TYPE_URL);

        // Unknown variant

        let any = Any {
            type_url: "fake_url".to_string(),
            value: Default::default(),
        };

        let cmd: Command = any.try_into().unwrap();
        assert!(matches!(cmd, Command::Unknown(_)));
        assert_eq!(cmd.type_url(), "fake_url");
    }
}
