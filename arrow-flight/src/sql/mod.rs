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

use arrow_schema::ArrowError;
use bytes::Bytes;
use paste::paste;
use prost::Message;

mod gen {
    #![allow(clippy::all)]
    include!("arrow.flight.protocol.sql.rs");
}

pub use gen::ActionClosePreparedStatementRequest;
pub use gen::ActionCreatePreparedStatementRequest;
pub use gen::ActionCreatePreparedStatementResult;
pub use gen::CommandGetCatalogs;
pub use gen::CommandGetCrossReference;
pub use gen::CommandGetDbSchemas;
pub use gen::CommandGetExportedKeys;
pub use gen::CommandGetImportedKeys;
pub use gen::CommandGetPrimaryKeys;
pub use gen::CommandGetSqlInfo;
pub use gen::CommandGetTableTypes;
pub use gen::CommandGetTables;
pub use gen::CommandPreparedStatementQuery;
pub use gen::CommandPreparedStatementUpdate;
pub use gen::CommandStatementQuery;
pub use gen::CommandStatementUpdate;
pub use gen::DoPutUpdateResult;
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
pub use gen::SqlSupportedTransactions;
pub use gen::SqlSupportedUnions;
pub use gen::SqlSupportsConvert;
pub use gen::SqlTransactionIsolationLevel;
pub use gen::SupportedSqlGrammar;
pub use gen::TicketStatementQuery;
pub use gen::UpdateDeleteRules;

pub mod client;
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
                /// Helper to convert to/from protobuf [`Any`] to a strongly typed enum.
                ///
                /// # Example
                /// ```rust
                /// # use arrow_flight::sql::{Any, CommandStatementQuery, Commands};
                /// let flightsql_message = CommandStatementQuery {
                ///   query: "SELECT * FROM foo".to_string(),
                /// };
                ///
                /// // Given a packed FlightSQL Any message
                /// let any_message = Any::pack(&flightsql_message).unwrap();
                ///
                /// // decode it to Commands:
                /// match Command::try_from(any_message).unwrap() {
                ///   Command::CommandStatementQuery(decoded) => {
                ///    assert_eq!(flightsql_message, decoded);
                ///   }
                ///   _ => panic!("Unexpected decoded message"),
                /// }
                /// ```
                #[derive(Clone, Debug, PartialEq)]
                pub enum Command {
                    $($name($name),)*
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
                        _ => Err(ArrowError::ParseError(format!("Unable to decode Any value: {}", any.type_url)))
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
    ActionClosePreparedStatementRequest,
    ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult,
    CommandGetCatalogs,
    CommandGetCrossReference,
    CommandGetDbSchemas,
    CommandGetExportedKeys,
    CommandGetImportedKeys,
    CommandGetPrimaryKeys,
    CommandGetSqlInfo,
    CommandGetTableTypes,
    CommandGetTables,
    CommandPreparedStatementQuery,
    CommandPreparedStatementUpdate,
    CommandStatementQuery,
    CommandStatementUpdate,
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
    pub fn is<M: ProstMessageExt>(&self) -> bool {
        M::type_url() == self.type_url
    }

    pub fn unpack<M: ProstMessageExt>(&self) -> Result<Option<M>, ArrowError> {
        if !self.is::<M>() {
            return Ok(None);
        }
        let m = Message::decode(&*self.value).map_err(|err| {
            ArrowError::ParseError(format!("Unable to decode Any value: {err}"))
        })?;
        Ok(Some(m))
    }

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
        };
        let any = Any::pack(&query).unwrap();
        assert!(any.is::<CommandStatementQuery>());
        let unpack_query: CommandStatementQuery = any.unpack().unwrap().unwrap();
        assert_eq!(query, unpack_query);
    }

    #[test]
    fn test_commands() {
        let query = CommandStatementQuery {
            query: "select 1".to_string(),
        };
        let any = Any::pack(&query).unwrap();
        assert!(matches!(
            any.try_into().unwrap(),
            Command::CommandStatementQuery(_)
        ));
    }
}
