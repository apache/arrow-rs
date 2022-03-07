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

use arrow::error::{ArrowError, Result as ArrowResult};
use prost::Message;

mod gen {
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

pub mod server;

/// ProstMessageExt are useful utility methods for prost::Message types
pub trait ProstMessageExt {
    /// Item is the return value of prost_type::Any::unpack()
    type Item: prost::Message + Default;

    /// type_url for this Message
    fn type_url() -> &'static str;

    /// Convert this Message to prost_types::Any
    fn as_any(&self) -> prost_types::Any;
}

macro_rules! prost_message_ext {
    ($($name:ty,)*) => {
        $(
            impl ProstMessageExt for $name {
                type Item = $name;
                fn type_url() -> &'static str {
                    concat!("type.googleapis.com/arrow.flight.protocol.sql.", stringify!($name))
                }

                fn as_any(&self) -> prost_types::Any {
                    prost_types::Any {
                        type_url: <$name>::type_url().to_string(),
                        value: self.encode_to_vec(),
                    }
                }
            }
        )*
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

/// ProstAnyExt are useful utility methods for prost_types::Any
/// The API design is inspired by https://github.com/stepancheg/rust-protobuf/blob/master/protobuf/src/well_known_types_util/any.rs
pub trait ProstAnyExt {
    /// Check if `Any` contains a message of given type.
    fn is<M: ProstMessageExt>(&self) -> bool;

    /// Extract a message from this `Any`.
    ///
    /// # Returns
    ///
    /// * `Ok(None)` when message type mismatch
    /// * `Err` when parse failed
    fn unpack<M: ProstMessageExt>(&self) -> ArrowResult<Option<M::Item>>;

    /// Pack any message into `prost_types::Any` value.
    fn pack<M: ProstMessageExt>(message: &M) -> ArrowResult<prost_types::Any>;
}

impl ProstAnyExt for prost_types::Any {
    fn is<M: ProstMessageExt>(&self) -> bool {
        M::type_url() == self.type_url
    }

    fn unpack<M: ProstMessageExt>(&self) -> ArrowResult<Option<M::Item>> {
        if !self.is::<M>() {
            return Ok(None);
        }
        let m = prost::Message::decode(&*self.value).map_err(|err| {
            ArrowError::ParseError(format!("Unable to decode Any value: {}", err))
        })?;
        Ok(Some(m))
    }

    fn pack<M: ProstMessageExt>(message: &M) -> ArrowResult<prost_types::Any> {
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
}
