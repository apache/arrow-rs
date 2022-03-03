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

mod gen {
    include!("FlightSql.rs");
}

pub use gen::proto::ActionClosePreparedStatementRequest;
pub use gen::proto::ActionCreatePreparedStatementRequest;
pub use gen::proto::ActionCreatePreparedStatementResult;
pub use gen::proto::CommandGetCatalogs;
pub use gen::proto::CommandGetCrossReference;
pub use gen::proto::CommandGetDbSchemas;
pub use gen::proto::CommandGetExportedKeys;
pub use gen::proto::CommandGetImportedKeys;
pub use gen::proto::CommandGetPrimaryKeys;
pub use gen::proto::CommandGetSqlInfo;
pub use gen::proto::CommandGetTableTypes;
pub use gen::proto::CommandGetTables;
pub use gen::proto::CommandPreparedStatementQuery;
pub use gen::proto::CommandPreparedStatementUpdate;
pub use gen::proto::CommandStatementQuery;
pub use gen::proto::CommandStatementUpdate;
pub use gen::proto::DoPutUpdateResult;
pub use gen::proto::SqlInfo;
pub use gen::proto::SqlNullOrdering;
pub use gen::proto::SqlOuterJoinsSupportLevel;
pub use gen::proto::SqlSupportedCaseSensitivity;
pub use gen::proto::SqlSupportedElementActions;
pub use gen::proto::SqlSupportedGroupBy;
pub use gen::proto::SqlSupportedPositionedCommands;
pub use gen::proto::SqlSupportedResultSetConcurrency;
pub use gen::proto::SqlSupportedResultSetType;
pub use gen::proto::SqlSupportedSubqueries;
pub use gen::proto::SqlSupportedTransactions;
pub use gen::proto::SqlSupportedUnions;
pub use gen::proto::SqlSupportsConvert;
pub use gen::proto::SqlTransactionIsolationLevel;
pub use gen::proto::SupportedSqlGrammar;
pub use gen::proto::TicketStatementQuery;
pub use gen::proto::UpdateDeleteRules;

pub mod server;
