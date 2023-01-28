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

//! Implement ADBC driver without unsafe code.
//!
//! Allows implementing ADBC driver in Rust without directly interacting with
//! FFI types, which requires unsafe code. Implement [AdbcDatabase],
//! [AdbcConnection], [AdbcStatement], and [AdbcError] first. Then pass the
//! statement type to [adbc_api] to generate C FFI interface for ADBC.

use std::{rc::Rc, sync::Arc};

use arrow::{array::ArrayRef, datatypes::Schema, record_batch::RecordBatchReader};

use crate::{error::AdbcError, ffi::AdbcObjectDepth};

pub mod internal;

/// Databases hold state shared by multiple connections. This typically means
/// configuration and caches. For in-memory databases, it provides a place to
/// hold ownership of the in-memory database.
///
/// Because it is shared by multiple connections, the implementation must be
/// thread safe. Internally, it is held with an [std::sync::Arc] by each connection.
pub trait AdbcDatabase {
    type Error: AdbcError;

    /// Set an option on the database.
    fn set_option(&self, key: &str, value: &str) -> Result<(), Self::Error>;

    /// Initialize the database.
    ///
    /// Some drivers may choose not to support setting options after this has
    /// been called.
    fn init(&self) -> Result<(), Self::Error>;
}

/// A connection is a single connection to a database.
///
/// It is never accessed concurrently from multiple threads.
///
/// # Autocommit
///
/// Connections should start in autocommit mode. They can be moved out by
/// setting `"adbc.connection.autocommit"` to `"false"` (using
/// [AdbcConnection::set_option]). Turning off autocommit allows customizing
/// the isolation level. Read more in [adbc.h](https://github.com/apache/arrow-adbc/blob/main/adbc.h).
pub trait AdbcConnection {
    type Error: AdbcError;
    type DatabaseType: AdbcDatabase + Default;

    /// Set an option on the connection.
    fn set_option(&self, key: &str, value: &str) -> Result<(), Self::Error>;

    /// Initialize the connection.
    ///
    /// The Arc to the database should be stored within your struct.
    ///
    /// Some drivers may choose not to support setting options after this has
    /// been called.
    fn init(&self, database: Arc<Self::DatabaseType>) -> Result<(), Self::Error>;

    /// Get metadata about the database/driver.
    ///
    /// The result is an Arrow dataset with the following schema:
    ///
    /// Field Name                  | Field Type
    /// ----------------------------|------------------------
    /// `info_name`                 | `uint32 not null`
    /// `info_value`                | `INFO_SCHEMA`
    ///
    /// `INFO_SCHEMA` is a dense union with members:
    ///
    /// Field Name (Type Code)        | Field Type
    /// ------------------------------|------------------------
    /// `string_value` (0)            | `utf8`
    /// `bool_value` (1)              | `bool`
    /// `int64_value` (2)             | `int64`
    /// `int32_bitmask` (3)           | `int32`
    /// `string_list` (4)             | `list<utf8>`
    /// `int32_to_int32_list_map` (5) | `map<int32, list<int32>>`
    ///
    /// Each metadatum is identified by an integer code.  The recognized
    /// codes are defined as constants.  Codes [0, 10_000) are reserved
    /// for ADBC usage.  Drivers/vendors will ignore requests for
    /// unrecognized codes (the row will be omitted from the result).
    ///
    /// For definitions of known ADBC codes, see <https://github.com/apache/arrow-adbc/blob/main/adbc.h>
    fn get_info(
        &self,
        info_codes: &[u32],
    ) -> Result<Box<dyn RecordBatchReader>, Self::Error>;

    /// Get a hierarchical view of all catalogs, database schemas, tables, and columns.
    ///
    /// # Schema
    ///
    /// The result is an Arrow dataset with the following schema:
    ///
    /// | Field Name                 | Field Type                |
    /// |----------------------------|---------------------------|
    /// | `catalog_name`             | `utf8`                    |
    /// | `catalog_db_schemas`       | `list<DB_SCHEMA_SCHEMA>`  |
    ///
    /// `DB_SCHEMA_SCHEMA` is a Struct with fields:
    ///
    /// | Field Name                 | Field Type                |
    /// |----------------------------|---------------------------|
    /// | `db_schema_name`           | `utf8`                    |
    /// | `db_schema_tables`         | `list<TABLE_SCHEMA>`      |
    ///
    /// `TABLE_SCHEMA` is a Struct with fields:
    ///
    /// | Field Name                 | Field Type                |
    /// |----------------------------|---------------------------|
    /// | `table_name`               | `utf8 not null`           |
    /// | `table_type`               | `utf8 not null`           |
    /// | `table_columns`            | `list<COLUMN_SCHEMA>`     |
    /// | `table_constraints`        | `list<CONSTRAINT_SCHEMA>` |
    ///
    /// `COLUMN_SCHEMA` is a Struct with fields:
    ///
    /// | Field Name                 | Field Type                | Comments |
    /// |----------------------------|---------------------------|----------|
    /// | `column_name`              | `utf8 not null`           |          |
    /// | `ordinal_position`         | `int32`                   | (1)      |
    /// | `remarks`                  | `utf8`                    | (2)      |
    /// | `xdbc_data_type`           | `int16`                   | (3)      |
    /// | `xdbc_type_name`           | `utf8`                    | (3)      |
    /// | `xdbc_column_size`         | `int32`                   | (3)      |
    /// | `xdbc_decimal_digits`      | `int16`                   | (3)      |
    /// | `xdbc_num_prec_radix`      | `int16`                   | (3)      |
    /// | `xdbc_nullable`            | `int16`                   | (3)      |
    /// | `xdbc_column_def`          | `utf8`                    | (3)      |
    /// | `xdbc_sql_data_type`       | `int16`                   | (3)      |
    /// | `xdbc_datetime_sub`        | `int16`                   | (3)      |
    /// | `xdbc_char_octet_length`   | `int32`                   | (3)      |
    /// | `xdbc_is_nullable`         | `utf8`                    | (3)      |
    /// | `xdbc_scope_catalog`       | `utf8`                    | (3)      |
    /// | `xdbc_scope_schema`        | `utf8`                    | (3)      |
    /// | `xdbc_scope_table`         | `utf8`                    | (3)      |
    /// | `xdbc_is_autoincrement`    | `bool`                    | (3)      |
    /// | `xdbc_is_generatedcolumn`  | `bool`                    | (3)      |
    ///
    /// 1. The column's ordinal position in the table (starting from 1).
    /// 2. Database-specific description of the column.
    /// 3. Optional value.  Should be null if not supported by the driver.
    ///    xdbc_ values are meant to provide JDBC/ODBC-compatible metadata
    ///    in an agnostic manner.
    ///
    /// `CONSTRAINT_SCHEMA` is a Struct with fields:
    ///
    /// | Field Name                 | Field Type                | Comments |
    /// |----------------------------|---------------------------|----------|
    /// | `constraint_name`          | `utf8`                    |          |
    /// | `constraint_type`          | `utf8 not null`           | (1)      |
    /// | `constraint_column_names`  | `list<utf8> not null`     | (2)      |
    /// | `constraint_column_usage`  | `list<USAGE_SCHEMA>`      | (3)      |
    ///
    /// 1. One of 'CHECK', 'FOREIGN KEY', 'PRIMARY KEY', or 'UNIQUE'.
    /// 2. The columns on the current table that are constrained, in
    ///    order.
    /// 3. For FOREIGN KEY only, the referenced table and columns.
    ///
    /// `USAGE_SCHEMA` is a Struct with fields:
    ///
    /// | Field Name                 | Field Type              |
    /// |----------------------------|-------------------------|
    /// | `fk_catalog`               | `utf8`                    |
    /// | `fk_db_schema`             | `utf8`                    |
    /// | `fk_table`                 | `utf8 not null`           |
    /// | `fk_column_name`           | `utf8 not null`           |
    ///
    /// # Parameters
    ///
    /// * **depth**: The level of nesting to display. If [AdbcObjectDepth::All], display
    ///   all levels. If [AdbcObjectDepth::Catalogs], display only catalogs (i.e.  `catalog_schemas`
    ///   will be null). If [AdbcObjectDepth::DBSchemas], display only catalogs and schemas
    ///   (i.e. `db_schema_tables` will be null), and so on.
    /// * **catalog**: Only show tables in the given catalog. If None,
    ///   do not filter by catalog. If an empty string, only show tables
    ///   without a catalog.  May be a search pattern (see next section).
    /// * **db_schema**: Only show tables in the given database schema. If
    ///   None, do not filter by database schema. If an empty string, only show
    ///   tables without a database schema. May be a search pattern (see next section).
    /// * **table_name**: Only show tables with the given name. If None, do not
    ///   filter by name. May be a search pattern (see next section).
    /// * **table_type**: Only show tables matching one of the given table
    ///   types. If None, show tables of any type. Valid table types should
    ///   match those returned by [AdbcConnection::get_table_schema].
    /// * **column_name**: Only show columns with the given name. If
    ///   None, do not filter by name.  May be a search pattern (see next section).
    ///
    /// # Search patterns
    ///
    /// Some parameters accept "search patterns", which are
    /// strings that can contain the special character `"%"` to match zero
    /// or more characters, or `"_"` to match exactly one character.  (See
    /// the documentation of DatabaseMetaData in JDBC or "Pattern Value
    /// Arguments" in the ODBC documentation.)
    fn get_objects(
        &self,
        depth: AdbcObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: &[&str],
        column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader>, Self::Error>;

    /// Get the Arrow schema of a table.
    ///
    /// `catalog` or `db_schema` may be `None` when not applicable.
    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema, Self::Error>;

    // TODO: should this just return &[&str]?
    /// Get a list of table types in the database.
    ///
    /// The result is an Arrow dataset with the following schema:
    ///
    /// Field Name       | Field Type
    /// -----------------|--------------
    /// `table_type`     | `utf8 not null`
    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader>, Self::Error>;

    // TODO: if Connection is only meant to be access from a single thread, does
    // that mean these partitions should be accessible across connections? Or
    // That read_partition should be called on the main thread and then the
    // RBR are then passed to other threads?
    /// Read part of a partitioned result set.
    fn read_partition(
        &self,
        partition: &[u8],
    ) -> Result<Box<dyn RecordBatchReader>, Self::Error>;

    /// Commit any pending transactions. Only used if autocommit is disabled.
    fn commit(&self) -> Result<(), Self::Error>;

    /// Roll back any pending transactions. Only used if autocommit is disabled.
    fn rollback(&self) -> Result<(), Self::Error>;
}

/// A container for all state needed to execute a database query, such as the
/// query itself, parameters for prepared statements, driver parameters, etc.
///
/// Statements may represent queries or prepared statements.
///
/// Statements may be used multiple times and can be reconfigured
/// (e.g. they can be reused to execute multiple different queries).
/// However, executing a statement (and changing certain other state)
/// will invalidate result sets obtained prior to that execution.
///
/// Multiple statements may be created from a single connection.
/// However, the driver may block or error if they are used
/// concurrently (whether from a single thread or multiple threads).
pub trait AdbcStatement {
    type Error: AdbcError;
    type ConnectionType: AdbcConnection + Default;

    /// Create a new statement.
    ///
    /// The conn should be saved within the struct.
    fn new_from_connection(conn: Rc<Self::ConnectionType>) -> Self;

    /// Turn this statement into a prepared statement to be executed multiple times.
    ///
    /// This should return an error if called before [AdbcStatement::set_sql_query].
    fn prepare(&self) -> Result<(), Self::Error>;

    /// Set a string option on a statement.
    fn set_option(&mut self, key: &str, value: &str) -> Result<(), Self::Error>;

    /// Set the SQL query to execute.
    fn set_sql_query(&mut self, query: &str) -> Result<(), Self::Error>;

    /// Set the Substrait plan to execute.
    fn set_substrait_plan(&mut self, plan: &[u8]) -> Result<(), Self::Error>;

    /// Get the schema for bound parameters.
    ///
    /// This retrieves an Arrow schema describing the number, names, and
    /// types of the parameters in a parameterized statement.  The fields
    /// of the schema should be in order of the ordinal position of the
    /// parameters; named parameters should appear only once.
    ///
    /// If the parameter does not have a name, or the name cannot be
    /// determined, the name of the corresponding field in the schema will
    /// be an empty string.  If the type cannot be determined, the type of
    /// the corresponding field will be NA (NullType).
    ///
    /// This should return an error if this was called before [AdbcStatement::prepare].
    fn get_param_schema(&self) -> Result<Schema, Self::Error>;

    /// Bind Arrow data, either for bulk inserts or prepared statements.
    fn bind_data(&mut self, arr: ArrayRef) -> Result<(), Self::Error>;

    /// Bind Arrow data, either for bulk inserts or prepared statements.
    fn bind_stream(
        &mut self,
        stream: Box<dyn RecordBatchReader>,
    ) -> Result<(), Self::Error>;

    /// Execute a statement and get the results.
    ///
    /// See [StatementResult].
    fn execute(&self) -> Result<StatementResult, Self::Error>;

    /// Execute a statement with a partitioned result set.
    ///
    /// This is not required to be implemented, as it only applies to backends
    /// that internally partition results. These backends can use this method
    /// to support threaded or distributed clients.
    ///
    /// See [PartitionedStatementResult].
    fn execute_partitioned(&self) -> Result<PartitionedStatementResult, Self::Error>;
}

/// Result of calling [AdbcStatement::execute].
///
/// `result` may be None if there is no meaningful result.
/// `row_affected` may be None if not applicable or if it is not supported.
pub struct StatementResult {
    pub result: Option<Box<dyn RecordBatchReader>>,
    pub rows_affected: Option<i64>,
}

/// Partitioned results
///
/// [AdbcConnection::read_partition] will be called to get the output stream
/// for each partition.
///
/// These may be used by a multi-threaded or a distributed client. Each partition
/// will be retrieved by a separate connection. For in-memory databases, these
/// may be connections on different threads that all reference the same database.
/// For remote databases, these may be connections in different processes.
#[derive(Debug, Clone)]
pub struct PartitionedStatementResult {
    pub schema: Schema,
    pub partition_ids: Vec<Vec<u8>>,
    pub rows_affected: Option<i64>,
}

/// Expose an ADBC driver entrypoint for the given statement type.
///
/// The type must implement [AdbcStatement].
#[macro_export]
macro_rules! adbc_api {
    ($statement_type:ident) => {
        mod _adbc_api {
            use super::$statement_type;
            use arrow_adbc::error::{AdbcStatusCode, FFI_AdbcError};
            use arrow_adbc::ffi::FFI_AdbcDriver;

            // TODO: Should this not be mangled? Is there a risk of symbol name collisions?
            // No mangle, since this is the main entrypoint.
            #[no_mangle]
            pub extern "C" fn AdbcDriverInit(
                version: ::std::os::raw::c_int,
                driver: *mut ::std::os::raw::c_void,
                mut error: *mut FFI_AdbcError,
            ) -> AdbcStatusCode {
                if version != 1000000 {
                    unsafe {
                        FFI_AdbcError::set_message(
                            error,
                            &format!("Unsupported ADBC version: {}", version),
                        );
                    }
                    return AdbcStatusCode::NotImplemented;
                }
                let driver_raw = arrow_adbc::interface::internal::init_adbc_driver::<
                    $statement_type,
                >();
                unsafe {
                    std::ptr::write_unaligned(driver as *mut FFI_AdbcDriver, driver_raw);
                }
                AdbcStatusCode::Ok
            }
        }
    };
}

pub use adbc_api;
