// This file was automatically generated through the build.rs script, and should not be edited.

///
/// Represents a metadata request. Used in the command member of FlightDescriptor
/// for the following RPC calls:
///  - GetSchema: return the Arrow schema of the query.
///  - GetFlightInfo: execute the metadata request.
///
/// The returned Arrow schema will be:
/// <
///  info_name: uint32 not null,
///  value: dense_union<
///              string_value: utf8,
///              bool_value: bool,
///              bigint_value: int64,
///              int32_bitmask: int32,
///              string_list: list<string_data: utf8>
///              int32_to_int32_list_map: map<key: int32, value: list<$data$: int32>>
/// >
/// where there is one row per requested piece of metadata information.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetSqlInfo {
    ///
    /// Values are modelled after ODBC's SQLGetInfo() function. This information is intended to provide
    /// Flight SQL clients with basic, SQL syntax and SQL functions related information.
    /// More information types can be added in future releases.
    /// E.g. more SQL syntax support types, scalar functions support, type conversion support etc.
    ///
    /// Note that the set of metadata may expand.
    ///
    /// Initially, Flight SQL will support the following information types:
    /// - Server Information - Range [0-500)
    /// - Syntax Information - Range [500-1000)
    /// Range [0-10,000) is reserved for defaults (see SqlInfo enum for default options). 
    /// Custom options should start at 10,000.
    ///
    /// If omitted, then all metadata will be retrieved.
    /// Flight SQL Servers may choose to include additional metadata above and beyond the specified set, however they must
    /// at least return the specified set. IDs ranging from 0 to 10,000 (exclusive) are reserved for future use.
    /// If additional metadata is included, the metadata IDs should start from 10,000.
    #[prost(uint32, repeated, tag="1")]
    pub info: ::prost::alloc::vec::Vec<u32>,
}
///
/// Represents a request to retrieve the list of catalogs on a Flight SQL enabled backend.
/// The definition of a catalog depends on vendor/implementation. It is usually the database itself
/// Used in the command member of FlightDescriptor for the following RPC calls:
///  - GetSchema: return the Arrow schema of the query.
///  - GetFlightInfo: execute the catalog metadata request.
///
/// The returned Arrow schema will be:
/// <
///  catalog_name: utf8 not null
/// >
/// The returned data should be ordered by catalog_name.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetCatalogs {
}
///
/// Represents a request to retrieve the list of database schemas on a Flight SQL enabled backend.
/// The definition of a database schema depends on vendor/implementation. It is usually a collection of tables.
/// Used in the command member of FlightDescriptor for the following RPC calls:
///  - GetSchema: return the Arrow schema of the query.
///  - GetFlightInfo: execute the catalog metadata request.
///
/// The returned Arrow schema will be:
/// <
///  catalog_name: utf8,
///  db_schema_name: utf8 not null
/// >
/// The returned data should be ordered by catalog_name, then db_schema_name.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetDbSchemas {
    ///
    /// Specifies the Catalog to search for the tables.
    /// An empty string retrieves those without a catalog.
    /// If omitted the catalog name should not be used to narrow the search.
    #[prost(string, optional, tag="1")]
    pub catalog: ::core::option::Option<::prost::alloc::string::String>,
    ///
    /// Specifies a filter pattern for schemas to search for.
    /// When no db_schema_filter_pattern is provided, the pattern will not be used to narrow the search.
    /// In the pattern string, two special characters can be used to denote matching rules:
    ///    - "%" means to match any substring with 0 or more characters.
    ///    - "_" means to match any one character.
    #[prost(string, optional, tag="2")]
    pub db_schema_filter_pattern: ::core::option::Option<::prost::alloc::string::String>,
}
///
/// Represents a request to retrieve the list of tables, and optionally their schemas, on a Flight SQL enabled backend.
/// Used in the command member of FlightDescriptor for the following RPC calls:
///  - GetSchema: return the Arrow schema of the query.
///  - GetFlightInfo: execute the catalog metadata request.
///
/// The returned Arrow schema will be:
/// <
///  catalog_name: utf8,
///  db_schema_name: utf8,
///  table_name: utf8 not null,
///  table_type: utf8 not null,
///  \[optional\] table_schema: bytes not null (schema of the table as described in Schema.fbs::Schema,
///                                           it is serialized as an IPC message.)
/// >
/// The returned data should be ordered by catalog_name, db_schema_name, table_name, then table_type, followed by table_schema if requested.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetTables {
    ///
    /// Specifies the Catalog to search for the tables.
    /// An empty string retrieves those without a catalog.
    /// If omitted the catalog name should not be used to narrow the search.
    #[prost(string, optional, tag="1")]
    pub catalog: ::core::option::Option<::prost::alloc::string::String>,
    ///
    /// Specifies a filter pattern for schemas to search for.
    /// When no db_schema_filter_pattern is provided, all schemas matching other filters are searched.
    /// In the pattern string, two special characters can be used to denote matching rules:
    ///    - "%" means to match any substring with 0 or more characters.
    ///    - "_" means to match any one character.
    #[prost(string, optional, tag="2")]
    pub db_schema_filter_pattern: ::core::option::Option<::prost::alloc::string::String>,
    ///
    /// Specifies a filter pattern for tables to search for.
    /// When no table_name_filter_pattern is provided, all tables matching other filters are searched.
    /// In the pattern string, two special characters can be used to denote matching rules:
    ///    - "%" means to match any substring with 0 or more characters.
    ///    - "_" means to match any one character.
    #[prost(string, optional, tag="3")]
    pub table_name_filter_pattern: ::core::option::Option<::prost::alloc::string::String>,
    ///
    /// Specifies a filter of table types which must match.
    /// The table types depend on vendor/implementation. It is usually used to separate tables from views or system tables.
    /// TABLE, VIEW, and SYSTEM TABLE are commonly supported.
    #[prost(string, repeated, tag="4")]
    pub table_types: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Specifies if the Arrow schema should be returned for found tables.
    #[prost(bool, tag="5")]
    pub include_schema: bool,
}
///
/// Represents a request to retrieve the list of table types on a Flight SQL enabled backend.
/// The table types depend on vendor/implementation. It is usually used to separate tables from views or system tables.
/// TABLE, VIEW, and SYSTEM TABLE are commonly supported.
/// Used in the command member of FlightDescriptor for the following RPC calls:
///  - GetSchema: return the Arrow schema of the query.
///  - GetFlightInfo: execute the catalog metadata request.
///
/// The returned Arrow schema will be:
/// <
///  table_type: utf8 not null
/// >
/// The returned data should be ordered by table_type.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetTableTypes {
}
///
/// Represents a request to retrieve the primary keys of a table on a Flight SQL enabled backend.
/// Used in the command member of FlightDescriptor for the following RPC calls:
///  - GetSchema: return the Arrow schema of the query.
///  - GetFlightInfo: execute the catalog metadata request.
///
/// The returned Arrow schema will be:
/// <
///  catalog_name: utf8,
///  db_schema_name: utf8,
///  table_name: utf8 not null,
///  column_name: utf8 not null,
///  key_name: utf8,
///  key_sequence: int not null
/// >
/// The returned data should be ordered by catalog_name, db_schema_name, table_name, key_name, then key_sequence.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetPrimaryKeys {
    ///
    /// Specifies the catalog to search for the table.
    /// An empty string retrieves those without a catalog.
    /// If omitted the catalog name should not be used to narrow the search.
    #[prost(string, optional, tag="1")]
    pub catalog: ::core::option::Option<::prost::alloc::string::String>,
    ///
    /// Specifies the schema to search for the table.
    /// An empty string retrieves those without a schema.
    /// If omitted the schema name should not be used to narrow the search.
    #[prost(string, optional, tag="2")]
    pub db_schema: ::core::option::Option<::prost::alloc::string::String>,
    /// Specifies the table to get the primary keys for.
    #[prost(string, tag="3")]
    pub table: ::prost::alloc::string::String,
}
///
/// Represents a request to retrieve a description of the foreign key columns that reference the given table's
/// primary key columns (the foreign keys exported by a table) of a table on a Flight SQL enabled backend.
/// Used in the command member of FlightDescriptor for the following RPC calls:
///  - GetSchema: return the Arrow schema of the query.
///  - GetFlightInfo: execute the catalog metadata request.
///
/// The returned Arrow schema will be:
/// <
///  pk_catalog_name: utf8,
///  pk_db_schema_name: utf8,
///  pk_table_name: utf8 not null,
///  pk_column_name: utf8 not null,
///  fk_catalog_name: utf8,
///  fk_db_schema_name: utf8,
///  fk_table_name: utf8 not null,
///  fk_column_name: utf8 not null,
///  key_sequence: int not null,
///  fk_key_name: utf8,
///  pk_key_name: utf8,
///  update_rule: uint1 not null,
///  delete_rule: uint1 not null
/// >
/// The returned data should be ordered by fk_catalog_name, fk_db_schema_name, fk_table_name, fk_key_name, then key_sequence.
/// update_rule and delete_rule returns a byte that is equivalent to actions declared on UpdateDeleteRules enum.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetExportedKeys {
    ///
    /// Specifies the catalog to search for the foreign key table.
    /// An empty string retrieves those without a catalog.
    /// If omitted the catalog name should not be used to narrow the search.
    #[prost(string, optional, tag="1")]
    pub catalog: ::core::option::Option<::prost::alloc::string::String>,
    ///
    /// Specifies the schema to search for the foreign key table.
    /// An empty string retrieves those without a schema.
    /// If omitted the schema name should not be used to narrow the search.
    #[prost(string, optional, tag="2")]
    pub db_schema: ::core::option::Option<::prost::alloc::string::String>,
    /// Specifies the foreign key table to get the foreign keys for.
    #[prost(string, tag="3")]
    pub table: ::prost::alloc::string::String,
}
///
/// Represents a request to retrieve the foreign keys of a table on a Flight SQL enabled backend.
/// Used in the command member of FlightDescriptor for the following RPC calls:
///  - GetSchema: return the Arrow schema of the query.
///  - GetFlightInfo: execute the catalog metadata request.
///
/// The returned Arrow schema will be:
/// <
///  pk_catalog_name: utf8,
///  pk_db_schema_name: utf8,
///  pk_table_name: utf8 not null,
///  pk_column_name: utf8 not null,
///  fk_catalog_name: utf8,
///  fk_db_schema_name: utf8,
///  fk_table_name: utf8 not null,
///  fk_column_name: utf8 not null,
///  key_sequence: int not null,
///  fk_key_name: utf8,
///  pk_key_name: utf8,
///  update_rule: uint1 not null,
///  delete_rule: uint1 not null
/// >
/// The returned data should be ordered by pk_catalog_name, pk_db_schema_name, pk_table_name, pk_key_name, then key_sequence.
/// update_rule and delete_rule returns a byte that is equivalent to actions:
///    - 0 = CASCADE
///    - 1 = RESTRICT
///    - 2 = SET NULL
///    - 3 = NO ACTION
///    - 4 = SET DEFAULT
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetImportedKeys {
    ///
    /// Specifies the catalog to search for the primary key table.
    /// An empty string retrieves those without a catalog.
    /// If omitted the catalog name should not be used to narrow the search.
    #[prost(string, optional, tag="1")]
    pub catalog: ::core::option::Option<::prost::alloc::string::String>,
    ///
    /// Specifies the schema to search for the primary key table.
    /// An empty string retrieves those without a schema.
    /// If omitted the schema name should not be used to narrow the search.
    #[prost(string, optional, tag="2")]
    pub db_schema: ::core::option::Option<::prost::alloc::string::String>,
    /// Specifies the primary key table to get the foreign keys for.
    #[prost(string, tag="3")]
    pub table: ::prost::alloc::string::String,
}
///
/// Represents a request to retrieve a description of the foreign key columns in the given foreign key table that
/// reference the primary key or the columns representing a unique constraint of the parent table (could be the same
/// or a different table) on a Flight SQL enabled backend.
/// Used in the command member of FlightDescriptor for the following RPC calls:
///  - GetSchema: return the Arrow schema of the query.
///  - GetFlightInfo: execute the catalog metadata request.
///
/// The returned Arrow schema will be:
/// <
///  pk_catalog_name: utf8,
///  pk_db_schema_name: utf8,
///  pk_table_name: utf8 not null,
///  pk_column_name: utf8 not null,
///  fk_catalog_name: utf8,
///  fk_db_schema_name: utf8,
///  fk_table_name: utf8 not null,
///  fk_column_name: utf8 not null,
///  key_sequence: int not null,
///  fk_key_name: utf8,
///  pk_key_name: utf8,
///  update_rule: uint1 not null,
///  delete_rule: uint1 not null
/// >
/// The returned data should be ordered by pk_catalog_name, pk_db_schema_name, pk_table_name, pk_key_name, then key_sequence.
/// update_rule and delete_rule returns a byte that is equivalent to actions:
///    - 0 = CASCADE
///    - 1 = RESTRICT
///    - 2 = SET NULL
///    - 3 = NO ACTION
///    - 4 = SET DEFAULT
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandGetCrossReference {
    ///*
    /// The catalog name where the parent table is.
    /// An empty string retrieves those without a catalog.
    /// If omitted the catalog name should not be used to narrow the search.
    #[prost(string, optional, tag="1")]
    pub pk_catalog: ::core::option::Option<::prost::alloc::string::String>,
    ///*
    /// The Schema name where the parent table is.
    /// An empty string retrieves those without a schema.
    /// If omitted the schema name should not be used to narrow the search.
    #[prost(string, optional, tag="2")]
    pub pk_db_schema: ::core::option::Option<::prost::alloc::string::String>,
    ///*
    /// The parent table name. It cannot be null.
    #[prost(string, tag="3")]
    pub pk_table: ::prost::alloc::string::String,
    ///*
    /// The catalog name where the foreign table is.
    /// An empty string retrieves those without a catalog.
    /// If omitted the catalog name should not be used to narrow the search.
    #[prost(string, optional, tag="4")]
    pub fk_catalog: ::core::option::Option<::prost::alloc::string::String>,
    ///*
    /// The schema name where the foreign table is.
    /// An empty string retrieves those without a schema.
    /// If omitted the schema name should not be used to narrow the search.
    #[prost(string, optional, tag="5")]
    pub fk_db_schema: ::core::option::Option<::prost::alloc::string::String>,
    ///*
    /// The foreign table name. It cannot be null.
    #[prost(string, tag="6")]
    pub fk_table: ::prost::alloc::string::String,
}
// SQL Execution Action Messages

///
/// Request message for the "CreatePreparedStatement" action on a Flight SQL enabled backend.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActionCreatePreparedStatementRequest {
    /// The valid SQL string to create a prepared statement for.
    #[prost(string, tag="1")]
    pub query: ::prost::alloc::string::String,
}
///
/// Wrap the result of a "GetPreparedStatement" action.
///
/// The resultant PreparedStatement can be closed either:
/// - Manually, through the "ClosePreparedStatement" action;
/// - Automatically, by a server timeout.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActionCreatePreparedStatementResult {
    /// Opaque handle for the prepared statement on the server.
    #[prost(bytes="vec", tag="1")]
    pub prepared_statement_handle: ::prost::alloc::vec::Vec<u8>,
    /// If a result set generating query was provided, dataset_schema contains the 
    /// schema of the dataset as described in Schema.fbs::Schema, it is serialized as an IPC message.
    #[prost(bytes="vec", tag="2")]
    pub dataset_schema: ::prost::alloc::vec::Vec<u8>,
    /// If the query provided contained parameters, parameter_schema contains the 
    /// schema of the expected parameters as described in Schema.fbs::Schema, it is serialized as an IPC message.
    #[prost(bytes="vec", tag="3")]
    pub parameter_schema: ::prost::alloc::vec::Vec<u8>,
}
///
/// Request message for the "ClosePreparedStatement" action on a Flight SQL enabled backend.
/// Closes server resources associated with the prepared statement handle.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ActionClosePreparedStatementRequest {
    /// Opaque handle for the prepared statement on the server.
    #[prost(bytes="vec", tag="1")]
    pub prepared_statement_handle: ::prost::alloc::vec::Vec<u8>,
}
// SQL Execution Messages.

///
/// Represents a SQL query. Used in the command member of FlightDescriptor
/// for the following RPC calls:
///  - GetSchema: return the Arrow schema of the query.
///  - GetFlightInfo: execute the query.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandStatementQuery {
    /// The SQL syntax.
    #[prost(string, tag="1")]
    pub query: ::prost::alloc::string::String,
}
///*
/// Represents a ticket resulting from GetFlightInfo with a CommandStatementQuery.
/// This should be used only once and treated as an opaque value, that is, clients should not attempt to parse this.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct TicketStatementQuery {
    /// Unique identifier for the instance of the statement to execute.
    #[prost(bytes="vec", tag="1")]
    pub statement_handle: ::prost::alloc::vec::Vec<u8>,
}
///
/// Represents an instance of executing a prepared statement. Used in the command member of FlightDescriptor for
/// the following RPC calls:
///  - DoPut: bind parameter values. All of the bound parameter sets will be executed as a single atomic execution.
///  - GetFlightInfo: execute the prepared statement instance.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandPreparedStatementQuery {
    /// Opaque handle for the prepared statement on the server.
    #[prost(bytes="vec", tag="1")]
    pub prepared_statement_handle: ::prost::alloc::vec::Vec<u8>,
}
///
/// Represents a SQL update query. Used in the command member of FlightDescriptor
/// for the the RPC call DoPut to cause the server to execute the included SQL update.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandStatementUpdate {
    /// The SQL syntax.
    #[prost(string, tag="1")]
    pub query: ::prost::alloc::string::String,
}
///
/// Represents a SQL update query. Used in the command member of FlightDescriptor
/// for the the RPC call DoPut to cause the server to execute the included 
/// prepared statement handle as an update.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CommandPreparedStatementUpdate {
    /// Opaque handle for the prepared statement on the server.
    #[prost(bytes="vec", tag="1")]
    pub prepared_statement_handle: ::prost::alloc::vec::Vec<u8>,
}
///
/// Returned from the RPC call DoPut when a CommandStatementUpdate
/// CommandPreparedStatementUpdate was in the request, containing
/// results from the update. 
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DoPutUpdateResult {
    /// The number of records updated. A return value of -1 represents 
    /// an unknown updated record count.
    #[prost(int64, tag="1")]
    pub record_count: i64,
}
/// Options for CommandGetSqlInfo.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlInfo {
    // Server Information [0-500): Provides basic information about the Flight SQL Server.

    /// Retrieves a UTF-8 string with the name of the Flight SQL Server.
    FlightSqlServerName = 0,
    /// Retrieves a UTF-8 string with the native version of the Flight SQL Server.
    FlightSqlServerVersion = 1,
    /// Retrieves a UTF-8 string with the Arrow format version of the Flight SQL Server.
    FlightSqlServerArrowVersion = 2,
    /// 
    /// Retrieves a boolean value indicating whether the Flight SQL Server is read only.
    ///
    /// Returns:
    /// - false: if read-write
    /// - true: if read only
    FlightSqlServerReadOnly = 3,
    // SQL Syntax Information [500-1000): provides information about SQL syntax supported by the Flight SQL Server.

    ///
    /// Retrieves a boolean value indicating whether the Flight SQL Server supports CREATE and DROP of catalogs.
    ///
    /// Returns:
    /// - false: if it doesn't support CREATE and DROP of catalogs.
    /// - true: if it supports CREATE and DROP of catalogs.
    SqlDdlCatalog = 500,
    ///
    /// Retrieves a boolean value indicating whether the Flight SQL Server supports CREATE and DROP of schemas.
    ///
    /// Returns:
    /// - false: if it doesn't support CREATE and DROP of schemas.
    /// - true: if it supports CREATE and DROP of schemas.
    SqlDdlSchema = 501,
    ///
    /// Indicates whether the Flight SQL Server supports CREATE and DROP of tables.
    ///
    /// Returns:
    /// - false: if it doesn't support CREATE and DROP of tables.
    /// - true: if it supports CREATE and DROP of tables.
    SqlDdlTable = 502,
    ///
    /// Retrieves a uint32 value representing the enu uint32 ordinal for the case sensitivity of catalog, table, schema and table names.
    ///
    /// The possible values are listed in `arrow.flight.protocol.sql.SqlSupportedCaseSensitivity`.
    SqlIdentifierCase = 503,
    /// Retrieves a UTF-8 string with the supported character(s) used to surround a delimited identifier.
    SqlIdentifierQuoteChar = 504,
    ///
    /// Retrieves a uint32 value representing the enu uint32 ordinal for the case sensitivity of quoted identifiers.
    ///
    /// The possible values are listed in `arrow.flight.protocol.sql.SqlSupportedCaseSensitivity`.
    SqlQuotedIdentifierCase = 505,
    ///
    /// Retrieves a boolean value indicating whether all tables are selectable.
    ///
    /// Returns:
    /// - false: if not all tables are selectable or if none are;
    /// - true: if all tables are selectable.
    SqlAllTablesAreSelectable = 506,
    ///
    /// Retrieves the null ordering.
    ///
    /// Returns a uint32 ordinal for the null ordering being used, as described in
    /// `arrow.flight.protocol.sql.SqlNullOrdering`.
    SqlNullOrdering = 507,
    /// Retrieves a UTF-8 string list with values of the supported keywords.
    SqlKeywords = 508,
    /// Retrieves a UTF-8 string list with values of the supported numeric functions.
    SqlNumericFunctions = 509,
    /// Retrieves a UTF-8 string list with values of the supported string functions.
    SqlStringFunctions = 510,
    /// Retrieves a UTF-8 string list with values of the supported system functions.
    SqlSystemFunctions = 511,
    /// Retrieves a UTF-8 string list with values of the supported datetime functions.
    SqlDatetimeFunctions = 512,
    ///
    /// Retrieves the UTF-8 string that can be used to escape wildcard characters.
    /// This is the string that can be used to escape '_' or '%' in the catalog search parameters that are a pattern
    /// (and therefore use one of the wildcard characters).
    /// The '_' character represents any single character; the '%' character represents any sequence of zero or more
    /// characters.
    SqlSearchStringEscape = 513,
    ///
    /// Retrieves a UTF-8 string with all the "extra" characters that can be used in unquoted identifier names
    /// (those beyond a-z, A-Z, 0-9 and _).
    SqlExtraNameCharacters = 514,
    ///
    /// Retrieves a boolean value indicating whether column aliasing is supported.
    /// If so, the SQL AS clause can be used to provide names for computed columns or to provide alias names for columns
    /// as required.
    ///
    /// Returns:
    /// - false: if column aliasing is unsupported;
    /// - true: if column aliasing is supported.
    SqlSupportsColumnAliasing = 515,
    ///
    /// Retrieves a boolean value indicating whether concatenations between null and non-null values being
    /// null are supported.
    ///
    /// - Returns:
    /// - false: if concatenations between null and non-null values being null are unsupported;
    /// - true: if concatenations between null and non-null values being null are supported.
    SqlNullPlusNullIsNull = 516,
    ///
    /// Retrieves a map where the key is the type to convert from and the value is a list with the types to convert to,
    /// indicating the supported conversions. Each key and each item on the list value is a value to a predefined type on
    /// SqlSupportsConvert enum.
    /// The returned map will be:  map<int32, list<int32>>
    SqlSupportsConvert = 517,
    ///
    /// Retrieves a boolean value indicating whether, when table correlation names are supported,
    /// they are restricted to being different from the names of the tables.
    ///
    /// Returns:
    /// - false: if table correlation names are unsupported;
    /// - true: if table correlation names are supported.
    SqlSupportsTableCorrelationNames = 518,
    ///
    /// Retrieves a boolean value indicating whether, when table correlation names are supported,
    /// they are restricted to being different from the names of the tables.
    ///
    /// Returns:
    /// - false: if different table correlation names are unsupported;
    /// - true: if different table correlation names are supported
    SqlSupportsDifferentTableCorrelationNames = 519,
    ///
    /// Retrieves a boolean value indicating whether expressions in ORDER BY lists are supported.
    ///
    /// Returns:
    /// - false: if expressions in ORDER BY are unsupported;
    /// - true: if expressions in ORDER BY are supported;
    SqlSupportsExpressionsInOrderBy = 520,
    ///
    /// Retrieves a boolean value indicating whether using a column that is not in the SELECT statement in a GROUP BY
    /// clause is supported.
    ///
    /// Returns:
    /// - false: if using a column that is not in the SELECT statement in a GROUP BY clause is unsupported;
    /// - true: if using a column that is not in the SELECT statement in a GROUP BY clause is supported.
    SqlSupportsOrderByUnrelated = 521,
    ///
    /// Retrieves the supported GROUP BY commands;
    ///
    /// Returns an int32 bitmask value representing the supported commands.
    /// The returned bitmask should be parsed in order to retrieve the supported commands.
    ///
    /// For instance:
    /// - return 0 (\b0)   => [] (GROUP BY is unsupported);
    /// - return 1 (\b1)   => \[SQL_GROUP_BY_UNRELATED\];
    /// - return 2 (\b10)  => \[SQL_GROUP_BY_BEYOND_SELECT\];
    /// - return 3 (\b11)  => [SQL_GROUP_BY_UNRELATED, SQL_GROUP_BY_BEYOND_SELECT].
    /// Valid GROUP BY types are described under `arrow.flight.protocol.sql.SqlSupportedGroupBy`.
    SqlSupportedGroupBy = 522,
    ///
    /// Retrieves a boolean value indicating whether specifying a LIKE escape clause is supported.
    ///
    /// Returns:
    /// - false: if specifying a LIKE escape clause is unsupported;
    /// - true: if specifying a LIKE escape clause is supported.
    SqlSupportsLikeEscapeClause = 523,
    ///
    /// Retrieves a boolean value indicating whether columns may be defined as non-nullable.
    ///
    /// Returns:
    /// - false: if columns cannot be defined as non-nullable;
    /// - true: if columns may be defined as non-nullable.
    SqlSupportsNonNullableColumns = 524,
    ///
    /// Retrieves the supported SQL grammar level as per the ODBC specification.
    ///
    /// Returns an int32 bitmask value representing the supported SQL grammar level.
    /// The returned bitmask should be parsed in order to retrieve the supported grammar levels.
    ///
    /// For instance:
    /// - return 0 (\b0)   => [] (SQL grammar is unsupported);
    /// - return 1 (\b1)   => \[SQL_MINIMUM_GRAMMAR\];
    /// - return 2 (\b10)  => \[SQL_CORE_GRAMMAR\];
    /// - return 3 (\b11)  => [SQL_MINIMUM_GRAMMAR, SQL_CORE_GRAMMAR];
    /// - return 4 (\b100) => \[SQL_EXTENDED_GRAMMAR\];
    /// - return 5 (\b101) => [SQL_MINIMUM_GRAMMAR, SQL_EXTENDED_GRAMMAR];
    /// - return 6 (\b110) => [SQL_CORE_GRAMMAR, SQL_EXTENDED_GRAMMAR];
    /// - return 7 (\b111) => [SQL_MINIMUM_GRAMMAR, SQL_CORE_GRAMMAR, SQL_EXTENDED_GRAMMAR].
    /// Valid SQL grammar levels are described under `arrow.flight.protocol.sql.SupportedSqlGrammar`.
    SqlSupportedGrammar = 525,
    ///
    /// Retrieves the supported ANSI92 SQL grammar level.
    ///
    /// Returns an int32 bitmask value representing the supported ANSI92 SQL grammar level.
    /// The returned bitmask should be parsed in order to retrieve the supported commands.
    ///
    /// For instance:
    /// - return 0 (\b0)   => [] (ANSI92 SQL grammar is unsupported);
    /// - return 1 (\b1)   => \[ANSI92_ENTRY_SQL\];
    /// - return 2 (\b10)  => \[ANSI92_INTERMEDIATE_SQL\];
    /// - return 3 (\b11)  => [ANSI92_ENTRY_SQL, ANSI92_INTERMEDIATE_SQL];
    /// - return 4 (\b100) => \[ANSI92_FULL_SQL\];
    /// - return 5 (\b101) => [ANSI92_ENTRY_SQL, ANSI92_FULL_SQL];
    /// - return 6 (\b110) => [ANSI92_INTERMEDIATE_SQL, ANSI92_FULL_SQL];
    /// - return 7 (\b111) => [ANSI92_ENTRY_SQL, ANSI92_INTERMEDIATE_SQL, ANSI92_FULL_SQL].
    /// Valid ANSI92 SQL grammar levels are described under `arrow.flight.protocol.sql.SupportedAnsi92SqlGrammarLevel`.
    SqlAnsi92SupportedLevel = 526,
    ///
    /// Retrieves a boolean value indicating whether the SQL Integrity Enhancement Facility is supported.
    ///
    /// Returns:
    /// - false: if the SQL Integrity Enhancement Facility is supported;
    /// - true: if the SQL Integrity Enhancement Facility is supported.
    SqlSupportsIntegrityEnhancementFacility = 527,
    ///
    /// Retrieves the support level for SQL OUTER JOINs.
    ///
    /// Returns a uint3 uint32 ordinal for the SQL ordering being used, as described in
    /// `arrow.flight.protocol.sql.SqlOuterJoinsSupportLevel`.
    SqlOuterJoinsSupportLevel = 528,
    /// Retrieves a UTF-8 string with the preferred term for "schema".
    SqlSchemaTerm = 529,
    /// Retrieves a UTF-8 string with the preferred term for "procedure".
    SqlProcedureTerm = 530,
    /// Retrieves a UTF-8 string with the preferred term for "catalog".
    SqlCatalogTerm = 531,
    ///
    /// Retrieves a boolean value indicating whether a catalog appears at the start of a fully qualified table name.
    ///
    /// - false: if a catalog does not appear at the start of a fully qualified table name;
    /// - true: if a catalog appears at the start of a fully qualified table name.
    SqlCatalogAtStart = 532,
    ///
    /// Retrieves the supported actions for a SQL schema.
    ///
    /// Returns an int32 bitmask value representing the supported actions for a SQL schema.
    /// The returned bitmask should be parsed in order to retrieve the supported actions for a SQL schema.
    ///
    /// For instance:
    /// - return 0 (\b0)   => [] (no supported actions for SQL schema);
    /// - return 1 (\b1)   => \[SQL_ELEMENT_IN_PROCEDURE_CALLS\];
    /// - return 2 (\b10)  => \[SQL_ELEMENT_IN_INDEX_DEFINITIONS\];
    /// - return 3 (\b11)  => [SQL_ELEMENT_IN_PROCEDURE_CALLS, SQL_ELEMENT_IN_INDEX_DEFINITIONS];
    /// - return 4 (\b100) => \[SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS\];
    /// - return 5 (\b101) => [SQL_ELEMENT_IN_PROCEDURE_CALLS, SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS];
    /// - return 6 (\b110) => [SQL_ELEMENT_IN_INDEX_DEFINITIONS, SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS];
    /// - return 7 (\b111) => [SQL_ELEMENT_IN_PROCEDURE_CALLS, SQL_ELEMENT_IN_INDEX_DEFINITIONS, SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS].
    /// Valid actions for a SQL schema described under `arrow.flight.protocol.sql.SqlSupportedElementActions`.
    SqlSchemasSupportedActions = 533,
    ///
    /// Retrieves the supported actions for a SQL schema.
    ///
    /// Returns an int32 bitmask value representing the supported actions for a SQL catalog.
    /// The returned bitmask should be parsed in order to retrieve the supported actions for a SQL catalog.
    ///
    /// For instance:
    /// - return 0 (\b0)   => [] (no supported actions for SQL catalog);
    /// - return 1 (\b1)   => \[SQL_ELEMENT_IN_PROCEDURE_CALLS\];
    /// - return 2 (\b10)  => \[SQL_ELEMENT_IN_INDEX_DEFINITIONS\];
    /// - return 3 (\b11)  => [SQL_ELEMENT_IN_PROCEDURE_CALLS, SQL_ELEMENT_IN_INDEX_DEFINITIONS];
    /// - return 4 (\b100) => \[SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS\];
    /// - return 5 (\b101) => [SQL_ELEMENT_IN_PROCEDURE_CALLS, SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS];
    /// - return 6 (\b110) => [SQL_ELEMENT_IN_INDEX_DEFINITIONS, SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS];
    /// - return 7 (\b111) => [SQL_ELEMENT_IN_PROCEDURE_CALLS, SQL_ELEMENT_IN_INDEX_DEFINITIONS, SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS].
    /// Valid actions for a SQL catalog are described under `arrow.flight.protocol.sql.SqlSupportedElementActions`.
    SqlCatalogsSupportedActions = 534,
    ///
    /// Retrieves the supported SQL positioned commands.
    ///
    /// Returns an int32 bitmask value representing the supported SQL positioned commands.
    /// The returned bitmask should be parsed in order to retrieve the supported SQL positioned commands.
    ///
    /// For instance:
    /// - return 0 (\b0)   => [] (no supported SQL positioned commands);
    /// - return 1 (\b1)   => \[SQL_POSITIONED_DELETE\];
    /// - return 2 (\b10)  => \[SQL_POSITIONED_UPDATE\];
    /// - return 3 (\b11)  => [SQL_POSITIONED_DELETE, SQL_POSITIONED_UPDATE].
    /// Valid SQL positioned commands are described under `arrow.flight.protocol.sql.SqlSupportedPositionedCommands`.
    SqlSupportedPositionedCommands = 535,
    ///
    /// Retrieves a boolean value indicating whether SELECT FOR UPDATE statements are supported.
    ///
    /// Returns:
    /// - false: if SELECT FOR UPDATE statements are unsupported;
    /// - true: if SELECT FOR UPDATE statements are supported.
    SqlSelectForUpdateSupported = 536,
    ///
    /// Retrieves a boolean value indicating whether stored procedure calls that use the stored procedure escape syntax
    /// are supported.
    ///
    /// Returns:
    /// - false: if stored procedure calls that use the stored procedure escape syntax are unsupported;
    /// - true: if stored procedure calls that use the stored procedure escape syntax are supported.
    SqlStoredProceduresSupported = 537,
    ///
    /// Retrieves the supported SQL subqueries.
    ///
    /// Returns an int32 bitmask value representing the supported SQL subqueries.
    /// The returned bitmask should be parsed in order to retrieve the supported SQL subqueries.
    ///
    /// For instance:
    /// - return 0   (\b0)     => [] (no supported SQL subqueries);
    /// - return 1   (\b1)     => \[SQL_SUBQUERIES_IN_COMPARISONS\];
    /// - return 2   (\b10)    => \[SQL_SUBQUERIES_IN_EXISTS\];
    /// - return 3   (\b11)    => [SQL_SUBQUERIES_IN_COMPARISONS, SQL_SUBQUERIES_IN_EXISTS];
    /// - return 4   (\b100)   => \[SQL_SUBQUERIES_IN_INS\];
    /// - return 5   (\b101)   => [SQL_SUBQUERIES_IN_COMPARISONS, SQL_SUBQUERIES_IN_INS];
    /// - return 6   (\b110)   => [SQL_SUBQUERIES_IN_INS, SQL_SUBQUERIES_IN_EXISTS];
    /// - return 7   (\b111)   => [SQL_SUBQUERIES_IN_COMPARISONS, SQL_SUBQUERIES_IN_EXISTS, SQL_SUBQUERIES_IN_INS];
    /// - return 8   (\b1000)  => \[SQL_SUBQUERIES_IN_QUANTIFIEDS\];
    /// - return 9   (\b1001)  => [SQL_SUBQUERIES_IN_COMPARISONS, SQL_SUBQUERIES_IN_QUANTIFIEDS];
    /// - return 10  (\b1010)  => [SQL_SUBQUERIES_IN_EXISTS, SQL_SUBQUERIES_IN_QUANTIFIEDS];
    /// - return 11  (\b1011)  => [SQL_SUBQUERIES_IN_COMPARISONS, SQL_SUBQUERIES_IN_EXISTS, SQL_SUBQUERIES_IN_QUANTIFIEDS];
    /// - return 12  (\b1100)  => [SQL_SUBQUERIES_IN_INS, SQL_SUBQUERIES_IN_QUANTIFIEDS];
    /// - return 13  (\b1101)  => [SQL_SUBQUERIES_IN_COMPARISONS, SQL_SUBQUERIES_IN_INS, SQL_SUBQUERIES_IN_QUANTIFIEDS];
    /// - return 14  (\b1110)  => [SQL_SUBQUERIES_IN_EXISTS, SQL_SUBQUERIES_IN_INS, SQL_SUBQUERIES_IN_QUANTIFIEDS];
    /// - return 15  (\b1111)  => [SQL_SUBQUERIES_IN_COMPARISONS, SQL_SUBQUERIES_IN_EXISTS, SQL_SUBQUERIES_IN_INS, SQL_SUBQUERIES_IN_QUANTIFIEDS];
    /// - ...
    /// Valid SQL subqueries are described under `arrow.flight.protocol.sql.SqlSupportedSubqueries`.
    SqlSupportedSubqueries = 538,
    ///
    /// Retrieves a boolean value indicating whether correlated subqueries are supported.
    ///
    /// Returns:
    /// - false: if correlated subqueries are unsupported;
    /// - true: if correlated subqueries are supported.
    SqlCorrelatedSubqueriesSupported = 539,
    ///
    /// Retrieves the supported SQL UNIONs.
    ///
    /// Returns an int32 bitmask value representing the supported SQL UNIONs.
    /// The returned bitmask should be parsed in order to retrieve the supported SQL UNIONs.
    ///
    /// For instance:
    /// - return 0 (\b0)   => [] (no supported SQL positioned commands);
    /// - return 1 (\b1)   => \[SQL_UNION\];
    /// - return 2 (\b10)  => \[SQL_UNION_ALL\];
    /// - return 3 (\b11)  => [SQL_UNION, SQL_UNION_ALL].
    /// Valid SQL positioned commands are described under `arrow.flight.protocol.sql.SqlSupportedUnions`.
    SqlSupportedUnions = 540,
    /// Retrieves a uint32 value representing the maximum number of hex characters allowed in an inline binary literal.
    SqlMaxBinaryLiteralLength = 541,
    /// Retrieves a uint32 value representing the maximum number of characters allowed for a character literal.
    SqlMaxCharLiteralLength = 542,
    /// Retrieves a uint32 value representing the maximum number of characters allowed for a column name.
    SqlMaxColumnNameLength = 543,
    /// Retrieves a uint32 value representing the the maximum number of columns allowed in a GROUP BY clause.
    SqlMaxColumnsInGroupBy = 544,
    /// Retrieves a uint32 value representing the maximum number of columns allowed in an index.
    SqlMaxColumnsInIndex = 545,
    /// Retrieves a uint32 value representing the maximum number of columns allowed in an ORDER BY clause.
    SqlMaxColumnsInOrderBy = 546,
    /// Retrieves a uint32 value representing the maximum number of columns allowed in a SELECT list.
    SqlMaxColumnsInSelect = 547,
    /// Retrieves a uint32 value representing the maximum number of columns allowed in a table.
    SqlMaxColumnsInTable = 548,
    /// Retrieves a uint32 value representing the maximum number of concurrent connections possible.
    SqlMaxConnections = 549,
    /// Retrieves a uint32 value the maximum number of characters allowed in a cursor name.
    SqlMaxCursorNameLength = 550,
    ///
    /// Retrieves a uint32 value representing the maximum number of bytes allowed for an index,
    /// including all of the parts of the index.
    SqlMaxIndexLength = 551,
    /// Retrieves a uint32 value representing the maximum number of characters allowed in a schema name.
    SqlDbSchemaNameLength = 552,
    /// Retrieves a uint32 value representing the maximum number of characters allowed in a procedure name.
    SqlMaxProcedureNameLength = 553,
    /// Retrieves a uint32 value representing the maximum number of characters allowed in a catalog name.
    SqlMaxCatalogNameLength = 554,
    /// Retrieves a uint32 value representing the maximum number of bytes allowed in a single row.
    SqlMaxRowSize = 555,
    ///
    /// Retrieves a boolean indicating whether the return value for the JDBC method getMaxRowSize includes the SQL
    /// data types LONGVARCHAR and LONGVARBINARY.
    ///
    /// Returns:
    /// - false: if return value for the JDBC method getMaxRowSize does
    ///          not include the SQL data types LONGVARCHAR and LONGVARBINARY;
    /// - true: if return value for the JDBC method getMaxRowSize includes
    ///         the SQL data types LONGVARCHAR and LONGVARBINARY.
    SqlMaxRowSizeIncludesBlobs = 556,
    ///
    /// Retrieves a uint32 value representing the maximum number of characters allowed for an SQL statement;
    /// a result of 0 (zero) means that there is no limit or the limit is not known.
    SqlMaxStatementLength = 557,
    /// Retrieves a uint32 value representing the maximum number of active statements that can be open at the same time.
    SqlMaxStatements = 558,
    /// Retrieves a uint32 value representing the maximum number of characters allowed in a table name.
    SqlMaxTableNameLength = 559,
    /// Retrieves a uint32 value representing the maximum number of tables allowed in a SELECT statement.
    SqlMaxTablesInSelect = 560,
    /// Retrieves a uint32 value representing the maximum number of characters allowed in a user name.
    SqlMaxUsernameLength = 561,
    ///
    /// Retrieves this database's default transaction isolation level as described in
    /// `arrow.flight.protocol.sql.SqlTransactionIsolationLevel`.
    ///
    /// Returns a uint32 ordinal for the SQL transaction isolation level.
    SqlDefaultTransactionIsolation = 562,
    ///
    /// Retrieves a boolean value indicating whether transactions are supported. If not, invoking the method commit is a
    /// noop, and the isolation level is `arrow.flight.protocol.sql.SqlTransactionIsolationLevel.TRANSACTION_NONE`.
    ///
    /// Returns:
    /// - false: if transactions are unsupported;
    /// - true: if transactions are supported.
    SqlTransactionsSupported = 563,
    ///
    /// Retrieves the supported transactions isolation levels.
    ///
    /// Returns an int32 bitmask value representing the supported transactions isolation levels.
    /// The returned bitmask should be parsed in order to retrieve the supported transactions isolation levels.
    ///
    /// For instance:
    /// - return 0   (\b0)     => [] (no supported SQL transactions isolation levels);
    /// - return 1   (\b1)     => \[SQL_TRANSACTION_NONE\];
    /// - return 2   (\b10)    => \[SQL_TRANSACTION_READ_UNCOMMITTED\];
    /// - return 3   (\b11)    => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_READ_UNCOMMITTED];
    /// - return 4   (\b100)   => \[SQL_TRANSACTION_REPEATABLE_READ\];
    /// - return 5   (\b101)   => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 6   (\b110)   => [SQL_TRANSACTION_READ_UNCOMMITTED, SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 7   (\b111)   => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_READ_UNCOMMITTED, SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 8   (\b1000)  => \[SQL_TRANSACTION_REPEATABLE_READ\];
    /// - return 9   (\b1001)  => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 10  (\b1010)  => [SQL_TRANSACTION_READ_UNCOMMITTED, SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 11  (\b1011)  => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_READ_UNCOMMITTED, SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 12  (\b1100)  => [SQL_TRANSACTION_REPEATABLE_READ, SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 13  (\b1101)  => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_REPEATABLE_READ, SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 14  (\b1110)  => [SQL_TRANSACTION_READ_UNCOMMITTED, SQL_TRANSACTION_REPEATABLE_READ, SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 15  (\b1111)  => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_READ_UNCOMMITTED, SQL_TRANSACTION_REPEATABLE_READ, SQL_TRANSACTION_REPEATABLE_READ];
    /// - return 16  (\b10000) => \[SQL_TRANSACTION_SERIALIZABLE\];
    /// - ...
    /// Valid SQL positioned commands are described under `arrow.flight.protocol.sql.SqlTransactionIsolationLevel`.
    SqlSupportedTransactionsIsolationLevels = 564,
    ///
    /// Retrieves a boolean value indicating whether a data definition statement within a transaction forces
    /// the transaction to commit.
    ///
    /// Returns:
    /// - false: if a data definition statement within a transaction does not force the transaction to commit;
    /// - true: if a data definition statement within a transaction forces the transaction to commit.
    SqlDataDefinitionCausesTransactionCommit = 565,
    ///
    /// Retrieves a boolean value indicating whether a data definition statement within a transaction is ignored.
    ///
    /// Returns:
    /// - false: if a data definition statement within a transaction is taken into account;
    /// - true: a data definition statement within a transaction is ignored.
    SqlDataDefinitionsInTransactionsIgnored = 566,
    ///
    /// Retrieves an int32 bitmask value representing the supported result set types.
    /// The returned bitmask should be parsed in order to retrieve the supported result set types.
    ///
    /// For instance:
    /// - return 0   (\b0)     => [] (no supported result set types);
    /// - return 1   (\b1)     => \[SQL_RESULT_SET_TYPE_UNSPECIFIED\];
    /// - return 2   (\b10)    => \[SQL_RESULT_SET_TYPE_FORWARD_ONLY\];
    /// - return 3   (\b11)    => [SQL_RESULT_SET_TYPE_UNSPECIFIED, SQL_RESULT_SET_TYPE_FORWARD_ONLY];
    /// - return 4   (\b100)   => \[SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE\];
    /// - return 5   (\b101)   => [SQL_RESULT_SET_TYPE_UNSPECIFIED, SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE];
    /// - return 6   (\b110)   => [SQL_RESULT_SET_TYPE_FORWARD_ONLY, SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE];
    /// - return 7   (\b111)   => [SQL_RESULT_SET_TYPE_UNSPECIFIED, SQL_RESULT_SET_TYPE_FORWARD_ONLY, SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE];
    /// - return 8   (\b1000)  => \[SQL_RESULT_SET_TYPE_SCROLL_SENSITIVE\];
    /// - ...
    /// Valid result set types are described under `arrow.flight.protocol.sql.SqlSupportedResultSetType`.
    SqlSupportedResultSetTypes = 567,
    ///
    /// Returns an int32 bitmask value concurrency types supported for
    /// `arrow.flight.protocol.sql.SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_UNSPECIFIED`.
    ///
    /// For instance:
    /// - return 0 (\b0)   => [] (no supported concurrency types for this result set type)
    /// - return 1 (\b1)   => \[SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED\]
    /// - return 2 (\b10)  => \[SQL_RESULT_SET_CONCURRENCY_READ_ONLY\]
    /// - return 3 (\b11)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_READ_ONLY]
    /// - return 4 (\b100) => \[SQL_RESULT_SET_CONCURRENCY_UPDATABLE\]
    /// - return 5 (\b101) => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 6 (\b110)  => [SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 7 (\b111)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// Valid result set types are described under `arrow.flight.protocol.sql.SqlSupportedResultSetConcurrency`.
    SqlSupportedConcurrenciesForResultSetUnspecified = 568,
    ///
    /// Returns an int32 bitmask value concurrency types supported for
    /// `arrow.flight.protocol.sql.SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_FORWARD_ONLY`.
    ///
    /// For instance:
    /// - return 0 (\b0)   => [] (no supported concurrency types for this result set type)
    /// - return 1 (\b1)   => \[SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED\]
    /// - return 2 (\b10)  => \[SQL_RESULT_SET_CONCURRENCY_READ_ONLY\]
    /// - return 3 (\b11)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_READ_ONLY]
    /// - return 4 (\b100) => \[SQL_RESULT_SET_CONCURRENCY_UPDATABLE\]
    /// - return 5 (\b101) => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 6 (\b110)  => [SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 7 (\b111)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// Valid result set types are described under `arrow.flight.protocol.sql.SqlSupportedResultSetConcurrency`.
    SqlSupportedConcurrenciesForResultSetForwardOnly = 569,
    ///
    /// Returns an int32 bitmask value concurrency types supported for
    /// `arrow.flight.protocol.sql.SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_SCROLL_SENSITIVE`.
    ///
    /// For instance:
    /// - return 0 (\b0)   => [] (no supported concurrency types for this result set type)
    /// - return 1 (\b1)   => \[SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED\]
    /// - return 2 (\b10)  => \[SQL_RESULT_SET_CONCURRENCY_READ_ONLY\]
    /// - return 3 (\b11)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_READ_ONLY]
    /// - return 4 (\b100) => \[SQL_RESULT_SET_CONCURRENCY_UPDATABLE\]
    /// - return 5 (\b101) => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 6 (\b110)  => [SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 7 (\b111)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// Valid result set types are described under `arrow.flight.protocol.sql.SqlSupportedResultSetConcurrency`.
    SqlSupportedConcurrenciesForResultSetScrollSensitive = 570,
    ///
    /// Returns an int32 bitmask value concurrency types supported for
    /// `arrow.flight.protocol.sql.SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE`.
    ///
    /// For instance:
    /// - return 0 (\b0)   => [] (no supported concurrency types for this result set type)
    /// - return 1 (\b1)   => \[SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED\]
    /// - return 2 (\b10)  => \[SQL_RESULT_SET_CONCURRENCY_READ_ONLY\]
    /// - return 3 (\b11)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_READ_ONLY]
    /// - return 4 (\b100) => \[SQL_RESULT_SET_CONCURRENCY_UPDATABLE\]
    /// - return 5 (\b101) => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 6 (\b110)  => [SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// - return 7 (\b111)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
    /// Valid result set types are described under `arrow.flight.protocol.sql.SqlSupportedResultSetConcurrency`.
    SqlSupportedConcurrenciesForResultSetScrollInsensitive = 571,
    ///
    /// Retrieves a boolean value indicating whether this database supports batch updates.
    ///
    /// - false: if this database does not support batch updates;
    /// - true: if this database supports batch updates.
    SqlBatchUpdatesSupported = 572,
    ///
    /// Retrieves a boolean value indicating whether this database supports savepoints.
    ///
    /// Returns:
    /// - false: if this database does not support savepoints;
    /// - true: if this database supports savepoints.
    SqlSavepointsSupported = 573,
    ///
    /// Retrieves a boolean value indicating whether named parameters are supported in callable statements.
    ///
    /// Returns:
    /// - false: if named parameters in callable statements are unsupported;
    /// - true: if named parameters in callable statements are supported.
    SqlNamedParametersSupported = 574,
    ///
    /// Retrieves a boolean value indicating whether updates made to a LOB are made on a copy or directly to the LOB.
    ///
    /// Returns:
    /// - false: if updates made to a LOB are made directly to the LOB;
    /// - true: if updates made to a LOB are made on a copy.
    SqlLocatorsUpdateCopy = 575,
    ///
    /// Retrieves a boolean value indicating whether invoking user-defined or vendor functions
    /// using the stored procedure escape syntax is supported.
    ///
    /// Returns:
    /// - false: if invoking user-defined or vendor functions using the stored procedure escape syntax is unsupported;
    /// - true: if invoking user-defined or vendor functions using the stored procedure escape syntax is supported.
    SqlStoredFunctionsUsingCallSyntaxSupported = 576,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlSupportedCaseSensitivity {
    SqlCaseSensitivityUnknown = 0,
    SqlCaseSensitivityCaseInsensitive = 1,
    SqlCaseSensitivityUppercase = 2,
    SqlCaseSensitivityLowercase = 3,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlNullOrdering {
    SqlNullsSortedHigh = 0,
    SqlNullsSortedLow = 1,
    SqlNullsSortedAtStart = 2,
    SqlNullsSortedAtEnd = 3,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SupportedSqlGrammar {
    SqlMinimumGrammar = 0,
    SqlCoreGrammar = 1,
    SqlExtendedGrammar = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SupportedAnsi92SqlGrammarLevel {
    Ansi92EntrySql = 0,
    Ansi92IntermediateSql = 1,
    Ansi92FullSql = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlOuterJoinsSupportLevel {
    SqlJoinsUnsupported = 0,
    SqlLimitedOuterJoins = 1,
    SqlFullOuterJoins = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlSupportedGroupBy {
    SqlGroupByUnrelated = 0,
    SqlGroupByBeyondSelect = 1,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlSupportedElementActions {
    SqlElementInProcedureCalls = 0,
    SqlElementInIndexDefinitions = 1,
    SqlElementInPrivilegeDefinitions = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlSupportedPositionedCommands {
    SqlPositionedDelete = 0,
    SqlPositionedUpdate = 1,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlSupportedSubqueries {
    SqlSubqueriesInComparisons = 0,
    SqlSubqueriesInExists = 1,
    SqlSubqueriesInIns = 2,
    SqlSubqueriesInQuantifieds = 3,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlSupportedUnions {
    SqlUnion = 0,
    SqlUnionAll = 1,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlTransactionIsolationLevel {
    SqlTransactionNone = 0,
    SqlTransactionReadUncommitted = 1,
    SqlTransactionReadCommitted = 2,
    SqlTransactionRepeatableRead = 3,
    SqlTransactionSerializable = 4,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlSupportedTransactions {
    SqlTransactionUnspecified = 0,
    SqlDataDefinitionTransactions = 1,
    SqlDataManipulationTransactions = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlSupportedResultSetType {
    SqlResultSetTypeUnspecified = 0,
    SqlResultSetTypeForwardOnly = 1,
    SqlResultSetTypeScrollInsensitive = 2,
    SqlResultSetTypeScrollSensitive = 3,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlSupportedResultSetConcurrency {
    SqlResultSetConcurrencyUnspecified = 0,
    SqlResultSetConcurrencyReadOnly = 1,
    SqlResultSetConcurrencyUpdatable = 2,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SqlSupportsConvert {
    SqlConvertBigint = 0,
    SqlConvertBinary = 1,
    SqlConvertBit = 2,
    SqlConvertChar = 3,
    SqlConvertDate = 4,
    SqlConvertDecimal = 5,
    SqlConvertFloat = 6,
    SqlConvertInteger = 7,
    SqlConvertIntervalDayTime = 8,
    SqlConvertIntervalYearMonth = 9,
    SqlConvertLongvarbinary = 10,
    SqlConvertLongvarchar = 11,
    SqlConvertNumeric = 12,
    SqlConvertReal = 13,
    SqlConvertSmallint = 14,
    SqlConvertTime = 15,
    SqlConvertTimestamp = 16,
    SqlConvertTinyint = 17,
    SqlConvertVarbinary = 18,
    SqlConvertVarchar = 19,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum UpdateDeleteRules {
    Cascade = 0,
    Restrict = 1,
    SetNull = 2,
    NoAction = 3,
    SetDefault = 4,
}
