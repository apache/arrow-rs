//! Implement ADBC driver without unsafe code.
//!
//! Allows implementing ADBC driver in Rust without directly interacting with
//! FFI types, which requires unsafe code. Implement [AdbcDatabase],
//! [AdbcConnection], [AdbcStatement], and [AdbcError] first. Then pass the
//! statement type to [adbc_api] to generate C FFI interface for ADBC.

use std::{rc::Rc, sync::Arc};

use arrow::{array::ArrayRef, datatypes::Schema, record_batch::RecordBatchReader};

use crate::error::{AdbcError};

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

    fn get_info(
        &self,
        info_codes: &[u32],
    ) -> Result<Box<dyn RecordBatchReader>, Self::Error>;

    fn get_objects(
        &self,
        depth: i32,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: &[&str],
        column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader>, Self::Error>;

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
    ) -> Result<Schema, Self::Error>;

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader>, Self::Error>;

    fn read_partition(
        &self,
        partition: &[u8],
    ) -> Result<Box<dyn RecordBatchReader>, Self::Error>;

    fn commit(&self) -> Result<(), Self::Error>;

    fn rollback(&self) -> Result<(), Self::Error>;
}

pub trait AdbcStatement {
    type Error: AdbcError;
    type ConnectionType: AdbcConnection + Default;

    fn new_from_connection(conn: Rc<Self::ConnectionType>) -> Self;

    fn prepare(&self) -> Result<(), Self::Error>;

    fn set_option(&mut self, key: &str, value: &str) -> Result<(), Self::Error>;

    fn set_sql_query(&mut self, query: &str) -> Result<(), Self::Error>;

    fn set_substrait_plan(&mut self, plan: &[u8]) -> Result<(), Self::Error>;

    fn get_param_schema(&self) -> Result<Schema, Self::Error>;

    fn bind_data(&mut self, arr: ArrayRef) -> Result<(), Self::Error>;

    fn bind_stream(
        &mut self,
        stream: Box<dyn RecordBatchReader>,
    ) -> Result<(), Self::Error>;

    /// Execute a statement and get the results.
    fn execute(&self) -> Result<StatementResult, Self::Error>;

    fn execute_partitioned(&self) -> Result<PartitionedStatementResult, Self::Error>;
}

pub struct StatementResult {
    pub result: Option<Box<dyn RecordBatchReader>>,
    pub rows_affected: Option<i64>,
}

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

            // No mangle, since this is the main entrypoint.
            #[no_mangle]
            pub extern "C" fn AdbcDriverInit(
                version: ::std::os::raw::c_int,
                driver: *mut ::std::os::raw::c_void,
                mut error: *mut FFI_AdbcError,
            ) -> AdbcStatusCode {
                if version != 1000000 {
                    FFI_AdbcError::set_message(
                        error,
                        &format!("Unsupported ADBC version: {}", version),
                    );
                    return AdbcStatusCode::NotImplemented;
                }
                let driver_raw = arrow_adbc::interface::internal::init_adbc_driver::<
                    $statement_type,
                >();
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        &driver_raw as *const FFI_AdbcDriver,
                        // For copy_nonoverlapping to know how many bytes to copy, it needs to
                        // know the side of the type, so we cast the pointer to *mut AdbcDriver
                        // rather than keeping as c_void.
                        driver as *mut FFI_AdbcDriver,
                        1,
                    );
                }
                AdbcStatusCode::Ok
            }
        }
    };
}

pub use adbc_api;
