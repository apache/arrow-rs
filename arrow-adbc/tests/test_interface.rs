use std::ffi::{CStr, CString};
use std::ptr::{null, null_mut};
use std::rc::Rc;
use std::sync::Arc;
use std::{cell::RefCell, ffi::c_void};

use arrow::array::ArrayRef;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatchReader;
use arrow::{error::ArrowError, record_batch::RecordBatch};
use arrow_adbc::adbc_init_func;
use arrow_adbc::error::AdbcError;

use arrow_adbc::ffi::{
    AdbcObjectDepth, FFI_AdbcConnection, FFI_AdbcDatabase, FFI_AdbcStatement,
};
use arrow_adbc::implement::{
    AdbcConnectionImpl, AdbcDatabaseImpl, AdbcStatementImpl,
};
use arrow_adbc::interface::{DatabaseApi, ConnectionApi, StatementApi};
use arrow_adbc::interface::{PartitionedStatementResult, StatementResult};
use arrow_adbc::{
    error::{self, AdbcStatusCode, FFI_AdbcError},
    ffi::FFI_AdbcDriver,
};

enum TestError {
    General(String),
}

impl AdbcError for TestError {
    fn message(&self) -> &str {
        match self {
            Self::General(msg) => msg,
        }
    }

    fn status_code(&self) -> crate::error::AdbcStatusCode {
        AdbcStatusCode::Internal
    }
}

// For testing, defines an ADBC database, connection, and statement. Methods
// that mutate return an error that includes the arguments passed, to verify
// they have been passed down correctly.

struct TestDatabase {}

impl Default for TestDatabase {
    fn default() -> Self {
        Self {}
    }
}

impl AdbcDatabaseImpl for TestDatabase {
    fn init(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}

impl DatabaseApi for TestDatabase {
    type Error = TestError;

    fn set_option(&self, key: &str, value: &str) -> Result<(), Self::Error> {
        Err(TestError::General(format!(
            "Not implemented: setting option with key \"{key}\" and value \"{value}\"."
        )))
    }
}

struct TestConnection {
    database: RefCell<Option<Arc<TestDatabase>>>,
}

impl Default for TestConnection {
    fn default() -> Self {
        Self {
            database: RefCell::new(None),
        }
    }
}

impl AdbcConnectionImpl for TestConnection {
    type DatabaseType = TestDatabase;

    fn init(&self, database: Arc<Self::DatabaseType>) -> Result<(), Self::Error> {
        if self.database.borrow().is_none() {
            self.database.replace(Some(database));
            Ok(())
        } else {
            Err(TestError::General(
                "Already called init on the connection.".to_string(),
            ))
        }
    }
}

impl ConnectionApi for TestConnection {
    type Error = TestError;

    fn set_option(&self, key: &str, value: &str) -> Result<(), Self::Error> {
        Err(TestError::General(format!(
            "Not implemented: setting option with key \"{key}\" and value \"{value}\"."
        )))
    }

    fn get_info(
        &self,
        info_codes: &[u32],
    ) -> Result<Box<dyn RecordBatchReader>, Self::Error> {
        Err(TestError::General(format!(
            "Not implemented: requesting info for codes: '{info_codes:?}'."
        )))
    }

    fn get_objects(
        &self,
        depth: AdbcObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: &[&str],
        column_name: Option<&str>,
    ) -> Result<Box<dyn RecordBatchReader>, Self::Error> {
        Err(TestError::General(
                    format!("Not implemented: getting objects with depth {depth:?}, catalog {catalog:?}, db_schema {db_schema:?}, table_name {table_name:?}, table_type {table_type:?}, column_name {column_name:?}.")
                ))
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema, Self::Error> {
        Err(TestError::General(
                    format!("Not implemented: getting schema for catalog {catalog:?}, db_schema {db_schema:?}, table_name {table_name:?}.")
                ))
    }

    fn get_table_types(&self) -> Result<Box<dyn RecordBatchReader>, Self::Error> {
        todo!()
    }

    fn read_partition(
        &self,
        partition: &[u8],
    ) -> Result<Box<dyn RecordBatchReader>, Self::Error> {
        Err(TestError::General(format!(
            "Not implemented: reading partition {partition:?}."
        )))
    }

    fn rollback(&self) -> Result<(), Self::Error> {
        todo!()
    }

    fn commit(&self) -> Result<(), Self::Error> {
        todo!()
    }
}

struct TestStatement {
    connection: Rc<TestConnection>,
}

impl AdbcStatementImpl for TestStatement {
    type ConnectionType = TestConnection;

    fn new_from_connection(connection: Rc<Self::ConnectionType>) -> Self {
        Self { connection }
    }
}

impl StatementApi for TestStatement {
    type Error = TestError;

    fn set_option(&mut self, key: &str, value: &str) -> Result<(), Self::Error> {
        Err(TestError::General(format!(
            "Not implemented: setting option with key '{key}' and value '{value}'."
        )))
    }

    fn set_sql_query(&mut self, query: &str) -> Result<(), Self::Error> {
        Err(TestError::General(format!(
            "Not implemented: setting query '{query}'."
        )))
    }

    fn set_substrait_plan(&mut self, plan: &[u8]) -> Result<(), Self::Error> {
        Err(TestError::General(format!(
            "Not implemented: setting plan '{plan:?}'."
        )))
    }

    fn prepare(&self) -> Result<(), Self::Error> {
        Err(TestError::General(format!(
            "Not implemented: preparing statement."
        )))
    }

    fn get_param_schema(&self) -> Result<Schema, Self::Error> {
        Err(TestError::General(format!(
            "Not implemented: get parameter schema."
        )))
    }

    fn bind_data(&mut self, arr: ArrayRef) -> Result<(), Self::Error> {
        Err(TestError::General(format!(
            "Not implemented: binding data {arr:?}."
        )))
    }

    fn bind_stream(
        &mut self,
        stream: Box<dyn RecordBatchReader>,
    ) -> Result<(), Self::Error> {
        let batches: Vec<RecordBatch> = stream
            .collect::<Result<_, ArrowError>>()
            .map_err(|_| TestError::General("Error collecting stream.".to_string()))?;

        Err(TestError::General(format!(
            "Not implemented: binding stream {batches:?}."
        )))
    }

    fn execute(&self) -> Result<StatementResult, Self::Error> {
        Err(TestError::General(format!("Not implemented: execute")))
    }

    fn execute_partitioned(&self) -> Result<PartitionedStatementResult, Self::Error> {
        Err(TestError::General(format!(
            "Not implemented: execute partitioned"
        )))
    }
}

adbc_init_func!(TestDriverInit, TestStatement);

/// Calls release on an error so it can be safely reused without leaking memory
fn reset_error(error: *mut FFI_AdbcError) {
    unsafe {
        assert!((*error).release.is_some());
        (*error).release.unwrap()(error)
    }
}

/// Try to retrieve the error message as a str
fn get_error_msg<'a>(error: *mut FFI_AdbcError) -> Option<&'a str> {
    unsafe {
        if (*error).message.is_null() {
            None
        } else {
            Some(CStr::from_ptr((*error).message).to_str().unwrap())
        }
    }
}

/// Test initialization and return an initialized driver
fn init_driver(error: *mut FFI_AdbcError) -> *mut FFI_AdbcDriver {
    let driver_layout = std::alloc::Layout::new::<FFI_AdbcDriver>();
    let driver_ptr: *mut FFI_AdbcDriver =
        unsafe { std::alloc::alloc(driver_layout) as *mut FFI_AdbcDriver };

    // Check invalid version
    let status: AdbcStatusCode =
        TestDriverInit(2000000, driver_ptr as *mut c_void, error);
    assert_eq!(status, AdbcStatusCode::NotImplemented);
    assert_eq!(
        get_error_msg(error),
        Some("Unsupported ADBC version: 2000000")
    );
    reset_error(error);

    // Check null pointer rejected
    let status: AdbcStatusCode = TestDriverInit(1000000, null_mut(), error);
    assert_eq!(status, AdbcStatusCode::InvalidState);
    assert_eq!(
        get_error_msg(error),
        Some("Passed a null pointer to ADBC driver init method.")
    );
    reset_error(error);

    let status: AdbcStatusCode =
        TestDriverInit(1000000, driver_ptr as *mut c_void, error);
    assert_eq!(status, AdbcStatusCode::Ok);
    reset_error(error);

    driver_ptr
}

fn release_driver(driver_ptr: *mut FFI_AdbcDriver, error: *mut FFI_AdbcError) {
    let driver_layout = std::alloc::Layout::new::<FFI_AdbcDriver>();
    unsafe {
        (*driver_ptr).release.unwrap()(driver_ptr, error);
        std::alloc::dealloc(driver_ptr as *mut u8, driver_layout);
    }
}

fn new_database(
    driver: &mut FFI_AdbcDriver,
    error: *mut FFI_AdbcError,
) -> *mut FFI_AdbcDatabase {
    let db_layout = std::alloc::Layout::new::<FFI_AdbcDatabase>();
    let db_ptr = unsafe { std::alloc::alloc(db_layout) as *mut FFI_AdbcDatabase };
    let database = FFI_AdbcDatabase {
        private_data: null_mut(),
        private_driver: driver,
    };
    unsafe { std::ptr::write_unaligned(db_ptr, database) };

    assert!(driver.DatabaseNew.is_some());

    // Verify null pointer disallowed
    let status = unsafe { driver.DatabaseNew.unwrap()(null_mut(), error) };
    assert_eq!(status, AdbcStatusCode::InvalidState);
    assert_eq!(
        get_error_msg(error),
        Some("Passed a null pointer to DatabaseNew")
    );
    reset_error(error);

    // Create new one
    let status = unsafe { driver.DatabaseNew.unwrap()(db_ptr, error) };
    assert_eq!(status, AdbcStatusCode::Ok);
    reset_error(error);

    // Test option setting
    assert!(driver.DatabaseSetOption.is_some());
    let key = CString::new("hello").unwrap();
    let value = CString::new("world").unwrap();
    let status = unsafe {
        driver.DatabaseSetOption.unwrap()(db_ptr, key.as_ptr(), value.as_ptr(), error)
    };
    assert_eq!(status, AdbcStatusCode::Internal);
    assert_eq!(
        get_error_msg(error),
        Some(
            format!(
                "Not implemented: setting option with key {key:?} and value {value:?}."
            )
            .as_ref()
        )
    );
    reset_error(error);

    // Test initialization
    assert!(driver.DatabaseInit.is_some());

    let status = unsafe { driver.DatabaseInit.unwrap()(null_mut(), error) };
    assert_eq!(status, AdbcStatusCode::InvalidState);
    assert_eq!(
        get_error_msg(error),
        Some("Passed a null pointer to DatabaseInit")
    );
    reset_error(error);

    let status = unsafe { driver.DatabaseInit.unwrap()(db_ptr, error) };
    assert_eq!(status, AdbcStatusCode::Ok);
    reset_error(error);

    db_ptr
}

fn new_connection(
    driver: &mut FFI_AdbcDriver,
    database: &mut FFI_AdbcDatabase,
    error: *mut FFI_AdbcError,
) -> *mut FFI_AdbcConnection {
    let conn_layout = std::alloc::Layout::new::<FFI_AdbcConnection>();
    let conn_ptr = unsafe { std::alloc::alloc(conn_layout) as *mut FFI_AdbcConnection };

    // It's the caller's responsibility to initialize this memory
    let conn = FFI_AdbcConnection {
        private_data: null_mut(),
        private_driver: driver,
    };
    unsafe { std::ptr::write_unaligned(conn_ptr, conn) };

    assert!(driver.ConnectionNew.is_some());

    // Verify null pointer disallowed
    let status = unsafe { driver.ConnectionNew.unwrap()(null_mut(), error) };
    assert_eq!(status, AdbcStatusCode::InvalidState);
    assert_eq!(
        get_error_msg(error),
        Some("Passed a null pointer to ConnectionNew")
    );
    reset_error(error);

    let status = unsafe { driver.ConnectionNew.unwrap()(conn_ptr, error) };
    assert_eq!(status, AdbcStatusCode::Ok);
    reset_error(error);

    // Test option setting
    assert!(driver.ConnectionSetOption.is_some());
    let key = CString::new("hello").unwrap();
    let value = CString::new("world").unwrap();
    let status = unsafe {
        driver.ConnectionSetOption.unwrap()(conn_ptr, key.as_ptr(), value.as_ptr(), error)
    };
    assert_eq!(status, AdbcStatusCode::Internal);
    assert_eq!(
        get_error_msg(error),
        Some(
            format!(
                "Not implemented: setting option with key {key:?} and value {value:?}."
            )
            .as_ref()
        )
    );
    reset_error(error);

    // Test initialization
    assert!(driver.ConnectionInit.is_some());

    let status = unsafe { driver.ConnectionInit.unwrap()(null_mut(), null_mut(), error) };
    assert_eq!(status, AdbcStatusCode::InvalidState);
    assert_eq!(get_error_msg(error), Some("Passed a null pointer."));
    reset_error(error);

    let status = unsafe { driver.ConnectionInit.unwrap()(conn_ptr, null_mut(), error) };
    assert_eq!(status, AdbcStatusCode::InvalidState);
    assert_eq!(get_error_msg(error), Some("Passed a null pointer."));
    reset_error(error);

    let status = unsafe { driver.ConnectionInit.unwrap()(conn_ptr, database, error) };
    assert_eq!(status, AdbcStatusCode::Ok);
    reset_error(error);

    conn_ptr
}

fn new_statement(
    driver: &mut FFI_AdbcDriver,
    conn: &mut FFI_AdbcConnection,
    error: *mut FFI_AdbcError,
) -> *mut FFI_AdbcStatement {
    let statement_layout = std::alloc::Layout::new::<FFI_AdbcStatement>();
    let statement_ptr =
        unsafe { std::alloc::alloc(statement_layout) as *mut FFI_AdbcStatement };

    let statement = FFI_AdbcStatement {
        private_data: null_mut(),
        private_driver: driver,
    };

    // Verify null pointer disallowed
    let status = unsafe { driver.StatementNew.unwrap()(null_mut(), null_mut(), error) };
    assert_eq!(status, AdbcStatusCode::InvalidState);
    assert_eq!(get_error_msg(error), Some("Passed a null pointer."));
    reset_error(error);

    let status = unsafe { driver.StatementNew.unwrap()(conn, null_mut(), error) };
    assert_eq!(status, AdbcStatusCode::InvalidState);
    assert_eq!(
        get_error_msg(error),
        Some("Passed a null pointer to StatementNew")
    );
    reset_error(error);

    //

    statement_ptr
}

#[test]
fn test_adbc_api() {
    let error_layout = std::alloc::Layout::new::<FFI_AdbcError>();
    let error_ptr: *mut FFI_AdbcError =
        unsafe { std::alloc::alloc(error_layout) as *mut FFI_AdbcError };

    let driver_ptr = init_driver(error_ptr);
    let driver: &mut FFI_AdbcDriver = unsafe { driver_ptr.as_mut().unwrap() };

    let db_ptr = new_database(driver, error_ptr);
    let database: &mut FFI_AdbcDatabase = unsafe { db_ptr.as_mut().unwrap() };

    let conn_ptr = new_connection(driver, database, error_ptr);
    let conn: &mut FFI_AdbcConnection = unsafe { &mut conn_ptr.as_mut().unwrap() };

    let statement_ptr = new_statement(driver, conn, error_ptr);

    // Cleanup
    let status = unsafe { driver.ConnectionRelease.unwrap()(conn_ptr, error_ptr) };
    assert_eq!(status, AdbcStatusCode::Ok);
    release_driver(driver_ptr, error_ptr);
    reset_error(error_ptr);
    unsafe { std::alloc::dealloc(error_ptr as *mut u8, error_layout) };
}
