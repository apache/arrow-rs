use std::{
    ffi::{c_void, CStr, CString},
    ptr::{null, null_mut},
    sync::Arc,
};

use crate::{
    error::{AdbcError, AdbcStatusCode, FFI_AdbcError},
    ffi::{FFI_AdbcConnection, FFI_AdbcDatabase, FFI_AdbcDriver},
    interface::{ConnectionApi, StatementApi},
};

pub struct AdbcDriverManagerError {
    message: String,
    vendor_code: i32,
    sqlstate: [i8; 5usize],
    status_code: AdbcStatusCode,
}

type Result<T> = std::result::Result<T, AdbcDriverManagerError>;

impl AdbcError for AdbcDriverManagerError {
    fn message(&self) -> &str {
        &self.message
    }

    fn sqlstate(&self) -> [i8; 5] {
        self.sqlstate
    }

    fn status_code(&self) -> AdbcStatusCode {
        self.status_code
    }

    fn vendor_code(&self) -> i32 {
        self.vendor_code
    }
}

fn check_status(status: AdbcStatusCode, error: &FFI_AdbcError) -> Result<()> {
    if status == AdbcStatusCode::Ok {
        Ok(())
    } else {
        let message = unsafe { CStr::from_ptr(error.message) }
            .to_string_lossy()
            .to_string();

        Err(AdbcDriverManagerError {
            message,
            vendor_code: error.vendor_code,
            sqlstate: error.sqlstate,
            status_code: status,
        })
    }
}

type AdbcDriverInitFunc = unsafe extern "C" fn(
    version: ::std::os::raw::c_int,
    driver: *mut ::std::os::raw::c_void,
    error: *mut crate::error::FFI_AdbcError,
) -> crate::error::AdbcStatusCode;

pub struct AdbcDriver {
    inner: Arc<FFI_AdbcDriver>,
}

impl AdbcDriver {
    pub fn load(name: &str, entrypoint: Option<&str>, version: u32) -> Result<Self> {
        todo!("Loading from a dynamic library");
    }

    pub fn load_from_init(init_func: AdbcDriverInitFunc, version: i32) -> Result<Self> {
        let mut error = Box::new(FFI_AdbcError::empty());
        let mut driver = Arc::new(FFI_AdbcDriver::default());

        let status = unsafe {
            init_func(
                version,
                Arc::get_mut(&mut driver).unwrap() as *mut FFI_AdbcDriver as *mut c_void,
                error.as_mut(),
            )
        };
        check_status(status, error.as_ref())?;

        Ok(Self { inner: driver })
    }

    pub fn new_database(&self) -> AdbcDatabaseBuilder {
        let mut database = Arc::new(FFI_AdbcDatabase::default());

        // This Arc must be reconstructed later so it can be free'd!
        let db_ref = Arc::get_mut(&mut database).unwrap();
        db_ref.private_driver = Arc::into_raw(self.inner.clone());

        AdbcDatabaseBuilder { inner: database }
    }
}

fn str_to_cstring(value: &str) -> Result<CString> {
    match CString::new(value) {
        Ok(out) => Ok(out),
        Err(err) => Err(AdbcDriverManagerError {
            message: format!(
                "Null character in string at position {}",
                err.nul_position()
            ),
            vendor_code: -1,
            sqlstate: [0; 5],
            status_code: AdbcStatusCode::InvalidArguments,
        }),
    }
}

pub struct AdbcDatabaseBuilder {
    inner: Arc<FFI_AdbcDatabase>,
}

impl AdbcDatabaseBuilder {
    fn get_driver(&self) -> &FFI_AdbcDriver {
        unsafe {
            self.inner
                .private_driver
                .as_ref()
                .expect("Driver pointer is null.")
        }
    }

    pub fn set_option(mut self, key: &str, value: &str) -> Result<Self> {
        let mut error = Box::new(FFI_AdbcError::empty());
        let key = str_to_cstring(key)?;
        let value = str_to_cstring(value)?;
        let status = unsafe {
            self.get_driver().DatabaseSetOption.unwrap()(
                Arc::get_mut(&mut self.inner).unwrap(),
                key.as_ptr(),
                value.as_ptr(),
                error.as_mut(),
            )
        };

        check_status(status, error.as_ref())?;

        Ok(self)
    }

    pub fn init(mut self) -> Result<AdbcDatabase> {
        // We cannot move out of inner directly, because we implement drop. So
        // replace with a default value, which has a null ptr for driver.
        let db = std::mem::take(&mut self.inner);
        Ok(AdbcDatabase { inner: db })
    }
}

impl Drop for AdbcDatabaseBuilder {
    fn drop(&mut self) {
        // To avoid a memory leak, need to rebuild the Arc so it can Drop safely.
        if !self.inner.private_driver.is_null() {
            let _private_driver = unsafe { Arc::from_raw(self.inner.private_driver) };
            let inner_mut = Arc::get_mut(&mut self.inner).unwrap();
            inner_mut.private_data = null_mut();
        }
    }
}

pub struct AdbcDatabase {
    inner: Arc<FFI_AdbcDatabase>,
}

impl AdbcDatabase {
    fn get_driver(&self) -> Result<&mut FFI_AdbcDriver> {
        todo!()
    }

    /// Some drivers allow setting option after init, but others may return an error.
    pub fn set_option(&mut self, key: &str, value: &str) -> Result<Self> {
        todo!()
    }

    pub fn new_connection(&self) -> AdbcConnectionBuilder {
        todo!()
    }
}

pub struct AdbcConnectionBuilder {
    inner: *mut FFI_AdbcConnection,
    database: *mut FFI_AdbcDatabase,
}

impl AdbcConnectionBuilder {
    pub fn set_option(&mut self, key: &str, value: &str) -> Result<Self> {
        todo!()
    }

    pub fn init(self) -> Result<AdbcConnection> {
        todo!()
    }
}

pub struct AdbcConnection {
    inner: *mut FFI_AdbcConnection,
}

impl ConnectionApi for AdbcConnection {
    type Error = AdbcDriverManagerError;

    fn set_option(&self, key: &str, value: &str) -> std::result::Result<(), Self::Error> {
        todo!()
    }

    fn get_table_types(
        &self,
    ) -> std::result::Result<Box<dyn arrow::record_batch::RecordBatchReader>, Self::Error>
    {
        todo!()
    }

    fn get_info(
        &self,
        info_codes: &[u32],
    ) -> std::result::Result<Box<dyn arrow::record_batch::RecordBatchReader>, Self::Error>
    {
        todo!()
    }

    fn get_objects(
        &self,
        depth: crate::ffi::AdbcObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: &[&str],
        column_name: Option<&str>,
    ) -> std::result::Result<Box<dyn arrow::record_batch::RecordBatchReader>, Self::Error>
    {
        todo!()
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> std::result::Result<arrow::datatypes::Schema, Self::Error> {
        todo!()
    }

    fn read_partition(
        &self,
        partition: &[u8],
    ) -> std::result::Result<Box<dyn arrow::record_batch::RecordBatchReader>, Self::Error>
    {
        todo!()
    }

    fn commit(&self) -> std::result::Result<(), Self::Error> {
        todo!()
    }

    fn rollback(&self) -> std::result::Result<(), Self::Error> {
        todo!()
    }
}

struct AdbcStatement {
    inner: *mut FFI_AdbcConnection,
}

impl StatementApi for AdbcStatement {
    type Error = AdbcDriverManagerError;

    fn prepare(&self) -> std::result::Result<(), Self::Error> {
        todo!()
    }

    fn set_option(
        &mut self,
        key: &str,
        value: &str,
    ) -> std::result::Result<(), Self::Error> {
        todo!()
    }

    fn set_sql_query(&mut self, query: &str) -> std::result::Result<(), Self::Error> {
        todo!()
    }

    fn set_substrait_plan(
        &mut self,
        plan: &[u8],
    ) -> std::result::Result<(), Self::Error> {
        todo!()
    }

    fn get_param_schema(
        &self,
    ) -> std::result::Result<arrow::datatypes::Schema, Self::Error> {
        todo!()
    }

    fn bind_data(
        &mut self,
        arr: arrow::array::ArrayRef,
    ) -> std::result::Result<(), Self::Error> {
        todo!()
    }

    fn bind_stream(
        &mut self,
        stream: Box<dyn arrow::record_batch::RecordBatchReader>,
    ) -> std::result::Result<(), Self::Error> {
        todo!()
    }

    fn execute(
        &self,
    ) -> std::result::Result<crate::interface::StatementResult, Self::Error> {
        todo!()
    }

    fn execute_partitioned(
        &self,
    ) -> std::result::Result<crate::interface::PartitionedStatementResult, Self::Error>
    {
        todo!()
    }
}
