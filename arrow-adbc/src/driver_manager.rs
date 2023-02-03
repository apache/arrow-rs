use std::{
    cell::RefCell,
    ffi::{c_void, CStr, CString},
    mem::MaybeUninit,
    ops::{Deref, DerefMut},
    ptr::{null, null_mut},
    rc::Rc,
    sync::{Arc, RwLock},
};

use arrow::{
    array::{export_array_into_raw, StringArray},
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    ffi_stream::{export_reader_into_raw, ArrowArrayStreamReader, FFI_ArrowArrayStream},
    record_batch::{RecordBatch, RecordBatchReader},
};

use crate::{
    error::{AdbcError, AdbcStatusCode, FFI_AdbcError},
    ffi::{
        FFI_AdbcConnection, FFI_AdbcDatabase, FFI_AdbcDriver, FFI_AdbcPartitions,
        FFI_AdbcStatement,
    },
    interface::{
        ConnectionApi, PartitionedStatementResult, StatementApi, StatementResult,
    },
};

pub struct AdbcDriverManagerError {
    pub message: String,
    pub vendor_code: i32,
    pub sqlstate: [i8; 5usize],
    pub status_code: AdbcStatusCode,
}

type Result<T> = std::result::Result<T, AdbcDriverManagerError>;

impl<T: AdbcError> From<T> for AdbcDriverManagerError {
    fn from(value: T) -> Self {
        Self {
            message: value.message().to_string(),
            vendor_code: value.vendor_code(),
            sqlstate: value.sqlstate(),
            status_code: value.status_code(),
        }
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

#[derive(Clone)]
pub struct AdbcDriver {
    inner: Arc<FFI_AdbcDriver>,
}

impl AdbcDriver {
    pub fn load(_name: &str, _entrypoint: Option<&str>, _version: u32) -> Result<Self> {
        todo!("Loading from a dynamic library");
    }

    pub fn load_from_init(init_func: AdbcDriverInitFunc, version: i32) -> Result<Self> {
        let mut error = FFI_AdbcError::empty();
        let mut driver = Arc::new(FFI_AdbcDriver::default());

        let status = unsafe {
            init_func(
                version,
                Arc::get_mut(&mut driver).unwrap() as *mut FFI_AdbcDriver as *mut c_void,
                &mut error,
            )
        };
        check_status(status, &error)?;

        Ok(Self { inner: driver })
    }

    pub fn new_database(&self) -> AdbcDatabaseBuilder {
        // TODO: do we even care about setting private_driver?

        AdbcDatabaseBuilder {
            inner: FFI_AdbcDatabase::default(),
            driver: self.inner.clone(),
        }
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
    inner: FFI_AdbcDatabase,
    driver: Arc<FFI_AdbcDriver>,
}

impl AdbcDatabaseBuilder {
    pub fn set_option(mut self, key: &str, value: &str) -> Result<Self> {
        let mut error = FFI_AdbcError::empty();
        let key = str_to_cstring(key)?;
        let value = str_to_cstring(value)?;
        let status = unsafe {
            self.driver.DatabaseSetOption.unwrap()(
                &mut self.inner,
                key.as_ptr(),
                value.as_ptr(),
                &mut error,
            )
        };

        check_status(status, &error)?;

        Ok(self)
    }

    pub fn init(self) -> Result<AdbcDatabase> {
        Ok(AdbcDatabase {
            inner: Arc::new(RwLock::new(self.inner)),
            driver: self.driver,
        })
    }
}

// TODO: make sure this is Send
#[derive(Clone)]
pub struct AdbcDatabase {
    // In general, ADBC objects allow serialized access from multiple threads,
    // but not concurrent access. Specific implementations may permit
    // multiple threads. To support safe access to all drivers, we wrap them in
    // RwLock. It is only write-locked for set_option.
    inner: Arc<RwLock<FFI_AdbcDatabase>>,
    driver: Arc<FFI_AdbcDriver>,
}

impl AdbcDatabase {
    pub fn set_option(self, key: &str, value: &str) -> Result<()> {
        let mut error = FFI_AdbcError::empty();
        let key = str_to_cstring(key)?;
        let value = str_to_cstring(value)?;

        let mut inner_mut = self
            .inner
            .write()
            .expect("Read-write lock of AdbcDatabase was poisoned.");
        let status = unsafe {
            self.driver.DatabaseSetOption.unwrap()(
                inner_mut.deref_mut(),
                key.as_ptr(),
                value.as_ptr(),
                &mut error,
            )
        };

        check_status(status, &error)?;

        Ok(())
    }

    pub fn new_connection(&self) -> AdbcConnectionBuilder {
        let inner = FFI_AdbcConnection::default();

        AdbcConnectionBuilder {
            inner,
            database: self.inner.clone(),
            driver: self.driver.clone(),
        }
    }
}

// TODO: make sure this is Send
pub struct AdbcConnectionBuilder {
    inner: FFI_AdbcConnection,
    database: Arc<RwLock<FFI_AdbcDatabase>>,
    driver: Arc<FFI_AdbcDriver>,
}

impl AdbcConnectionBuilder {
    pub fn set_option(mut self, key: &str, value: &str) -> Result<Self> {
        let mut error = FFI_AdbcError::empty();
        let key = str_to_cstring(key)?;
        let value = str_to_cstring(value)?;
        let status = unsafe {
            self.driver.ConnectionSetOption.unwrap()(
                &mut self.inner,
                key.as_ptr(),
                value.as_ptr(),
                &mut error,
            )
        };

        check_status(status, &error)?;

        Ok(self)
    }

    pub fn init(mut self) -> Result<AdbcConnection> {
        let mut error = FFI_AdbcError::empty();

        let database_ref = self
            .database
            .read()
            .expect("Read-write lock of AdbcDatabase was poisoned.");

        let status = unsafe {
            self.driver.ConnectionInit.unwrap()(
                &mut self.inner,
                database_ref.deref(),
                &mut error,
            )
        };

        check_status(status, &error)?;

        Ok(AdbcConnection {
            inner: Rc::new(RefCell::new(self.inner)),
            driver: self.driver,
        })
    }
}

/// An ADBC Connection associated with the driver.
///
/// Connections should be used on a single thread. To use a driver from multiple
/// threads, create a connection for each thread.
pub struct AdbcConnection {
    inner: Rc<RefCell<FFI_AdbcConnection>>,
    driver: Arc<FFI_AdbcDriver>,
}

impl ConnectionApi for AdbcConnection {
    type Error = AdbcDriverManagerError;

    fn set_option(&self, key: &str, value: &str) -> std::result::Result<(), Self::Error> {
        let mut error = FFI_AdbcError::empty();
        let key = str_to_cstring(key)?;
        let value = str_to_cstring(value)?;
        let status = unsafe {
            self.driver.ConnectionSetOption.unwrap()(
                self.inner.borrow_mut().deref_mut(),
                key.as_ptr(),
                value.as_ptr(),
                &mut error,
            )
        };

        check_status(status, &error)?;

        Ok(())
    }

    /// Get the valid table types for the database.
    ///
    /// For example, in sqlite the table types are "view" and "table".
    ///
    /// This can error if not implemented by the driver.
    fn get_table_types(&self) -> std::result::Result<Vec<String>, Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let mut reader = MaybeUninit::uninit();

        let status = unsafe {
            self.driver.ConnectionGetTableTypes.unwrap()(
                self.inner.borrow_mut().deref_mut(),
                reader.as_mut_ptr(),
                &mut error,
            )
        };
        check_status(status, &error)?;

        let mut reader: FFI_ArrowArrayStream = unsafe { reader.assume_init() };
        let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut reader)? };

        let expected_schema =
            Schema::new(vec![Field::new("table_type", DataType::Utf8, false)]);
        let schema_mismatch_error = |found_schema| AdbcDriverManagerError {
            message: format!("Driver returned unexpected schema: {found_schema:?}"),
            vendor_code: -1,
            sqlstate: [0; 5],
            status_code: AdbcStatusCode::Internal,
        };
        if reader.schema().deref() != &expected_schema {
            return Err(schema_mismatch_error(reader.schema()));
        }

        let batches: Vec<RecordBatch> =
            reader.collect::<std::result::Result<_, ArrowError>>()?;

        let mut out: Vec<String> =
            Vec::with_capacity(batches.iter().map(|batch| batch.num_rows()).sum());
        for batch in &batches {
            if batch.schema().deref() != &expected_schema {
                return Err(schema_mismatch_error(batch.schema()));
            }
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for value in column.into_iter().flatten() {
                out.push(value.to_string());
            }
        }

        Ok(out)
    }

    fn get_info(
        &self,
        info_codes: &[u32],
    ) -> std::result::Result<Box<dyn arrow::record_batch::RecordBatchReader>, Self::Error>
    {
        // TODO: Should this return a more usable type
        let mut error = FFI_AdbcError::empty();

        let mut reader = MaybeUninit::uninit();

        let status = unsafe {
            self.driver.ConnectionGetInfo.unwrap()(
                self.inner.borrow_mut().deref_mut(),
                info_codes.as_ptr(),
                info_codes.len(),
                reader.as_mut_ptr(),
                &mut error,
            )
        };
        check_status(status, &error)?;

        let mut reader: FFI_ArrowArrayStream = unsafe { reader.assume_init() };
        let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut reader)? };

        Ok(Box::new(reader))
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
        let mut error = FFI_AdbcError::empty();

        let mut reader = MaybeUninit::uninit();

        let catalog = catalog.map(str_to_cstring).transpose()?;
        let catalog_ptr = catalog.map(|s| s.as_ptr()).unwrap_or(null());

        let db_schema = db_schema.map(str_to_cstring).transpose()?;
        let db_schema_ptr = db_schema.map(|s| s.as_ptr()).unwrap_or(null());

        let table_name = table_name.map(str_to_cstring).transpose()?;
        let table_name_ptr = table_name.map(|s| s.as_ptr()).unwrap_or(null());

        let column_name = column_name.map(str_to_cstring).transpose()?;
        let column_name_ptr = column_name.map(|s| s.as_ptr()).unwrap_or(null());

        let table_type: Vec<CString> = table_type
            .iter()
            .map(|&s| str_to_cstring(s))
            .collect::<Result<_>>()?;
        let mut table_type_ptrs: Vec<_> = table_type.iter().map(|s| s.as_ptr()).collect();
        // Make sure the array is null-terminated
        table_type_ptrs.push(null());

        let status = unsafe {
            self.driver.ConnectionGetObjects.unwrap()(
                self.inner.borrow_mut().deref_mut(),
                depth,
                catalog_ptr,
                db_schema_ptr,
                table_name_ptr,
                table_type_ptrs.as_ptr(),
                column_name_ptr,
                reader.as_mut_ptr(),
                &mut error,
            )
        };
        check_status(status, &error)?;

        let mut reader: FFI_ArrowArrayStream = unsafe { reader.assume_init() };
        let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut reader)? };

        Ok(Box::new(reader))
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> std::result::Result<arrow::datatypes::Schema, Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let catalog = catalog.map(str_to_cstring).transpose()?;
        let catalog_ptr = catalog.map(|s| s.as_ptr()).unwrap_or(null());

        let db_schema = db_schema.map(str_to_cstring).transpose()?;
        let db_schema_ptr = db_schema.map(|s| s.as_ptr()).unwrap_or(null());

        let table_name = str_to_cstring(table_name)?;

        let mut schema = FFI_ArrowSchema::empty();

        let status = unsafe {
            self.driver.ConnectionGetTableSchema.unwrap()(
                self.inner.borrow_mut().deref_mut(),
                catalog_ptr,
                db_schema_ptr,
                table_name.as_ptr(),
                &mut schema,
                &mut error,
            )
        };
        check_status(status, &error)?;

        Ok(Schema::try_from(&schema)?)
    }

    fn read_partition(
        &self,
        partition: &[u8],
    ) -> std::result::Result<Box<dyn arrow::record_batch::RecordBatchReader>, Self::Error>
    {
        let mut error = FFI_AdbcError::empty();

        let mut reader = MaybeUninit::uninit();

        let status = unsafe {
            self.driver.ConnectionReadPartition.unwrap()(
                self.inner.borrow_mut().deref_mut(),
                partition.as_ptr(),
                partition.len(),
                reader.as_mut_ptr(),
                &mut error,
            )
        };
        check_status(status, &error)?;

        let mut reader: FFI_ArrowArrayStream = unsafe { reader.assume_init() };
        let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut reader)? };

        Ok(Box::new(reader))
    }

    fn commit(&self) -> std::result::Result<(), Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let status = unsafe {
            self.driver.ConnectionCommit.unwrap()(
                self.inner.borrow_mut().deref_mut(),
                &mut error,
            )
        };
        check_status(status, &error)?;
        Ok(())
    }

    fn rollback(&self) -> std::result::Result<(), Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let status = unsafe {
            self.driver.ConnectionRollback.unwrap()(
                self.inner.borrow_mut().deref_mut(),
                &mut error,
            )
        };
        check_status(status, &error)?;
        Ok(())
    }
}

impl AdbcConnection {
    pub fn new_statement(&self) -> Result<AdbcStatement> {
        let mut inner = FFI_AdbcStatement::default();
        let mut error = FFI_AdbcError::empty();

        let status = unsafe {
            self.driver.StatementNew.unwrap()(
                self.inner.borrow_mut().deref_mut(),
                &mut inner,
                &mut error,
            )
        };
        check_status(status, &error)?;

        Ok(AdbcStatement {
            inner,
            _connection: self.inner.clone(),
            driver: self.driver.clone(),
        })
    }
}

pub struct AdbcStatement {
    inner: FFI_AdbcStatement,
    // We hold onto the connection to make sure it is kept alive
    _connection: Rc<RefCell<FFI_AdbcConnection>>,
    driver: Arc<FFI_AdbcDriver>,
}

impl StatementApi for AdbcStatement {
    type Error = AdbcDriverManagerError;

    fn prepare(&mut self) -> std::result::Result<(), Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let status =
            unsafe { self.driver.StatementPrepare.unwrap()(&mut self.inner, &mut error) };
        check_status(status, &error)?;
        Ok(())
    }

    fn set_option(
        &mut self,
        key: &str,
        value: &str,
    ) -> std::result::Result<(), Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let key = str_to_cstring(key)?;
        let value = str_to_cstring(value)?;

        let status = unsafe {
            self.driver.StatementSetOption.unwrap()(
                &mut self.inner,
                key.as_ptr(),
                value.as_ptr(),
                &mut error,
            )
        };
        check_status(status, &error)?;
        Ok(())
    }

    fn set_sql_query(&mut self, query: &str) -> std::result::Result<(), Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let query = str_to_cstring(query)?;

        let status = unsafe {
            self.driver.StatementSetSqlQuery.unwrap()(
                &mut self.inner,
                query.as_ptr(),
                &mut error,
            )
        };
        check_status(status, &error)?;
        Ok(())
    }

    fn set_substrait_plan(
        &mut self,
        plan: &[u8],
    ) -> std::result::Result<(), Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let status = unsafe {
            self.driver.StatementSetSubstraitPlan.unwrap()(
                &mut self.inner,
                plan.as_ptr(),
                plan.len(),
                &mut error,
            )
        };
        check_status(status, &error)?;
        Ok(())
    }

    fn get_param_schema(
        &self,
    ) -> std::result::Result<arrow::datatypes::Schema, Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let mut schema = FFI_ArrowSchema::empty();

        let status = unsafe {
            self.driver.StatementGetParameterSchema.unwrap()(
                &self.inner,
                &mut schema,
                &mut error,
            )
        };
        check_status(status, &error)?;

        Ok(Schema::try_from(&schema)?)
    }

    fn bind_data(
        &mut self,
        arr: arrow::array::ArrayRef,
    ) -> std::result::Result<(), Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let mut schema = FFI_ArrowSchema::empty();
        let mut array = FFI_ArrowArray::empty();

        unsafe { export_array_into_raw(arr, &mut array, &mut schema)? };

        let status = unsafe {
            self.driver.StatementBind.unwrap()(
                &mut self.inner,
                &array,
                &schema,
                &mut error,
            )
        };
        check_status(status, &error)?;

        Ok(())
    }

    fn bind_stream(
        &mut self,
        reader: Box<dyn arrow::record_batch::RecordBatchReader>,
    ) -> std::result::Result<(), Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let mut stream = FFI_ArrowArrayStream::empty();

        unsafe { export_reader_into_raw(reader, &mut stream) };

        let status = unsafe {
            self.driver.StatementBindStream.unwrap()(
                &mut self.inner,
                &mut stream,
                &mut error,
            )
        };
        check_status(status, &error)?;

        Ok(())
    }

    fn execute(&mut self) -> std::result::Result<StatementResult, Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let mut stream = FFI_ArrowArrayStream::empty();
        let mut rows_affected: i64 = -1;

        let status = unsafe {
            self.driver.StatementExecuteQuery.unwrap()(
                &mut self.inner,
                &mut stream,
                &mut rows_affected,
                &mut error,
            )
        };
        check_status(status, &error)?;

        let result: Option<Box<dyn RecordBatchReader>> = if stream.release.is_none() {
            // There was no result
            None
        } else {
            unsafe { Some(Box::new(ArrowArrayStreamReader::from_raw(&mut stream)?)) }
        };

        Ok(StatementResult {
            result,
            rows_affected,
        })
    }

    fn execute_update(&mut self) -> std::result::Result<i64, Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let stream = null_mut();
        let mut rows_affected: i64 = -1;

        let status = unsafe {
            self.driver.StatementExecuteQuery.unwrap()(
                &mut self.inner,
                stream,
                &mut rows_affected,
                &mut error,
            )
        };
        check_status(status, &error)?;

        Ok(rows_affected)
    }

    fn execute_partitioned(
        &mut self,
    ) -> std::result::Result<PartitionedStatementResult, Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let mut schema = FFI_ArrowSchema::empty();
        let mut partitions = FFI_AdbcPartitions::empty();
        let mut rows_affected: i64 = -1;

        let status = unsafe {
            self.driver.StatementExecutePartitions.unwrap()(
                &mut self.inner,
                &mut schema,
                &mut partitions,
                &mut rows_affected,
                &mut error,
            )
        };
        check_status(status, &error)?;

        let schema = Schema::try_from(&schema)?;

        let partition_lengths = unsafe {
            std::slice::from_raw_parts(
                partitions.partition_lengths,
                partitions.num_partitions,
            )
        };
        let partition_ptrs = unsafe {
            std::slice::from_raw_parts(partitions.partitions, partitions.num_partitions)
        };
        let partition_ids = partition_ptrs
            .iter()
            .zip(partition_lengths.iter())
            .map(|(&part_ptr, &len)| unsafe {
                std::slice::from_raw_parts(part_ptr, len).to_vec()
            })
            .collect();

        Ok(PartitionedStatementResult {
            schema,
            partition_ids,
            rows_affected,
        })
    }
}
