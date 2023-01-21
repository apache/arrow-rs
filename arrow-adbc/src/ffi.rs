//! ADBC FFI structs
#![allow(non_snake_case)]
use std::ptr::null_mut;

use crate::error::{AdbcStatusCode, FFI_AdbcError};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::FFI_ArrowArrayStream;

/// An instance of a database.
///
/// Must be kept alive as long as any connections exist.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct FFI_AdbcDatabase {
    /// Opaque implementation-defined state.
    /// This field is NULLPTR iff the connection is unintialized/freed.
    pub private_data: *mut ::std::os::raw::c_void,
    /// The associated driver (used by the driver manager to help
    ///   track state).
    pub private_driver: *mut FFI_AdbcDriver,
}

/// An active database connection.
///
/// Provides methods for query execution, managing prepared
/// statements, using transactions, and so on.
///
/// Connections are not required to be thread-safe, but they can be
/// used from multiple threads so long as clients take care to
/// serialize accesses to a connection.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct FFI_AdbcConnection {
    /// Opaque implementation-defined state.
    /// This field is NULLPTR iff the connection is unintialized/freed.
    pub private_data: *mut ::std::os::raw::c_void,
    /// The associated driver (used by the driver manager to help
    ///   track state).
    pub private_driver: *mut FFI_AdbcDriver,
}

///  A container for all state needed to execute a database
/// query, such as the query itself, parameters for prepared
/// statements, driver parameters, etc.
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
///
/// Statements are not required to be thread-safe, but they can be
/// used from multiple threads so long as clients take care to
/// serialize accesses to a statement.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct FFI_AdbcStatement {
    /// Opaque implementation-defined state.
    /// This field is NULLPTR iff the connection is unintialized/freed.
    pub private_data: *mut ::std::os::raw::c_void,
    /// The associated driver (used by the driver manager to help
    /// track state).
    pub private_driver: *mut FFI_AdbcDriver,
}

/// The partitions of a distributed/partitioned result set.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct FFI_AdbcPartitions {
    /// The number of partitions.
    pub num_partitions: usize,
    /// The partitions of the result set, where each entry (up to
    /// num_partitions entries) is an opaque identifier that can be
    /// passed to FFI_AdbcConnectionReadPartition.
    pub partitions: *mut *const u8,
    /// The length of each corresponding entry in partitions.
    pub partition_lengths: *const usize,
    /// Opaque implementation-defined state.
    /// This field is NULLPTR iff the connection is unintialized/freed.
    pub private_data: *mut ::std::os::raw::c_void,
    /// Release the contained partitions.
    ///
    /// Unlike other structures, this is an embedded callback to make it
    /// easier for the driver manager and driver to cooperate.
    pub release:
        ::std::option::Option<unsafe extern "C" fn(partitions: *mut FFI_AdbcPartitions)>,
}

impl From<Vec<Vec<u8>>> for FFI_AdbcPartitions {
    fn from(mut value: Vec<Vec<u8>>) -> Self {
        // Make sure capacity and length are the same, so it's easier to reconstruct them.
        value.shrink_to_fit();

        let num_partitions = value.len();
        let mut lengths: Vec<usize> = value.iter().map(|v| v.len()).collect();
        let partition_lengths = lengths.as_mut_ptr();
        std::mem::forget(lengths);

        let mut partitions_vec: Vec<*const u8> = value
            .into_iter()
            .map(|mut p| {
                p.shrink_to_fit();
                let ptr = p.as_ptr();
                std::mem::forget(p);
                ptr
            })
            .collect();
        partitions_vec.shrink_to_fit();
        let partitions = partitions_vec.as_mut_ptr();
        std::mem::forget(partitions_vec);

        Self {
            num_partitions,
            partitions,
            partition_lengths,
            private_data: 42 as *mut ::std::os::raw::c_void, // Arbitrary non-null pointer
            release: Some(drop_adbc_partitions),
        }
    }
}

unsafe extern "C" fn drop_adbc_partitions(partitions: *mut FFI_AdbcPartitions) {
    if let Some(partitions) = partitions.as_mut() {
        // This must reconstruct every Vec that we called mem::forget on when
        // constructing the FFI struct.
        let partition_lengths: Vec<usize> = Vec::from_raw_parts(
            partitions.partition_lengths as *mut usize,
            partitions.num_partitions,
            partitions.num_partitions,
        );

        let partitions_vec = Vec::from_raw_parts(
            partitions.partitions,
            partitions.num_partitions,
            partitions.num_partitions,
        );

        let _each_partition: Vec<Vec<u8>> = partitions_vec
            .into_iter()
            .zip(partition_lengths)
            .map(|(ptr, size)| Vec::from_raw_parts(ptr as *mut u8, size, size))
            .collect();

        partitions.private_data = null_mut();
        partitions.release = None;
    }
}

/// An instance of an initialized database driver.
///
/// This provides a common interface for vendor-specific driver
/// initialization routines. Drivers should populate this struct, and
/// applications can call ADBC functions through this struct, without
/// worrying about multiple definitions of the same symbol.
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct FFI_AdbcDriver {
    /// Opaque driver-defined state.
    /// This field is NULL if the driver is unintialized/freed (but
    /// it need not have a value even if the driver is initialized).
    pub private_data: *mut ::std::os::raw::c_void,
    /// Opaque driver manager-defined state.
    /// This field is NULL if the driver is unintialized/freed (but
    /// it need not have a value even if the driver is initialized).
    pub private_manager: *mut ::std::os::raw::c_void,
    ///  Release the driver and perform any cleanup.
    ///
    /// This is an embedded callback to make it easier for the driver
    /// manager and driver to cooperate.
    pub release: ::std::option::Option<
        unsafe extern "C" fn(
            driver: *mut FFI_AdbcDriver,
            error: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub DatabaseInit: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcDatabase,
            arg2: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub DatabaseNew: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcDatabase,
            arg2: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub DatabaseSetOption: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcDatabase,
            arg2: *const ::std::os::raw::c_char,
            arg3: *const ::std::os::raw::c_char,
            arg4: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub DatabaseRelease: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcDatabase,
            arg2: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub ConnectionCommit: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcConnection,
            arg2: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub ConnectionGetInfo: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcConnection,
            arg2: *const u32,
            arg3: usize,
            arg4: *mut FFI_ArrowArrayStream,
            arg5: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub ConnectionGetObjects: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcConnection,
            arg2: ::std::os::raw::c_int,
            arg3: *const ::std::os::raw::c_char,
            arg4: *const ::std::os::raw::c_char,
            arg5: *const ::std::os::raw::c_char,
            arg6: *mut *const ::std::os::raw::c_char,
            arg7: *const ::std::os::raw::c_char,
            arg8: *mut FFI_ArrowArrayStream,
            arg9: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub ConnectionGetTableSchema: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcConnection,
            arg2: *const ::std::os::raw::c_char,
            arg3: *const ::std::os::raw::c_char,
            arg4: *const ::std::os::raw::c_char,
            arg5: *mut FFI_ArrowSchema,
            arg6: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub ConnectionGetTableTypes: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcConnection,
            arg2: *mut FFI_ArrowArrayStream,
            arg3: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub ConnectionInit: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcConnection,
            arg2: *mut FFI_AdbcDatabase,
            arg3: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub ConnectionNew: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcConnection,
            arg2: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub ConnectionSetOption: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcConnection,
            arg2: *const ::std::os::raw::c_char,
            arg3: *const ::std::os::raw::c_char,
            arg4: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub ConnectionReadPartition: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcConnection,
            arg2: *const u8,
            arg3: usize,
            arg4: *mut FFI_ArrowArrayStream,
            arg5: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub ConnectionRelease: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcConnection,
            arg2: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub ConnectionRollback: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcConnection,
            arg2: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub StatementBind: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcStatement,
            arg2: *mut FFI_ArrowArray,
            arg3: *mut FFI_ArrowSchema,
            arg4: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub StatementBindStream: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcStatement,
            arg2: *mut FFI_ArrowArrayStream,
            arg3: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub StatementExecuteQuery: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcStatement,
            arg2: *mut FFI_ArrowArrayStream,
            arg3: *mut i64,
            arg4: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub StatementExecutePartitions: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcStatement,
            arg2: *mut FFI_ArrowSchema,
            arg3: *mut FFI_AdbcPartitions,
            arg4: *mut i64,
            arg5: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub StatementGetParameterSchema: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcStatement,
            arg2: *mut FFI_ArrowSchema,
            arg3: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub StatementNew: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcConnection,
            arg2: *mut FFI_AdbcStatement,
            arg3: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub StatementPrepare: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcStatement,
            arg2: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub StatementRelease: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcStatement,
            arg2: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub StatementSetOption: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcStatement,
            arg2: *const ::std::os::raw::c_char,
            arg3: *const ::std::os::raw::c_char,
            arg4: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub StatementSetSqlQuery: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcStatement,
            arg2: *const ::std::os::raw::c_char,
            arg3: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
    pub StatementSetSubstraitPlan: ::std::option::Option<
        unsafe extern "C" fn(
            arg1: *mut FFI_AdbcStatement,
            arg2: *const u8,
            arg3: usize,
            arg4: *mut FFI_AdbcError,
        ) -> AdbcStatusCode,
    >,
}
