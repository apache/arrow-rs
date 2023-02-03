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
//! FFI types, which requires unsafe code. Implement [AdbcDatabaseImpl],
//! [AdbcConnectionImpl], [AdbcStatementImpl], and [AdbcError] first. Then pass the
//! statement type to [adbc_init_func] to generate an ADBC entrypoint.

use std::{rc::Rc, sync::Arc};

use crate::{
    error::AdbcError,
    interface::{ConnectionApi, DatabaseApi, StatementApi},
};

pub mod internal;

/// An implementation of an ADBC database. Must implement [DatabaseApi].
///
/// Because it is shared by multiple connections, the implementation must be
/// thread safe. Internally, it is held with an [std::sync::Arc] by each connection.
pub trait AdbcDatabaseImpl: DatabaseApi {
    /// Initialize the database.
    ///
    /// Some drivers may choose not to support setting options after this has
    /// been called.
    fn init(&self) -> Result<(), Self::Error>;
}

/// An implementation of an ADBC connection. Must implement [ConnectionApi].
pub trait AdbcConnectionImpl: ConnectionApi {
    type DatabaseType: AdbcDatabaseImpl + Default;

    /// Initialize the connection.
    ///
    /// The Arc to the database should be stored within your struct.
    ///
    /// Some drivers may choose not to support setting options after this has
    /// been called.
    fn init(&self, database: Arc<Self::DatabaseType>) -> Result<(), Self::Error>;
}

pub trait AdbcStatementImpl: StatementApi {
    type ConnectionType: AdbcConnectionImpl + Default;

    /// Create a new statement.
    ///
    /// The conn should be saved within the struct.
    fn new_from_connection(conn: Rc<Self::ConnectionType>) -> Self;
}

/// Expose an ADBC driver entrypoint for the given name and statement type.
///
/// The default name recommended is `AdbcDriverInit` or `<Prefix>DriverInit`.
///
/// The type must implement [AdbcStatementImpl].
#[macro_export]
macro_rules! adbc_init_func {
    ($func_name:ident, $statement_type:ident) => {
        #[no_mangle]
        pub unsafe extern "C" fn $func_name(
            version: ::std::os::raw::c_int,
            driver: *mut ::std::os::raw::c_void,
            mut error: *mut arrow_adbc::error::FFI_AdbcError,
        ) -> arrow_adbc::error::AdbcStatusCode {
            if version != 1000000 {
                unsafe {
                    arrow_adbc::error::FFI_AdbcError::set_message(
                        error,
                        &format!("Unsupported ADBC version: {}", version),
                    );
                }
                return arrow_adbc::error::AdbcStatusCode::NotImplemented;
            }

            if driver.is_null() {
                unsafe {
                    arrow_adbc::error::FFI_AdbcError::set_message(
                        error,
                        "Passed a null pointer to ADBC driver init method.",
                    );
                }
                return arrow_adbc::error::AdbcStatusCode::InvalidState;
            }

            let driver_raw =
                arrow_adbc::implement::internal::init_adbc_driver::<$statement_type>();
            unsafe {
                std::ptr::write_unaligned(
                    driver as *mut arrow_adbc::ffi::FFI_AdbcDriver,
                    driver_raw,
                );
            }
            arrow_adbc::error::AdbcStatusCode::Ok
        }
    };
}

pub use adbc_init_func;
