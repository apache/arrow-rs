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

//! Rust structs and utilities for building Arrow Database Connectivity (ADBC) drivers.
//!
//! ADBC drivers provide an ABI-stable interface for interacting with databases,
//! that:
//!
//!  * Uses the Arrow [C Data interface](https://arrow.apache.org/docs/format/CDataInterface.html)
//!    and [C Stream Interface](https://arrow.apache.org/docs/format/CStreamInterface.html)
//!    for efficient data interchange.
//!  * Supports partitioned result sets for multi-threaded or distributed
//!    applications.
//!  * Support for [Substrait](https://substrait.io/) plans in addition to SQL queries.
//!
//! When implemented for remote databases, [Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)
//! can be used as the communication protocol. This means data can be in Arrow
//! format through the whole connection, minimizing serialization and deserialization
//! overhead.
//!
//! Read more about ADBC at <https://arrow.apache.org/adbc/>
//!
//! ## Using ADBC drivers
//!
//! The [driver_manager] mod allows loading drivers, either from an initialization
//! function or by dynamically finding such a function in a dynamic library.
//!
//! ```
//! use arrow_adbc::driver_manager::AdbcDriver;
//! use arrow_adbc::version::adbc_version_1_0_0;
//! use arrow::record_batch::RecordBatchReader;
//!
//! let sqlite_driver = AdbcDriver::load("adbc_driver_sqlite", None, adbc_version_1_0_0);
//! let sqlite_database = sqlite_driver.new_database().init()?;
//! let sqlite_conn = sqlite_database.new_connection().init()?;
//! let sqlite_statement = sqlite_conn.new_statement();
//! sqlite_statement.set_sql_query("SELECT 1");
//! let results: RecordBatchReader = sqlite_statement.execute()?;
//! assert_eq!(results.next().unwrap(), record_batch([1], ["1"]));
//! ```
//!
//! ## Implementing ADBC drivers
//!
//! To implement an ADBC driver, use the [interface] module. The macro
//! [adbc_init_func] will generate adapters from the safe Rust traits you implement
//! to the FFI interface recognized by ADBC.
pub mod driver_manager;
pub mod error;
pub mod ffi;
pub mod implement;
pub mod interface;
