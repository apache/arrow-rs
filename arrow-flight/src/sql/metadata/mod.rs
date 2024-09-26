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

//! Builders and function for building responses to FlightSQL metadata
//! / information schema requests.
//!
//! - [`GetCatalogsBuilder`] for building responses to [`CommandGetCatalogs`] queries.
//! - [`GetDbSchemasBuilder`] for building responses to [`CommandGetDbSchemas`] queries.
//! - [`GetTablesBuilder`]for building responses to [`CommandGetTables`] queries.
//! - [`SqlInfoDataBuilder`]for building responses to [`CommandGetSqlInfo`] queries.
//! - [`XdbcTypeInfoDataBuilder`]for building responses to [`CommandGetXdbcTypeInfo`] queries.
//!
//! [`CommandGetCatalogs`]: crate::sql::CommandGetCatalogs
//! [`CommandGetDbSchemas`]: crate::sql::CommandGetDbSchemas
//! [`CommandGetTables`]: crate::sql::CommandGetTables
//! [`CommandGetSqlInfo`]: crate::sql::CommandGetSqlInfo
//! [`CommandGetXdbcTypeInfo`]: crate::sql::CommandGetXdbcTypeInfo

mod catalogs;
mod db_schemas;
mod sql_info;
mod table_types;
mod tables;
mod xdbc_info;

pub use catalogs::GetCatalogsBuilder;
pub use db_schemas::GetDbSchemasBuilder;
pub use sql_info::{SqlInfoData, SqlInfoDataBuilder};
pub use tables::GetTablesBuilder;
pub use xdbc_info::{XdbcTypeInfo, XdbcTypeInfoData, XdbcTypeInfoDataBuilder};

use arrow_array::ArrayRef;
use arrow_array::UInt32Array;
use arrow_row::RowConverter;
use arrow_row::SortField;

/// Helper function to sort all the columns in an array
fn lexsort_to_indices(arrays: &[ArrayRef]) -> UInt32Array {
    let fields = arrays
        .iter()
        .map(|a| SortField::new(a.data_type().clone()))
        .collect();
    let converter = RowConverter::new(fields).unwrap();
    let rows = converter.convert_columns(arrays).unwrap();
    let mut sort: Vec<_> = rows.iter().enumerate().collect();
    sort.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));
    UInt32Array::from_iter_values(sort.iter().map(|(i, _)| *i as u32))
}

#[cfg(test)]
mod tests {
    use arrow_array::RecordBatch;
    use arrow_cast::pretty::pretty_format_batches;
    pub fn assert_batches_eq(batches: &[RecordBatch], expected_lines: &[&str]) {
        let formatted = pretty_format_batches(batches).unwrap().to_string();
        let actual_lines: Vec<_> = formatted.trim().lines().collect();
        assert_eq!(
            &actual_lines, expected_lines,
            "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
            expected_lines, actual_lines
        );
    }
}
