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

//! Builders and function for building responses to infromation schema requests
//!
//! - [`get_catalogs_batch`] and [`get_catalogs_schema`] for building responses to [`CommandGetCatalogs`] queries.
//! - [`GetSchemasBuilder`] and [`get_db_schemas_schema`] for building responses to [`CommandGetDbSchemas`] queries.
//! - [`GetTablesBuilder`] and [`get_tables_schema`] for building responses to [`CommandGetTables`] queries.
//!
//! [`CommandGetCatalogs`]: crate::sql::CommandGetCatalogs
//! [`CommandGetDbSchemas`]: crate::sql::CommandGetDbSchemas
//! [`CommandGetTables`]: crate::sql::CommandGetTables

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, StringArray, UInt32Array};
use arrow_row::{RowConverter, SortField};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use once_cell::sync::Lazy;

use crate::error::Result;

pub use db_schemas::{get_db_schemas_schema, GetSchemasBuilder};
pub use tables::{get_tables_schema, GetTablesBuilder};

mod db_schemas;
mod tables;

/// Returns the RecordBatch for
pub fn get_catalogs_batch(mut catalog_names: Vec<String>) -> Result<RecordBatch> {
    catalog_names.sort_unstable();

    let batch = RecordBatch::try_new(
        Arc::clone(&GET_CATALOG_SCHEMA),
        vec![Arc::new(StringArray::from_iter_values(catalog_names)) as _],
    )?;

    Ok(batch)
}

/// Returns the schema that will result from [`CommandGetCatalogs`]
///
/// [`CommandGetCatalogs`]: crate::sql::CommandGetCatalogs
pub fn get_catalogs_schema() -> &'static Schema {
    &GET_CATALOG_SCHEMA
}

/// The schema for GetCatalogs
static GET_CATALOG_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "catalog_name",
        DataType::Utf8,
        false,
    )]))
});

fn lexsort_to_indices(arrays: &[ArrayRef]) -> UInt32Array {
    let fields = arrays
        .iter()
        .map(|a| SortField::new(a.data_type().clone()))
        .collect();
    let mut converter = RowConverter::new(fields).unwrap();
    let rows = converter.convert_columns(arrays).unwrap();
    let mut sort: Vec<_> = rows.iter().enumerate().collect();
    sort.sort_unstable_by(|(_, a), (_, b)| a.cmp(b));
    UInt32Array::from_iter_values(sort.iter().map(|(i, _)| *i as u32))
}
