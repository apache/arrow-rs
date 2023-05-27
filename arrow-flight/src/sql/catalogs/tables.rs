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

use std::sync::Arc;

use arrow_arith::boolean::and;
use arrow_array::builder::{BinaryBuilder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::{filter::filter_record_batch, take::take};
use arrow_string::like::like_utf8_scalar;
use once_cell::sync::Lazy;

use super::lexsort_to_indices;
use crate::error::*;
use crate::{IpcMessage, IpcWriteOptions, SchemaAsIpc};

/// Return the schema of the RecordBatch that will be returned from
/// [`get_tables`].
///
/// Note the schema differs based on the values of `include_schema`
pub fn get_tables_schema(include_schema: bool) -> SchemaRef {
    if include_schema {
        Arc::clone(&GET_TABLES_SCHEMA_WITH_TABLE_SCHEMA)
    } else {
        Arc::clone(&GET_TABLES_SCHEMA_WITHOUT_TABLE_SCHEMA)
    }
}

/// Builds rows like this:
///
/// * catalog_name: utf8,
/// * db_schema_name: utf8,
/// * table_name: utf8 not null,
/// * table_type: utf8 not null,
/// * (optional) table_schema: bytes not null (schema of the table as described
///   in Schema.fbs::Schema it is serialized as an IPC message.)
///
/// Applies filters for as described on [`get_tables`]
pub struct GetTablesBuilder {
    // Optional filters to apply to schemas
    db_schema_filter_pattern: Option<String>,
    // Optional filters to apply to tables
    table_name_filter_pattern: Option<String>,
    catalog_name: StringBuilder,
    db_schema_name: StringBuilder,
    table_name: StringBuilder,
    table_type: StringBuilder,
    table_schema: Option<BinaryBuilder>,
}

impl GetTablesBuilder {
    pub fn new(
        db_schema_filter_pattern: Option<String>,
        table_name_filter_pattern: Option<String>,
        include_schema: bool,
    ) -> Self {
        let catalog_name = StringBuilder::new();
        let db_schema_name = StringBuilder::new();
        let table_name = StringBuilder::new();
        let table_type = StringBuilder::new();

        let table_schema = if include_schema {
            Some(BinaryBuilder::new())
        } else {
            None
        };

        Self {
            db_schema_filter_pattern,
            table_name_filter_pattern,
            catalog_name,
            db_schema_name,
            table_name,
            table_type,
            table_schema,
        }
    }

    /// Append a row
    pub fn append(
        &mut self,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        table_type: &str,
        table_schema: &Schema,
    ) -> Result<()> {
        self.catalog_name.append_value(catalog_name);
        self.db_schema_name.append_value(schema_name);
        self.table_name.append_value(table_name);
        self.table_type.append_value(table_type);
        if let Some(self_table_schema) = self.table_schema.as_mut() {
            let options = IpcWriteOptions::default();
            // encode the schema into the correct form
            let message: std::result::Result<IpcMessage, _> =
                SchemaAsIpc::new(table_schema, &options).try_into();
            let IpcMessage(schema) = message?;
            self_table_schema.append_value(schema);
        }

        Ok(())
    }

    /// builds the correct schema
    pub fn build(self) -> Result<RecordBatch> {
        let Self {
            db_schema_filter_pattern,
            table_name_filter_pattern,

            mut catalog_name,
            mut db_schema_name,
            mut table_name,
            mut table_type,
            table_schema,
        } = self;

        // Make the arrays
        let catalog_name = catalog_name.finish();
        let db_schema_name = db_schema_name.finish();
        let table_name = table_name.finish();
        let table_type = table_type.finish();
        let table_schema = table_schema.map(|mut table_schema| table_schema.finish());

        // apply any filters, getting a BooleanArray that represents
        // the rows that passed the filter
        let mut filters = vec![];

        if let Some(db_schema_filter_pattern) = db_schema_filter_pattern {
            // use like kernel to get wildcard matching
            filters.push(like_utf8_scalar(
                &db_schema_name,
                &db_schema_filter_pattern,
            )?)
        }

        if let Some(table_name_filter_pattern) = table_name_filter_pattern {
            // use like kernel to get wildcard matching
            filters.push(like_utf8_scalar(&table_name, &table_name_filter_pattern)?)
        }

        let include_schema = table_schema.is_some();
        let batch = if let Some(table_schema) = table_schema {
            RecordBatch::try_new(
                get_tables_schema(include_schema),
                vec![
                    Arc::new(catalog_name) as ArrayRef,
                    Arc::new(db_schema_name) as ArrayRef,
                    Arc::new(table_name) as ArrayRef,
                    Arc::new(table_type) as ArrayRef,
                    Arc::new(table_schema) as ArrayRef,
                ],
            )
        } else {
            RecordBatch::try_new(
                get_tables_schema(include_schema),
                vec![
                    Arc::new(catalog_name) as ArrayRef,
                    Arc::new(db_schema_name) as ArrayRef,
                    Arc::new(table_name) as ArrayRef,
                    Arc::new(table_type) as ArrayRef,
                ],
            )
        }?;

        // `AND` any filters together
        let mut total_filter = None;
        while let Some(filter) = filters.pop() {
            let new_filter = match total_filter {
                Some(total_filter) => and(&total_filter, &filter)?,
                None => filter,
            };
            total_filter = Some(new_filter);
        }

        // Apply the filters if needed
        let filtered_batch = if let Some(total_filter) = total_filter {
            filter_record_batch(&batch, &total_filter)?
        } else {
            batch
        };

        // Order filtered results by catalog_name, then db_schema_name, then table_name
        let sort_cols = filtered_batch.project(&[0, 1, 2])?;
        let indices = lexsort_to_indices(sort_cols.columns());
        let columns = filtered_batch
            .columns()
            .iter()
            .map(|c| take(c, &indices, None))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(RecordBatch::try_new(
            get_tables_schema(include_schema),
            columns,
        )?)
    }
}

/// The schema for GetTables without `table_schema` column
static GET_TABLES_SCHEMA_WITHOUT_TABLE_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, false),
        Field::new("db_schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("table_type", DataType::Utf8, false),
    ]))
});

/// The schema for GetTables with `table_schema` column
static GET_TABLES_SCHEMA_WITH_TABLE_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, false),
        Field::new("db_schema_name", DataType::Utf8, false),
        Field::new("table_name", DataType::Utf8, false),
        Field::new("table_type", DataType::Utf8, false),
        Field::new("table_schema", DataType::Binary, false),
    ]))
});
