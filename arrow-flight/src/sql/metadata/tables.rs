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

//! [`GetTablesBuilder`] for building responses to [`CommandGetTables`] queries.
//!
//! [`CommandGetTables`]: crate::sql::CommandGetTables

use std::sync::Arc;

use arrow_arith::boolean::{and, or};
use arrow_array::builder::{BinaryBuilder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch, StringArray};
use arrow_ord::cmp::eq;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::{filter::filter_record_batch, take::take};
use arrow_string::like::like;
use once_cell::sync::Lazy;

use super::lexsort_to_indices;
use crate::error::*;
use crate::sql::CommandGetTables;
use crate::{IpcMessage, IpcWriteOptions, SchemaAsIpc};

/// A builder for a [`CommandGetTables`] response.
///
/// Builds rows like this:
///
/// * catalog_name: utf8,
/// * db_schema_name: utf8,
/// * table_name: utf8 not null,
/// * table_type: utf8 not null,
/// * (optional) table_schema: bytes not null (schema of the table as described
///   in Schema.fbs::Schema it is serialized as an IPC message.)
pub struct GetTablesBuilder {
    catalog_filter: Option<String>,
    table_types_filter: Vec<String>,
    // Optional filters to apply to schemas
    db_schema_filter_pattern: Option<String>,
    // Optional filters to apply to tables
    table_name_filter_pattern: Option<String>,
    // array builder for catalog names
    catalog_name: StringBuilder,
    // array builder for db schema names
    db_schema_name: StringBuilder,
    // array builder for tables names
    table_name: StringBuilder,
    // array builder for table types
    table_type: StringBuilder,
    // array builder for table schemas
    table_schema: Option<BinaryBuilder>,
}

impl CommandGetTables {
    /// Create a builder suitable for constructing a response
    pub fn into_builder(self) -> GetTablesBuilder {
        self.into()
    }
}

impl From<CommandGetTables> for GetTablesBuilder {
    fn from(value: CommandGetTables) -> Self {
        Self::new(
            value.catalog,
            value.db_schema_filter_pattern,
            value.table_name_filter_pattern,
            value.table_types,
            value.include_schema,
        )
    }
}

impl GetTablesBuilder {
    /// Create a new instance of [`GetTablesBuilder`]
    ///
    /// # Parameters
    ///
    /// - `catalog`:  Specifies the Catalog to search for the tables.
    ///   - An empty string retrieves those without a catalog.
    ///   - If omitted the catalog name is not used to narrow the search.
    /// - `db_schema_filter_pattern`: Specifies a filter pattern for schemas to search for.
    ///   When no pattern is provided, the pattern will not be used to narrow the search.
    ///   In the pattern string, two special characters can be used to denote matching rules:
    ///     - "%" means to match any substring with 0 or more characters.
    ///     - "_" means to match any one character.
    /// - `table_name_filter_pattern`: Specifies a filter pattern for tables to search for.
    ///   When no pattern is provided, all tables matching other filters are searched.
    ///   In the pattern string, two special characters can be used to denote matching rules:
    ///     - "%" means to match any substring with 0 or more characters.
    ///     - "_" means to match any one character.
    /// - `table_types`:  Specifies a filter of table types which must match.
    ///   An empy Vec matches all table types.
    /// - `include_schema`: Specifies if the Arrow schema should be returned for found tables.
    ///
    /// [`CommandGetTables`]: crate::sql::CommandGetTables
    pub fn new(
        catalog: Option<impl Into<String>>,
        db_schema_filter_pattern: Option<impl Into<String>>,
        table_name_filter_pattern: Option<impl Into<String>>,
        table_types: impl IntoIterator<Item = impl Into<String>>,
        include_schema: bool,
    ) -> Self {
        let table_schema = if include_schema {
            Some(BinaryBuilder::new())
        } else {
            None
        };
        Self {
            catalog_filter: catalog.map(|s| s.into()),
            table_types_filter: table_types.into_iter().map(|tt| tt.into()).collect(),
            db_schema_filter_pattern: db_schema_filter_pattern.map(|s| s.into()),
            table_name_filter_pattern: table_name_filter_pattern.map(|t| t.into()),
            catalog_name: StringBuilder::new(),
            db_schema_name: StringBuilder::new(),
            table_name: StringBuilder::new(),
            table_type: StringBuilder::new(),
            table_schema,
        }
    }

    /// Append a row
    pub fn append(
        &mut self,
        catalog_name: impl AsRef<str>,
        schema_name: impl AsRef<str>,
        table_name: impl AsRef<str>,
        table_type: impl AsRef<str>,
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

    /// builds a `RecordBatch` for `CommandGetTables`
    pub fn build(self) -> Result<RecordBatch> {
        let schema = self.schema();
        let Self {
            catalog_filter,
            table_types_filter,
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

        if let Some(catalog_filter_name) = catalog_filter {
            let scalar = StringArray::new_scalar(catalog_filter_name);
            filters.push(eq(&catalog_name, &scalar)?);
        }

        let tt_filter = table_types_filter
            .into_iter()
            .map(|tt| eq(&table_type, &StringArray::new_scalar(tt)))
            .collect::<std::result::Result<Vec<_>, _>>()?
            .into_iter()
            // We know the arrays are of same length as they are produced fromn the same root array
            .reduce(|filter, arr| or(&filter, &arr).unwrap());
        if let Some(filter) = tt_filter {
            filters.push(filter);
        }

        if let Some(db_schema_filter_pattern) = db_schema_filter_pattern {
            // use like kernel to get wildcard matching
            let scalar = StringArray::new_scalar(db_schema_filter_pattern);
            filters.push(like(&db_schema_name, &scalar)?)
        }

        if let Some(table_name_filter_pattern) = table_name_filter_pattern {
            // use like kernel to get wildcard matching
            let scalar = StringArray::new_scalar(table_name_filter_pattern);
            filters.push(like(&table_name, &scalar)?)
        }

        let batch = if let Some(table_schema) = table_schema {
            RecordBatch::try_new(
                schema,
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
                // schema is different if table_schema is none
                schema,
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

        // Order filtered results by catalog_name, then db_schema_name, then table_name, then table_type
        // https://github.com/apache/arrow/blob/130f9e981aa98c25de5f5bfe55185db270cec313/format/FlightSql.proto#LL1202C1-L1202C1
        let sort_cols = filtered_batch.project(&[0, 1, 2, 3])?;
        let indices = lexsort_to_indices(sort_cols.columns());
        let columns = filtered_batch
            .columns()
            .iter()
            .map(|c| take(c, &indices, None))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(RecordBatch::try_new(filtered_batch.schema(), columns)?)
    }

    /// Return the schema of the RecordBatch that will be returned from [`CommandGetTables`]
    ///
    /// Note the schema differs based on the values of `include_schema
    ///
    /// [`CommandGetTables`]: crate::sql::CommandGetTables
    pub fn schema(&self) -> SchemaRef {
        get_tables_schema(self.include_schema())
    }

    /// Should the "schema" column be included
    pub fn include_schema(&self) -> bool {
        self.table_schema.is_some()
    }
}

fn get_tables_schema(include_schema: bool) -> SchemaRef {
    if include_schema {
        Arc::clone(&GET_TABLES_SCHEMA_WITH_TABLE_SCHEMA)
    } else {
        Arc::clone(&GET_TABLES_SCHEMA_WITHOUT_TABLE_SCHEMA)
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{StringArray, UInt32Array};

    fn get_ref_batch() -> RecordBatch {
        RecordBatch::try_new(
            get_tables_schema(false),
            vec![
                Arc::new(StringArray::from(vec![
                    "a_catalog",
                    "a_catalog",
                    "a_catalog",
                    "a_catalog",
                    "b_catalog",
                    "b_catalog",
                    "b_catalog",
                    "b_catalog",
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    "a_schema", "a_schema", "b_schema", "b_schema", "a_schema", "a_schema",
                    "b_schema", "b_schema",
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    "a_table", "b_table", "a_table", "b_table", "a_table", "a_table", "b_table",
                    "b_table",
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    "TABLE", "TABLE", "TABLE", "TABLE", "TABLE", "VIEW", "TABLE", "VIEW",
                ])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    fn get_ref_builder(
        catalog: Option<&str>,
        db_schema_filter_pattern: Option<&str>,
        table_name_filter_pattern: Option<&str>,
        table_types: Vec<&str>,
        include_schema: bool,
    ) -> GetTablesBuilder {
        let dummy_schema = Schema::empty();
        let tables = [
            ("a_catalog", "a_schema", "a_table", "TABLE"),
            ("a_catalog", "a_schema", "b_table", "TABLE"),
            ("a_catalog", "b_schema", "a_table", "TABLE"),
            ("a_catalog", "b_schema", "b_table", "TABLE"),
            ("b_catalog", "a_schema", "a_table", "TABLE"),
            ("b_catalog", "a_schema", "a_table", "VIEW"),
            ("b_catalog", "b_schema", "b_table", "TABLE"),
            ("b_catalog", "b_schema", "b_table", "VIEW"),
        ];
        let mut builder = GetTablesBuilder::new(
            catalog,
            db_schema_filter_pattern,
            table_name_filter_pattern,
            table_types,
            include_schema,
        );
        for (catalog_name, schema_name, table_name, table_type) in tables {
            builder
                .append(
                    catalog_name,
                    schema_name,
                    table_name,
                    table_type,
                    &dummy_schema,
                )
                .unwrap();
        }
        builder
    }

    #[test]
    fn test_tables_are_filtered() {
        let ref_batch = get_ref_batch();

        let builder = get_ref_builder(None, None, None, Vec::new(), false);
        let table_batch = builder.build().unwrap();
        assert_eq!(table_batch, ref_batch);

        let builder = get_ref_builder(None, Some("a%"), Some("a%"), Vec::new(), false);
        let table_batch = builder.build().unwrap();
        let indices = UInt32Array::from(vec![0, 4, 5]);
        let ref_filtered = RecordBatch::try_new(
            get_tables_schema(false),
            ref_batch
                .columns()
                .iter()
                .map(|c| take(c, &indices, None))
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(table_batch, ref_filtered);

        let builder = get_ref_builder(Some("a_catalog"), None, None, Vec::new(), false);
        let table_batch = builder.build().unwrap();
        let indices = UInt32Array::from(vec![0, 1, 2, 3]);
        let ref_filtered = RecordBatch::try_new(
            get_tables_schema(false),
            ref_batch
                .columns()
                .iter()
                .map(|c| take(c, &indices, None))
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(table_batch, ref_filtered);

        let builder = get_ref_builder(None, None, None, vec!["VIEW"], false);
        let table_batch = builder.build().unwrap();
        let indices = UInt32Array::from(vec![5, 7]);
        let ref_filtered = RecordBatch::try_new(
            get_tables_schema(false),
            ref_batch
                .columns()
                .iter()
                .map(|c| take(c, &indices, None))
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(table_batch, ref_filtered);
    }

    #[test]
    fn test_tables_are_sorted() {
        let ref_batch = get_ref_batch();
        let dummy_schema = Schema::empty();

        let tables = [
            ("b_catalog", "a_schema", "a_table", "TABLE"),
            ("b_catalog", "b_schema", "b_table", "TABLE"),
            ("b_catalog", "b_schema", "b_table", "VIEW"),
            ("b_catalog", "a_schema", "a_table", "VIEW"),
            ("a_catalog", "a_schema", "a_table", "TABLE"),
            ("a_catalog", "b_schema", "a_table", "TABLE"),
            ("a_catalog", "b_schema", "b_table", "TABLE"),
            ("a_catalog", "a_schema", "b_table", "TABLE"),
        ];
        let mut builder = GetTablesBuilder::new(
            None::<String>,
            None::<String>,
            None::<String>,
            None::<String>,
            false,
        );
        for (catalog_name, schema_name, table_name, table_type) in tables {
            builder
                .append(
                    catalog_name,
                    schema_name,
                    table_name,
                    table_type,
                    &dummy_schema,
                )
                .unwrap();
        }
        let table_batch = builder.build().unwrap();
        assert_eq!(table_batch, ref_batch);
    }
}
