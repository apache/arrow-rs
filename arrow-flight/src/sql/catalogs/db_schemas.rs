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

//! [`GetSchemasBuilder`] for building responses to [`CommandGetDbSchemas`] queries.
//!
//! [`CommandGetDbSchemas`]: crate::sql::CommandGetDbSchemas

use std::sync::Arc;

use arrow_array::{builder::StringBuilder, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::{filter::filter_record_batch, take::take};
use arrow_string::like::like_utf8_scalar;
use once_cell::sync::Lazy;

use super::lexsort_to_indices;
use crate::error::*;

/// Return the schema of the RecordBatch that will be returned from [`CommandGetDbSchemas`]
///
/// [`CommandGetDbSchemas`]: crate::sql::CommandGetDbSchemas
pub fn get_db_schemas_schema() -> SchemaRef {
    Arc::clone(&GET_DB_SCHEMAS_SCHEMA)
}

/// The schema for GetDbSchemas
static GET_DB_SCHEMAS_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, false),
        Field::new("db_schema_name", DataType::Utf8, false),
    ]))
});

/// Builds rows like this:
///
/// * catalog_name: utf8,
/// * db_schema_name: utf8,
pub struct GetSchemasBuilder {
    // Optional filters to apply
    db_schema_filter_pattern: Option<String>,
    // array builder for catalog names
    catalog_name: StringBuilder,
    // array builder for schema names
    db_schema_name: StringBuilder,
}

impl GetSchemasBuilder {
    /// Create a new instance of [`GetSchemasBuilder`]
    ///
    /// The builder handles filtering by schemapatterns, the caller
    /// is expected to only pass in tables that match the catalog
    /// from the [`CommandGetDbSchemas`] request.
    ///
    /// # Parameters
    ///
    /// - `db_schema_filter_pattern`: Specifies a filter pattern for schemas to search for.
    ///   When no pattern is provided, the pattern will not be used to narrow the search.
    ///   In the pattern string, two special characters can be used to denote matching rules:
    ///     - "%" means to match any substring with 0 or more characters.
    ///     - "_" means to match any one character.
    ///
    /// [`CommandGetDbSchemas`]: crate::sql::CommandGetDbSchemas
    pub fn new(db_schema_filter_pattern: Option<impl Into<String>>) -> Self {
        let catalog_name = StringBuilder::new();
        let db_schema_name = StringBuilder::new();
        Self {
            db_schema_filter_pattern: db_schema_filter_pattern.map(|v| v.into()),
            catalog_name,
            db_schema_name,
        }
    }

    /// Append a row
    pub fn append(
        &mut self,
        catalog_name: impl AsRef<str>,
        schema_name: impl AsRef<str>,
    ) -> Result<()> {
        self.catalog_name.append_value(catalog_name);
        self.db_schema_name.append_value(schema_name);
        Ok(())
    }

    /// builds the correct schema
    pub fn build(self) -> Result<RecordBatch> {
        let Self {
            db_schema_filter_pattern,
            mut catalog_name,
            mut db_schema_name,
        } = self;

        // Make the arrays
        let catalog_name = catalog_name.finish();
        let db_schema_name = db_schema_name.finish();

        // the filter, if requested, getting a BooleanArray that represents the rows that passed the filter
        let filter = db_schema_filter_pattern
            .map(|db_schema_filter_pattern| {
                // use like kernel to get wildcard matching
                like_utf8_scalar(&db_schema_name, &db_schema_filter_pattern)
            })
            .transpose()?;

        let batch = RecordBatch::try_new(
            get_db_schemas_schema(),
            vec![
                Arc::new(catalog_name) as ArrayRef,
                Arc::new(db_schema_name) as ArrayRef,
            ],
        )?;

        // Apply the filters if needed
        let filtered_batch = if let Some(filter) = filter {
            filter_record_batch(&batch, &filter)?
        } else {
            batch
        };

        // Order filtered results by catalog_name, then db_schema_name
        let indices = lexsort_to_indices(filtered_batch.columns());
        let columns = filtered_batch
            .columns()
            .iter()
            .map(|c| take(c, &indices, None))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(RecordBatch::try_new(get_db_schemas_schema(), columns)?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{StringArray, UInt32Array};

    fn get_ref_batch() -> RecordBatch {
        RecordBatch::try_new(
            get_db_schemas_schema(),
            vec![
                Arc::new(StringArray::from(vec![
                    "a_catalog",
                    "a_catalog",
                    "b_catalog",
                    "b_catalog",
                ])) as ArrayRef,
                Arc::new(StringArray::from(vec![
                    "a_schema", "b_schema", "a_schema", "b_schema",
                ])) as ArrayRef,
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_schemas_are_filterd() {
        let ref_batch = get_ref_batch();

        let mut builder = GetSchemasBuilder::new(None::<String>);
        builder.append("a_catalog", "a_schema").unwrap();
        builder.append("a_catalog", "b_schema").unwrap();
        builder.append("b_catalog", "a_schema").unwrap();
        builder.append("b_catalog", "b_schema").unwrap();
        let schema_batch = builder.build().unwrap();

        assert_eq!(schema_batch, ref_batch);

        let mut builder = GetSchemasBuilder::new(Some("a%"));
        builder.append("a_catalog", "a_schema").unwrap();
        builder.append("a_catalog", "b_schema").unwrap();
        builder.append("b_catalog", "a_schema").unwrap();
        builder.append("b_catalog", "b_schema").unwrap();
        let schema_batch = builder.build().unwrap();

        let indices = UInt32Array::from(vec![0, 2]);
        let ref_filtered = RecordBatch::try_new(
            get_db_schemas_schema(),
            ref_batch
                .columns()
                .iter()
                .map(|c| take(c, &indices, None))
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap(),
        )
        .unwrap();

        assert_eq!(schema_batch, ref_filtered);
    }

    #[test]
    fn test_schemas_are_sorted() {
        let ref_batch = get_ref_batch();

        let mut builder = GetSchemasBuilder::new(None::<String>);
        builder.append("a_catalog", "b_schema").unwrap();
        builder.append("b_catalog", "a_schema").unwrap();
        builder.append("a_catalog", "a_schema").unwrap();
        builder.append("b_catalog", "b_schema").unwrap();
        let schema_batch = builder.build().unwrap();

        assert_eq!(schema_batch, ref_batch)
    }
}
