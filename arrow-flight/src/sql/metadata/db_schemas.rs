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

//! [`GetDbSchemasBuilder`] for building responses to [`CommandGetDbSchemas`] queries.
//!
//! [`CommandGetDbSchemas`]: crate::sql::CommandGetDbSchemas

use std::sync::Arc;

use arrow_arith::boolean::and;
use arrow_array::{builder::StringBuilder, ArrayRef, RecordBatch, StringArray};
use arrow_ord::cmp::eq;
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::{filter::filter_record_batch, take::take};
use arrow_string::like::like;
use once_cell::sync::Lazy;

use super::lexsort_to_indices;
use crate::error::*;
use crate::sql::CommandGetDbSchemas;

/// A builder for a [`CommandGetDbSchemas`] response.
///
/// Builds rows like this:
///
/// * catalog_name: utf8,
/// * db_schema_name: utf8,
pub struct GetDbSchemasBuilder {
    // Specifies the Catalog to search for the tables.
    // - An empty string retrieves those without a catalog.
    // - If omitted the catalog name is not used to narrow the search.
    catalog_filter: Option<String>,
    // Optional filters to apply
    db_schema_filter_pattern: Option<String>,
    // array builder for catalog names
    catalog_name: StringBuilder,
    // array builder for schema names
    db_schema_name: StringBuilder,
}

impl CommandGetDbSchemas {
    /// Create a builder suitable for constructing a response
    pub fn into_builder(self) -> GetDbSchemasBuilder {
        self.into()
    }
}

impl From<CommandGetDbSchemas> for GetDbSchemasBuilder {
    fn from(value: CommandGetDbSchemas) -> Self {
        Self::new(value.catalog, value.db_schema_filter_pattern)
    }
}

impl GetDbSchemasBuilder {
    /// Create a new instance of [`GetDbSchemasBuilder`]
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
    ///
    /// [`CommandGetDbSchemas`]: crate::sql::CommandGetDbSchemas
    pub fn new(
        catalog: Option<impl Into<String>>,
        db_schema_filter_pattern: Option<impl Into<String>>,
    ) -> Self {
        Self {
            catalog_filter: catalog.map(|v| v.into()),
            db_schema_filter_pattern: db_schema_filter_pattern.map(|v| v.into()),
            catalog_name: StringBuilder::new(),
            db_schema_name: StringBuilder::new(),
        }
    }

    /// Append a row
    ///
    /// In case the catalog should be considered as empty, pass in an empty string '""'.
    pub fn append(&mut self, catalog_name: impl AsRef<str>, schema_name: impl AsRef<str>) {
        self.catalog_name.append_value(catalog_name);
        self.db_schema_name.append_value(schema_name);
    }

    /// builds a `RecordBatch` with the correct schema for a `CommandGetDbSchemas` response
    pub fn build(self) -> Result<RecordBatch> {
        let schema = self.schema();
        let Self {
            catalog_filter,
            db_schema_filter_pattern,
            mut catalog_name,
            mut db_schema_name,
        } = self;

        // Make the arrays
        let catalog_name = catalog_name.finish();
        let db_schema_name = db_schema_name.finish();

        let mut filters = vec![];

        if let Some(db_schema_filter_pattern) = db_schema_filter_pattern {
            // use like kernel to get wildcard matching
            let scalar = StringArray::new_scalar(db_schema_filter_pattern);
            filters.push(like(&db_schema_name, &scalar)?)
        }

        if let Some(catalog_filter_name) = catalog_filter {
            let scalar = StringArray::new_scalar(catalog_filter_name);
            filters.push(eq(&catalog_name, &scalar)?);
        }

        // `AND` any filters together
        let mut total_filter = None;
        while let Some(filter) = filters.pop() {
            let new_filter = match total_filter {
                Some(total_filter) => and(&total_filter, &filter)?,
                None => filter,
            };
            total_filter = Some(new_filter);
        }

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(catalog_name) as ArrayRef,
                Arc::new(db_schema_name) as ArrayRef,
            ],
        )?;

        // Apply the filters if needed
        let filtered_batch = if let Some(filter) = total_filter {
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

        Ok(RecordBatch::try_new(filtered_batch.schema(), columns)?)
    }

    /// Return the schema of the RecordBatch that will be returned
    /// from [`CommandGetDbSchemas`]
    pub fn schema(&self) -> SchemaRef {
        get_db_schemas_schema()
    }
}

fn get_db_schemas_schema() -> SchemaRef {
    Arc::clone(&GET_DB_SCHEMAS_SCHEMA)
}

/// The schema for GetDbSchemas
static GET_DB_SCHEMAS_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, false),
        Field::new("db_schema_name", DataType::Utf8, false),
    ]))
});

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
    fn test_schemas_are_filtered() {
        let ref_batch = get_ref_batch();

        let mut builder = GetDbSchemasBuilder::new(None::<String>, None::<String>);
        builder.append("a_catalog", "a_schema");
        builder.append("a_catalog", "b_schema");
        builder.append("b_catalog", "a_schema");
        builder.append("b_catalog", "b_schema");
        let schema_batch = builder.build().unwrap();

        assert_eq!(schema_batch, ref_batch);

        let mut builder = GetDbSchemasBuilder::new(None::<String>, Some("a%"));
        builder.append("a_catalog", "a_schema");
        builder.append("a_catalog", "b_schema");
        builder.append("b_catalog", "a_schema");
        builder.append("b_catalog", "b_schema");
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

        let mut builder = GetDbSchemasBuilder::new(None::<String>, None::<String>);
        builder.append("a_catalog", "b_schema");
        builder.append("b_catalog", "a_schema");
        builder.append("a_catalog", "a_schema");
        builder.append("b_catalog", "b_schema");
        let schema_batch = builder.build().unwrap();

        assert_eq!(schema_batch, ref_batch)
    }

    #[test]
    fn test_builder_from_query() {
        let ref_batch = get_ref_batch();
        let query = CommandGetDbSchemas {
            catalog: Some("a_catalog".into()),
            db_schema_filter_pattern: Some("b%".into()),
        };

        let mut builder = query.into_builder();
        builder.append("a_catalog", "a_schema");
        builder.append("a_catalog", "b_schema");
        builder.append("b_catalog", "a_schema");
        builder.append("b_catalog", "b_schema");
        let schema_batch = builder.build().unwrap();

        let indices = UInt32Array::from(vec![1]);
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
}
