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

//! [`GetTableTypesBuilder`] for building responses to [`CommandGetTableTypes`] queries.
//!
//! [`CommandGetTableTypes`]: crate::sql::CommandGetTableTypes

use std::sync::Arc;

use arrow_array::{builder::StringBuilder, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrow_select::take::take;
use once_cell::sync::Lazy;

use crate::error::*;
use crate::sql::CommandGetTableTypes;

use super::lexsort_to_indices;

/// A builder for a [`CommandGetTableTypes`] response.
///
/// Builds rows like this:
///
/// * table_type: utf8,
#[derive(Default)]
pub struct GetTableTypesBuilder {
    // array builder for table types
    table_type: StringBuilder,
}

impl CommandGetTableTypes {
    /// Create a builder suitable for constructing a response
    pub fn into_builder(self) -> GetTableTypesBuilder {
        self.into()
    }
}

impl From<CommandGetTableTypes> for GetTableTypesBuilder {
    fn from(_value: CommandGetTableTypes) -> Self {
        Self::new()
    }
}

impl GetTableTypesBuilder {
    /// Create a new instance of [`GetTableTypesBuilder`]
    pub fn new() -> Self {
        Self {
            table_type: StringBuilder::new(),
        }
    }

    /// Append a row
    pub fn append(&mut self, table_type: impl AsRef<str>) {
        self.table_type.append_value(table_type);
    }

    /// builds a `RecordBatch` with the correct schema for a `CommandGetTableTypes` response
    pub fn build(self) -> Result<RecordBatch> {
        let schema = self.schema();
        let Self { mut table_type } = self;

        // Make the arrays
        let table_type = table_type.finish();

        let batch = RecordBatch::try_new(schema, vec![Arc::new(table_type) as ArrayRef])?;

        // Order filtered results by table_type
        let indices = lexsort_to_indices(batch.columns());
        let columns = batch
            .columns()
            .iter()
            .map(|c| take(c, &indices, None))
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(RecordBatch::try_new(batch.schema(), columns)?)
    }

    /// Return the schema of the RecordBatch that will be returned
    /// from [`CommandGetTableTypes`]
    pub fn schema(&self) -> SchemaRef {
        get_table_types_schema()
    }
}

fn get_table_types_schema() -> SchemaRef {
    Arc::clone(&GET_TABLE_TYPES_SCHEMA)
}

/// The schema for [`CommandGetTableTypes`].
static GET_TABLE_TYPES_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "table_type",
        DataType::Utf8,
        false,
    )]))
});

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::StringArray;

    fn get_ref_batch() -> RecordBatch {
        RecordBatch::try_new(
            get_table_types_schema(),
            vec![Arc::new(StringArray::from(vec![
                "a_table_type",
                "b_table_type",
                "c_table_type",
                "d_table_type",
            ])) as ArrayRef],
        )
        .unwrap()
    }

    #[test]
    fn test_table_types_are_sorted() {
        let ref_batch = get_ref_batch();

        let mut builder = GetTableTypesBuilder::new();
        builder.append("b_table_type");
        builder.append("a_table_type");
        builder.append("d_table_type");
        builder.append("c_table_type");
        let schema_batch = builder.build().unwrap();

        assert_eq!(schema_batch, ref_batch)
    }

    #[test]
    fn test_builder_from_query() {
        let ref_batch = get_ref_batch();
        let query = CommandGetTableTypes {};

        let mut builder = query.into_builder();
        builder.append("a_table_type");
        builder.append("b_table_type");
        builder.append("c_table_type");
        builder.append("d_table_type");
        let schema_batch = builder.build().unwrap();

        assert_eq!(schema_batch, ref_batch)
    }
}
