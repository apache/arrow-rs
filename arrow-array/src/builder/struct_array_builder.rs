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

use crate::{ArrayRef, StructArray};
use arrow_buffer::NullBuffer;
use arrow_schema::{Field, FieldRef, Fields};
use std::sync::Arc;

/// Builds a [`StructArray`] from completed child arrays.
///
/// Unlike [`StructBuilder`](super::StructBuilder), which incrementally builds
/// child arrays, this builder assembles arrays that have already been built.
///
/// # Example
///
/// ```
/// use std::sync::Arc;
/// use arrow_array::builder::StructArrayBuilder;
/// use arrow_array::{Array, ArrayRef, Int32Array, StringArray};
///
/// let names = Arc::new(StringArray::from(vec!["one", "two"])) as ArrayRef;
/// let values = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
/// let array = StructArrayBuilder::new()
///     .with_field("name", names, false)
///     .with_field("value", values, false)
///     .build();
///
/// assert_eq!(array.len(), 2);
/// ```
#[derive(Debug, Default, Clone)]
pub struct StructArrayBuilder {
    fields: Vec<FieldRef>,
    arrays: Vec<ArrayRef>,
    nulls: Option<NullBuffer>,
}

impl StructArrayBuilder {
    /// Creates a new empty [`StructArrayBuilder`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds an array with a field constructed from its data type.
    pub fn with_field(
        mut self,
        field_name: impl Into<String>,
        array: ArrayRef,
        nullable: bool,
    ) -> Self {
        self.fields.push(Arc::new(Field::new(
            field_name,
            array.data_type().clone(),
            nullable,
        )));
        self.arrays.push(array);
        self
    }

    /// Adds an array using a caller-supplied field.
    ///
    /// This preserves field metadata that would be lost if the field were
    /// constructed from the array's data type alone.
    pub fn with_field_ref(mut self, field: FieldRef, array: ArrayRef) -> Self {
        self.fields.push(field);
        self.arrays.push(array);
        self
    }

    /// Sets the top-level null buffer for the struct array.
    pub fn with_nulls(mut self, nulls: NullBuffer) -> Self {
        self.nulls = Some(nulls);
        self
    }

    /// Builds the [`StructArray`].
    ///
    /// # Panics
    ///
    /// Panics if the fields, child arrays, or null buffer have incompatible
    /// data types or lengths.
    pub fn build(self) -> StructArray {
        StructArray::new(Fields::from(self.fields), self.arrays, self.nulls)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Array, Int32Array, StringArray};
    use arrow_schema::DataType;
    use std::collections::HashMap;

    #[test]
    fn build_from_completed_arrays() {
        let names = Arc::new(StringArray::from(vec!["one", "two"])) as ArrayRef;
        let values = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;
        let metadata = HashMap::from([("key".to_string(), "value".to_string())]);
        let value_field =
            Arc::new(Field::new("value", DataType::Int32, false).with_metadata(metadata.clone()));
        let nulls = NullBuffer::from(vec![true, false]);

        let array = StructArrayBuilder::new()
            .with_field("name", names, false)
            .with_field_ref(value_field, values)
            .with_nulls(nulls.clone())
            .build();

        assert_eq!(array.len(), 2);
        assert_eq!(array.column_names(), &["name", "value"]);
        assert_eq!(array.fields()[1].metadata(), &metadata);
        assert_eq!(array.nulls(), Some(&nulls));
    }

    #[test]
    #[should_panic(
        expected = "Incorrect array length for StructArray field \\\"value\\\", expected 2 got 1"
    )]
    fn build_panics_on_mismatched_child_lengths() {
        let names = Arc::new(StringArray::from(vec!["one", "two"])) as ArrayRef;
        let values = Arc::new(Int32Array::from(vec![1])) as ArrayRef;

        StructArrayBuilder::new()
            .with_field("name", names, false)
            .with_field("value", values, false)
            .build();
    }

    #[test]
    #[should_panic(
        expected = "Incorrect datatype for StructArray field \\\"value\\\", expected Int64 got Int32"
    )]
    fn build_panics_on_mismatched_field_data_type() {
        let value_field = Arc::new(Field::new("value", DataType::Int64, false));
        let values = Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef;

        StructArrayBuilder::new()
            .with_field_ref(value_field, values)
            .build();
    }
}
