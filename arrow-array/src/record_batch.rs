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

//! A two-dimensional batch of column-oriented data with a defined
//! [schema](arrow_schema::Schema).

use crate::{new_empty_array, Array, ArrayRef, StructArray};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use std::sync::Arc;

/// Trait for types that can read `RecordBatch`'s.
pub trait RecordBatchReader: Iterator<Item = Result<RecordBatch, ArrowError>> {
    /// Returns the schema of this `RecordBatchReader`.
    ///
    /// Implementation of this trait should guarantee that all `RecordBatch`'s returned by this
    /// reader should have the same schema as returned from this method.
    fn schema(&self) -> SchemaRef;

    /// Reads the next `RecordBatch`.
    #[deprecated(
        since = "2.0.0",
        note = "This method is deprecated in favour of `next` from the trait Iterator."
    )]
    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        self.next().transpose()
    }
}

/// A two-dimensional batch of column-oriented data with a defined
/// [schema](arrow_schema::Schema).
///
/// A `RecordBatch` is a two-dimensional dataset of a number of
/// contiguous arrays, each the same length.
/// A record batch has a schema which must match its arrays’
/// datatypes.
///
/// Record batches are a convenient unit of work for various
/// serialization and computation functions, possibly incremental.
#[derive(Clone, Debug, PartialEq)]
pub struct RecordBatch {
    schema: SchemaRef,
    columns: Vec<Arc<dyn Array>>,

    /// The number of rows in this RecordBatch
    ///
    /// This is stored separately from the columns to handle the case of no columns
    row_count: usize,
}

impl RecordBatch {
    /// Creates a `RecordBatch` from a schema and columns.
    ///
    /// Expects the following:
    ///  * the vec of columns to not be empty
    ///  * the schema and column data types to have equal lengths
    ///    and match
    ///  * each array in columns to have the same length
    ///
    /// If the conditions are not met, an error is returned.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::{Int32Array, RecordBatch};
    /// # use arrow_schema::{DataType, Field, Schema};
    ///
    /// let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false)
    /// ]);
    ///
    /// let batch = RecordBatch::try_new(
    ///     Arc::new(schema),
    ///     vec![Arc::new(id_array)]
    /// ).unwrap();
    /// ```
    pub fn try_new(
        schema: SchemaRef,
        columns: Vec<ArrayRef>,
    ) -> Result<Self, ArrowError> {
        let options = RecordBatchOptions::new();
        Self::try_new_impl(schema, columns, &options)
    }

    /// Creates a `RecordBatch` from a schema and columns, with additional options,
    /// such as whether to strictly validate field names.
    ///
    /// See [`RecordBatch::try_new`] for the expected conditions.
    pub fn try_new_with_options(
        schema: SchemaRef,
        columns: Vec<ArrayRef>,
        options: &RecordBatchOptions,
    ) -> Result<Self, ArrowError> {
        Self::try_new_impl(schema, columns, options)
    }

    /// Creates a new empty [`RecordBatch`].
    pub fn new_empty(schema: SchemaRef) -> Self {
        let columns = schema
            .fields()
            .iter()
            .map(|field| new_empty_array(field.data_type()))
            .collect();

        RecordBatch {
            schema,
            columns,
            row_count: 0,
        }
    }

    /// Validate the schema and columns using [`RecordBatchOptions`]. Returns an error
    /// if any validation check fails, otherwise returns the created [`Self`]
    fn try_new_impl(
        schema: SchemaRef,
        columns: Vec<ArrayRef>,
        options: &RecordBatchOptions,
    ) -> Result<Self, ArrowError> {
        // check that number of fields in schema match column length
        if schema.fields().len() != columns.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "number of columns({}) must match number of fields({}) in schema",
                columns.len(),
                schema.fields().len(),
            )));
        }

        // check that all columns have the same row count
        let row_count = options
            .row_count
            .or_else(|| columns.first().map(|col| col.len()))
            .ok_or_else(|| {
                ArrowError::InvalidArgumentError(
                    "must either specify a row count or at least one column".to_string(),
                )
            })?;

        for (c, f) in columns.iter().zip(&schema.fields) {
            if !f.is_nullable() && c.null_count() > 0 {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Column '{}' is declared as non-nullable but contains null values",
                    f.name()
                )));
            }
        }

        if columns.iter().any(|c| c.len() != row_count) {
            let err = match options.row_count {
                Some(_) => {
                    "all columns in a record batch must have the specified row count"
                }
                None => "all columns in a record batch must have the same length",
            };
            return Err(ArrowError::InvalidArgumentError(err.to_string()));
        }

        // function for comparing column type and field type
        // return true if 2 types are not matched
        let type_not_match = if options.match_field_names {
            |(_, (col_type, field_type)): &(usize, (&DataType, &DataType))| {
                col_type != field_type
            }
        } else {
            |(_, (col_type, field_type)): &(usize, (&DataType, &DataType))| {
                !col_type.equals_datatype(field_type)
            }
        };

        // check that all columns match the schema
        let not_match = columns
            .iter()
            .zip(schema.fields().iter())
            .map(|(col, field)| (col.data_type(), field.data_type()))
            .enumerate()
            .find(type_not_match);

        if let Some((i, (col_type, field_type))) = not_match {
            return Err(ArrowError::InvalidArgumentError(format!(
                "column types must match schema types, expected {:?} but found {:?} at column index {}",
                field_type,
                col_type,
                i)));
        }

        Ok(RecordBatch {
            schema,
            columns,
            row_count,
        })
    }

    /// Returns the [`Schema`](arrow_schema::Schema) of the record batch.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Projects the schema onto the specified columns
    pub fn project(&self, indices: &[usize]) -> Result<RecordBatch, ArrowError> {
        let projected_schema = self.schema.project(indices)?;
        let batch_fields = indices
            .iter()
            .map(|f| {
                self.columns.get(*f).cloned().ok_or_else(|| {
                    ArrowError::SchemaError(format!(
                        "project index {} out of bounds, max field {}",
                        f,
                        self.columns.len()
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        RecordBatch::try_new_with_options(
            SchemaRef::new(projected_schema),
            batch_fields,
            &RecordBatchOptions {
                match_field_names: true,
                row_count: Some(self.row_count),
            },
        )
    }

    /// Returns the number of columns in the record batch.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::{Int32Array, RecordBatch};
    /// # use arrow_schema::{DataType, Field, Schema};
    ///
    /// let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false)
    /// ]);
    ///
    /// let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap();
    ///
    /// assert_eq!(batch.num_columns(), 1);
    /// ```
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Returns the number of rows in each column.
    ///
    /// # Example
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::{Int32Array, RecordBatch};
    /// # use arrow_schema::{DataType, Field, Schema};
    ///
    /// let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false)
    /// ]);
    ///
    /// let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)]).unwrap();
    ///
    /// assert_eq!(batch.num_rows(), 5);
    /// ```
    pub fn num_rows(&self) -> usize {
        self.row_count
    }

    /// Get a reference to a column's array by index.
    ///
    /// # Panics
    ///
    /// Panics if `index` is outside of `0..num_columns`.
    pub fn column(&self, index: usize) -> &ArrayRef {
        &self.columns[index]
    }

    /// Get a reference to all columns in the record batch.
    pub fn columns(&self) -> &[ArrayRef] {
        &self.columns[..]
    }

    /// Return a new RecordBatch where each column is sliced
    /// according to `offset` and `length`
    ///
    /// # Panics
    ///
    /// Panics if `offset` with `length` is greater than column length.
    pub fn slice(&self, offset: usize, length: usize) -> RecordBatch {
        assert!((offset + length) <= self.num_rows());

        let columns = self
            .columns()
            .iter()
            .map(|column| column.slice(offset, length))
            .collect();

        Self {
            schema: self.schema.clone(),
            columns,
            row_count: length,
        }
    }

    /// Create a `RecordBatch` from an iterable list of pairs of the
    /// form `(field_name, array)`, with the same requirements on
    /// fields and arrays as [`RecordBatch::try_new`]. This method is
    /// often used to create a single `RecordBatch` from arrays,
    /// e.g. for testing.
    ///
    /// The resulting schema is marked as nullable for each column if
    /// the array for that column is has any nulls. To explicitly
    /// specify nullibility, use [`RecordBatch::try_from_iter_with_nullable`]
    ///
    /// Example:
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
    ///
    /// let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    /// let b: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
    ///
    /// let record_batch = RecordBatch::try_from_iter(vec![
    ///   ("a", a),
    ///   ("b", b),
    /// ]);
    /// ```
    pub fn try_from_iter<I, F>(value: I) -> Result<Self, ArrowError>
    where
        I: IntoIterator<Item = (F, ArrayRef)>,
        F: AsRef<str>,
    {
        // TODO: implement `TryFrom` trait, once
        // https://github.com/rust-lang/rust/issues/50133 is no longer an
        // issue
        let iter = value.into_iter().map(|(field_name, array)| {
            let nullable = array.null_count() > 0;
            (field_name, array, nullable)
        });

        Self::try_from_iter_with_nullable(iter)
    }

    /// Create a `RecordBatch` from an iterable list of tuples of the
    /// form `(field_name, array, nullable)`, with the same requirements on
    /// fields and arrays as [`RecordBatch::try_new`]. This method is often
    /// used to create a single `RecordBatch` from arrays, e.g. for
    /// testing.
    ///
    /// Example:
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::{ArrayRef, Int32Array, RecordBatch, StringArray};
    ///
    /// let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    /// let b: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), Some("b")]));
    ///
    /// // Note neither `a` nor `b` has any actual nulls, but we mark
    /// // b an nullable
    /// let record_batch = RecordBatch::try_from_iter_with_nullable(vec![
    ///   ("a", a, false),
    ///   ("b", b, true),
    /// ]);
    /// ```
    pub fn try_from_iter_with_nullable<I, F>(value: I) -> Result<Self, ArrowError>
    where
        I: IntoIterator<Item = (F, ArrayRef, bool)>,
        F: AsRef<str>,
    {
        // TODO: implement `TryFrom` trait, once
        // https://github.com/rust-lang/rust/issues/50133 is no longer an
        // issue
        let (fields, columns) = value
            .into_iter()
            .map(|(field_name, array, nullable)| {
                let field_name = field_name.as_ref();
                let field = Field::new(field_name, array.data_type().clone(), nullable);
                (field, array)
            })
            .unzip();

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns)
    }

    /// Returns the total number of bytes of memory occupied physically by this batch.
    pub fn get_array_memory_size(&self) -> usize {
        self.columns()
            .iter()
            .map(|array| array.get_array_memory_size())
            .sum()
    }
}

/// Options that control the behaviour used when creating a [`RecordBatch`].
#[derive(Debug)]
#[non_exhaustive]
pub struct RecordBatchOptions {
    /// Match field names of structs and lists. If set to `true`, the names must match.
    pub match_field_names: bool,

    /// Optional row count, useful for specifying a row count for a RecordBatch with no columns
    pub row_count: Option<usize>,
}

impl RecordBatchOptions {
    /// Creates a new `RecordBatchOptions`
    pub fn new() -> Self {
        Self {
            match_field_names: true,
            row_count: None,
        }
    }
    /// Sets the row_count of RecordBatchOptions and returns self
    pub fn with_row_count(mut self, row_count: Option<usize>) -> Self {
        self.row_count = row_count;
        self
    }
    /// Sets the match_field_names of RecordBatchOptions and returns self
    pub fn with_match_field_names(mut self, match_field_names: bool) -> Self {
        self.match_field_names = match_field_names;
        self
    }
}
impl Default for RecordBatchOptions {
    fn default() -> Self {
        Self::new()
    }
}
impl From<&StructArray> for RecordBatch {
    /// Create a record batch from struct array, where each field of
    /// the `StructArray` becomes a `Field` in the schema.
    ///
    /// This currently does not flatten and nested struct types
    fn from(struct_array: &StructArray) -> Self {
        if let DataType::Struct(fields) = struct_array.data_type() {
            let schema = Schema::new(fields.clone());
            let columns = struct_array.boxed_fields.clone();
            RecordBatch {
                schema: Arc::new(schema),
                row_count: struct_array.len(),
                columns,
            }
        } else {
            unreachable!("unable to get datatype as struct")
        }
    }
}

impl From<RecordBatch> for StructArray {
    fn from(batch: RecordBatch) -> Self {
        batch
            .schema
            .fields
            .iter()
            .zip(batch.columns.iter())
            .map(|t| (t.0.clone(), t.1.clone()))
            .collect::<Vec<(Field, ArrayRef)>>()
            .into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BooleanArray, Int32Array, Int64Array, Int8Array, ListArray, StringArray,
    };
    use arrow_buffer::{Buffer, ToByteSlice};
    use arrow_data::ArrayDataBuilder;

    #[test]
    fn create_record_batch() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]);

        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = StringArray::from(vec!["a", "b", "c", "d", "e"]);

        let record_batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)])
                .unwrap();
        check_batch(record_batch, 5)
    }

    #[test]
    fn byte_size_should_not_regress() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]);

        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = StringArray::from(vec!["a", "b", "c", "d", "e"]);

        let record_batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)])
                .unwrap();
        assert_eq!(record_batch.get_array_memory_size(), 592);
    }

    fn check_batch(record_batch: RecordBatch, num_rows: usize) {
        assert_eq!(num_rows, record_batch.num_rows());
        assert_eq!(2, record_batch.num_columns());
        assert_eq!(&DataType::Int32, record_batch.schema().field(0).data_type());
        assert_eq!(&DataType::Utf8, record_batch.schema().field(1).data_type());
        assert_eq!(num_rows, record_batch.column(0).data().len());
        assert_eq!(num_rows, record_batch.column(1).data().len());
    }

    #[test]
    #[should_panic(expected = "assertion failed: (offset + length) <= self.num_rows()")]
    fn create_record_batch_slice() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]);
        let expected_schema = schema.clone();

        let a = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8]);
        let b = StringArray::from(vec!["a", "b", "c", "d", "e", "f", "h", "i"]);

        let record_batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)])
                .unwrap();

        let offset = 2;
        let length = 5;
        let record_batch_slice = record_batch.slice(offset, length);

        assert_eq!(record_batch_slice.schema().as_ref(), &expected_schema);
        check_batch(record_batch_slice, 5);

        let offset = 2;
        let length = 0;
        let record_batch_slice = record_batch.slice(offset, length);

        assert_eq!(record_batch_slice.schema().as_ref(), &expected_schema);
        check_batch(record_batch_slice, 0);

        let offset = 2;
        let length = 10;
        let _record_batch_slice = record_batch.slice(offset, length);
    }

    #[test]
    #[should_panic(expected = "assertion failed: (offset + length) <= self.num_rows()")]
    fn create_record_batch_slice_empty_batch() {
        let schema = Schema::new(vec![]);

        let record_batch = RecordBatch::new_empty(Arc::new(schema));

        let offset = 0;
        let length = 0;
        let record_batch_slice = record_batch.slice(offset, length);
        assert_eq!(0, record_batch_slice.schema().fields().len());

        let offset = 1;
        let length = 2;
        let _record_batch_slice = record_batch.slice(offset, length);
    }

    #[test]
    fn create_record_batch_try_from_iter() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
        ]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));

        let record_batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)])
            .expect("valid conversion");

        let expected_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Utf8, false),
        ]);
        assert_eq!(record_batch.schema().as_ref(), &expected_schema);
        check_batch(record_batch, 5);
    }

    #[test]
    fn create_record_batch_try_from_iter_with_nullable() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"]));

        // Note there are no nulls in a or b, but we specify that b is nullable
        let record_batch = RecordBatch::try_from_iter_with_nullable(vec![
            ("a", a, false),
            ("b", b, true),
        ])
        .expect("valid conversion");

        let expected_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        assert_eq!(record_batch.schema().as_ref(), &expected_schema);
        check_batch(record_batch, 5);
    }

    #[test]
    fn create_record_batch_schema_mismatch() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let a = Int64Array::from(vec![1, 2, 3, 4, 5]);

        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]);
        assert!(batch.is_err());
    }

    #[test]
    fn create_record_batch_field_name_mismatch() {
        let struct_fields = vec![
            Field::new("a1", DataType::Int32, false),
            Field::new(
                "a2",
                DataType::List(Box::new(Field::new("item", DataType::Int8, false))),
                false,
            ),
        ];
        let struct_type = DataType::Struct(struct_fields);
        let schema = Arc::new(Schema::new(vec![Field::new("a", struct_type, true)]));

        let a1: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let a2_child = Int8Array::from(vec![1, 2, 3, 4]);
        let a2 = ArrayDataBuilder::new(DataType::List(Box::new(Field::new(
            "array",
            DataType::Int8,
            false,
        ))))
        .add_child_data(a2_child.into_data())
        .len(2)
        .add_buffer(Buffer::from(vec![0i32, 3, 4].to_byte_slice()))
        .build()
        .unwrap();
        let a2: ArrayRef = Arc::new(ListArray::from(a2));
        let a = ArrayDataBuilder::new(DataType::Struct(vec![
            Field::new("aa1", DataType::Int32, false),
            Field::new("a2", a2.data_type().clone(), false),
        ]))
        .add_child_data(a1.into_data())
        .add_child_data(a2.into_data())
        .len(2)
        .build()
        .unwrap();
        let a: ArrayRef = Arc::new(StructArray::from(a));

        // creating the batch with field name validation should fail
        let batch = RecordBatch::try_new(schema.clone(), vec![a.clone()]);
        assert!(batch.is_err());

        // creating the batch without field name validation should pass
        let options = RecordBatchOptions {
            match_field_names: false,
            row_count: None,
        };
        let batch = RecordBatch::try_new_with_options(schema, vec![a], &options);
        assert!(batch.is_ok());
    }

    #[test]
    fn create_record_batch_record_mismatch() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let a = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let b = Int32Array::from(vec![1, 2, 3, 4, 5]);

        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a), Arc::new(b)]);
        assert!(batch.is_err());
    }

    #[test]
    fn create_record_batch_from_struct_array() {
        let boolean = Arc::new(BooleanArray::from(vec![false, false, true, true]));
        let int = Arc::new(Int32Array::from(vec![42, 28, 19, 31]));
        let struct_array = StructArray::from(vec![
            (
                Field::new("b", DataType::Boolean, false),
                boolean.clone() as ArrayRef,
            ),
            (
                Field::new("c", DataType::Int32, false),
                int.clone() as ArrayRef,
            ),
        ]);

        let batch = RecordBatch::from(&struct_array);
        assert_eq!(2, batch.num_columns());
        assert_eq!(4, batch.num_rows());
        assert_eq!(
            struct_array.data_type(),
            &DataType::Struct(batch.schema().fields().to_vec())
        );
        assert_eq!(batch.column(0).as_ref(), boolean.as_ref());
        assert_eq!(batch.column(1).as_ref(), int.as_ref());
    }

    #[test]
    fn record_batch_equality() {
        let id_arr1 = Int32Array::from(vec![1, 2, 3, 4]);
        let val_arr1 = Int32Array::from(vec![5, 6, 7, 8]);
        let schema1 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]);

        let id_arr2 = Int32Array::from(vec![1, 2, 3, 4]);
        let val_arr2 = Int32Array::from(vec![5, 6, 7, 8]);
        let schema2 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]);

        let batch1 = RecordBatch::try_new(
            Arc::new(schema1),
            vec![Arc::new(id_arr1), Arc::new(val_arr1)],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            Arc::new(schema2),
            vec![Arc::new(id_arr2), Arc::new(val_arr2)],
        )
        .unwrap();

        assert_eq!(batch1, batch2);
    }

    #[test]
    fn record_batch_vals_ne() {
        let id_arr1 = Int32Array::from(vec![1, 2, 3, 4]);
        let val_arr1 = Int32Array::from(vec![5, 6, 7, 8]);
        let schema1 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]);

        let id_arr2 = Int32Array::from(vec![1, 2, 3, 4]);
        let val_arr2 = Int32Array::from(vec![1, 2, 3, 4]);
        let schema2 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]);

        let batch1 = RecordBatch::try_new(
            Arc::new(schema1),
            vec![Arc::new(id_arr1), Arc::new(val_arr1)],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            Arc::new(schema2),
            vec![Arc::new(id_arr2), Arc::new(val_arr2)],
        )
        .unwrap();

        assert_ne!(batch1, batch2);
    }

    #[test]
    fn record_batch_column_names_ne() {
        let id_arr1 = Int32Array::from(vec![1, 2, 3, 4]);
        let val_arr1 = Int32Array::from(vec![5, 6, 7, 8]);
        let schema1 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]);

        let id_arr2 = Int32Array::from(vec![1, 2, 3, 4]);
        let val_arr2 = Int32Array::from(vec![5, 6, 7, 8]);
        let schema2 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("num", DataType::Int32, false),
        ]);

        let batch1 = RecordBatch::try_new(
            Arc::new(schema1),
            vec![Arc::new(id_arr1), Arc::new(val_arr1)],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            Arc::new(schema2),
            vec![Arc::new(id_arr2), Arc::new(val_arr2)],
        )
        .unwrap();

        assert_ne!(batch1, batch2);
    }

    #[test]
    fn record_batch_column_number_ne() {
        let id_arr1 = Int32Array::from(vec![1, 2, 3, 4]);
        let val_arr1 = Int32Array::from(vec![5, 6, 7, 8]);
        let schema1 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]);

        let id_arr2 = Int32Array::from(vec![1, 2, 3, 4]);
        let val_arr2 = Int32Array::from(vec![5, 6, 7, 8]);
        let num_arr2 = Int32Array::from(vec![5, 6, 7, 8]);
        let schema2 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
            Field::new("num", DataType::Int32, false),
        ]);

        let batch1 = RecordBatch::try_new(
            Arc::new(schema1),
            vec![Arc::new(id_arr1), Arc::new(val_arr1)],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            Arc::new(schema2),
            vec![Arc::new(id_arr2), Arc::new(val_arr2), Arc::new(num_arr2)],
        )
        .unwrap();

        assert_ne!(batch1, batch2);
    }

    #[test]
    fn record_batch_row_count_ne() {
        let id_arr1 = Int32Array::from(vec![1, 2, 3]);
        let val_arr1 = Int32Array::from(vec![5, 6, 7]);
        let schema1 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("val", DataType::Int32, false),
        ]);

        let id_arr2 = Int32Array::from(vec![1, 2, 3, 4]);
        let val_arr2 = Int32Array::from(vec![5, 6, 7, 8]);
        let schema2 = Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("num", DataType::Int32, false),
        ]);

        let batch1 = RecordBatch::try_new(
            Arc::new(schema1),
            vec![Arc::new(id_arr1), Arc::new(val_arr1)],
        )
        .unwrap();

        let batch2 = RecordBatch::try_new(
            Arc::new(schema2),
            vec![Arc::new(id_arr2), Arc::new(val_arr2)],
        )
        .unwrap();

        assert_ne!(batch1, batch2);
    }

    #[test]
    fn project() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3)]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let c: ArrayRef = Arc::new(StringArray::from(vec!["d", "e", "f"]));

        let record_batch = RecordBatch::try_from_iter(vec![
            ("a", a.clone()),
            ("b", b.clone()),
            ("c", c.clone()),
        ])
        .expect("valid conversion");

        let expected = RecordBatch::try_from_iter(vec![("a", a), ("c", c)])
            .expect("valid conversion");

        assert_eq!(expected, record_batch.project(&[0, 2]).unwrap());
    }

    #[test]
    fn project_empty() {
        let c: ArrayRef = Arc::new(StringArray::from(vec!["d", "e", "f"]));

        let record_batch =
            RecordBatch::try_from_iter(vec![("c", c.clone())]).expect("valid conversion");

        let expected = RecordBatch::try_new_with_options(
            Arc::new(Schema::empty()),
            vec![],
            &RecordBatchOptions {
                match_field_names: true,
                row_count: Some(3),
            },
        )
        .expect("valid conversion");

        assert_eq!(expected, record_batch.project(&[]).unwrap());
    }

    #[test]
    fn test_no_column_record_batch() {
        let schema = Arc::new(Schema::new(vec![]));

        let err = RecordBatch::try_new(schema.clone(), vec![]).unwrap_err();
        assert!(err
            .to_string()
            .contains("must either specify a row count or at least one column"));

        let options = RecordBatchOptions::new().with_row_count(Some(10));

        let ok =
            RecordBatch::try_new_with_options(schema.clone(), vec![], &options).unwrap();
        assert_eq!(ok.num_rows(), 10);

        let a = ok.slice(2, 5);
        assert_eq!(a.num_rows(), 5);

        let b = ok.slice(5, 0);
        assert_eq!(b.num_rows(), 0);

        assert_ne!(a, b);
        assert_eq!(b, RecordBatch::new_empty(schema))
    }

    #[test]
    fn test_nulls_in_non_nullable_field() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let maybe_batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![Some(1), None]))],
        );
        assert_eq!("Invalid argument error: Column 'a' is declared as non-nullable but contains null values", format!("{}", maybe_batch.err().unwrap()));
    }
    #[test]
    fn test_record_batch_options() {
        let options = RecordBatchOptions::new()
            .with_match_field_names(false)
            .with_row_count(Some(20));
        assert!(!options.match_field_names);
        assert_eq!(options.row_count.unwrap(), 20)
    }
}
