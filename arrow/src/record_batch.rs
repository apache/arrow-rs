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
//! [schema](crate::datatypes::Schema).

use std::sync::Arc;

use crate::array::*;
use crate::compute::kernels::concat::concat;
use crate::datatypes::*;
use crate::error::{ArrowError, Result};

/// A two-dimensional batch of column-oriented data with a defined
/// [schema](crate::datatypes::Schema).
///
/// A `RecordBatch` is a two-dimensional dataset of a number of
/// contiguous arrays, each the same length.
/// A record batch has a schema which must match its arrays’
/// datatypes.
///
/// Record batches are a convenient unit of work for various
/// serialization and computation functions, possibly incremental.
/// See also [CSV reader](crate::csv::Reader) and
/// [JSON reader](crate::json::Reader).
#[derive(Clone, Debug, PartialEq)]
pub struct RecordBatch {
    schema: SchemaRef,
    columns: Vec<Arc<dyn Array>>,
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
    /// use std::sync::Arc;
    /// use arrow::array::Int32Array;
    /// use arrow::datatypes::{Schema, Field, DataType};
    /// use arrow::record_batch::RecordBatch;
    ///
    /// # fn main() -> arrow::error::Result<()> {
    /// let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false)
    /// ]);
    ///
    /// let batch = RecordBatch::try_new(
    ///     Arc::new(schema),
    ///     vec![Arc::new(id_array)]
    /// )?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn try_new(schema: SchemaRef, columns: Vec<ArrayRef>) -> Result<Self> {
        let options = RecordBatchOptions::default();
        Self::validate_new_batch(&schema, columns.as_slice(), &options)?;
        Ok(RecordBatch { schema, columns })
    }

    /// Creates a `RecordBatch` from a schema and columns, with additional options,
    /// such as whether to strictly validate field names.
    ///
    /// See [`RecordBatch::try_new`] for the expected conditions.
    pub fn try_new_with_options(
        schema: SchemaRef,
        columns: Vec<ArrayRef>,
        options: &RecordBatchOptions,
    ) -> Result<Self> {
        Self::validate_new_batch(&schema, columns.as_slice(), options)?;
        Ok(RecordBatch { schema, columns })
    }

    /// Creates a new empty [`RecordBatch`].
    pub fn new_empty(schema: SchemaRef) -> Self {
        let columns = schema
            .fields()
            .iter()
            .map(|field| new_empty_array(field.data_type()))
            .collect();
        RecordBatch { schema, columns }
    }

    /// Validate the schema and columns using [`RecordBatchOptions`]. Returns an error
    /// if any validation check fails.
    fn validate_new_batch(
        schema: &SchemaRef,
        columns: &[ArrayRef],
        options: &RecordBatchOptions,
    ) -> Result<()> {
        // check that there are some columns
        if columns.is_empty() {
            return Err(ArrowError::InvalidArgumentError(
                "at least one column must be defined to create a record batch"
                    .to_string(),
            ));
        }
        // check that number of fields in schema match column length
        if schema.fields().len() != columns.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "number of columns({}) must match number of fields({}) in schema",
                columns.len(),
                schema.fields().len(),
            )));
        }

        // check that all columns have the same row count
        let row_count = columns[0].data().len();
        if columns.iter().any(|c| c.len() != row_count) {
            return Err(ArrowError::InvalidArgumentError(
                "all columns in a record batch must have the same length".to_string(),
            ));
        }

        // function for comparing schema
        let compare_type = if options.match_field_names {
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
            .map(|c| c.data_type())
            .zip(schema.fields().iter().map(|f| f.data_type()))
            .enumerate()
            .find(compare_type);

        if let Some((i, (col_type, field_type))) = not_match {
            return Err(ArrowError::InvalidArgumentError(format!(
                "column types must match schema types, expected {:?} but found {:?} at column index {}",
                field_type,
                col_type,
                i)));
        }

        Ok(())
    }

    /// Returns the [`Schema`](crate::datatypes::Schema) of the record batch.
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Projects the schema onto the specified columns
    pub fn project(&self, indices: &[usize]) -> Result<RecordBatch> {
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
            .collect::<Result<Vec<_>>>()?;

        RecordBatch::try_new(SchemaRef::new(projected_schema), batch_fields)
    }

    /// Returns the number of columns in the record batch.
    ///
    /// # Example
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow::array::Int32Array;
    /// use arrow::datatypes::{Schema, Field, DataType};
    /// use arrow::record_batch::RecordBatch;
    ///
    /// # fn main() -> arrow::error::Result<()> {
    /// let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false)
    /// ]);
    ///
    /// let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)])?;
    ///
    /// assert_eq!(batch.num_columns(), 1);
    /// # Ok(())
    /// # }
    /// ```
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// Returns the number of rows in each column.
    ///
    /// # Panics
    ///
    /// Panics if the `RecordBatch` contains no columns.
    ///
    /// # Example
    ///
    /// ```
    /// use std::sync::Arc;
    /// use arrow::array::Int32Array;
    /// use arrow::datatypes::{Schema, Field, DataType};
    /// use arrow::record_batch::RecordBatch;
    ///
    /// # fn main() -> arrow::error::Result<()> {
    /// let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let schema = Schema::new(vec![
    ///     Field::new("id", DataType::Int32, false)
    /// ]);
    ///
    /// let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id_array)])?;
    ///
    /// assert_eq!(batch.num_rows(), 5);
    /// # Ok(())
    /// # }
    /// ```
    pub fn num_rows(&self) -> usize {
        self.columns[0].data().len()
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
        if self.schema.fields().is_empty() {
            assert!((offset + length) == 0);
            return RecordBatch::new_empty(self.schema.clone());
        }
        assert!((offset + length) <= self.num_rows());

        let columns = self
            .columns()
            .iter()
            .map(|column| column.slice(offset, length))
            .collect();

        Self {
            schema: self.schema.clone(),
            columns,
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
    /// use std::sync::Arc;
    /// use arrow::array::{ArrayRef, Int32Array, StringArray};
    /// use arrow::datatypes::{Schema, Field, DataType};
    /// use arrow::record_batch::RecordBatch;
    ///
    /// let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
    /// let b: ArrayRef = Arc::new(StringArray::from(vec!["a", "b"]));
    ///
    /// let record_batch = RecordBatch::try_from_iter(vec![
    ///   ("a", a),
    ///   ("b", b),
    /// ]);
    /// ```
    pub fn try_from_iter<I, F>(value: I) -> Result<Self>
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
    /// use std::sync::Arc;
    /// use arrow::array::{ArrayRef, Int32Array, StringArray};
    /// use arrow::datatypes::{Schema, Field, DataType};
    /// use arrow::record_batch::RecordBatch;
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
    pub fn try_from_iter_with_nullable<I, F>(value: I) -> Result<Self>
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

    /// Concatenates `batches` together into a single record batch.
    pub fn concat(schema: &SchemaRef, batches: &[Self]) -> Result<Self> {
        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(schema.clone()));
        }
        if let Some((i, _)) = batches
            .iter()
            .enumerate()
            .find(|&(_, batch)| batch.schema() != *schema)
        {
            return Err(ArrowError::InvalidArgumentError(format!(
                "batches[{}] schema is different with argument schema.",
                i
            )));
        }
        let field_num = schema.fields().len();
        let mut arrays = Vec::with_capacity(field_num);
        for i in 0..field_num {
            let array = concat(
                &batches
                    .iter()
                    .map(|batch| batch.column(i).as_ref())
                    .collect::<Vec<_>>(),
            )?;
            arrays.push(array);
        }
        Self::try_new(schema.clone(), arrays)
    }
}

/// Options that control the behaviour used when creating a [`RecordBatch`].
#[derive(Debug)]
pub struct RecordBatchOptions {
    /// Match field names of structs and lists. If set to `true`, the names must match.
    pub match_field_names: bool,
}

impl Default for RecordBatchOptions {
    fn default() -> Self {
        Self {
            match_field_names: true,
        }
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

/// Trait for types that can read `RecordBatch`'s.
pub trait RecordBatchReader: Iterator<Item = Result<RecordBatch>> {
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
    fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.next().transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::buffer::Buffer;

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
    #[should_panic(expected = "assertion failed: (offset + length) == 0")]
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
        assert!(!batch.is_ok());
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
        .add_child_data(a2_child.data().clone())
        .len(2)
        .add_buffer(Buffer::from(vec![0i32, 3, 4].to_byte_slice()))
        .build()
        .unwrap();
        let a2: ArrayRef = Arc::new(ListArray::from(a2));
        let a = ArrayDataBuilder::new(DataType::Struct(vec![
            Field::new("aa1", DataType::Int32, false),
            Field::new("a2", a2.data_type().clone(), false),
        ]))
        .add_child_data(a1.data().clone())
        .add_child_data(a2.data().clone())
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
        assert!(!batch.is_ok());
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
    fn concat_record_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![3, 4])),
                Arc::new(StringArray::from(vec!["c", "d"])),
            ],
        )
        .unwrap();
        let new_batch = RecordBatch::concat(&schema, &[batch1, batch2]).unwrap();
        assert_eq!(new_batch.schema().as_ref(), schema.as_ref());
        assert_eq!(2, new_batch.num_columns());
        assert_eq!(4, new_batch.num_rows());
    }

    #[test]
    fn concat_empty_record_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::concat(&schema, &[]).unwrap();
        assert_eq!(batch.schema().as_ref(), schema.as_ref());
        assert_eq!(0, batch.num_rows());
    }

    #[test]
    fn concat_record_batches_of_different_schemas() {
        let schema1 = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("c", DataType::Int32, false),
            Field::new("d", DataType::Utf8, false),
        ]));
        let batch1 = RecordBatch::try_new(
            schema1.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            schema2,
            vec![
                Arc::new(Int32Array::from(vec![3, 4])),
                Arc::new(StringArray::from(vec!["c", "d"])),
            ],
        )
        .unwrap();
        let error = RecordBatch::concat(&schema1, &[batch1, batch2]).unwrap_err();
        assert_eq!(
            error.to_string(),
            "Invalid argument error: batches[1] schema is different with argument schema.",
        );
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
}
