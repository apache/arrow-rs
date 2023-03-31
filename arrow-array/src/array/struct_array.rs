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

use crate::{make_array, Array, ArrayRef, RecordBatch};
use arrow_buffer::{buffer_bin_or, Buffer, NullBuffer};
use arrow_data::ArrayData;
use arrow_schema::{ArrowError, DataType, Field, SchemaBuilder};
use std::sync::Arc;
use std::{any::Any, ops::Index};

/// A nested array type where each child (called *field*) is represented by a separate
/// array.
/// # Example: Create an array from a vector of fields
///
/// ```
/// use std::sync::Arc;
/// use arrow_array::{Array, ArrayRef, BooleanArray, Int32Array, StructArray};
/// use arrow_schema::{DataType, Field};
///
/// let boolean = Arc::new(BooleanArray::from(vec![false, false, true, true]));
/// let int = Arc::new(Int32Array::from(vec![42, 28, 19, 31]));
///
/// let struct_array = StructArray::from(vec![
///     (
///         Field::new("b", DataType::Boolean, false),
///         boolean.clone() as ArrayRef,
///     ),
///     (
///         Field::new("c", DataType::Int32, false),
///         int.clone() as ArrayRef,
///     ),
/// ]);
/// assert_eq!(struct_array.column(0).as_ref(), boolean.as_ref());
/// assert_eq!(struct_array.column(1).as_ref(), int.as_ref());
/// assert_eq!(4, struct_array.len());
/// assert_eq!(0, struct_array.null_count());
/// assert_eq!(0, struct_array.offset());
/// ```
#[derive(Clone)]
pub struct StructArray {
    data: ArrayData,
    pub(crate) boxed_fields: Vec<ArrayRef>,
}

impl StructArray {
    /// Returns the field at `pos`.
    pub fn column(&self, pos: usize) -> &ArrayRef {
        &self.boxed_fields[pos]
    }

    /// Return the number of fields in this struct array
    pub fn num_columns(&self) -> usize {
        self.boxed_fields.len()
    }

    /// Returns the fields of the struct array
    pub fn columns(&self) -> &[ArrayRef] {
        &self.boxed_fields
    }

    /// Returns child array refs of the struct array
    #[deprecated(note = "Use columns().to_vec()")]
    pub fn columns_ref(&self) -> Vec<ArrayRef> {
        self.columns().to_vec()
    }

    /// Return field names in this struct array
    pub fn column_names(&self) -> Vec<&str> {
        match self.data.data_type() {
            DataType::Struct(fields) => fields
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<&str>>(),
            _ => unreachable!("Struct array's data type is not struct!"),
        }
    }

    /// Return child array whose field name equals to column_name
    ///
    /// Note: A schema can currently have duplicate field names, in which case
    /// the first field will always be selected.
    /// This issue will be addressed in [ARROW-11178](https://issues.apache.org/jira/browse/ARROW-11178)
    pub fn column_by_name(&self, column_name: &str) -> Option<&ArrayRef> {
        self.column_names()
            .iter()
            .position(|c| c == &column_name)
            .map(|pos| self.column(pos))
    }

    /// Returns a zero-copy slice of this array with the indicated offset and length.
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        // TODO: Slice buffers directly (#3880)
        self.data.slice(offset, length).into()
    }
}

impl From<ArrayData> for StructArray {
    fn from(data: ArrayData) -> Self {
        let boxed_fields = data
            .child_data()
            .iter()
            .map(|cd| make_array(cd.clone()))
            .collect();

        Self { data, boxed_fields }
    }
}

impl From<StructArray> for ArrayData {
    fn from(array: StructArray) -> Self {
        array.data
    }
}

impl TryFrom<Vec<(&str, ArrayRef)>> for StructArray {
    type Error = ArrowError;

    /// builds a StructArray from a vector of names and arrays.
    /// This errors if the values have a different length.
    /// An entry is set to Null when all values are null.
    fn try_from(values: Vec<(&str, ArrayRef)>) -> Result<Self, ArrowError> {
        let values_len = values.len();

        // these will be populated
        let mut fields = Vec::with_capacity(values_len);
        let mut child_data = Vec::with_capacity(values_len);

        // len: the size of the arrays.
        let mut len: Option<usize> = None;
        // null: the null mask of the arrays.
        let mut null: Option<Buffer> = None;
        for (field_name, array) in values {
            let child_datum = array.data();
            let child_datum_len = child_datum.len();
            if let Some(len) = len {
                if len != child_datum_len {
                    return Err(ArrowError::InvalidArgumentError(
                        format!("Array of field \"{field_name}\" has length {child_datum_len}, but previous elements have length {len}.
                        All arrays in every entry in a struct array must have the same length.")
                    ));
                }
            } else {
                len = Some(child_datum_len)
            }
            child_data.push(child_datum.clone());
            fields.push(Arc::new(Field::new(
                field_name,
                array.data_type().clone(),
                child_datum.nulls().is_some(),
            )));

            if let Some(child_nulls) = child_datum.nulls() {
                null = Some(if let Some(null_buffer) = &null {
                    buffer_bin_or(
                        null_buffer,
                        0,
                        child_nulls.buffer(),
                        child_nulls.offset(),
                        child_datum_len,
                    )
                } else {
                    child_nulls.inner().sliced()
                });
            } else if null.is_some() {
                // when one of the fields has no nulls, then there is no null in the array
                null = None;
            }
        }
        let len = len.unwrap();

        let builder = ArrayData::builder(DataType::Struct(fields.into()))
            .len(len)
            .null_bit_buffer(null)
            .child_data(child_data);

        let array_data = unsafe { builder.build_unchecked() };

        Ok(StructArray::from(array_data))
    }
}

impl Array for StructArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data(&self) -> &ArrayData {
        &self.data
    }

    fn to_data(&self) -> ArrayData {
        self.data.clone()
    }

    fn into_data(self) -> ArrayData {
        self.into()
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(self.slice(offset, length))
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.data.nulls()
    }
}

impl From<Vec<(Field, ArrayRef)>> for StructArray {
    fn from(v: Vec<(Field, ArrayRef)>) -> Self {
        let iter = v.into_iter();
        let capacity = iter.size_hint().0;

        let mut len = None;
        let mut schema = SchemaBuilder::with_capacity(capacity);
        let mut child_data = Vec::with_capacity(capacity);
        for (field, array) in iter {
            // Check the length of the child arrays
            assert_eq!(
                *len.get_or_insert(array.len()),
                array.len(),
                "all child arrays of a StructArray must have the same length"
            );
            // Check data types of child arrays
            assert_eq!(
                field.data_type(),
                array.data_type(),
                "the field data types must match the array data in a StructArray"
            );
            schema.push(field);
            child_data.push(array.to_data());
        }
        let field_types = schema.finish().fields;
        let array_data = ArrayData::builder(DataType::Struct(field_types))
            .child_data(child_data)
            .len(len.unwrap_or_default());
        let array_data = unsafe { array_data.build_unchecked() };

        // We must validate nullability
        array_data.validate_nulls().unwrap();

        Self::from(array_data)
    }
}

impl std::fmt::Debug for StructArray {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "StructArray\n[\n")?;
        for (child_index, name) in self.column_names().iter().enumerate() {
            let column = self.column(child_index);
            writeln!(
                f,
                "-- child {}: \"{}\" ({:?})",
                child_index,
                name,
                column.data_type()
            )?;
            std::fmt::Debug::fmt(column, f)?;
            writeln!(f)?;
        }
        write!(f, "]")
    }
}

impl From<(Vec<(Field, ArrayRef)>, Buffer)> for StructArray {
    fn from(pair: (Vec<(Field, ArrayRef)>, Buffer)) -> Self {
        let capacity = pair.0.len();
        let mut len = None;
        let mut schema = SchemaBuilder::with_capacity(capacity);
        let mut child_data = Vec::with_capacity(capacity);
        for (field, array) in pair.0 {
            // Check the length of the child arrays
            assert_eq!(
                *len.get_or_insert(array.len()),
                array.len(),
                "all child arrays of a StructArray must have the same length"
            );
            // Check data types of child arrays
            assert_eq!(
                field.data_type(),
                array.data_type(),
                "the field data types must match the array data in a StructArray"
            );
            schema.push(field);
            child_data.push(array.to_data());
        }
        let field_types = schema.finish().fields;
        let array_data = ArrayData::builder(DataType::Struct(field_types))
            .null_bit_buffer(Some(pair.1))
            .child_data(child_data)
            .len(len.unwrap_or_default());
        let array_data = unsafe { array_data.build_unchecked() };

        // We must validate nullability
        array_data.validate_nulls().unwrap();

        Self::from(array_data)
    }
}

impl From<RecordBatch> for StructArray {
    fn from(value: RecordBatch) -> Self {
        // TODO: Don't store ArrayData inside arrays (#3880)
        let builder = ArrayData::builder(DataType::Struct(value.schema().fields.clone()))
            .child_data(value.columns().iter().map(|x| x.to_data()).collect())
            .len(value.num_rows());

        // Safety: RecordBatch must be valid
        Self {
            data: unsafe { builder.build_unchecked() },
            boxed_fields: value.columns().to_vec(),
        }
    }
}

impl Index<&str> for StructArray {
    type Output = ArrayRef;

    /// Get a reference to a column's array by name.
    ///
    /// Note: A schema can currently have duplicate field names, in which case
    /// the first field will always be selected.
    /// This issue will be addressed in [ARROW-11178](https://issues.apache.org/jira/browse/ARROW-11178)
    ///
    /// # Panics
    ///
    /// Panics if the name is not in the schema.
    fn index(&self, name: &str) -> &Self::Output {
        self.column_by_name(name).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
    };
    use arrow_buffer::ToByteSlice;
    use std::sync::Arc;

    #[test]
    fn test_struct_array_builder() {
        let array = BooleanArray::from(vec![false, false, true, true]);
        let boolean_data = array.data();
        let array = Int64Array::from(vec![42, 28, 19, 31]);
        let int_data = array.data();

        let fields = vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Int64, false),
        ];
        let struct_array_data = ArrayData::builder(DataType::Struct(fields.into()))
            .len(4)
            .add_child_data(boolean_data.clone())
            .add_child_data(int_data.clone())
            .build()
            .unwrap();
        let struct_array = StructArray::from(struct_array_data);

        assert_eq!(boolean_data, struct_array.column(0).data());
        assert_eq!(int_data, struct_array.column(1).data());
    }

    #[test]
    fn test_struct_array_from() {
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
        assert_eq!(struct_array.column(0).as_ref(), boolean.as_ref());
        assert_eq!(struct_array.column(1).as_ref(), int.as_ref());
        assert_eq!(4, struct_array.len());
        assert_eq!(0, struct_array.null_count());
        assert_eq!(0, struct_array.offset());
    }

    /// validates that struct can be accessed using `column_name` as index i.e. `struct_array["column_name"]`.
    #[test]
    fn test_struct_array_index_access() {
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
        assert_eq!(struct_array["b"].as_ref(), boolean.as_ref());
        assert_eq!(struct_array["c"].as_ref(), int.as_ref());
    }

    /// validates that the in-memory representation follows [the spec](https://arrow.apache.org/docs/format/Columnar.html#struct-layout)
    #[test]
    fn test_struct_array_from_vec() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("joe"),
            None,
            None,
            Some("mark"),
        ]));
        let ints: ArrayRef =
            Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)]));

        let arr =
            StructArray::try_from(vec![("f1", strings.clone()), ("f2", ints.clone())])
                .unwrap();

        let struct_data = arr.data();
        assert_eq!(4, struct_data.len());
        assert_eq!(1, struct_data.null_count());
        assert_eq!(
            // 00001011
            &[11_u8],
            struct_data.nulls().unwrap().validity()
        );

        let expected_string_data = ArrayData::builder(DataType::Utf8)
            .len(4)
            .null_bit_buffer(Some(Buffer::from(&[9_u8])))
            .add_buffer(Buffer::from(&[0, 3, 3, 3, 7].to_byte_slice()))
            .add_buffer(Buffer::from(b"joemark"))
            .build()
            .unwrap();

        let expected_int_data = ArrayData::builder(DataType::Int32)
            .len(4)
            .null_bit_buffer(Some(Buffer::from(&[11_u8])))
            .add_buffer(Buffer::from(&[1, 2, 0, 4].to_byte_slice()))
            .build()
            .unwrap();

        assert_eq!(expected_string_data, *arr.column(0).data());
        assert_eq!(expected_int_data, *arr.column(1).data());
    }

    #[test]
    fn test_struct_array_from_vec_error() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("joe"),
            None,
            None,
            // 3 elements, not 4
        ]));
        let ints: ArrayRef =
            Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)]));

        let arr =
            StructArray::try_from(vec![("f1", strings.clone()), ("f2", ints.clone())]);

        match arr {
            Err(ArrowError::InvalidArgumentError(e)) => {
                assert!(e.starts_with("Array of field \"f2\" has length 4, but previous elements have length 3."));
            }
            _ => panic!("This test got an unexpected error type"),
        };
    }

    #[test]
    #[should_panic(
        expected = "the field data types must match the array data in a StructArray"
    )]
    fn test_struct_array_from_mismatched_types_single() {
        drop(StructArray::from(vec![(
            Field::new("b", DataType::Int16, false),
            Arc::new(BooleanArray::from(vec![false, false, true, true]))
                as Arc<dyn Array>,
        )]));
    }

    #[test]
    #[should_panic(
        expected = "the field data types must match the array data in a StructArray"
    )]
    fn test_struct_array_from_mismatched_types_multiple() {
        drop(StructArray::from(vec![
            (
                Field::new("b", DataType::Int16, false),
                Arc::new(BooleanArray::from(vec![false, false, true, true]))
                    as Arc<dyn Array>,
            ),
            (
                Field::new("c", DataType::Utf8, false),
                Arc::new(Int32Array::from(vec![42, 28, 19, 31])),
            ),
        ]));
    }

    #[test]
    fn test_struct_array_slice() {
        let boolean_data = ArrayData::builder(DataType::Boolean)
            .len(5)
            .add_buffer(Buffer::from([0b00010000]))
            .null_bit_buffer(Some(Buffer::from([0b00010001])))
            .build()
            .unwrap();
        let int_data = ArrayData::builder(DataType::Int32)
            .len(5)
            .add_buffer(Buffer::from([0, 28, 42, 0, 0].to_byte_slice()))
            .null_bit_buffer(Some(Buffer::from([0b00000110])))
            .build()
            .unwrap();

        let field_types = vec![
            Field::new("a", DataType::Boolean, true),
            Field::new("b", DataType::Int32, true),
        ];
        let struct_array_data = ArrayData::builder(DataType::Struct(field_types.into()))
            .len(5)
            .add_child_data(boolean_data.clone())
            .add_child_data(int_data.clone())
            .null_bit_buffer(Some(Buffer::from([0b00010111])))
            .build()
            .unwrap();
        let struct_array = StructArray::from(struct_array_data);

        assert_eq!(5, struct_array.len());
        assert_eq!(1, struct_array.null_count());
        assert!(struct_array.is_valid(0));
        assert!(struct_array.is_valid(1));
        assert!(struct_array.is_valid(2));
        assert!(struct_array.is_null(3));
        assert!(struct_array.is_valid(4));
        assert_eq!(&boolean_data, struct_array.column(0).data());
        assert_eq!(&int_data, struct_array.column(1).data());

        let c0 = struct_array.column(0);
        let c0 = c0.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(5, c0.len());
        assert_eq!(3, c0.null_count());
        assert!(c0.is_valid(0));
        assert!(!c0.value(0));
        assert!(c0.is_null(1));
        assert!(c0.is_null(2));
        assert!(c0.is_null(3));
        assert!(c0.is_valid(4));
        assert!(c0.value(4));

        let c1 = struct_array.column(1);
        let c1 = c1.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(5, c1.len());
        assert_eq!(3, c1.null_count());
        assert!(c1.is_null(0));
        assert!(c1.is_valid(1));
        assert_eq!(28, c1.value(1));
        assert!(c1.is_valid(2));
        assert_eq!(42, c1.value(2));
        assert!(c1.is_null(3));
        assert!(c1.is_null(4));

        let sliced_array = struct_array.slice(2, 3);
        let sliced_array = sliced_array.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(3, sliced_array.len());
        assert_eq!(2, sliced_array.offset());
        assert_eq!(1, sliced_array.null_count());
        assert!(sliced_array.is_valid(0));
        assert!(sliced_array.is_null(1));
        assert!(sliced_array.is_valid(2));

        let sliced_c0 = sliced_array.column(0);
        let sliced_c0 = sliced_c0.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(3, sliced_c0.len());
        assert_eq!(2, sliced_c0.offset());
        assert!(sliced_c0.is_null(0));
        assert!(sliced_c0.is_null(1));
        assert!(sliced_c0.is_valid(2));
        assert!(sliced_c0.value(2));

        let sliced_c1 = sliced_array.column(1);
        let sliced_c1 = sliced_c1.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(3, sliced_c1.len());
        assert_eq!(2, sliced_c1.offset());
        assert!(sliced_c1.is_valid(0));
        assert_eq!(42, sliced_c1.value(0));
        assert!(sliced_c1.is_null(1));
        assert!(sliced_c1.is_null(2));
    }

    #[test]
    #[should_panic(
        expected = "all child arrays of a StructArray must have the same length"
    )]
    fn test_invalid_struct_child_array_lengths() {
        drop(StructArray::from(vec![
            (
                Field::new("b", DataType::Float32, false),
                Arc::new(Float32Array::from(vec![1.1])) as Arc<dyn Array>,
            ),
            (
                Field::new("c", DataType::Float64, false),
                Arc::new(Float64Array::from(vec![2.2, 3.3])),
            ),
        ]));
    }

    #[test]
    fn test_struct_array_from_empty() {
        let sa = StructArray::from(vec![]);
        assert!(sa.is_empty())
    }

    #[test]
    #[should_panic(
        expected = "non-nullable child of type Int32 contains nulls not present in parent Struct"
    )]
    fn test_struct_array_from_mismatched_nullability() {
        drop(StructArray::from(vec![(
            Field::new("c", DataType::Int32, false),
            Arc::new(Int32Array::from(vec![Some(42), None, Some(19)])) as ArrayRef,
        )]));
    }
}
