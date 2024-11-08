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

use crate::array::print_long_array;
use crate::{make_array, new_null_array, Array, ArrayRef, RecordBatch};
use arrow_buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType, Field, FieldRef, Fields};
use std::sync::Arc;
use std::{any::Any, ops::Index};

/// An array of [structs](https://arrow.apache.org/docs/format/Columnar.html#struct-layout)
///
/// Each child (called *field*) is represented by a separate array.
///
/// # Comparison with [RecordBatch]
///
/// Both [`RecordBatch`] and [`StructArray`] represent a collection of columns / arrays with the
/// same length.
///
/// However, there are a couple of key differences:
///
/// * [`StructArray`] can be nested within other [`Array`], including itself
/// * [`RecordBatch`] can contain top-level metadata on its associated [`Schema`][arrow_schema::Schema]
/// * [`StructArray`] can contain top-level nulls, i.e. `null`
/// * [`RecordBatch`] can only represent nulls in its child columns, i.e. `{"field": null}`
///
/// [`StructArray`] is therefore a more general data container than [`RecordBatch`], and as such
/// code that needs to handle both will typically share an implementation in terms of
/// [`StructArray`] and convert to/from [`RecordBatch`] as necessary.
///
/// [`From`] implementations are provided to facilitate this conversion, however, converting
/// from a [`StructArray`] containing top-level nulls to a [`RecordBatch`] will panic, as there
/// is no way to preserve them.
///
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
///         Arc::new(Field::new("b", DataType::Boolean, false)),
///         boolean.clone() as ArrayRef,
///     ),
///     (
///         Arc::new(Field::new("c", DataType::Int32, false)),
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
    len: usize,
    data_type: DataType,
    nulls: Option<NullBuffer>,
    fields: Vec<ArrayRef>,
}

impl StructArray {
    /// Create a new [`StructArray`] from the provided parts, panicking on failure
    ///
    /// # Panics
    ///
    /// Panics if [`Self::try_new`] returns an error
    pub fn new(fields: Fields, arrays: Vec<ArrayRef>, nulls: Option<NullBuffer>) -> Self {
        Self::try_new(fields, arrays, nulls).unwrap()
    }

    /// Create a new [`StructArray`] from the provided parts, returning an error on failure
    ///
    /// # Errors
    ///
    /// Errors if
    ///
    /// * `fields.len() != arrays.len()`
    /// * `fields[i].data_type() != arrays[i].data_type()`
    /// * `arrays[i].len() != arrays[j].len()`
    /// * `arrays[i].len() != nulls.len()`
    /// * `!fields[i].is_nullable() && !nulls.contains(arrays[i].nulls())`
    pub fn try_new(
        fields: Fields,
        arrays: Vec<ArrayRef>,
        nulls: Option<NullBuffer>,
    ) -> Result<Self, ArrowError> {
        if fields.len() != arrays.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Incorrect number of arrays for StructArray fields, expected {} got {}",
                fields.len(),
                arrays.len()
            )));
        }
        let len = arrays.first().map(|x| x.len()).unwrap_or_default();

        if let Some(n) = nulls.as_ref() {
            if n.len() != len {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Incorrect number of nulls for StructArray, expected {len} got {}",
                    n.len(),
                )));
            }
        }

        for (f, a) in fields.iter().zip(&arrays) {
            if f.data_type() != a.data_type() {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Incorrect datatype for StructArray field {:?}, expected {} got {}",
                    f.name(),
                    f.data_type(),
                    a.data_type()
                )));
            }

            if a.len() != len {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Incorrect array length for StructArray field {:?}, expected {} got {}",
                    f.name(),
                    len,
                    a.len()
                )));
            }

            if !f.is_nullable() {
                if let Some(a) = a.logical_nulls() {
                    if !nulls.as_ref().map(|n| n.contains(&a)).unwrap_or_default() {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Found unmasked nulls for non-nullable StructArray field {:?}",
                            f.name()
                        )));
                    }
                }
            }
        }

        Ok(Self {
            len,
            data_type: DataType::Struct(fields),
            nulls: nulls.filter(|n| n.null_count() > 0),
            fields: arrays,
        })
    }

    /// Create a new [`StructArray`] of length `len` where all values are null
    pub fn new_null(fields: Fields, len: usize) -> Self {
        let arrays = fields
            .iter()
            .map(|f| new_null_array(f.data_type(), len))
            .collect();

        Self {
            len,
            data_type: DataType::Struct(fields),
            nulls: Some(NullBuffer::new_null(len)),
            fields: arrays,
        }
    }

    /// Create a new [`StructArray`] from the provided parts without validation
    ///
    /// # Safety
    ///
    /// Safe if [`Self::new`] would not panic with the given arguments
    pub unsafe fn new_unchecked(
        fields: Fields,
        arrays: Vec<ArrayRef>,
        nulls: Option<NullBuffer>,
    ) -> Self {
        let len = arrays.first().map(|x| x.len()).unwrap_or_default();
        Self {
            len,
            data_type: DataType::Struct(fields),
            nulls,
            fields: arrays,
        }
    }

    /// Create a new [`StructArray`] containing no fields
    ///
    /// # Panics
    ///
    /// If `len != nulls.len()`
    pub fn new_empty_fields(len: usize, nulls: Option<NullBuffer>) -> Self {
        if let Some(n) = &nulls {
            assert_eq!(len, n.len())
        }
        Self {
            len,
            data_type: DataType::Struct(Fields::empty()),
            fields: vec![],
            nulls,
        }
    }

    /// Deconstruct this array into its constituent parts
    pub fn into_parts(self) -> (Fields, Vec<ArrayRef>, Option<NullBuffer>) {
        let f = match self.data_type {
            DataType::Struct(f) => f,
            _ => unreachable!(),
        };
        (f, self.fields, self.nulls)
    }

    /// Returns the field at `pos`.
    pub fn column(&self, pos: usize) -> &ArrayRef {
        &self.fields[pos]
    }

    /// Return the number of fields in this struct array
    pub fn num_columns(&self) -> usize {
        self.fields.len()
    }

    /// Returns the fields of the struct array
    pub fn columns(&self) -> &[ArrayRef] {
        &self.fields
    }

    /// Returns child array refs of the struct array
    #[deprecated(note = "Use columns().to_vec()")]
    pub fn columns_ref(&self) -> Vec<ArrayRef> {
        self.columns().to_vec()
    }

    /// Return field names in this struct array
    pub fn column_names(&self) -> Vec<&str> {
        match self.data_type() {
            DataType::Struct(fields) => fields
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<&str>>(),
            _ => unreachable!("Struct array's data type is not struct!"),
        }
    }

    /// Returns the [`Fields`] of this [`StructArray`]
    pub fn fields(&self) -> &Fields {
        match self.data_type() {
            DataType::Struct(f) => f,
            _ => unreachable!(),
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
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        assert!(
            offset.saturating_add(len) <= self.len,
            "the length + offset of the sliced StructArray cannot exceed the existing length"
        );

        let fields = self.fields.iter().map(|a| a.slice(offset, len)).collect();

        Self {
            len,
            data_type: self.data_type.clone(),
            nulls: self.nulls.as_ref().map(|n| n.slice(offset, len)),
            fields,
        }
    }
}

impl From<ArrayData> for StructArray {
    fn from(data: ArrayData) -> Self {
        let fields = data
            .child_data()
            .iter()
            .map(|cd| make_array(cd.clone()))
            .collect();

        Self {
            len: data.len(),
            data_type: data.data_type().clone(),
            nulls: data.nulls().cloned(),
            fields,
        }
    }
}

impl From<StructArray> for ArrayData {
    fn from(array: StructArray) -> Self {
        let builder = ArrayDataBuilder::new(array.data_type)
            .len(array.len)
            .nulls(array.nulls)
            .child_data(array.fields.iter().map(|x| x.to_data()).collect());

        unsafe { builder.build_unchecked() }
    }
}

impl TryFrom<Vec<(&str, ArrayRef)>> for StructArray {
    type Error = ArrowError;

    /// builds a StructArray from a vector of names and arrays.
    fn try_from(values: Vec<(&str, ArrayRef)>) -> Result<Self, ArrowError> {
        let (fields, arrays): (Vec<_>, _) = values
            .into_iter()
            .map(|(name, array)| {
                (
                    Field::new(name, array.data_type().clone(), array.is_nullable()),
                    array,
                )
            })
            .unzip();

        StructArray::try_new(fields.into(), arrays, None)
    }
}

impl Array for StructArray {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn to_data(&self) -> ArrayData {
        self.clone().into()
    }

    fn into_data(self) -> ArrayData {
        self.into()
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(self.slice(offset, length))
    }

    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn offset(&self) -> usize {
        0
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    fn logical_null_count(&self) -> usize {
        // More efficient that the default implementation
        self.null_count()
    }

    fn get_buffer_memory_size(&self) -> usize {
        let mut size = self.fields.iter().map(|a| a.get_buffer_memory_size()).sum();
        if let Some(n) = self.nulls.as_ref() {
            size += n.buffer().capacity();
        }
        size
    }

    fn get_array_memory_size(&self) -> usize {
        let mut size = self.fields.iter().map(|a| a.get_array_memory_size()).sum();
        size += std::mem::size_of::<Self>();
        if let Some(n) = self.nulls.as_ref() {
            size += n.buffer().capacity();
        }
        size
    }
}

impl From<Vec<(FieldRef, ArrayRef)>> for StructArray {
    fn from(v: Vec<(FieldRef, ArrayRef)>) -> Self {
        let (fields, arrays): (Vec<_>, _) = v.into_iter().unzip();
        StructArray::new(fields.into(), arrays, None)
    }
}

impl std::fmt::Debug for StructArray {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        writeln!(f, "StructArray")?;
        writeln!(f, "-- validity: ")?;
        writeln!(f, "[")?;
        print_long_array(self, f, |_array, _index, f| write!(f, "valid"))?;
        writeln!(f, "]\n[")?;
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

impl From<(Vec<(FieldRef, ArrayRef)>, Buffer)> for StructArray {
    fn from(pair: (Vec<(FieldRef, ArrayRef)>, Buffer)) -> Self {
        let len = pair.0.first().map(|x| x.1.len()).unwrap_or_default();
        let (fields, arrays): (Vec<_>, Vec<_>) = pair.0.into_iter().unzip();
        let nulls = NullBuffer::new(BooleanBuffer::new(pair.1, 0, len));
        Self::new(fields.into(), arrays, Some(nulls))
    }
}

impl From<RecordBatch> for StructArray {
    fn from(value: RecordBatch) -> Self {
        Self {
            len: value.num_rows(),
            data_type: DataType::Struct(value.schema().fields().clone()),
            nulls: None,
            fields: value.columns().to_vec(),
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

    use crate::{BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray};
    use arrow_buffer::ToByteSlice;

    #[test]
    fn test_struct_array_builder() {
        let boolean_array = BooleanArray::from(vec![false, false, true, true]);
        let int_array = Int64Array::from(vec![42, 28, 19, 31]);

        let fields = vec![
            Field::new("a", DataType::Boolean, false),
            Field::new("b", DataType::Int64, false),
        ];
        let struct_array_data = ArrayData::builder(DataType::Struct(fields.into()))
            .len(4)
            .add_child_data(boolean_array.to_data())
            .add_child_data(int_array.to_data())
            .build()
            .unwrap();
        let struct_array = StructArray::from(struct_array_data);

        assert_eq!(struct_array.column(0).as_ref(), &boolean_array);
        assert_eq!(struct_array.column(1).as_ref(), &int_array);
    }

    #[test]
    fn test_struct_array_from() {
        let boolean = Arc::new(BooleanArray::from(vec![false, false, true, true]));
        let int = Arc::new(Int32Array::from(vec![42, 28, 19, 31]));

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("b", DataType::Boolean, false)),
                boolean.clone() as ArrayRef,
            ),
            (
                Arc::new(Field::new("c", DataType::Int32, false)),
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
                Arc::new(Field::new("b", DataType::Boolean, false)),
                boolean.clone() as ArrayRef,
            ),
            (
                Arc::new(Field::new("c", DataType::Int32, false)),
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
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)]));

        let arr =
            StructArray::try_from(vec![("f1", strings.clone()), ("f2", ints.clone())]).unwrap();

        let struct_data = arr.into_data();
        assert_eq!(4, struct_data.len());
        assert_eq!(0, struct_data.null_count());

        let expected_string_data = ArrayData::builder(DataType::Utf8)
            .len(4)
            .null_bit_buffer(Some(Buffer::from(&[9_u8])))
            .add_buffer(Buffer::from([0, 3, 3, 3, 7].to_byte_slice()))
            .add_buffer(Buffer::from(b"joemark"))
            .build()
            .unwrap();

        let expected_int_data = ArrayData::builder(DataType::Int32)
            .len(4)
            .null_bit_buffer(Some(Buffer::from(&[11_u8])))
            .add_buffer(Buffer::from([1, 2, 0, 4].to_byte_slice()))
            .build()
            .unwrap();

        assert_eq!(expected_string_data, struct_data.child_data()[0]);
        assert_eq!(expected_int_data, struct_data.child_data()[1]);
    }

    #[test]
    fn test_struct_array_from_vec_error() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("joe"),
            None,
            None,
            // 3 elements, not 4
        ]));
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2), None, Some(4)]));

        let err = StructArray::try_from(vec![("f1", strings.clone()), ("f2", ints.clone())])
            .unwrap_err()
            .to_string();

        assert_eq!(
            err,
            "Invalid argument error: Incorrect array length for StructArray field \"f2\", expected 3 got 4"
        )
    }

    #[test]
    #[should_panic(
        expected = "Incorrect datatype for StructArray field \\\"b\\\", expected Int16 got Boolean"
    )]
    fn test_struct_array_from_mismatched_types_single() {
        drop(StructArray::from(vec![(
            Arc::new(Field::new("b", DataType::Int16, false)),
            Arc::new(BooleanArray::from(vec![false, false, true, true])) as Arc<dyn Array>,
        )]));
    }

    #[test]
    #[should_panic(
        expected = "Incorrect datatype for StructArray field \\\"b\\\", expected Int16 got Boolean"
    )]
    fn test_struct_array_from_mismatched_types_multiple() {
        drop(StructArray::from(vec![
            (
                Arc::new(Field::new("b", DataType::Int16, false)),
                Arc::new(BooleanArray::from(vec![false, false, true, true])) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("c", DataType::Utf8, false)),
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
        assert_eq!(boolean_data, struct_array.column(0).to_data());
        assert_eq!(int_data, struct_array.column(1).to_data());

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
        assert_eq!(1, sliced_array.null_count());
        assert!(sliced_array.is_valid(0));
        assert!(sliced_array.is_null(1));
        assert!(sliced_array.is_valid(2));

        let sliced_c0 = sliced_array.column(0);
        let sliced_c0 = sliced_c0.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(3, sliced_c0.len());
        assert!(sliced_c0.is_null(0));
        assert!(sliced_c0.is_null(1));
        assert!(sliced_c0.is_valid(2));
        assert!(sliced_c0.value(2));

        let sliced_c1 = sliced_array.column(1);
        let sliced_c1 = sliced_c1.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(3, sliced_c1.len());
        assert!(sliced_c1.is_valid(0));
        assert_eq!(42, sliced_c1.value(0));
        assert!(sliced_c1.is_null(1));
        assert!(sliced_c1.is_null(2));
    }

    #[test]
    #[should_panic(
        expected = "Incorrect array length for StructArray field \\\"c\\\", expected 1 got 2"
    )]
    fn test_invalid_struct_child_array_lengths() {
        drop(StructArray::from(vec![
            (
                Arc::new(Field::new("b", DataType::Float32, false)),
                Arc::new(Float32Array::from(vec![1.1])) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("c", DataType::Float64, false)),
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
    #[should_panic(expected = "Found unmasked nulls for non-nullable StructArray field \\\"c\\\"")]
    fn test_struct_array_from_mismatched_nullability() {
        drop(StructArray::from(vec![(
            Arc::new(Field::new("c", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![Some(42), None, Some(19)])) as ArrayRef,
        )]));
    }

    #[test]
    fn test_struct_array_fmt_debug() {
        let arr: StructArray = StructArray::new(
            vec![Arc::new(Field::new("c", DataType::Int32, true))].into(),
            vec![Arc::new(Int32Array::from((0..30).collect::<Vec<_>>())) as ArrayRef],
            Some(NullBuffer::new(BooleanBuffer::from(
                (0..30).map(|i| i % 2 == 0).collect::<Vec<_>>(),
            ))),
        );
        assert_eq!(format!("{arr:?}"), "StructArray\n-- validity: \n[\n  valid,\n  null,\n  valid,\n  null,\n  valid,\n  null,\n  valid,\n  null,\n  valid,\n  null,\n  ...10 elements...,\n  valid,\n  null,\n  valid,\n  null,\n  valid,\n  null,\n  valid,\n  null,\n  valid,\n  null,\n]\n[\n-- child 0: \"c\" (Int32)\nPrimitiveArray<Int32>\n[\n  0,\n  1,\n  2,\n  3,\n  4,\n  5,\n  6,\n  7,\n  8,\n  9,\n  ...10 elements...,\n  20,\n  21,\n  22,\n  23,\n  24,\n  25,\n  26,\n  27,\n  28,\n  29,\n]\n]")
    }
}
