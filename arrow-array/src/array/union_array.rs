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

use crate::{make_array, Array, ArrayRef};
use arrow_buffer::buffer::NullBuffer;
use arrow_buffer::{Buffer, ScalarBuffer};
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType, Field, UnionFields, UnionMode};
/// Contains the `UnionArray` type.
///
use std::any::Any;
use std::sync::Arc;

/// An array of [values of varying types](https://arrow.apache.org/docs/format/Columnar.html#union-layout)
///
/// Each slot in a [UnionArray] can have a value chosen from a number
/// of types.  Each of the possible types are named like the fields of
/// a [`StructArray`](crate::StructArray).  A `UnionArray` can
/// have two possible memory layouts, "dense" or "sparse".  For more
/// information on please see the
/// [specification](https://arrow.apache.org/docs/format/Columnar.html#union-layout).
///
/// [UnionBuilder](crate::builder::UnionBuilder) can be used to
/// create [UnionArray]'s of primitive types. `UnionArray`'s of nested
/// types are also supported but not via `UnionBuilder`, see the tests
/// for examples.
///
/// # Examples
/// ## Create a dense UnionArray `[1, 3.2, 34]`
/// ```
/// use arrow_buffer::Buffer;
/// use arrow_schema::*;
/// use std::sync::Arc;
/// use arrow_array::{Array, Int32Array, Float64Array, UnionArray};
///
/// let int_array = Int32Array::from(vec![1, 34]);
/// let float_array = Float64Array::from(vec![3.2]);
/// let type_id_buffer = Buffer::from_slice_ref(&[0_i8, 1, 0]);
/// let value_offsets_buffer = Buffer::from_slice_ref(&[0_i32, 0, 1]);
///
/// let children: Vec<(Field, Arc<dyn Array>)> = vec![
///     (Field::new("A", DataType::Int32, false), Arc::new(int_array)),
///     (Field::new("B", DataType::Float64, false), Arc::new(float_array)),
/// ];
///
/// let array = UnionArray::try_new(
///     &vec![0, 1],
///     type_id_buffer,
///     Some(value_offsets_buffer),
///     children,
/// ).unwrap();
///
/// let value = array.value(0).as_any().downcast_ref::<Int32Array>().unwrap().value(0);
/// assert_eq!(1, value);
///
/// let value = array.value(1).as_any().downcast_ref::<Float64Array>().unwrap().value(0);
/// assert!(3.2 - value < f64::EPSILON);
///
/// let value = array.value(2).as_any().downcast_ref::<Int32Array>().unwrap().value(0);
/// assert_eq!(34, value);
/// ```
///
/// ## Create a sparse UnionArray `[1, 3.2, 34]`
/// ```
/// use arrow_buffer::Buffer;
/// use arrow_schema::*;
/// use std::sync::Arc;
/// use arrow_array::{Array, Int32Array, Float64Array, UnionArray};
///
/// let int_array = Int32Array::from(vec![Some(1), None, Some(34)]);
/// let float_array = Float64Array::from(vec![None, Some(3.2), None]);
/// let type_id_buffer = Buffer::from_slice_ref(&[0_i8, 1, 0]);
///
/// let children: Vec<(Field, Arc<dyn Array>)> = vec![
///     (Field::new("A", DataType::Int32, false), Arc::new(int_array)),
///     (Field::new("B", DataType::Float64, false), Arc::new(float_array)),
/// ];
///
/// let array = UnionArray::try_new(
///     &vec![0, 1],
///     type_id_buffer,
///     None,
///     children,
/// ).unwrap();
///
/// let value = array.value(0).as_any().downcast_ref::<Int32Array>().unwrap().value(0);
/// assert_eq!(1, value);
///
/// let value = array.value(1).as_any().downcast_ref::<Float64Array>().unwrap().value(0);
/// assert!(3.2 - value < f64::EPSILON);
///
/// let value = array.value(2).as_any().downcast_ref::<Int32Array>().unwrap().value(0);
/// assert_eq!(34, value);
/// ```
#[derive(Clone)]
pub struct UnionArray {
    data_type: DataType,
    type_ids: ScalarBuffer<i8>,
    offsets: Option<ScalarBuffer<i32>>,
    fields: Vec<Option<ArrayRef>>,
}

impl UnionArray {
    /// Creates a new `UnionArray`.
    ///
    /// Accepts type ids, child arrays and optionally offsets (for dense unions) to create
    /// a new `UnionArray`.  This method makes no attempt to validate the data provided by the
    /// caller and assumes that each of the components are correct and consistent with each other.
    /// See `try_new` for an alternative that validates the data provided.
    ///
    /// # Safety
    ///
    /// The `type_ids` `Buffer` should contain `i8` values.  These values should be greater than
    /// zero and must be less than the number of children provided in `child_arrays`.  These values
    /// are used to index into the `child_arrays`.
    ///
    /// The `value_offsets` `Buffer` is only provided in the case of a dense union, sparse unions
    /// should use `None`.  If provided the `value_offsets` `Buffer` should contain `i32` values.
    /// The values in this array should be greater than zero and must be less than the length of the
    /// overall array.
    ///
    /// In both cases above we use signed integer types to maintain compatibility with other
    /// Arrow implementations.
    ///
    /// In both of the cases above we are accepting `Buffer`'s which are assumed to be representing
    /// `i8` and `i32` values respectively.  `Buffer` objects are untyped and no attempt is made
    /// to ensure that the data provided is valid.
    pub unsafe fn new_unchecked(
        field_type_ids: &[i8],
        type_ids: Buffer,
        value_offsets: Option<Buffer>,
        child_arrays: Vec<(Field, ArrayRef)>,
    ) -> Self {
        let (fields, field_values): (Vec<_>, Vec<_>) = child_arrays.into_iter().unzip();
        let len = type_ids.len();

        let mode = if value_offsets.is_some() {
            UnionMode::Dense
        } else {
            UnionMode::Sparse
        };

        let builder = ArrayData::builder(DataType::Union(
            UnionFields::new(field_type_ids.iter().copied(), fields),
            mode,
        ))
        .add_buffer(type_ids)
        .child_data(field_values.into_iter().map(|a| a.into_data()).collect())
        .len(len);

        let data = match value_offsets {
            Some(b) => builder.add_buffer(b).build_unchecked(),
            None => builder.build_unchecked(),
        };
        Self::from(data)
    }

    /// Attempts to create a new `UnionArray`, validating the inputs provided.
    pub fn try_new(
        field_type_ids: &[i8],
        type_ids: Buffer,
        value_offsets: Option<Buffer>,
        child_arrays: Vec<(Field, ArrayRef)>,
    ) -> Result<Self, ArrowError> {
        if let Some(b) = &value_offsets {
            if ((type_ids.len()) * 4) != b.len() {
                return Err(ArrowError::InvalidArgumentError(
                    "Type Ids and Offsets represent a different number of array slots.".to_string(),
                ));
            }
        }

        // Check the type_ids
        let type_id_slice: &[i8] = type_ids.typed_data();
        let invalid_type_ids = type_id_slice
            .iter()
            .filter(|i| *i < &0)
            .collect::<Vec<&i8>>();
        if !invalid_type_ids.is_empty() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Type Ids must be positive and cannot be greater than the number of \
                child arrays, found:\n{invalid_type_ids:?}"
            )));
        }

        // Check the value offsets if provided
        if let Some(offset_buffer) = &value_offsets {
            let max_len = type_ids.len() as i32;
            let offsets_slice: &[i32] = offset_buffer.typed_data();
            let invalid_offsets = offsets_slice
                .iter()
                .filter(|i| *i < &0 || *i > &max_len)
                .collect::<Vec<&i32>>();
            if !invalid_offsets.is_empty() {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Offsets must be positive and within the length of the Array, \
                    found:\n{invalid_offsets:?}"
                )));
            }
        }

        // Unsafe Justification: arguments were validated above (and
        // re-revalidated as part of data().validate() below)
        let new_self =
            unsafe { Self::new_unchecked(field_type_ids, type_ids, value_offsets, child_arrays) };
        new_self.to_data().validate()?;

        Ok(new_self)
    }

    /// Accesses the child array for `type_id`.
    ///
    /// # Panics
    ///
    /// Panics if the `type_id` provided is not present in the array's DataType
    /// in the `Union`.
    pub fn child(&self, type_id: i8) -> &ArrayRef {
        assert!((type_id as usize) < self.fields.len());
        let boxed = &self.fields[type_id as usize];
        boxed.as_ref().expect("invalid type id")
    }

    /// Returns the `type_id` for the array slot at `index`.
    ///
    /// # Panics
    ///
    /// Panics if `index` is greater than or equal to the number of child arrays
    pub fn type_id(&self, index: usize) -> i8 {
        assert!(index < self.type_ids.len());
        self.type_ids[index]
    }

    /// Returns the `type_ids` buffer for this array
    pub fn type_ids(&self) -> &ScalarBuffer<i8> {
        &self.type_ids
    }

    /// Returns the `offsets` buffer if this is a dense array
    pub fn offsets(&self) -> Option<&ScalarBuffer<i32>> {
        self.offsets.as_ref()
    }

    /// Returns the offset into the underlying values array for the array slot at `index`.
    ///
    /// # Panics
    ///
    /// Panics if `index` is greater than or equal the length of the array.
    pub fn value_offset(&self, index: usize) -> usize {
        assert!(index < self.len());
        match &self.offsets {
            Some(offsets) => offsets[index] as usize,
            None => self.offset() + index,
        }
    }

    /// Returns the array's value at index `i`.
    /// # Panics
    /// Panics if index `i` is out of bounds
    pub fn value(&self, i: usize) -> ArrayRef {
        let type_id = self.type_id(i);
        let value_offset = self.value_offset(i);
        let child = self.child(type_id);
        child.slice(value_offset, 1)
    }

    /// Returns the names of the types in the union.
    pub fn type_names(&self) -> Vec<&str> {
        match self.data_type() {
            DataType::Union(fields, _) => fields
                .iter()
                .map(|(_, f)| f.name().as_str())
                .collect::<Vec<&str>>(),
            _ => unreachable!("Union array's data type is not a union!"),
        }
    }

    /// Returns whether the `UnionArray` is dense (or sparse if `false`).
    fn is_dense(&self) -> bool {
        match self.data_type() {
            DataType::Union(_, mode) => mode == &UnionMode::Dense,
            _ => unreachable!("Union array's data type is not a union!"),
        }
    }

    /// Returns a zero-copy slice of this array with the indicated offset and length.
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        let (offsets, fields) = match self.offsets.as_ref() {
            // If dense union, slice offsets
            Some(offsets) => (Some(offsets.slice(offset, length)), self.fields.clone()),
            // Otherwise need to slice sparse children
            None => {
                let fields = self
                    .fields
                    .iter()
                    .map(|x| x.as_ref().map(|x| x.slice(offset, length)))
                    .collect();
                (None, fields)
            }
        };

        Self {
            data_type: self.data_type.clone(),
            type_ids: self.type_ids.slice(offset, length),
            offsets,
            fields,
        }
    }
}

impl From<ArrayData> for UnionArray {
    fn from(data: ArrayData) -> Self {
        let (fields, mode) = match data.data_type() {
            DataType::Union(fields, mode) => (fields, *mode),
            d => panic!("UnionArray expected ArrayData with type Union got {d}"),
        };
        let (type_ids, offsets) = match mode {
            UnionMode::Sparse => (
                ScalarBuffer::new(data.buffers()[0].clone(), data.offset(), data.len()),
                None,
            ),
            UnionMode::Dense => (
                ScalarBuffer::new(data.buffers()[0].clone(), data.offset(), data.len()),
                Some(ScalarBuffer::new(
                    data.buffers()[1].clone(),
                    data.offset(),
                    data.len(),
                )),
            ),
        };

        let max_id = fields.iter().map(|(i, _)| i).max().unwrap_or_default() as usize;
        let mut boxed_fields = vec![None; max_id + 1];
        for (cd, (field_id, _)) in data.child_data().iter().zip(fields.iter()) {
            boxed_fields[field_id as usize] = Some(make_array(cd.clone()));
        }
        Self {
            data_type: data.data_type().clone(),
            type_ids,
            offsets,
            fields: boxed_fields,
        }
    }
}

impl From<UnionArray> for ArrayData {
    fn from(array: UnionArray) -> Self {
        let len = array.len();
        let f = match &array.data_type {
            DataType::Union(f, _) => f,
            _ => unreachable!(),
        };
        let buffers = match array.offsets {
            Some(o) => vec![array.type_ids.into_inner(), o.into_inner()],
            None => vec![array.type_ids.into_inner()],
        };

        let child = f
            .iter()
            .map(|(i, _)| array.fields[i as usize].as_ref().unwrap().to_data())
            .collect();

        let builder = ArrayDataBuilder::new(array.data_type)
            .len(len)
            .buffers(buffers)
            .child_data(child);
        unsafe { builder.build_unchecked() }
    }
}

impl Array for UnionArray {
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
        self.type_ids.len()
    }

    fn is_empty(&self) -> bool {
        self.type_ids.is_empty()
    }

    fn offset(&self) -> usize {
        0
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        None
    }

    /// Union types always return non null as there is no validity buffer.
    /// To check validity correctly you must check the underlying vector.
    fn is_null(&self, _index: usize) -> bool {
        false
    }

    /// Union types always return non null as there is no validity buffer.
    /// To check validity correctly you must check the underlying vector.
    fn is_valid(&self, _index: usize) -> bool {
        true
    }

    /// Union types always return 0 null count as there is no validity buffer.
    /// To get null count correctly you must check the underlying vector.
    fn null_count(&self) -> usize {
        0
    }

    fn get_buffer_memory_size(&self) -> usize {
        let mut sum = self.type_ids.inner().capacity();
        if let Some(o) = self.offsets.as_ref() {
            sum += o.inner().capacity()
        }
        self.fields
            .iter()
            .flat_map(|x| x.as_ref().map(|x| x.get_buffer_memory_size()))
            .sum::<usize>()
            + sum
    }

    fn get_array_memory_size(&self) -> usize {
        let mut sum = self.type_ids.inner().capacity();
        if let Some(o) = self.offsets.as_ref() {
            sum += o.inner().capacity()
        }
        std::mem::size_of::<Self>()
            + self
                .fields
                .iter()
                .flat_map(|x| x.as_ref().map(|x| x.get_array_memory_size()))
                .sum::<usize>()
            + sum
    }
}

impl std::fmt::Debug for UnionArray {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let header = if self.is_dense() {
            "UnionArray(Dense)\n["
        } else {
            "UnionArray(Sparse)\n["
        };
        writeln!(f, "{header}")?;

        writeln!(f, "-- type id buffer:")?;
        writeln!(f, "{:?}", self.type_ids)?;

        if let Some(offsets) = &self.offsets {
            writeln!(f, "-- offsets buffer:")?;
            writeln!(f, "{:?}", offsets)?;
        }

        let fields = match self.data_type() {
            DataType::Union(fields, _) => fields,
            _ => unreachable!(),
        };

        for (type_id, field) in fields.iter() {
            let child = self.child(type_id);
            writeln!(
                f,
                "-- child {}: \"{}\" ({:?})",
                type_id,
                field.name(),
                field.data_type()
            )?;
            std::fmt::Debug::fmt(child, f)?;
            writeln!(f)?;
        }
        writeln!(f, "]")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::builder::UnionBuilder;
    use crate::cast::AsArray;
    use crate::types::{Float32Type, Float64Type, Int32Type, Int64Type};
    use crate::RecordBatch;
    use crate::{Float64Array, Int32Array, Int64Array, StringArray};
    use arrow_schema::Schema;

    #[test]
    fn test_dense_i32() {
        let mut builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Int32Type>("b", 2).unwrap();
        builder.append::<Int32Type>("c", 3).unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        builder.append::<Int32Type>("c", 5).unwrap();
        builder.append::<Int32Type>("a", 6).unwrap();
        builder.append::<Int32Type>("b", 7).unwrap();
        let union = builder.build().unwrap();

        let expected_type_ids = vec![0_i8, 1, 2, 0, 2, 0, 1];
        let expected_offsets = vec![0_i32, 0, 0, 1, 1, 2, 1];
        let expected_array_values = [1_i32, 2, 3, 4, 5, 6, 7];

        // Check type ids
        assert_eq!(*union.type_ids(), expected_type_ids);
        for (i, id) in expected_type_ids.iter().enumerate() {
            assert_eq!(id, &union.type_id(i));
        }

        // Check offsets
        assert_eq!(*union.offsets().unwrap(), expected_offsets);
        for (i, id) in expected_offsets.iter().enumerate() {
            assert_eq!(union.value_offset(i), *id as usize);
        }

        // Check data
        assert_eq!(
            *union.child(0).as_primitive::<Int32Type>().values(),
            [1_i32, 4, 6]
        );
        assert_eq!(
            *union.child(1).as_primitive::<Int32Type>().values(),
            [2_i32, 7]
        );
        assert_eq!(
            *union.child(2).as_primitive::<Int32Type>().values(),
            [3_i32, 5]
        );

        assert_eq!(expected_array_values.len(), union.len());
        for (i, expected_value) in expected_array_values.iter().enumerate() {
            assert!(!union.is_null(i));
            let slot = union.value(i);
            let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(slot.len(), 1);
            let value = slot.value(0);
            assert_eq!(expected_value, &value);
        }
    }

    #[test]
    #[cfg_attr(miri, ignore)]
    fn test_dense_i32_large() {
        let mut builder = UnionBuilder::new_dense();

        let expected_type_ids = vec![0_i8; 1024];
        let expected_offsets: Vec<_> = (0..1024).collect();
        let expected_array_values: Vec<_> = (1..=1024).collect();

        expected_array_values
            .iter()
            .for_each(|v| builder.append::<Int32Type>("a", *v).unwrap());

        let union = builder.build().unwrap();

        // Check type ids
        assert_eq!(*union.type_ids(), expected_type_ids);
        for (i, id) in expected_type_ids.iter().enumerate() {
            assert_eq!(id, &union.type_id(i));
        }

        // Check offsets
        assert_eq!(*union.offsets().unwrap(), expected_offsets);
        for (i, id) in expected_offsets.iter().enumerate() {
            assert_eq!(union.value_offset(i), *id as usize);
        }

        for (i, expected_value) in expected_array_values.iter().enumerate() {
            assert!(!union.is_null(i));
            let slot = union.value(i);
            let slot = slot.as_primitive::<Int32Type>();
            assert_eq!(slot.len(), 1);
            let value = slot.value(0);
            assert_eq!(expected_value, &value);
        }
    }

    #[test]
    fn test_dense_mixed() {
        let mut builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Int64Type>("c", 3).unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        builder.append::<Int64Type>("c", 5).unwrap();
        builder.append::<Int32Type>("a", 6).unwrap();
        let union = builder.build().unwrap();

        assert_eq!(5, union.len());
        for i in 0..union.len() {
            let slot = union.value(i);
            assert!(!union.is_null(i));
            match i {
                0 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(1_i32, value);
                }
                1 => {
                    let slot = slot.as_any().downcast_ref::<Int64Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(3_i64, value);
                }
                2 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(4_i32, value);
                }
                3 => {
                    let slot = slot.as_any().downcast_ref::<Int64Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(5_i64, value);
                }
                4 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(6_i32, value);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_dense_mixed_with_nulls() {
        let mut builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Int64Type>("c", 3).unwrap();
        builder.append::<Int32Type>("a", 10).unwrap();
        builder.append_null::<Int32Type>("a").unwrap();
        builder.append::<Int32Type>("a", 6).unwrap();
        let union = builder.build().unwrap();

        assert_eq!(5, union.len());
        for i in 0..union.len() {
            let slot = union.value(i);
            match i {
                0 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert!(!slot.is_null(0));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(1_i32, value);
                }
                1 => {
                    let slot = slot.as_any().downcast_ref::<Int64Array>().unwrap();
                    assert!(!slot.is_null(0));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(3_i64, value);
                }
                2 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert!(!slot.is_null(0));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(10_i32, value);
                }
                3 => assert!(slot.is_null(0)),
                4 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert!(!slot.is_null(0));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(6_i32, value);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_dense_mixed_with_nulls_and_offset() {
        let mut builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Int64Type>("c", 3).unwrap();
        builder.append::<Int32Type>("a", 10).unwrap();
        builder.append_null::<Int32Type>("a").unwrap();
        builder.append::<Int32Type>("a", 6).unwrap();
        let union = builder.build().unwrap();

        let slice = union.slice(2, 3);
        let new_union = slice.as_any().downcast_ref::<UnionArray>().unwrap();

        assert_eq!(3, new_union.len());
        for i in 0..new_union.len() {
            let slot = new_union.value(i);
            match i {
                0 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert!(!slot.is_null(0));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(10_i32, value);
                }
                1 => assert!(slot.is_null(0)),
                2 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert!(!slot.is_null(0));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(6_i32, value);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_dense_mixed_with_str() {
        let string_array = StringArray::from(vec!["foo", "bar", "baz"]);
        let int_array = Int32Array::from(vec![5, 6]);
        let float_array = Float64Array::from(vec![10.0]);

        let type_ids = [1_i8, 0, 0, 2, 0, 1];
        let offsets = [0_i32, 0, 1, 0, 2, 1];

        let type_id_buffer = Buffer::from_slice_ref(type_ids);
        let value_offsets_buffer = Buffer::from_slice_ref(offsets);

        let children: Vec<(Field, Arc<dyn Array>)> = vec![
            (
                Field::new("A", DataType::Utf8, false),
                Arc::new(string_array),
            ),
            (Field::new("B", DataType::Int32, false), Arc::new(int_array)),
            (
                Field::new("C", DataType::Float64, false),
                Arc::new(float_array),
            ),
        ];
        let array = UnionArray::try_new(
            &[0, 1, 2],
            type_id_buffer,
            Some(value_offsets_buffer),
            children,
        )
        .unwrap();

        // Check type ids
        assert_eq!(*array.type_ids(), type_ids);
        for (i, id) in type_ids.iter().enumerate() {
            assert_eq!(id, &array.type_id(i));
        }

        // Check offsets
        assert_eq!(*array.offsets().unwrap(), offsets);
        for (i, id) in offsets.iter().enumerate() {
            assert_eq!(*id as usize, array.value_offset(i));
        }

        // Check values
        assert_eq!(6, array.len());

        let slot = array.value(0);
        let value = slot.as_any().downcast_ref::<Int32Array>().unwrap().value(0);
        assert_eq!(5, value);

        let slot = array.value(1);
        let value = slot
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!("foo", value);

        let slot = array.value(2);
        let value = slot
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!("bar", value);

        let slot = array.value(3);
        let value = slot
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .value(0);
        assert_eq!(10.0, value);

        let slot = array.value(4);
        let value = slot
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        assert_eq!("baz", value);

        let slot = array.value(5);
        let value = slot.as_any().downcast_ref::<Int32Array>().unwrap().value(0);
        assert_eq!(6, value);
    }

    #[test]
    fn test_sparse_i32() {
        let mut builder = UnionBuilder::new_sparse();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Int32Type>("b", 2).unwrap();
        builder.append::<Int32Type>("c", 3).unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        builder.append::<Int32Type>("c", 5).unwrap();
        builder.append::<Int32Type>("a", 6).unwrap();
        builder.append::<Int32Type>("b", 7).unwrap();
        let union = builder.build().unwrap();

        let expected_type_ids = vec![0_i8, 1, 2, 0, 2, 0, 1];
        let expected_array_values = [1_i32, 2, 3, 4, 5, 6, 7];

        // Check type ids
        assert_eq!(*union.type_ids(), expected_type_ids);
        for (i, id) in expected_type_ids.iter().enumerate() {
            assert_eq!(id, &union.type_id(i));
        }

        // Check offsets, sparse union should only have a single buffer
        assert!(union.offsets().is_none());

        // Check data
        assert_eq!(
            *union.child(0).as_primitive::<Int32Type>().values(),
            [1_i32, 0, 0, 4, 0, 6, 0],
        );
        assert_eq!(
            *union.child(1).as_primitive::<Int32Type>().values(),
            [0_i32, 2_i32, 0, 0, 0, 0, 7]
        );
        assert_eq!(
            *union.child(2).as_primitive::<Int32Type>().values(),
            [0_i32, 0, 3_i32, 0, 5, 0, 0]
        );

        assert_eq!(expected_array_values.len(), union.len());
        for (i, expected_value) in expected_array_values.iter().enumerate() {
            assert!(!union.is_null(i));
            let slot = union.value(i);
            let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
            assert_eq!(slot.len(), 1);
            let value = slot.value(0);
            assert_eq!(expected_value, &value);
        }
    }

    #[test]
    fn test_sparse_mixed() {
        let mut builder = UnionBuilder::new_sparse();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Float64Type>("c", 3.0).unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        builder.append::<Float64Type>("c", 5.0).unwrap();
        builder.append::<Int32Type>("a", 6).unwrap();
        let union = builder.build().unwrap();

        let expected_type_ids = vec![0_i8, 1, 0, 1, 0];

        // Check type ids
        assert_eq!(*union.type_ids(), expected_type_ids);
        for (i, id) in expected_type_ids.iter().enumerate() {
            assert_eq!(id, &union.type_id(i));
        }

        // Check offsets, sparse union should only have a single buffer, i.e. no offsets
        assert!(union.offsets().is_none());

        for i in 0..union.len() {
            let slot = union.value(i);
            assert!(!union.is_null(i));
            match i {
                0 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(1_i32, value);
                }
                1 => {
                    let slot = slot.as_any().downcast_ref::<Float64Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(value, 3_f64);
                }
                2 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(4_i32, value);
                }
                3 => {
                    let slot = slot.as_any().downcast_ref::<Float64Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(5_f64, value);
                }
                4 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(6_i32, value);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_sparse_mixed_with_nulls() {
        let mut builder = UnionBuilder::new_sparse();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append_null::<Int32Type>("a").unwrap();
        builder.append::<Float64Type>("c", 3.0).unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        let union = builder.build().unwrap();

        let expected_type_ids = vec![0_i8, 0, 1, 0];

        // Check type ids
        assert_eq!(*union.type_ids(), expected_type_ids);
        for (i, id) in expected_type_ids.iter().enumerate() {
            assert_eq!(id, &union.type_id(i));
        }

        // Check offsets, sparse union should only have a single buffer, i.e. no offsets
        assert!(union.offsets().is_none());

        for i in 0..union.len() {
            let slot = union.value(i);
            match i {
                0 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert!(!slot.is_null(0));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(1_i32, value);
                }
                1 => assert!(slot.is_null(0)),
                2 => {
                    let slot = slot.as_any().downcast_ref::<Float64Array>().unwrap();
                    assert!(!slot.is_null(0));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(value, 3_f64);
                }
                3 => {
                    let slot = slot.as_any().downcast_ref::<Int32Array>().unwrap();
                    assert!(!slot.is_null(0));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(4_i32, value);
                }
                _ => unreachable!(),
            }
        }
    }

    #[test]
    fn test_sparse_mixed_with_nulls_and_offset() {
        let mut builder = UnionBuilder::new_sparse();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append_null::<Int32Type>("a").unwrap();
        builder.append::<Float64Type>("c", 3.0).unwrap();
        builder.append_null::<Float64Type>("c").unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        let union = builder.build().unwrap();

        let slice = union.slice(1, 4);
        let new_union = slice.as_any().downcast_ref::<UnionArray>().unwrap();

        assert_eq!(4, new_union.len());
        for i in 0..new_union.len() {
            let slot = new_union.value(i);
            match i {
                0 => assert!(slot.is_null(0)),
                1 => {
                    let slot = slot.as_primitive::<Float64Type>();
                    assert!(!slot.is_null(0));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(value, 3_f64);
                }
                2 => assert!(slot.is_null(0)),
                3 => {
                    let slot = slot.as_primitive::<Int32Type>();
                    assert!(!slot.is_null(0));
                    assert_eq!(slot.len(), 1);
                    let value = slot.value(0);
                    assert_eq!(4_i32, value);
                }
                _ => unreachable!(),
            }
        }
    }

    fn test_union_validity(union_array: &UnionArray) {
        assert_eq!(union_array.null_count(), 0);

        for i in 0..union_array.len() {
            assert!(!union_array.is_null(i));
            assert!(union_array.is_valid(i));
        }
    }

    #[test]
    fn test_union_array_validity() {
        let mut builder = UnionBuilder::new_sparse();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append_null::<Int32Type>("a").unwrap();
        builder.append::<Float64Type>("c", 3.0).unwrap();
        builder.append_null::<Float64Type>("c").unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        let union = builder.build().unwrap();

        test_union_validity(&union);

        let mut builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append_null::<Int32Type>("a").unwrap();
        builder.append::<Float64Type>("c", 3.0).unwrap();
        builder.append_null::<Float64Type>("c").unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        let union = builder.build().unwrap();

        test_union_validity(&union);
    }

    #[test]
    fn test_type_check() {
        let mut builder = UnionBuilder::new_sparse();
        builder.append::<Float32Type>("a", 1.0).unwrap();
        let err = builder.append::<Int32Type>("a", 1).unwrap_err().to_string();
        assert!(
            err.contains(
                "Attempt to write col \"a\" with type Int32 doesn't match existing type Float32"
            ),
            "{}",
            err
        );
    }

    #[test]
    fn slice_union_array() {
        // [1, null, 3.0, null, 4]
        fn create_union(mut builder: UnionBuilder) -> UnionArray {
            builder.append::<Int32Type>("a", 1).unwrap();
            builder.append_null::<Int32Type>("a").unwrap();
            builder.append::<Float64Type>("c", 3.0).unwrap();
            builder.append_null::<Float64Type>("c").unwrap();
            builder.append::<Int32Type>("a", 4).unwrap();
            builder.build().unwrap()
        }

        fn create_batch(union: UnionArray) -> RecordBatch {
            let schema = Schema::new(vec![Field::new(
                "struct_array",
                union.data_type().clone(),
                true,
            )]);

            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(union)]).unwrap()
        }

        fn test_slice_union(record_batch_slice: RecordBatch) {
            let union_slice = record_batch_slice
                .column(0)
                .as_any()
                .downcast_ref::<UnionArray>()
                .unwrap();

            assert_eq!(union_slice.type_id(0), 0);
            assert_eq!(union_slice.type_id(1), 1);
            assert_eq!(union_slice.type_id(2), 1);

            let slot = union_slice.value(0);
            let array = slot.as_primitive::<Int32Type>();
            assert_eq!(array.len(), 1);
            assert!(array.is_null(0));

            let slot = union_slice.value(1);
            let array = slot.as_primitive::<Float64Type>();
            assert_eq!(array.len(), 1);
            assert!(array.is_valid(0));
            assert_eq!(array.value(0), 3.0);

            let slot = union_slice.value(2);
            let array = slot.as_primitive::<Float64Type>();
            assert_eq!(array.len(), 1);
            assert!(array.is_null(0));
        }

        // Sparse Union
        let builder = UnionBuilder::new_sparse();
        let record_batch = create_batch(create_union(builder));
        // [null, 3.0, null]
        let record_batch_slice = record_batch.slice(1, 3);
        test_slice_union(record_batch_slice);

        // Dense Union
        let builder = UnionBuilder::new_dense();
        let record_batch = create_batch(create_union(builder));
        // [null, 3.0, null]
        let record_batch_slice = record_batch.slice(1, 3);
        test_slice_union(record_batch_slice);
    }

    #[test]
    fn test_custom_type_ids() {
        let data_type = DataType::Union(
            UnionFields::new(
                vec![8, 4, 9],
                vec![
                    Field::new("strings", DataType::Utf8, false),
                    Field::new("integers", DataType::Int32, false),
                    Field::new("floats", DataType::Float64, false),
                ],
            ),
            UnionMode::Dense,
        );

        let string_array = StringArray::from(vec!["foo", "bar", "baz"]);
        let int_array = Int32Array::from(vec![5, 6, 4]);
        let float_array = Float64Array::from(vec![10.0]);

        let type_ids = Buffer::from_vec(vec![4_i8, 8, 4, 8, 9, 4, 8]);
        let value_offsets = Buffer::from_vec(vec![0_i32, 0, 1, 1, 0, 2, 2]);

        let data = ArrayData::builder(data_type)
            .len(7)
            .buffers(vec![type_ids, value_offsets])
            .child_data(vec![
                string_array.into_data(),
                int_array.into_data(),
                float_array.into_data(),
            ])
            .build()
            .unwrap();

        let array = UnionArray::from(data);

        let v = array.value(0);
        assert_eq!(v.data_type(), &DataType::Int32);
        assert_eq!(v.len(), 1);
        assert_eq!(v.as_primitive::<Int32Type>().value(0), 5);

        let v = array.value(1);
        assert_eq!(v.data_type(), &DataType::Utf8);
        assert_eq!(v.len(), 1);
        assert_eq!(v.as_string::<i32>().value(0), "foo");

        let v = array.value(2);
        assert_eq!(v.data_type(), &DataType::Int32);
        assert_eq!(v.len(), 1);
        assert_eq!(v.as_primitive::<Int32Type>().value(0), 6);

        let v = array.value(3);
        assert_eq!(v.data_type(), &DataType::Utf8);
        assert_eq!(v.len(), 1);
        assert_eq!(v.as_string::<i32>().value(0), "bar");

        let v = array.value(4);
        assert_eq!(v.data_type(), &DataType::Float64);
        assert_eq!(v.len(), 1);
        assert_eq!(v.as_primitive::<Float64Type>().value(0), 10.0);

        let v = array.value(5);
        assert_eq!(v.data_type(), &DataType::Int32);
        assert_eq!(v.len(), 1);
        assert_eq!(v.as_primitive::<Int32Type>().value(0), 4);

        let v = array.value(6);
        assert_eq!(v.data_type(), &DataType::Utf8);
        assert_eq!(v.len(), 1);
        assert_eq!(v.as_string::<i32>().value(0), "baz");
    }
}
