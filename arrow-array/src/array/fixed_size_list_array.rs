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
use crate::builder::{FixedSizeListBuilder, PrimitiveBuilder};
use crate::iterator::FixedSizeListIter;
use crate::{make_array, Array, ArrayAccessor, ArrayRef, ArrowPrimitiveType};
use arrow_buffer::buffer::NullBuffer;
use arrow_buffer::ArrowNativeType;
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType, FieldRef};
use std::any::Any;
use std::sync::Arc;

/// An array of [fixed length lists], similar to JSON arrays
/// (e.g. `["A", "B"]`).
///
/// Lists are represented using a `values` child
/// array where each list has a fixed size of `value_length`.
///
/// Use [`FixedSizeListBuilder`] to construct a [`FixedSizeListArray`].
///
/// # Representation
///
/// A [`FixedSizeListArray`] can represent a list of values of any other
/// supported Arrow type. Each element of the `FixedSizeListArray` itself is
/// a list which may contain NULL and non-null values,
/// or may itself be NULL.
///
/// For example, this `FixedSizeListArray` stores lists of strings:
///
/// ```text
/// ┌─────────────┐
/// │    [A,B]    │
/// ├─────────────┤
/// │    NULL     │
/// ├─────────────┤
/// │   [C,NULL]  │
/// └─────────────┘
/// ```
///
/// The `values` of this `FixedSizeListArray`s are stored in a child
/// [`StringArray`] where logical null values take up `values_length` slots in the array
/// as shown in the following diagram. The logical values
/// are shown on the left, and the actual `FixedSizeListArray` encoding on the right
///
/// ```text
///                                 ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
///                                                         ┌ ─ ─ ─ ─ ─ ─ ─ ─┐
///  ┌─────────────┐                │     ┌───┐               ┌───┐ ┌──────┐      │
///  │   [A,B]     │                      │ 1 │             │ │ 1 │ │  A   │ │ 0
///  ├─────────────┤                │     ├───┤               ├───┤ ├──────┤      │
///  │    NULL     │                      │ 0 │             │ │ 1 │ │  B   │ │ 1
///  ├─────────────┤                │     ├───┤               ├───┤ ├──────┤      │
///  │  [C,NULL]   │                      │ 1 │             │ │ 0 │ │ ???? │ │ 2
///  └─────────────┘                │     └───┘               ├───┤ ├──────┤      │
///                                                         | │ 0 │ │ ???? │ │ 3
///  Logical Values                 │   Validity              ├───┤ ├──────┤      │
///                                     (nulls)             │ │ 1 │ │  C   │ │ 4
///                                 │                         ├───┤ ├──────┤      │
///                                                         │ │ 0 │ │ ???? │ │ 5
///                                 │                         └───┘ └──────┘      │
///                                                         │     Values     │
///                                 │   FixedSizeListArray        (Array)         │
///                                                         └ ─ ─ ─ ─ ─ ─ ─ ─┘
///                                 └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┘
/// ```
///
/// # Example
///
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{Array, FixedSizeListArray, Int32Array};
/// # use arrow_data::ArrayData;
/// # use arrow_schema::{DataType, Field};
/// # use arrow_buffer::Buffer;
/// // Construct a value array
/// let value_data = ArrayData::builder(DataType::Int32)
///     .len(9)
///     .add_buffer(Buffer::from_slice_ref(&[0, 1, 2, 3, 4, 5, 6, 7, 8]))
///     .build()
///     .unwrap();
/// let list_data_type = DataType::FixedSizeList(
///     Arc::new(Field::new("item", DataType::Int32, false)),
///     3,
/// );
/// let list_data = ArrayData::builder(list_data_type.clone())
///     .len(3)
///     .add_child_data(value_data.clone())
///     .build()
///     .unwrap();
/// let list_array = FixedSizeListArray::from(list_data);
/// let list0 = list_array.value(0);
/// let list1 = list_array.value(1);
/// let list2 = list_array.value(2);
///
/// assert_eq!( &[0, 1, 2], list0.as_any().downcast_ref::<Int32Array>().unwrap().values());
/// assert_eq!( &[3, 4, 5], list1.as_any().downcast_ref::<Int32Array>().unwrap().values());
/// assert_eq!( &[6, 7, 8], list2.as_any().downcast_ref::<Int32Array>().unwrap().values());
/// ```
///
/// [`StringArray`]: crate::array::StringArray
/// [fixed size arrays](https://arrow.apache.org/docs/format/Columnar.html#fixed-size-list-layout)
#[derive(Clone)]
pub struct FixedSizeListArray {
    data_type: DataType, // Must be DataType::FixedSizeList(value_length)
    values: ArrayRef,
    nulls: Option<NullBuffer>,
    value_length: i32,
    len: usize,
}

impl FixedSizeListArray {
    /// Create a new [`FixedSizeListArray`] with `size` element size, panicking on failure
    ///
    /// # Panics
    ///
    /// Panics if [`Self::try_new`] returns an error
    pub fn new(field: FieldRef, size: i32, values: ArrayRef, nulls: Option<NullBuffer>) -> Self {
        Self::try_new(field, size, values, nulls).unwrap()
    }

    /// Create a new [`FixedSizeListArray`] from the provided parts, returning an error on failure
    ///
    /// # Errors
    ///
    /// * `size < 0`
    /// * `values.len() / size != nulls.len()`
    /// * `values.data_type() != field.data_type()`
    /// * `!field.is_nullable() && !nulls.expand(size).contains(values.logical_nulls())`
    pub fn try_new(
        field: FieldRef,
        size: i32,
        values: ArrayRef,
        nulls: Option<NullBuffer>,
    ) -> Result<Self, ArrowError> {
        let s = size.to_usize().ok_or_else(|| {
            ArrowError::InvalidArgumentError(format!("Size cannot be negative, got {}", size))
        })?;

        let len = match s {
            0 => nulls.as_ref().map(|x| x.len()).unwrap_or_default(),
            _ => {
                let len = values.len() / s.max(1);
                if let Some(n) = nulls.as_ref() {
                    if n.len() != len {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Incorrect length of null buffer for FixedSizeListArray, expected {} got {}",
                            len,
                            n.len(),
                        )));
                    }
                }
                len
            }
        };

        if field.data_type() != values.data_type() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "FixedSizeListArray expected data type {} got {} for {:?}",
                field.data_type(),
                values.data_type(),
                field.name()
            )));
        }

        if let Some(a) = values.logical_nulls() {
            let nulls_valid = field.is_nullable()
                || nulls
                    .as_ref()
                    .map(|n| n.expand(size as _).contains(&a))
                    .unwrap_or_default()
                || (nulls.is_none() && a.null_count() == 0);

            if !nulls_valid {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Found unmasked nulls for non-nullable FixedSizeListArray field {:?}",
                    field.name()
                )));
            }
        }

        let data_type = DataType::FixedSizeList(field, size);
        Ok(Self {
            data_type,
            values,
            value_length: size,
            nulls,
            len,
        })
    }

    /// Create a new [`FixedSizeListArray`] of length `len` where all values are null
    ///
    /// # Panics
    ///
    /// Panics if
    ///
    /// * `size < 0`
    /// * `size * len` would overflow `usize`
    pub fn new_null(field: FieldRef, size: i32, len: usize) -> Self {
        let capacity = size.to_usize().unwrap().checked_mul(len).unwrap();
        Self {
            values: make_array(ArrayData::new_null(field.data_type(), capacity)),
            data_type: DataType::FixedSizeList(field, size),
            nulls: Some(NullBuffer::new_null(len)),
            value_length: size,
            len,
        }
    }

    /// Deconstruct this array into its constituent parts
    pub fn into_parts(self) -> (FieldRef, i32, ArrayRef, Option<NullBuffer>) {
        let f = match self.data_type {
            DataType::FixedSizeList(f, _) => f,
            _ => unreachable!(),
        };
        (f, self.value_length, self.values, self.nulls)
    }

    /// Returns a reference to the values of this list.
    pub fn values(&self) -> &ArrayRef {
        &self.values
    }

    /// Returns a clone of the value type of this list.
    pub fn value_type(&self) -> DataType {
        self.values.data_type().clone()
    }

    /// Returns ith value of this list array.
    pub fn value(&self, i: usize) -> ArrayRef {
        self.values
            .slice(self.value_offset_at(i), self.value_length() as usize)
    }

    /// Returns the offset for value at index `i`.
    ///
    /// Note this doesn't do any bound checking, for performance reason.
    #[inline]
    pub fn value_offset(&self, i: usize) -> i32 {
        self.value_offset_at(i) as i32
    }

    /// Returns the length for an element.
    ///
    /// All elements have the same length as the array is a fixed size.
    #[inline]
    pub const fn value_length(&self) -> i32 {
        self.value_length
    }

    #[inline]
    const fn value_offset_at(&self, i: usize) -> usize {
        i * self.value_length as usize
    }

    /// Returns a zero-copy slice of this array with the indicated offset and length.
    pub fn slice(&self, offset: usize, len: usize) -> Self {
        assert!(
            offset.saturating_add(len) <= self.len,
            "the length + offset of the sliced FixedSizeListArray cannot exceed the existing length"
        );
        let size = self.value_length as usize;

        Self {
            data_type: self.data_type.clone(),
            values: self.values.slice(offset * size, len * size),
            nulls: self.nulls.as_ref().map(|n| n.slice(offset, len)),
            value_length: self.value_length,
            len,
        }
    }

    /// Creates a [`FixedSizeListArray`] from an iterator of primitive values
    /// # Example
    /// ```
    /// # use arrow_array::FixedSizeListArray;
    /// # use arrow_array::types::Int32Type;
    ///
    /// let data = vec![
    ///    Some(vec![Some(0), Some(1), Some(2)]),
    ///    None,
    ///    Some(vec![Some(3), None, Some(5)]),
    ///    Some(vec![Some(6), Some(7), Some(45)]),
    /// ];
    /// let list_array = FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(data, 3);
    /// println!("{:?}", list_array);
    /// ```
    pub fn from_iter_primitive<T, P, I>(iter: I, length: i32) -> Self
    where
        T: ArrowPrimitiveType,
        P: IntoIterator<Item = Option<<T as ArrowPrimitiveType>::Native>>,
        I: IntoIterator<Item = Option<P>>,
    {
        let l = length as usize;
        let iter = iter.into_iter();
        let size_hint = iter.size_hint().0;
        let mut builder = FixedSizeListBuilder::with_capacity(
            PrimitiveBuilder::<T>::with_capacity(size_hint * l),
            length,
            size_hint,
        );

        for i in iter {
            match i {
                Some(p) => {
                    for t in p {
                        builder.values().append_option(t);
                    }
                    builder.append(true);
                }
                None => {
                    builder.values().append_nulls(l);
                    builder.append(false)
                }
            }
        }
        builder.finish()
    }

    /// constructs a new iterator
    pub fn iter(&self) -> FixedSizeListIter<'_> {
        FixedSizeListIter::new(self)
    }
}

impl From<ArrayData> for FixedSizeListArray {
    fn from(data: ArrayData) -> Self {
        let value_length = match data.data_type() {
            DataType::FixedSizeList(_, len) => *len,
            _ => {
                panic!("FixedSizeListArray data should contain a FixedSizeList data type")
            }
        };

        let size = value_length as usize;
        let values =
            make_array(data.child_data()[0].slice(data.offset() * size, data.len() * size));
        Self {
            data_type: data.data_type().clone(),
            values,
            nulls: data.nulls().cloned(),
            value_length,
            len: data.len(),
        }
    }
}

impl From<FixedSizeListArray> for ArrayData {
    fn from(array: FixedSizeListArray) -> Self {
        let builder = ArrayDataBuilder::new(array.data_type)
            .len(array.len)
            .nulls(array.nulls)
            .child_data(vec![array.values.to_data()]);

        unsafe { builder.build_unchecked() }
    }
}

impl Array for FixedSizeListArray {
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

    fn get_buffer_memory_size(&self) -> usize {
        let mut size = self.values.get_buffer_memory_size();
        if let Some(n) = self.nulls.as_ref() {
            size += n.buffer().capacity();
        }
        size
    }

    fn get_array_memory_size(&self) -> usize {
        let mut size = std::mem::size_of::<Self>() + self.values.get_array_memory_size();
        if let Some(n) = self.nulls.as_ref() {
            size += n.buffer().capacity();
        }
        size
    }
}

impl ArrayAccessor for FixedSizeListArray {
    type Item = ArrayRef;

    fn value(&self, index: usize) -> Self::Item {
        FixedSizeListArray::value(self, index)
    }

    unsafe fn value_unchecked(&self, index: usize) -> Self::Item {
        FixedSizeListArray::value(self, index)
    }
}

impl std::fmt::Debug for FixedSizeListArray {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "FixedSizeListArray<{}>\n[\n", self.value_length())?;
        print_long_array(self, f, |array, index, f| {
            std::fmt::Debug::fmt(&array.value(index), f)
        })?;
        write!(f, "]")
    }
}

impl ArrayAccessor for &FixedSizeListArray {
    type Item = ArrayRef;

    fn value(&self, index: usize) -> Self::Item {
        FixedSizeListArray::value(self, index)
    }

    unsafe fn value_unchecked(&self, index: usize) -> Self::Item {
        FixedSizeListArray::value(self, index)
    }
}

#[cfg(test)]
mod tests {
    use arrow_buffer::{bit_util, BooleanBuffer, Buffer};
    use arrow_schema::Field;

    use crate::cast::AsArray;
    use crate::types::Int32Type;
    use crate::{new_empty_array, Int32Array};

    use super::*;

    #[test]
    fn test_fixed_size_list_array() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(9)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7, 8]))
            .build()
            .unwrap();

        // Construct a list array from the above two
        let list_data_type =
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, false)), 3);
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(3)
            .add_child_data(value_data.clone())
            .build()
            .unwrap();
        let list_array = FixedSizeListArray::from(list_data);

        assert_eq!(value_data, list_array.values().to_data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(3, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offset(2));
        assert_eq!(3, list_array.value_length());
        assert_eq!(0, list_array.value(0).as_primitive::<Int32Type>().value(0));
        for i in 0..3 {
            assert!(list_array.is_valid(i));
            assert!(!list_array.is_null(i));
        }

        // Now test with a non-zero offset
        let list_data = ArrayData::builder(list_data_type)
            .len(2)
            .offset(1)
            .add_child_data(value_data.clone())
            .build()
            .unwrap();
        let list_array = FixedSizeListArray::from(list_data);

        assert_eq!(value_data.slice(3, 6), list_array.values().to_data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(2, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(3, list_array.value(0).as_primitive::<Int32Type>().value(0));
        assert_eq!(3, list_array.value_offset(1));
        assert_eq!(3, list_array.value_length());
    }

    #[test]
    #[should_panic(expected = "assertion failed: (offset + length) <= self.len()")]
    // Different error messages, so skip for now
    // https://github.com/apache/arrow-rs/issues/1545
    #[cfg(not(feature = "force_validate"))]
    fn test_fixed_size_list_array_unequal_children() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7]))
            .build()
            .unwrap();

        // Construct a list array from the above two
        let list_data_type =
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, false)), 3);
        let list_data = unsafe {
            ArrayData::builder(list_data_type)
                .len(3)
                .add_child_data(value_data)
                .build_unchecked()
        };
        drop(FixedSizeListArray::from(list_data));
    }

    #[test]
    fn test_fixed_size_list_array_slice() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(10)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
            .build()
            .unwrap();

        // Set null buts for the nested array:
        //  [[0, 1], null, null, [6, 7], [8, 9]]
        // 01011001 00000001
        let mut null_bits: [u8; 1] = [0; 1];
        bit_util::set_bit(&mut null_bits, 0);
        bit_util::set_bit(&mut null_bits, 3);
        bit_util::set_bit(&mut null_bits, 4);

        // Construct a fixed size list array from the above two
        let list_data_type =
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, false)), 2);
        let list_data = ArrayData::builder(list_data_type)
            .len(5)
            .add_child_data(value_data.clone())
            .null_bit_buffer(Some(Buffer::from(null_bits)))
            .build()
            .unwrap();
        let list_array = FixedSizeListArray::from(list_data);

        assert_eq!(value_data, list_array.values().to_data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(5, list_array.len());
        assert_eq!(2, list_array.null_count());
        assert_eq!(6, list_array.value_offset(3));
        assert_eq!(2, list_array.value_length());

        let sliced_array = list_array.slice(1, 4);
        assert_eq!(4, sliced_array.len());
        assert_eq!(2, sliced_array.null_count());

        for i in 0..sliced_array.len() {
            if bit_util::get_bit(&null_bits, 1 + i) {
                assert!(sliced_array.is_valid(i));
            } else {
                assert!(sliced_array.is_null(i));
            }
        }

        // Check offset and length for each non-null value.
        let sliced_list_array = sliced_array
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(2, sliced_list_array.value_length());
        assert_eq!(4, sliced_list_array.value_offset(2));
        assert_eq!(6, sliced_list_array.value_offset(3));
    }

    #[test]
    #[should_panic(expected = "the offset of the new Buffer cannot exceed the existing length")]
    fn test_fixed_size_list_array_index_out_of_bound() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(10)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
            .build()
            .unwrap();

        // Set null buts for the nested array:
        //  [[0, 1], null, null, [6, 7], [8, 9]]
        // 01011001 00000001
        let mut null_bits: [u8; 1] = [0; 1];
        bit_util::set_bit(&mut null_bits, 0);
        bit_util::set_bit(&mut null_bits, 3);
        bit_util::set_bit(&mut null_bits, 4);

        // Construct a fixed size list array from the above two
        let list_data_type =
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Int32, false)), 2);
        let list_data = ArrayData::builder(list_data_type)
            .len(5)
            .add_child_data(value_data)
            .null_bit_buffer(Some(Buffer::from(null_bits)))
            .build()
            .unwrap();
        let list_array = FixedSizeListArray::from(list_data);

        list_array.value(10);
    }

    #[test]
    fn test_fixed_size_list_constructors() {
        let values = Arc::new(Int32Array::from_iter([
            Some(1),
            Some(2),
            None,
            None,
            Some(3),
            Some(4),
        ]));

        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let list = FixedSizeListArray::new(field.clone(), 2, values.clone(), None);
        assert_eq!(list.len(), 3);

        let nulls = NullBuffer::new_null(3);
        let list = FixedSizeListArray::new(field.clone(), 2, values.clone(), Some(nulls));
        assert_eq!(list.len(), 3);

        let list = FixedSizeListArray::new(field.clone(), 4, values.clone(), None);
        assert_eq!(list.len(), 1);

        let err = FixedSizeListArray::try_new(field.clone(), -1, values.clone(), None).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Invalid argument error: Size cannot be negative, got -1"
        );

        let list = FixedSizeListArray::new(field.clone(), 0, values.clone(), None);
        assert_eq!(list.len(), 0);

        let nulls = NullBuffer::new_null(2);
        let err = FixedSizeListArray::try_new(field, 2, values.clone(), Some(nulls)).unwrap_err();
        assert_eq!(err.to_string(), "Invalid argument error: Incorrect length of null buffer for FixedSizeListArray, expected 3 got 2");

        let field = Arc::new(Field::new("item", DataType::Int32, false));
        let err = FixedSizeListArray::try_new(field.clone(), 2, values.clone(), None).unwrap_err();
        assert_eq!(err.to_string(), "Invalid argument error: Found unmasked nulls for non-nullable FixedSizeListArray field \"item\"");

        // Valid as nulls in child masked by parent
        let nulls = NullBuffer::new(BooleanBuffer::new(Buffer::from([0b0000101]), 0, 3));
        FixedSizeListArray::new(field, 2, values.clone(), Some(nulls));

        let field = Arc::new(Field::new("item", DataType::Int64, true));
        let err = FixedSizeListArray::try_new(field, 2, values, None).unwrap_err();
        assert_eq!(err.to_string(), "Invalid argument error: FixedSizeListArray expected data type Int64 got Int32 for \"item\"");
    }

    #[test]
    fn empty_fixed_size_list() {
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        let nulls = NullBuffer::new_null(2);
        let values = new_empty_array(&DataType::Int32);
        let list = FixedSizeListArray::new(field.clone(), 0, values, Some(nulls));
        assert_eq!(list.len(), 2);
    }
}
