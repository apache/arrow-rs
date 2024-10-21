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

use crate::array::{get_offsets, make_array, print_long_array};
use crate::builder::{GenericListBuilder, PrimitiveBuilder};
use crate::{
    iterator::GenericListArrayIter, new_empty_array, Array, ArrayAccessor, ArrayRef,
    ArrowPrimitiveType, FixedSizeListArray,
};
use arrow_buffer::{ArrowNativeType, NullBuffer, OffsetBuffer};
use arrow_data::{ArrayData, ArrayDataBuilder};
use arrow_schema::{ArrowError, DataType, FieldRef};
use num::Integer;
use std::any::Any;
use std::sync::Arc;

/// A type that can be used within a variable-size array to encode offset information
///
/// See [`ListArray`], [`LargeListArray`], [`BinaryArray`], [`LargeBinaryArray`],
/// [`StringArray`] and [`LargeStringArray`]
///
/// [`BinaryArray`]: crate::array::BinaryArray
/// [`LargeBinaryArray`]: crate::array::LargeBinaryArray
/// [`StringArray`]: crate::array::StringArray
/// [`LargeStringArray`]: crate::array::LargeStringArray
pub trait OffsetSizeTrait: ArrowNativeType + std::ops::AddAssign + Integer {
    /// True for 64 bit offset size and false for 32 bit offset size
    const IS_LARGE: bool;
    /// Prefix for the offset size
    const PREFIX: &'static str;
}

impl OffsetSizeTrait for i32 {
    const IS_LARGE: bool = false;
    const PREFIX: &'static str = "";
}

impl OffsetSizeTrait for i64 {
    const IS_LARGE: bool = true;
    const PREFIX: &'static str = "Large";
}

/// An array of [variable length lists], similar to JSON arrays
/// (e.g. `["A", "B", "C"]`).
///
/// Lists are represented using `offsets` into a `values` child
/// array. Offsets are stored in two adjacent entries of an
/// [`OffsetBuffer`].
///
/// Arrow defines [`ListArray`] with `i32` offsets and
/// [`LargeListArray`] with `i64` offsets.
///
/// Use [`GenericListBuilder`] to construct a [`GenericListArray`].
///
/// # Representation
///
/// A [`ListArray`] can represent a list of values of any other
/// supported Arrow type. Each element of the `ListArray` itself is
/// a list which may be empty, may contain NULL and non-null values,
/// or may itself be NULL.
///
/// For example, the `ListArray` shown in the following diagram stores
/// lists of strings. Note that `[]` represents an empty (length
/// 0), but non NULL list.
///
/// ```text
/// ┌─────────────┐
/// │   [A,B,C]   │
/// ├─────────────┤
/// │     []      │
/// ├─────────────┤
/// │    NULL     │
/// ├─────────────┤
/// │     [D]     │
/// ├─────────────┤
/// │  [NULL, F]  │
/// └─────────────┘
/// ```
///
/// The `values` are stored in a child [`StringArray`] and the offsets
/// are stored in an [`OffsetBuffer`] as shown in the following
/// diagram. The logical values and offsets are shown on the left, and
/// the actual `ListArray` encoding on the right.
///
/// ```text
///                                         ┌ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
///                                                                 ┌ ─ ─ ─ ─ ─ ─ ┐    │
///  ┌─────────────┐  ┌───────┐             │     ┌───┐   ┌───┐       ┌───┐ ┌───┐
///  │   [A,B,C]   │  │ (0,3) │                   │ 1 │   │ 0 │     │ │ 1 │ │ A │ │ 0  │
///  ├─────────────┤  ├───────┤             │     ├───┤   ├───┤       ├───┤ ├───┤
///  │      []     │  │ (3,3) │                   │ 1 │   │ 3 │     │ │ 1 │ │ B │ │ 1  │
///  ├─────────────┤  ├───────┤             │     ├───┤   ├───┤       ├───┤ ├───┤
///  │    NULL     │  │ (3,4) │                   │ 0 │   │ 3 │     │ │ 1 │ │ C │ │ 2  │
///  ├─────────────┤  ├───────┤             │     ├───┤   ├───┤       ├───┤ ├───┤
///  │     [D]     │  │ (4,5) │                   │ 1 │   │ 4 │     │ │ ? │ │ ? │ │ 3  │
///  ├─────────────┤  ├───────┤             │     ├───┤   ├───┤       ├───┤ ├───┤
///  │  [NULL, F]  │  │ (5,7) │                   │ 1 │   │ 5 │     │ │ 1 │ │ D │ │ 4  │
///  └─────────────┘  └───────┘             │     └───┘   ├───┤       ├───┤ ├───┤
///                                                       │ 7 │     │ │ 0 │ │ ? │ │ 5  │
///                                         │  Validity   └───┘       ├───┤ ├───┤
///     Logical       Logical                  (nulls)   Offsets    │ │ 1 │ │ F │ │ 6  │
///      Values       Offsets               │                         └───┘ └───┘
///                                                                 │    Values   │    │
///                 (offsets[i],            │   ListArray               (Array)
///                offsets[i+1])                                    └ ─ ─ ─ ─ ─ ─ ┘    │
///                                         └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─
///
///
/// ```
///
/// [`StringArray`]: crate::array::StringArray
/// [variable length lists]: https://arrow.apache.org/docs/format/Columnar.html#variable-size-list-layout
pub struct GenericListArray<OffsetSize: OffsetSizeTrait> {
    data_type: DataType,
    nulls: Option<NullBuffer>,
    values: ArrayRef,
    value_offsets: OffsetBuffer<OffsetSize>,
}

impl<OffsetSize: OffsetSizeTrait> Clone for GenericListArray<OffsetSize> {
    fn clone(&self) -> Self {
        Self {
            data_type: self.data_type.clone(),
            nulls: self.nulls.clone(),
            values: self.values.clone(),
            value_offsets: self.value_offsets.clone(),
        }
    }
}

impl<OffsetSize: OffsetSizeTrait> GenericListArray<OffsetSize> {
    /// The data type constructor of list array.
    /// The input is the schema of the child array and
    /// the output is the [`DataType`], List or LargeList.
    pub const DATA_TYPE_CONSTRUCTOR: fn(FieldRef) -> DataType = if OffsetSize::IS_LARGE {
        DataType::LargeList
    } else {
        DataType::List
    };

    /// Create a new [`GenericListArray`] from the provided parts
    ///
    /// # Errors
    ///
    /// Errors if
    ///
    /// * `offsets.len() - 1 != nulls.len()`
    /// * `offsets.last() > values.len()`
    /// * `!field.is_nullable() && values.is_nullable()`
    /// * `field.data_type() != values.data_type()`
    pub fn try_new(
        field: FieldRef,
        offsets: OffsetBuffer<OffsetSize>,
        values: ArrayRef,
        nulls: Option<NullBuffer>,
    ) -> Result<Self, ArrowError> {
        let len = offsets.len() - 1; // Offsets guaranteed to not be empty
        let end_offset = offsets.last().unwrap().as_usize();
        // don't need to check other values of `offsets` because they are checked
        // during construction of `OffsetBuffer`
        if end_offset > values.len() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Max offset of {end_offset} exceeds length of values {}",
                values.len()
            )));
        }

        if let Some(n) = nulls.as_ref() {
            if n.len() != len {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "Incorrect length of null buffer for {}ListArray, expected {len} got {}",
                    OffsetSize::PREFIX,
                    n.len(),
                )));
            }
        }
        if !field.is_nullable() && values.is_nullable() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "Non-nullable field of {}ListArray {:?} cannot contain nulls",
                OffsetSize::PREFIX,
                field.name()
            )));
        }

        if field.data_type() != values.data_type() {
            return Err(ArrowError::InvalidArgumentError(format!(
                "{}ListArray expected data type {} got {} for {:?}",
                OffsetSize::PREFIX,
                field.data_type(),
                values.data_type(),
                field.name()
            )));
        }

        Ok(Self {
            data_type: Self::DATA_TYPE_CONSTRUCTOR(field),
            nulls,
            values,
            value_offsets: offsets,
        })
    }

    /// Create a new [`GenericListArray`] from the provided parts
    ///
    /// # Panics
    ///
    /// Panics if [`Self::try_new`] returns an error
    pub fn new(
        field: FieldRef,
        offsets: OffsetBuffer<OffsetSize>,
        values: ArrayRef,
        nulls: Option<NullBuffer>,
    ) -> Self {
        Self::try_new(field, offsets, values, nulls).unwrap()
    }

    /// Create a new [`GenericListArray`] of length `len` where all values are null
    pub fn new_null(field: FieldRef, len: usize) -> Self {
        let values = new_empty_array(field.data_type());
        Self {
            data_type: Self::DATA_TYPE_CONSTRUCTOR(field),
            nulls: Some(NullBuffer::new_null(len)),
            value_offsets: OffsetBuffer::new_zeroed(len),
            values,
        }
    }

    /// Deconstruct this array into its constituent parts
    pub fn into_parts(
        self,
    ) -> (
        FieldRef,
        OffsetBuffer<OffsetSize>,
        ArrayRef,
        Option<NullBuffer>,
    ) {
        let f = match self.data_type {
            DataType::List(f) | DataType::LargeList(f) => f,
            _ => unreachable!(),
        };
        (f, self.value_offsets, self.values, self.nulls)
    }

    /// Returns a reference to the offsets of this list
    ///
    /// Unlike [`Self::value_offsets`] this returns the [`OffsetBuffer`]
    /// allowing for zero-copy cloning
    #[inline]
    pub fn offsets(&self) -> &OffsetBuffer<OffsetSize> {
        &self.value_offsets
    }

    /// Returns a reference to the values of this list
    #[inline]
    pub fn values(&self) -> &ArrayRef {
        &self.values
    }

    /// Returns a clone of the value type of this list.
    pub fn value_type(&self) -> DataType {
        self.values.data_type().clone()
    }

    /// Returns ith value of this list array.
    /// # Safety
    /// Caller must ensure that the index is within the array bounds
    pub unsafe fn value_unchecked(&self, i: usize) -> ArrayRef {
        let end = self.value_offsets().get_unchecked(i + 1).as_usize();
        let start = self.value_offsets().get_unchecked(i).as_usize();
        self.values.slice(start, end - start)
    }

    /// Returns ith value of this list array.
    pub fn value(&self, i: usize) -> ArrayRef {
        let end = self.value_offsets()[i + 1].as_usize();
        let start = self.value_offsets()[i].as_usize();
        self.values.slice(start, end - start)
    }

    /// Returns the offset values in the offsets buffer
    #[inline]
    pub fn value_offsets(&self) -> &[OffsetSize] {
        &self.value_offsets
    }

    /// Returns the length for value at index `i`.
    #[inline]
    pub fn value_length(&self, i: usize) -> OffsetSize {
        let offsets = self.value_offsets();
        offsets[i + 1] - offsets[i]
    }

    /// constructs a new iterator
    pub fn iter<'a>(&'a self) -> GenericListArrayIter<'a, OffsetSize> {
        GenericListArrayIter::<'a, OffsetSize>::new(self)
    }

    #[inline]
    fn get_type(data_type: &DataType) -> Option<&DataType> {
        match (OffsetSize::IS_LARGE, data_type) {
            (true, DataType::LargeList(child)) | (false, DataType::List(child)) => {
                Some(child.data_type())
            }
            _ => None,
        }
    }

    /// Returns a zero-copy slice of this array with the indicated offset and length.
    pub fn slice(&self, offset: usize, length: usize) -> Self {
        Self {
            data_type: self.data_type.clone(),
            nulls: self.nulls.as_ref().map(|n| n.slice(offset, length)),
            values: self.values.clone(),
            value_offsets: self.value_offsets.slice(offset, length),
        }
    }

    /// Creates a [`GenericListArray`] from an iterator of primitive values
    /// # Example
    /// ```
    /// # use arrow_array::ListArray;
    /// # use arrow_array::types::Int32Type;
    ///
    /// let data = vec![
    ///    Some(vec![Some(0), Some(1), Some(2)]),
    ///    None,
    ///    Some(vec![Some(3), None, Some(5)]),
    ///    Some(vec![Some(6), Some(7)]),
    /// ];
    /// let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(data);
    /// println!("{:?}", list_array);
    /// ```
    pub fn from_iter_primitive<T, P, I>(iter: I) -> Self
    where
        T: ArrowPrimitiveType,
        P: IntoIterator<Item = Option<<T as ArrowPrimitiveType>::Native>>,
        I: IntoIterator<Item = Option<P>>,
    {
        let iter = iter.into_iter();
        let size_hint = iter.size_hint().0;
        let mut builder =
            GenericListBuilder::with_capacity(PrimitiveBuilder::<T>::new(), size_hint);

        for i in iter {
            match i {
                Some(p) => {
                    for t in p {
                        builder.values().append_option(t);
                    }
                    builder.append(true);
                }
                None => builder.append(false),
            }
        }
        builder.finish()
    }
}

impl<OffsetSize: OffsetSizeTrait> From<ArrayData> for GenericListArray<OffsetSize> {
    fn from(data: ArrayData) -> Self {
        Self::try_new_from_array_data(data)
            .expect("Expected infallible creation of GenericListArray from ArrayDataRef failed")
    }
}

impl<OffsetSize: OffsetSizeTrait> From<GenericListArray<OffsetSize>> for ArrayData {
    fn from(array: GenericListArray<OffsetSize>) -> Self {
        let len = array.len();
        let builder = ArrayDataBuilder::new(array.data_type)
            .len(len)
            .nulls(array.nulls)
            .buffers(vec![array.value_offsets.into_inner().into_inner()])
            .child_data(vec![array.values.to_data()]);

        unsafe { builder.build_unchecked() }
    }
}

impl<OffsetSize: OffsetSizeTrait> From<FixedSizeListArray> for GenericListArray<OffsetSize> {
    fn from(value: FixedSizeListArray) -> Self {
        let (field, size) = match value.data_type() {
            DataType::FixedSizeList(f, size) => (f, *size as usize),
            _ => unreachable!(),
        };

        let offsets = OffsetBuffer::from_lengths(std::iter::repeat(size).take(value.len()));

        Self {
            data_type: Self::DATA_TYPE_CONSTRUCTOR(field.clone()),
            nulls: value.nulls().cloned(),
            values: value.values().clone(),
            value_offsets: offsets,
        }
    }
}

impl<OffsetSize: OffsetSizeTrait> GenericListArray<OffsetSize> {
    fn try_new_from_array_data(data: ArrayData) -> Result<Self, ArrowError> {
        if data.buffers().len() != 1 {
            return Err(ArrowError::InvalidArgumentError(format!(
                "ListArray data should contain a single buffer only (value offsets), had {}",
                data.buffers().len()
            )));
        }

        if data.child_data().len() != 1 {
            return Err(ArrowError::InvalidArgumentError(format!(
                "ListArray should contain a single child array (values array), had {}",
                data.child_data().len()
            )));
        }

        let values = data.child_data()[0].clone();

        if let Some(child_data_type) = Self::get_type(data.data_type()) {
            if values.data_type() != child_data_type {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "[Large]ListArray's child datatype {:?} does not \
                             correspond to the List's datatype {:?}",
                    values.data_type(),
                    child_data_type
                )));
            }
        } else {
            return Err(ArrowError::InvalidArgumentError(format!(
                "[Large]ListArray's datatype must be [Large]ListArray(). It is {:?}",
                data.data_type()
            )));
        }

        let values = make_array(values);
        // SAFETY:
        // ArrayData is valid, and verified type above
        let value_offsets = unsafe { get_offsets(&data) };

        Ok(Self {
            data_type: data.data_type().clone(),
            nulls: data.nulls().cloned(),
            values,
            value_offsets,
        })
    }
}

impl<OffsetSize: OffsetSizeTrait> Array for GenericListArray<OffsetSize> {
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
        self.value_offsets.len() - 1
    }

    fn is_empty(&self) -> bool {
        self.value_offsets.len() <= 1
    }

    fn offset(&self) -> usize {
        0
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.nulls.as_ref()
    }

    fn get_buffer_memory_size(&self) -> usize {
        let mut size = self.values.get_buffer_memory_size();
        size += self.value_offsets.inner().inner().capacity();
        if let Some(n) = self.nulls.as_ref() {
            size += n.buffer().capacity();
        }
        size
    }

    fn get_array_memory_size(&self) -> usize {
        let mut size = std::mem::size_of::<Self>() + self.values.get_array_memory_size();
        size += self.value_offsets.inner().inner().capacity();
        if let Some(n) = self.nulls.as_ref() {
            size += n.buffer().capacity();
        }
        size
    }
}

impl<OffsetSize: OffsetSizeTrait> ArrayAccessor for &GenericListArray<OffsetSize> {
    type Item = ArrayRef;

    fn value(&self, index: usize) -> Self::Item {
        GenericListArray::value(self, index)
    }

    unsafe fn value_unchecked(&self, index: usize) -> Self::Item {
        GenericListArray::value(self, index)
    }
}

impl<OffsetSize: OffsetSizeTrait> std::fmt::Debug for GenericListArray<OffsetSize> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let prefix = OffsetSize::PREFIX;

        write!(f, "{prefix}ListArray\n[\n")?;
        print_long_array(self, f, |array, index, f| {
            std::fmt::Debug::fmt(&array.value(index), f)
        })?;
        write!(f, "]")
    }
}

/// A [`GenericListArray`] of variable size lists, storing offsets as `i32`.
///
// See [`ListBuilder`](crate::builder::ListBuilder) for how to construct a [`ListArray`]
pub type ListArray = GenericListArray<i32>;

/// A [`GenericListArray`] of variable size lists, storing offsets as `i64`.
///
// See [`LargeListBuilder`](crate::builder::LargeListBuilder) for how to construct a [`LargeListArray`]
pub type LargeListArray = GenericListArray<i64>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::{FixedSizeListBuilder, Int32Builder, ListBuilder, UnionBuilder};
    use crate::cast::AsArray;
    use crate::types::Int32Type;
    use crate::{Int32Array, Int64Array};
    use arrow_buffer::{bit_util, Buffer, ScalarBuffer};
    use arrow_schema::Field;

    fn create_from_buffers() -> ListArray {
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let values = Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7]);
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0, 3, 6, 8]));
        let field = Arc::new(Field::new("item", DataType::Int32, true));
        ListArray::new(field, offsets, Arc::new(values), None)
    }

    #[test]
    fn test_from_iter_primitive() {
        let data = vec![
            Some(vec![Some(0), Some(1), Some(2)]),
            Some(vec![Some(3), Some(4), Some(5)]),
            Some(vec![Some(6), Some(7)]),
        ];
        let list_array = ListArray::from_iter_primitive::<Int32Type, _, _>(data);

        let another = create_from_buffers();
        assert_eq!(list_array, another)
    }

    #[test]
    fn test_empty_list_array() {
        // Construct an empty value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(0)
            .add_buffer(Buffer::from([]))
            .build()
            .unwrap();

        // Construct an empty offset buffer
        let value_offsets = Buffer::from([]);

        // Construct a list array from the above two
        let list_data_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(0)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build()
            .unwrap();

        let list_array = ListArray::from(list_data);
        assert_eq!(list_array.len(), 0)
    }

    #[test]
    fn test_list_array() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7]))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let value_offsets = Buffer::from_slice_ref([0, 3, 6, 8]);

        // Construct a list array from the above two
        let list_data_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(3)
            .add_buffer(value_offsets.clone())
            .add_child_data(value_data.clone())
            .build()
            .unwrap();
        let list_array = ListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(value_data, values.to_data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(3, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offsets()[2]);
        assert_eq!(2, list_array.value_length(2));
        assert_eq!(0, list_array.value(0).as_primitive::<Int32Type>().value(0));
        assert_eq!(
            0,
            unsafe { list_array.value_unchecked(0) }
                .as_primitive::<Int32Type>()
                .value(0)
        );
        for i in 0..3 {
            assert!(list_array.is_valid(i));
            assert!(!list_array.is_null(i));
        }

        // Now test with a non-zero offset (skip first element)
        //  [[3, 4, 5], [6, 7]]
        let list_data = ArrayData::builder(list_data_type)
            .len(2)
            .offset(1)
            .add_buffer(value_offsets)
            .add_child_data(value_data.clone())
            .build()
            .unwrap();
        let list_array = ListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(value_data, values.to_data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(2, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offsets()[1]);
        assert_eq!(2, list_array.value_length(1));
        assert_eq!(3, list_array.value(0).as_primitive::<Int32Type>().value(0));
        assert_eq!(
            3,
            unsafe { list_array.value_unchecked(0) }
                .as_primitive::<Int32Type>()
                .value(0)
        );
    }

    #[test]
    fn test_large_list_array() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7]))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1, 2], [3, 4, 5], [6, 7]]
        let value_offsets = Buffer::from_slice_ref([0i64, 3, 6, 8]);

        // Construct a list array from the above two
        let list_data_type = DataType::new_large_list(DataType::Int32, false);
        let list_data = ArrayData::builder(list_data_type.clone())
            .len(3)
            .add_buffer(value_offsets.clone())
            .add_child_data(value_data.clone())
            .build()
            .unwrap();
        let list_array = LargeListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(value_data, values.to_data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(3, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offsets()[2]);
        assert_eq!(2, list_array.value_length(2));
        assert_eq!(0, list_array.value(0).as_primitive::<Int32Type>().value(0));
        assert_eq!(
            0,
            unsafe { list_array.value_unchecked(0) }
                .as_primitive::<Int32Type>()
                .value(0)
        );
        for i in 0..3 {
            assert!(list_array.is_valid(i));
            assert!(!list_array.is_null(i));
        }

        // Now test with a non-zero offset
        //  [[3, 4, 5], [6, 7]]
        let list_data = ArrayData::builder(list_data_type)
            .len(2)
            .offset(1)
            .add_buffer(value_offsets)
            .add_child_data(value_data.clone())
            .build()
            .unwrap();
        let list_array = LargeListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(value_data, values.to_data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(2, list_array.len());
        assert_eq!(0, list_array.null_count());
        assert_eq!(6, list_array.value_offsets()[1]);
        assert_eq!(2, list_array.value_length(1));
        assert_eq!(3, list_array.value(0).as_primitive::<Int32Type>().value(0));
        assert_eq!(
            3,
            unsafe { list_array.value_unchecked(0) }
                .as_primitive::<Int32Type>()
                .value(0)
        );
    }

    #[test]
    fn test_list_array_slice() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(10)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1], null, null, [2, 3], [4, 5], null, [6, 7, 8], null, [9]]
        let value_offsets = Buffer::from_slice_ref([0, 2, 2, 2, 4, 6, 6, 9, 9, 10]);
        // 01011001 00000001
        let mut null_bits: [u8; 2] = [0; 2];
        bit_util::set_bit(&mut null_bits, 0);
        bit_util::set_bit(&mut null_bits, 3);
        bit_util::set_bit(&mut null_bits, 4);
        bit_util::set_bit(&mut null_bits, 6);
        bit_util::set_bit(&mut null_bits, 8);

        // Construct a list array from the above two
        let list_data_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(9)
            .add_buffer(value_offsets)
            .add_child_data(value_data.clone())
            .null_bit_buffer(Some(Buffer::from(null_bits)))
            .build()
            .unwrap();
        let list_array = ListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(value_data, values.to_data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(9, list_array.len());
        assert_eq!(4, list_array.null_count());
        assert_eq!(2, list_array.value_offsets()[3]);
        assert_eq!(2, list_array.value_length(3));

        let sliced_array = list_array.slice(1, 6);
        assert_eq!(6, sliced_array.len());
        assert_eq!(3, sliced_array.null_count());

        for i in 0..sliced_array.len() {
            if bit_util::get_bit(&null_bits, 1 + i) {
                assert!(sliced_array.is_valid(i));
            } else {
                assert!(sliced_array.is_null(i));
            }
        }

        // Check offset and length for each non-null value.
        let sliced_list_array = sliced_array.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(2, sliced_list_array.value_offsets()[2]);
        assert_eq!(2, sliced_list_array.value_length(2));
        assert_eq!(4, sliced_list_array.value_offsets()[3]);
        assert_eq!(2, sliced_list_array.value_length(3));
        assert_eq!(6, sliced_list_array.value_offsets()[5]);
        assert_eq!(3, sliced_list_array.value_length(5));
    }

    #[test]
    fn test_large_list_array_slice() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(10)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1], null, null, [2, 3], [4, 5], null, [6, 7, 8], null, [9]]
        let value_offsets = Buffer::from_slice_ref([0i64, 2, 2, 2, 4, 6, 6, 9, 9, 10]);
        // 01011001 00000001
        let mut null_bits: [u8; 2] = [0; 2];
        bit_util::set_bit(&mut null_bits, 0);
        bit_util::set_bit(&mut null_bits, 3);
        bit_util::set_bit(&mut null_bits, 4);
        bit_util::set_bit(&mut null_bits, 6);
        bit_util::set_bit(&mut null_bits, 8);

        // Construct a list array from the above two
        let list_data_type = DataType::new_large_list(DataType::Int32, false);
        let list_data = ArrayData::builder(list_data_type)
            .len(9)
            .add_buffer(value_offsets)
            .add_child_data(value_data.clone())
            .null_bit_buffer(Some(Buffer::from(null_bits)))
            .build()
            .unwrap();
        let list_array = LargeListArray::from(list_data);

        let values = list_array.values();
        assert_eq!(value_data, values.to_data());
        assert_eq!(DataType::Int32, list_array.value_type());
        assert_eq!(9, list_array.len());
        assert_eq!(4, list_array.null_count());
        assert_eq!(2, list_array.value_offsets()[3]);
        assert_eq!(2, list_array.value_length(3));

        let sliced_array = list_array.slice(1, 6);
        assert_eq!(6, sliced_array.len());
        assert_eq!(3, sliced_array.null_count());

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
            .downcast_ref::<LargeListArray>()
            .unwrap();
        assert_eq!(2, sliced_list_array.value_offsets()[2]);
        assert_eq!(2, sliced_list_array.value_length(2));
        assert_eq!(4, sliced_list_array.value_offsets()[3]);
        assert_eq!(2, sliced_list_array.value_length(3));
        assert_eq!(6, sliced_list_array.value_offsets()[5]);
        assert_eq!(3, sliced_list_array.value_length(5));
    }

    #[test]
    #[should_panic(expected = "index out of bounds: the len is 10 but the index is 11")]
    fn test_list_array_index_out_of_bound() {
        // Construct a value array
        let value_data = ArrayData::builder(DataType::Int32)
            .len(10)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]))
            .build()
            .unwrap();

        // Construct a buffer for value offsets, for the nested array:
        //  [[0, 1], null, null, [2, 3], [4, 5], null, [6, 7, 8], null, [9]]
        let value_offsets = Buffer::from_slice_ref([0i64, 2, 2, 2, 4, 6, 6, 9, 9, 10]);
        // 01011001 00000001
        let mut null_bits: [u8; 2] = [0; 2];
        bit_util::set_bit(&mut null_bits, 0);
        bit_util::set_bit(&mut null_bits, 3);
        bit_util::set_bit(&mut null_bits, 4);
        bit_util::set_bit(&mut null_bits, 6);
        bit_util::set_bit(&mut null_bits, 8);

        // Construct a list array from the above two
        let list_data_type = DataType::new_large_list(DataType::Int32, false);
        let list_data = ArrayData::builder(list_data_type)
            .len(9)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .null_bit_buffer(Some(Buffer::from(null_bits)))
            .build()
            .unwrap();
        let list_array = LargeListArray::from(list_data);
        assert_eq!(9, list_array.len());

        list_array.value(10);
    }
    #[test]
    #[should_panic(expected = "ListArray data should contain a single buffer only (value offsets)")]
    // Different error messages, so skip for now
    // https://github.com/apache/arrow-rs/issues/1545
    #[cfg(not(feature = "force_validate"))]
    fn test_list_array_invalid_buffer_len() {
        let value_data = unsafe {
            ArrayData::builder(DataType::Int32)
                .len(8)
                .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7]))
                .build_unchecked()
        };
        let list_data_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, false)));
        let list_data = unsafe {
            ArrayData::builder(list_data_type)
                .len(3)
                .add_child_data(value_data)
                .build_unchecked()
        };
        drop(ListArray::from(list_data));
    }

    #[test]
    #[should_panic(expected = "ListArray should contain a single child array (values array)")]
    // Different error messages, so skip for now
    // https://github.com/apache/arrow-rs/issues/1545
    #[cfg(not(feature = "force_validate"))]
    fn test_list_array_invalid_child_array_len() {
        let value_offsets = Buffer::from_slice_ref([0, 2, 5, 7]);
        let list_data_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, false)));
        let list_data = unsafe {
            ArrayData::builder(list_data_type)
                .len(3)
                .add_buffer(value_offsets)
                .build_unchecked()
        };
        drop(ListArray::from(list_data));
    }

    #[test]
    #[should_panic(expected = "[Large]ListArray's datatype must be [Large]ListArray(). It is List")]
    fn test_from_array_data_validation() {
        let mut builder = ListBuilder::new(Int32Builder::new());
        builder.values().append_value(1);
        builder.append(true);
        let array = builder.finish();
        let _ = LargeListArray::from(array.into_data());
    }

    #[test]
    fn test_list_array_offsets_need_not_start_at_zero() {
        let value_data = ArrayData::builder(DataType::Int32)
            .len(8)
            .add_buffer(Buffer::from_slice_ref([0, 1, 2, 3, 4, 5, 6, 7]))
            .build()
            .unwrap();

        let value_offsets = Buffer::from_slice_ref([2, 2, 5, 7]);

        let list_data_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, false)));
        let list_data = ArrayData::builder(list_data_type)
            .len(3)
            .add_buffer(value_offsets)
            .add_child_data(value_data)
            .build()
            .unwrap();

        let list_array = ListArray::from(list_data);
        assert_eq!(list_array.value_length(0), 0);
        assert_eq!(list_array.value_length(1), 3);
        assert_eq!(list_array.value_length(2), 2);
    }

    #[test]
    #[should_panic(expected = "Memory pointer is not aligned with the specified scalar type")]
    // Different error messages, so skip for now
    // https://github.com/apache/arrow-rs/issues/1545
    #[cfg(not(feature = "force_validate"))]
    fn test_primitive_array_alignment() {
        let buf = Buffer::from_slice_ref([0_u64]);
        let buf2 = buf.slice(1);
        let array_data = unsafe {
            ArrayData::builder(DataType::Int32)
                .add_buffer(buf2)
                .build_unchecked()
        };
        drop(Int32Array::from(array_data));
    }

    #[test]
    #[should_panic(expected = "Memory pointer is not aligned with the specified scalar type")]
    // Different error messages, so skip for now
    // https://github.com/apache/arrow-rs/issues/1545
    #[cfg(not(feature = "force_validate"))]
    fn test_list_array_alignment() {
        let buf = Buffer::from_slice_ref([0_u64]);
        let buf2 = buf.slice(1);

        let values: [i32; 8] = [0; 8];
        let value_data = unsafe {
            ArrayData::builder(DataType::Int32)
                .add_buffer(Buffer::from_slice_ref(values))
                .build_unchecked()
        };

        let list_data_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, false)));
        let list_data = unsafe {
            ArrayData::builder(list_data_type)
                .add_buffer(buf2)
                .add_child_data(value_data)
                .build_unchecked()
        };
        drop(ListArray::from(list_data));
    }

    #[test]
    fn list_array_equality() {
        // test scaffold
        fn do_comparison(
            lhs_data: Vec<Option<Vec<Option<i32>>>>,
            rhs_data: Vec<Option<Vec<Option<i32>>>>,
            should_equal: bool,
        ) {
            let lhs = ListArray::from_iter_primitive::<Int32Type, _, _>(lhs_data.clone());
            let rhs = ListArray::from_iter_primitive::<Int32Type, _, _>(rhs_data.clone());
            assert_eq!(lhs == rhs, should_equal);

            let lhs = LargeListArray::from_iter_primitive::<Int32Type, _, _>(lhs_data);
            let rhs = LargeListArray::from_iter_primitive::<Int32Type, _, _>(rhs_data);
            assert_eq!(lhs == rhs, should_equal);
        }

        do_comparison(
            vec![
                Some(vec![Some(0), Some(1), Some(2)]),
                None,
                Some(vec![Some(3), None, Some(5)]),
                Some(vec![Some(6), Some(7)]),
            ],
            vec![
                Some(vec![Some(0), Some(1), Some(2)]),
                None,
                Some(vec![Some(3), None, Some(5)]),
                Some(vec![Some(6), Some(7)]),
            ],
            true,
        );

        do_comparison(
            vec![
                None,
                None,
                Some(vec![Some(3), None, Some(5)]),
                Some(vec![Some(6), Some(7)]),
            ],
            vec![
                Some(vec![Some(0), Some(1), Some(2)]),
                None,
                Some(vec![Some(3), None, Some(5)]),
                Some(vec![Some(6), Some(7)]),
            ],
            false,
        );

        do_comparison(
            vec![
                None,
                None,
                Some(vec![Some(3), None, Some(5)]),
                Some(vec![Some(6), Some(7)]),
            ],
            vec![
                None,
                None,
                Some(vec![Some(3), None, Some(5)]),
                Some(vec![Some(0), Some(0)]),
            ],
            false,
        );

        do_comparison(
            vec![None, None, Some(vec![Some(1)])],
            vec![None, None, Some(vec![Some(2)])],
            false,
        );
    }

    #[test]
    fn test_empty_offsets() {
        let f = Arc::new(Field::new("element", DataType::Int32, true));
        let string = ListArray::from(
            ArrayData::builder(DataType::List(f.clone()))
                .buffers(vec![Buffer::from(&[])])
                .add_child_data(ArrayData::new_empty(&DataType::Int32))
                .build()
                .unwrap(),
        );
        assert_eq!(string.value_offsets(), &[0]);
        let string = LargeListArray::from(
            ArrayData::builder(DataType::LargeList(f))
                .buffers(vec![Buffer::from(&[])])
                .add_child_data(ArrayData::new_empty(&DataType::Int32))
                .build()
                .unwrap(),
        );
        assert_eq!(string.len(), 0);
        assert_eq!(string.value_offsets(), &[0]);
    }

    #[test]
    fn test_try_new() {
        let offsets = OffsetBuffer::new(vec![0, 1, 4, 5].into());
        let values = Int32Array::new(vec![1, 2, 3, 4, 5].into(), None);
        let values = Arc::new(values) as ArrayRef;

        let field = Arc::new(Field::new("element", DataType::Int32, false));
        ListArray::new(field.clone(), offsets.clone(), values.clone(), None);

        let nulls = NullBuffer::new_null(3);
        ListArray::new(field.clone(), offsets, values.clone(), Some(nulls));

        let nulls = NullBuffer::new_null(3);
        let offsets = OffsetBuffer::new(vec![0, 1, 2, 4, 5].into());
        let err = LargeListArray::try_new(field, offsets.clone(), values.clone(), Some(nulls))
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Invalid argument error: Incorrect length of null buffer for LargeListArray, expected 4 got 3"
        );

        let field = Arc::new(Field::new("element", DataType::Int64, false));
        let err = LargeListArray::try_new(field.clone(), offsets.clone(), values.clone(), None)
            .unwrap_err();

        assert_eq!(
            err.to_string(),
            "Invalid argument error: LargeListArray expected data type Int64 got Int32 for \"element\""
        );

        let nulls = NullBuffer::new_null(7);
        let values = Int64Array::new(vec![0; 7].into(), Some(nulls));
        let values = Arc::new(values);

        let err =
            LargeListArray::try_new(field, offsets.clone(), values.clone(), None).unwrap_err();

        assert_eq!(
            err.to_string(),
            "Invalid argument error: Non-nullable field of LargeListArray \"element\" cannot contain nulls"
        );

        let field = Arc::new(Field::new("element", DataType::Int64, true));
        LargeListArray::new(field.clone(), offsets.clone(), values, None);

        let values = Int64Array::new(vec![0; 2].into(), None);
        let err = LargeListArray::try_new(field, offsets, Arc::new(values), None).unwrap_err();

        assert_eq!(
            err.to_string(),
            "Invalid argument error: Max offset of 5 exceeds length of values 2"
        );
    }

    #[test]
    fn test_from_fixed_size_list() {
        let mut builder = FixedSizeListBuilder::new(Int32Builder::new(), 3);
        builder.values().append_slice(&[1, 2, 3]);
        builder.append(true);
        builder.values().append_slice(&[0, 0, 0]);
        builder.append(false);
        builder.values().append_slice(&[4, 5, 6]);
        builder.append(true);
        let list: ListArray = builder.finish().into();

        let values: Vec<_> = list
            .iter()
            .map(|x| x.map(|x| x.as_primitive::<Int32Type>().values().to_vec()))
            .collect();
        assert_eq!(values, vec![Some(vec![1, 2, 3]), None, Some(vec![4, 5, 6])])
    }

    #[test]
    fn test_nullable_union() {
        let offsets = OffsetBuffer::new(vec![0, 1, 4, 5].into());
        let mut builder = UnionBuilder::new_dense();
        builder.append::<Int32Type>("a", 1).unwrap();
        builder.append::<Int32Type>("b", 2).unwrap();
        builder.append::<Int32Type>("b", 3).unwrap();
        builder.append::<Int32Type>("a", 4).unwrap();
        builder.append::<Int32Type>("a", 5).unwrap();
        let values = builder.build().unwrap();
        let field = Arc::new(Field::new("element", values.data_type().clone(), false));
        ListArray::new(field.clone(), offsets, Arc::new(values), None);
    }
}
