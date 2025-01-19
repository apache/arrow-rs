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

//! The concrete array definitions

mod binary_array;

use crate::types::*;
use arrow_buffer::{ArrowNativeType, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_data::ArrayData;
use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use std::any::Any;
use std::sync::Arc;

pub use binary_array::*;

mod boolean_array;
pub use boolean_array::*;

mod byte_array;
pub use byte_array::*;

mod dictionary_array;
pub use dictionary_array::*;

mod fixed_size_binary_array;
pub use fixed_size_binary_array::*;

mod fixed_size_list_array;
pub use fixed_size_list_array::*;

mod list_array;
pub use list_array::*;

mod map_array;
pub use map_array::*;

mod null_array;
pub use null_array::*;

mod primitive_array;
pub use primitive_array::*;

mod string_array;
pub use string_array::*;

mod struct_array;
pub use struct_array::*;

mod union_array;
pub use union_array::*;

mod run_array;

pub use run_array::*;

mod byte_view_array;

pub use byte_view_array::*;

mod list_view_array;

pub use list_view_array::*;

use crate::iterator::ArrayIter;

/// An array in the [arrow columnar format](https://arrow.apache.org/docs/format/Columnar.html)
pub trait Array: std::fmt::Debug + Send + Sync {
    /// Returns the array as [`Any`] so that it can be
    /// downcasted to a specific implementation.
    ///
    /// # Example:
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use arrow_array::{Int32Array, RecordBatch};
    /// # use arrow_schema::{Schema, Field, DataType, ArrowError};
    ///
    /// let id = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// let batch = RecordBatch::try_new(
    ///     Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
    ///     vec![Arc::new(id)]
    /// ).unwrap();
    ///
    /// let int32array = batch
    ///     .column(0)
    ///     .as_any()
    ///     .downcast_ref::<Int32Array>()
    ///     .expect("Failed to downcast");
    /// ```
    fn as_any(&self) -> &dyn Any;

    /// Returns the underlying data of this array
    fn to_data(&self) -> ArrayData;

    /// Returns the underlying data of this array
    ///
    /// Unlike [`Array::to_data`] this consumes self, allowing it avoid unnecessary clones
    fn into_data(self) -> ArrayData;

    /// Returns a reference to the [`DataType`] of this array.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow_schema::DataType;
    /// use arrow_array::{Array, Int32Array};
    ///
    /// let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    ///
    /// assert_eq!(*array.data_type(), DataType::Int32);
    /// ```
    fn data_type(&self) -> &DataType;

    /// Returns a zero-copy slice of this array with the indicated offset and length.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow_array::{Array, Int32Array};
    ///
    /// let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    /// // Make slice over the values [2, 3, 4]
    /// let array_slice = array.slice(1, 3);
    ///
    /// assert_eq!(&array_slice, &Int32Array::from(vec![2, 3, 4]));
    /// ```
    fn slice(&self, offset: usize, length: usize) -> ArrayRef;

    /// Returns the length (i.e., number of elements) of this array.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow_array::{Array, Int32Array};
    ///
    /// let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    ///
    /// assert_eq!(array.len(), 5);
    /// ```
    fn len(&self) -> usize;

    /// Returns whether this array is empty.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow_array::{Array, Int32Array};
    ///
    /// let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    ///
    /// assert_eq!(array.is_empty(), false);
    /// ```
    fn is_empty(&self) -> bool;

    /// Shrinks the capacity of any exclusively owned buffer as much as possible
    ///
    /// Shared or externally allocated buffers will be ignored, and
    /// any buffer offsets will be preserved.
    fn shrink_to_fit(&mut self) {}

    /// Returns the offset into the underlying data used by this array(-slice).
    /// Note that the underlying data can be shared by many arrays.
    /// This defaults to `0`.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow_array::{Array, BooleanArray};
    ///
    /// let array = BooleanArray::from(vec![false, false, true, true]);
    /// let array_slice = array.slice(1, 3);
    ///
    /// assert_eq!(array.offset(), 0);
    /// assert_eq!(array_slice.offset(), 1);
    /// ```
    fn offset(&self) -> usize;

    /// Returns the null buffer of this array if any.
    ///
    /// The null buffer contains the "physical" nulls of an array, that is how
    /// the nulls are represented in the underlying arrow format.
    ///
    /// The physical representation is efficient, but is sometimes non intuitive
    /// for certain array types such as those with nullable child arrays like
    /// [`DictionaryArray::values`], [`RunArray::values`] or [`UnionArray`], or without a
    /// null buffer, such as [`NullArray`].
    ///
    /// To determine if each element of such an array is "logically" null,
    /// use the slower [`Array::logical_nulls`] to obtain a computed mask.
    fn nulls(&self) -> Option<&NullBuffer>;

    /// Returns a potentially computed [`NullBuffer`] that represents the logical
    /// null values of this array, if any.
    ///
    /// Logical nulls represent the values that are null in the array,
    /// regardless of the underlying physical arrow representation.
    ///
    /// For most array types, this is equivalent to the "physical" nulls
    /// returned by [`Array::nulls`]. It is different for the following cases, because which
    /// elements are null is not encoded in a single null buffer:
    ///
    /// * [`DictionaryArray`] where [`DictionaryArray::values`] contains nulls
    /// * [`RunArray`] where [`RunArray::values`] contains nulls
    /// * [`NullArray`] where all indices are nulls
    /// * [`UnionArray`] where the selected values contains nulls
    ///
    /// In these cases a logical [`NullBuffer`] will be computed, encoding the
    /// logical nullability of these arrays, beyond what is encoded in
    /// [`Array::nulls`]
    fn logical_nulls(&self) -> Option<NullBuffer> {
        self.nulls().cloned()
    }

    /// Returns whether the element at `index` is null according to [`Array::nulls`]
    ///
    /// Note: For performance reasons, this method returns nullability solely as determined by the
    /// null buffer. This difference can lead to surprising results, for example, [`NullArray::is_null`] always
    /// returns `false` as the array lacks a null buffer. Similarly [`DictionaryArray`], [`RunArray`] and [`UnionArray`] may
    /// encode nullability in their children. See [`Self::logical_nulls`] for more information.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow_array::{Array, Int32Array, NullArray};
    ///
    /// let array = Int32Array::from(vec![Some(1), None]);
    /// assert_eq!(array.is_null(0), false);
    /// assert_eq!(array.is_null(1), true);
    ///
    /// // NullArrays do not have a null buffer, and therefore always
    /// // return false for is_null.
    /// let array = NullArray::new(1);
    /// assert_eq!(array.is_null(0), false);
    /// ```
    fn is_null(&self, index: usize) -> bool {
        self.nulls().map(|n| n.is_null(index)).unwrap_or_default()
    }

    /// Returns whether the element at `index` is *not* null, the
    /// opposite of [`Self::is_null`].
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow_array::{Array, Int32Array};
    ///
    /// let array = Int32Array::from(vec![Some(1), None]);
    ///
    /// assert_eq!(array.is_valid(0), true);
    /// assert_eq!(array.is_valid(1), false);
    /// ```
    fn is_valid(&self, index: usize) -> bool {
        !self.is_null(index)
    }

    /// Returns the total number of physical null values in this array.
    ///
    /// Note: this method returns the physical null count, i.e. that encoded in [`Array::nulls`],
    /// see [`Array::logical_nulls`] for logical nullability
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow_array::{Array, Int32Array};
    ///
    /// // Construct an array with values [1, NULL, NULL]
    /// let array = Int32Array::from(vec![Some(1), None, None]);
    ///
    /// assert_eq!(array.null_count(), 2);
    /// ```
    fn null_count(&self) -> usize {
        self.nulls().map(|n| n.null_count()).unwrap_or_default()
    }

    /// Returns the total number of logical null values in this array.
    ///
    /// Note: this method returns the logical null count, i.e. that encoded in
    /// [`Array::logical_nulls`]. In general this is equivalent to [`Array::null_count`] but may differ in the
    /// presence of logical nullability, see [`Array::nulls`] and [`Array::logical_nulls`].
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow_array::{Array, Int32Array};
    ///
    /// // Construct an array with values [1, NULL, NULL]
    /// let array = Int32Array::from(vec![Some(1), None, None]);
    ///
    /// assert_eq!(array.logical_null_count(), 2);
    /// ```
    fn logical_null_count(&self) -> usize {
        self.logical_nulls()
            .map(|n| n.null_count())
            .unwrap_or_default()
    }

    /// Returns `false` if the array is guaranteed to not contain any logical nulls
    ///
    /// This is generally equivalent to `Array::logical_null_count() != 0` unless determining
    /// the logical nulls is expensive, in which case this method can return true even for an
    /// array without nulls.
    ///
    /// This is also generally equivalent to `Array::null_count() != 0` but may differ in the
    /// presence of logical nullability, see [`Array::logical_null_count`] and [`Array::null_count`].
    ///
    /// Implementations will return `true` unless they can cheaply prove no logical nulls
    /// are present. For example a [`DictionaryArray`] with nullable values will still return true,
    /// even if the nulls present in [`DictionaryArray::values`] are not referenced by any key,
    /// and therefore would not appear in [`Array::logical_nulls`].
    fn is_nullable(&self) -> bool {
        self.logical_null_count() != 0
    }

    /// Returns the total number of bytes of memory pointed to by this array.
    /// The buffers store bytes in the Arrow memory format, and include the data as well as the validity map.
    /// Note that this does not always correspond to the exact memory usage of an array,
    /// since multiple arrays can share the same buffers or slices thereof.
    fn get_buffer_memory_size(&self) -> usize;

    /// Returns the total number of bytes of memory occupied physically by this array.
    /// This value will always be greater than returned by `get_buffer_memory_size()` and
    /// includes the overhead of the data structures that contain the pointers to the various buffers.
    fn get_array_memory_size(&self) -> usize;
}

/// A reference-counted reference to a generic `Array`
pub type ArrayRef = Arc<dyn Array>;

/// Ergonomics: Allow use of an ArrayRef as an `&dyn Array`
impl Array for ArrayRef {
    fn as_any(&self) -> &dyn Any {
        self.as_ref().as_any()
    }

    fn to_data(&self) -> ArrayData {
        self.as_ref().to_data()
    }

    fn into_data(self) -> ArrayData {
        self.to_data()
    }

    fn data_type(&self) -> &DataType {
        self.as_ref().data_type()
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        self.as_ref().slice(offset, length)
    }

    fn len(&self) -> usize {
        self.as_ref().len()
    }

    fn is_empty(&self) -> bool {
        self.as_ref().is_empty()
    }

    /// For shared buffers, this is a no-op.
    fn shrink_to_fit(&mut self) {
        if let Some(slf) = Arc::get_mut(self) {
            slf.shrink_to_fit();
        } else {
            // We ignore shared buffers.
        }
    }

    fn offset(&self) -> usize {
        self.as_ref().offset()
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        self.as_ref().nulls()
    }

    fn logical_nulls(&self) -> Option<NullBuffer> {
        self.as_ref().logical_nulls()
    }

    fn is_null(&self, index: usize) -> bool {
        self.as_ref().is_null(index)
    }

    fn is_valid(&self, index: usize) -> bool {
        self.as_ref().is_valid(index)
    }

    fn null_count(&self) -> usize {
        self.as_ref().null_count()
    }

    fn logical_null_count(&self) -> usize {
        self.as_ref().logical_null_count()
    }

    fn is_nullable(&self) -> bool {
        self.as_ref().is_nullable()
    }

    fn get_buffer_memory_size(&self) -> usize {
        self.as_ref().get_buffer_memory_size()
    }

    fn get_array_memory_size(&self) -> usize {
        self.as_ref().get_array_memory_size()
    }
}

impl<T: Array> Array for &T {
    fn as_any(&self) -> &dyn Any {
        T::as_any(self)
    }

    fn to_data(&self) -> ArrayData {
        T::to_data(self)
    }

    fn into_data(self) -> ArrayData {
        self.to_data()
    }

    fn data_type(&self) -> &DataType {
        T::data_type(self)
    }

    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        T::slice(self, offset, length)
    }

    fn len(&self) -> usize {
        T::len(self)
    }

    fn is_empty(&self) -> bool {
        T::is_empty(self)
    }

    fn offset(&self) -> usize {
        T::offset(self)
    }

    fn nulls(&self) -> Option<&NullBuffer> {
        T::nulls(self)
    }

    fn logical_nulls(&self) -> Option<NullBuffer> {
        T::logical_nulls(self)
    }

    fn is_null(&self, index: usize) -> bool {
        T::is_null(self, index)
    }

    fn is_valid(&self, index: usize) -> bool {
        T::is_valid(self, index)
    }

    fn null_count(&self) -> usize {
        T::null_count(self)
    }

    fn logical_null_count(&self) -> usize {
        T::logical_null_count(self)
    }

    fn is_nullable(&self) -> bool {
        T::is_nullable(self)
    }

    fn get_buffer_memory_size(&self) -> usize {
        T::get_buffer_memory_size(self)
    }

    fn get_array_memory_size(&self) -> usize {
        T::get_array_memory_size(self)
    }
}

/// A generic trait for accessing the values of an [`Array`]
///
/// This trait helps write specialized implementations of algorithms for
/// different array types. Specialized implementations allow the compiler
/// to optimize the code for the specific array type, which can lead to
/// significant performance improvements.
///
/// # Example
/// For example, to write three different implementations of a string length function
/// for [`StringArray`], [`LargeStringArray`], and [`StringViewArray`], you can write
///
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{ArrayAccessor, ArrayRef, ArrowPrimitiveType, OffsetSizeTrait, PrimitiveArray};
/// # use arrow_buffer::ArrowNativeType;
/// # use arrow_array::cast::AsArray;
/// # use arrow_array::iterator::ArrayIter;
/// # use arrow_array::types::{Int32Type, Int64Type};
/// # use arrow_schema::{ArrowError, DataType};
/// /// This function takes a dynamically typed `ArrayRef` and calls
/// /// calls one of three specialized implementations
/// fn character_length(arg: ArrayRef) -> Result<ArrayRef, ArrowError> {
///     match arg.data_type() {
///         DataType::Utf8 => {
///             // downcast the ArrayRef to a StringArray and call the specialized implementation
///             let string_array = arg.as_string::<i32>();
///             character_length_general::<Int32Type, _>(string_array)
///         }
///         DataType::LargeUtf8 => {
///             character_length_general::<Int64Type, _>(arg.as_string::<i64>())
///         }
///         DataType::Utf8View => {
///             character_length_general::<Int32Type, _>(arg.as_string_view())
///         }
///         _ => Err(ArrowError::InvalidArgumentError("Unsupported data type".to_string())),
///     }
/// }
///
/// /// A generic implementation of the character_length function
/// /// This function uses the `ArrayAccessor` trait to access the values of the array
/// /// so the compiler can generated specialized implementations for different array types
/// ///
/// /// Returns a new array with the length of each string in the input array
/// /// * Int32Array for Utf8 and Utf8View arrays (lengths are 32-bit integers)
/// /// * Int64Array for LargeUtf8 arrays (lengths are 64-bit integers)
/// ///
/// /// This is generic on the type of the primitive array (different string arrays have
/// /// different lengths) and the type of the array accessor (different string arrays
/// /// have different ways to access the values)
/// fn character_length_general<'a, T: ArrowPrimitiveType, V: ArrayAccessor<Item = &'a str>>(
///     array: V,
/// ) -> Result<ArrayRef, ArrowError>
/// where
///     T::Native: OffsetSizeTrait,
/// {
///     let iter = ArrayIter::new(array);
///     // Create a Int32Array / Int64Array with the length of each string
///     let result = iter
///         .map(|string| {
///             string.map(|string: &str| {
///                 T::Native::from_usize(string.chars().count())
///                     .expect("should not fail as string.chars will always return integer")
///             })
///         })
///         .collect::<PrimitiveArray<T>>();
///
///     /// Return the result as a new ArrayRef (dynamically typed)
///     Ok(Arc::new(result) as ArrayRef)
/// }
/// ```
///
/// # Validity
///
/// An [`ArrayAccessor`] must always return a well-defined value for an index
/// that is within the bounds `0..Array::len`, including for null indexes where
/// [`Array::is_null`] is true.
///
/// The value at null indexes is unspecified, and implementations must not rely
/// on a specific value such as [`Default::default`] being returned, however, it
/// must not be undefined
pub trait ArrayAccessor: Array {
    /// The Arrow type of the element being accessed.
    type Item: Send + Sync;

    /// Returns the element at index `i`
    /// # Panics
    /// Panics if the value is outside the bounds of the array
    fn value(&self, index: usize) -> Self::Item;

    /// Returns the element at index `i`
    /// # Safety
    /// Caller is responsible for ensuring that the index is within the bounds of the array
    unsafe fn value_unchecked(&self, index: usize) -> Self::Item;
}

/// A trait for Arrow String Arrays, currently three types are supported:
/// - `StringArray`
/// - `LargeStringArray`
/// - `StringViewArray`
///
/// This trait helps to abstract over the different types of string arrays
/// so that we don't need to duplicate the implementation for each type.
pub trait StringArrayType<'a>: ArrayAccessor<Item = &'a str> + Sized {
    /// Returns true if all data within this string array is ASCII
    fn is_ascii(&self) -> bool;

    /// Constructs a new iterator
    fn iter(&self) -> ArrayIter<Self>;
}

impl<'a, O: OffsetSizeTrait> StringArrayType<'a> for &'a GenericStringArray<O> {
    fn is_ascii(&self) -> bool {
        GenericStringArray::<O>::is_ascii(self)
    }

    fn iter(&self) -> ArrayIter<Self> {
        GenericStringArray::<O>::iter(self)
    }
}
impl<'a> StringArrayType<'a> for &'a StringViewArray {
    fn is_ascii(&self) -> bool {
        StringViewArray::is_ascii(self)
    }

    fn iter(&self) -> ArrayIter<Self> {
        StringViewArray::iter(self)
    }
}

/// A trait for Arrow String Arrays, currently three types are supported:
/// - `BinaryArray`
/// - `LargeBinaryArray`
/// - `BinaryViewArray`
///
/// This trait helps to abstract over the different types of binary arrays
/// so that we don't need to duplicate the implementation for each type.
pub trait BinaryArrayType<'a>: ArrayAccessor<Item = &'a [u8]> + Sized {
    /// Constructs a new iterator
    fn iter(&self) -> ArrayIter<Self>;
}

impl<'a, O: OffsetSizeTrait> BinaryArrayType<'a> for &'a GenericBinaryArray<O> {
    fn iter(&self) -> ArrayIter<Self> {
        GenericBinaryArray::<O>::iter(self)
    }
}
impl<'a> BinaryArrayType<'a> for &'a BinaryViewArray {
    fn iter(&self) -> ArrayIter<Self> {
        BinaryViewArray::iter(self)
    }
}

impl PartialEq for dyn Array + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.to_data().eq(&other.to_data())
    }
}

impl<T: Array> PartialEq<T> for dyn Array + '_ {
    fn eq(&self, other: &T) -> bool {
        self.to_data().eq(&other.to_data())
    }
}

impl PartialEq for NullArray {
    fn eq(&self, other: &NullArray) -> bool {
        self.to_data().eq(&other.to_data())
    }
}

impl<T: ArrowPrimitiveType> PartialEq for PrimitiveArray<T> {
    fn eq(&self, other: &PrimitiveArray<T>) -> bool {
        self.to_data().eq(&other.to_data())
    }
}

impl<K: ArrowDictionaryKeyType> PartialEq for DictionaryArray<K> {
    fn eq(&self, other: &Self) -> bool {
        self.to_data().eq(&other.to_data())
    }
}

impl PartialEq for BooleanArray {
    fn eq(&self, other: &BooleanArray) -> bool {
        self.to_data().eq(&other.to_data())
    }
}

impl<OffsetSize: OffsetSizeTrait> PartialEq for GenericStringArray<OffsetSize> {
    fn eq(&self, other: &Self) -> bool {
        self.to_data().eq(&other.to_data())
    }
}

impl<OffsetSize: OffsetSizeTrait> PartialEq for GenericBinaryArray<OffsetSize> {
    fn eq(&self, other: &Self) -> bool {
        self.to_data().eq(&other.to_data())
    }
}

impl PartialEq for FixedSizeBinaryArray {
    fn eq(&self, other: &Self) -> bool {
        self.to_data().eq(&other.to_data())
    }
}

impl<OffsetSize: OffsetSizeTrait> PartialEq for GenericListArray<OffsetSize> {
    fn eq(&self, other: &Self) -> bool {
        self.to_data().eq(&other.to_data())
    }
}

impl<OffsetSize: OffsetSizeTrait> PartialEq for GenericListViewArray<OffsetSize> {
    fn eq(&self, other: &Self) -> bool {
        self.to_data().eq(&other.to_data())
    }
}

impl PartialEq for MapArray {
    fn eq(&self, other: &Self) -> bool {
        self.to_data().eq(&other.to_data())
    }
}

impl PartialEq for FixedSizeListArray {
    fn eq(&self, other: &Self) -> bool {
        self.to_data().eq(&other.to_data())
    }
}

impl PartialEq for StructArray {
    fn eq(&self, other: &Self) -> bool {
        self.to_data().eq(&other.to_data())
    }
}

impl<T: ByteViewType + ?Sized> PartialEq for GenericByteViewArray<T> {
    fn eq(&self, other: &Self) -> bool {
        self.to_data().eq(&other.to_data())
    }
}

/// Constructs an array using the input `data`.
/// Returns a reference-counted `Array` instance.
pub fn make_array(data: ArrayData) -> ArrayRef {
    match data.data_type() {
        DataType::Boolean => Arc::new(BooleanArray::from(data)) as ArrayRef,
        DataType::Int8 => Arc::new(Int8Array::from(data)) as ArrayRef,
        DataType::Int16 => Arc::new(Int16Array::from(data)) as ArrayRef,
        DataType::Int32 => Arc::new(Int32Array::from(data)) as ArrayRef,
        DataType::Int64 => Arc::new(Int64Array::from(data)) as ArrayRef,
        DataType::UInt8 => Arc::new(UInt8Array::from(data)) as ArrayRef,
        DataType::UInt16 => Arc::new(UInt16Array::from(data)) as ArrayRef,
        DataType::UInt32 => Arc::new(UInt32Array::from(data)) as ArrayRef,
        DataType::UInt64 => Arc::new(UInt64Array::from(data)) as ArrayRef,
        DataType::Float16 => Arc::new(Float16Array::from(data)) as ArrayRef,
        DataType::Float32 => Arc::new(Float32Array::from(data)) as ArrayRef,
        DataType::Float64 => Arc::new(Float64Array::from(data)) as ArrayRef,
        DataType::Date32 => Arc::new(Date32Array::from(data)) as ArrayRef,
        DataType::Date64 => Arc::new(Date64Array::from(data)) as ArrayRef,
        DataType::Time32(TimeUnit::Second) => Arc::new(Time32SecondArray::from(data)) as ArrayRef,
        DataType::Time32(TimeUnit::Millisecond) => {
            Arc::new(Time32MillisecondArray::from(data)) as ArrayRef
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            Arc::new(Time64MicrosecondArray::from(data)) as ArrayRef
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            Arc::new(Time64NanosecondArray::from(data)) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            Arc::new(TimestampSecondArray::from(data)) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Arc::new(TimestampMillisecondArray::from(data)) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Arc::new(TimestampMicrosecondArray::from(data)) as ArrayRef
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Arc::new(TimestampNanosecondArray::from(data)) as ArrayRef
        }
        DataType::Interval(IntervalUnit::YearMonth) => {
            Arc::new(IntervalYearMonthArray::from(data)) as ArrayRef
        }
        DataType::Interval(IntervalUnit::DayTime) => {
            Arc::new(IntervalDayTimeArray::from(data)) as ArrayRef
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            Arc::new(IntervalMonthDayNanoArray::from(data)) as ArrayRef
        }
        DataType::Duration(TimeUnit::Second) => {
            Arc::new(DurationSecondArray::from(data)) as ArrayRef
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            Arc::new(DurationMillisecondArray::from(data)) as ArrayRef
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            Arc::new(DurationMicrosecondArray::from(data)) as ArrayRef
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            Arc::new(DurationNanosecondArray::from(data)) as ArrayRef
        }
        DataType::Binary => Arc::new(BinaryArray::from(data)) as ArrayRef,
        DataType::LargeBinary => Arc::new(LargeBinaryArray::from(data)) as ArrayRef,
        DataType::FixedSizeBinary(_) => Arc::new(FixedSizeBinaryArray::from(data)) as ArrayRef,
        DataType::BinaryView => Arc::new(BinaryViewArray::from(data)) as ArrayRef,
        DataType::Utf8 => Arc::new(StringArray::from(data)) as ArrayRef,
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(data)) as ArrayRef,
        DataType::Utf8View => Arc::new(StringViewArray::from(data)) as ArrayRef,
        DataType::List(_) => Arc::new(ListArray::from(data)) as ArrayRef,
        DataType::LargeList(_) => Arc::new(LargeListArray::from(data)) as ArrayRef,
        DataType::ListView(_) => Arc::new(ListViewArray::from(data)) as ArrayRef,
        DataType::LargeListView(_) => Arc::new(LargeListViewArray::from(data)) as ArrayRef,
        DataType::Struct(_) => Arc::new(StructArray::from(data)) as ArrayRef,
        DataType::Map(_, _) => Arc::new(MapArray::from(data)) as ArrayRef,
        DataType::Union(_, _) => Arc::new(UnionArray::from(data)) as ArrayRef,
        DataType::FixedSizeList(_, _) => Arc::new(FixedSizeListArray::from(data)) as ArrayRef,
        DataType::Dictionary(ref key_type, _) => match key_type.as_ref() {
            DataType::Int8 => Arc::new(DictionaryArray::<Int8Type>::from(data)) as ArrayRef,
            DataType::Int16 => Arc::new(DictionaryArray::<Int16Type>::from(data)) as ArrayRef,
            DataType::Int32 => Arc::new(DictionaryArray::<Int32Type>::from(data)) as ArrayRef,
            DataType::Int64 => Arc::new(DictionaryArray::<Int64Type>::from(data)) as ArrayRef,
            DataType::UInt8 => Arc::new(DictionaryArray::<UInt8Type>::from(data)) as ArrayRef,
            DataType::UInt16 => Arc::new(DictionaryArray::<UInt16Type>::from(data)) as ArrayRef,
            DataType::UInt32 => Arc::new(DictionaryArray::<UInt32Type>::from(data)) as ArrayRef,
            DataType::UInt64 => Arc::new(DictionaryArray::<UInt64Type>::from(data)) as ArrayRef,
            dt => panic!("Unexpected dictionary key type {dt:?}"),
        },
        DataType::RunEndEncoded(ref run_ends_type, _) => match run_ends_type.data_type() {
            DataType::Int16 => Arc::new(RunArray::<Int16Type>::from(data)) as ArrayRef,
            DataType::Int32 => Arc::new(RunArray::<Int32Type>::from(data)) as ArrayRef,
            DataType::Int64 => Arc::new(RunArray::<Int64Type>::from(data)) as ArrayRef,
            dt => panic!("Unexpected data type for run_ends array {dt:?}"),
        },
        DataType::Null => Arc::new(NullArray::from(data)) as ArrayRef,
        DataType::Decimal128(_, _) => Arc::new(Decimal128Array::from(data)) as ArrayRef,
        DataType::Decimal256(_, _) => Arc::new(Decimal256Array::from(data)) as ArrayRef,
        dt => panic!("Unexpected data type {dt:?}"),
    }
}

/// Creates a new empty array
///
/// ```
/// use std::sync::Arc;
/// use arrow_schema::DataType;
/// use arrow_array::{ArrayRef, Int32Array, new_empty_array};
///
/// let empty_array = new_empty_array(&DataType::Int32);
/// let array: ArrayRef = Arc::new(Int32Array::from(vec![] as Vec<i32>));
///
/// assert_eq!(&array, &empty_array);
/// ```
pub fn new_empty_array(data_type: &DataType) -> ArrayRef {
    let data = ArrayData::new_empty(data_type);
    make_array(data)
}

/// Creates a new array of `data_type` of length `length` filled
/// entirely of `NULL` values
///
/// ```
/// use std::sync::Arc;
/// use arrow_schema::DataType;
/// use arrow_array::{ArrayRef, Int32Array, new_null_array};
///
/// let null_array = new_null_array(&DataType::Int32, 3);
/// let array: ArrayRef = Arc::new(Int32Array::from(vec![None, None, None]));
///
/// assert_eq!(&array, &null_array);
/// ```
pub fn new_null_array(data_type: &DataType, length: usize) -> ArrayRef {
    make_array(ArrayData::new_null(data_type, length))
}

/// Helper function that gets offset from an [`ArrayData`]
///
/// # Safety
///
/// - ArrayData must contain a valid [`OffsetBuffer`] as its first buffer
unsafe fn get_offsets<O: ArrowNativeType>(data: &ArrayData) -> OffsetBuffer<O> {
    match data.is_empty() && data.buffers()[0].is_empty() {
        true => OffsetBuffer::new_empty(),
        false => {
            let buffer =
                ScalarBuffer::new(data.buffers()[0].clone(), data.offset(), data.len() + 1);
            // Safety:
            // ArrayData is valid
            unsafe { OffsetBuffer::new_unchecked(buffer) }
        }
    }
}

/// Helper function for printing potentially long arrays.
fn print_long_array<A, F>(array: &A, f: &mut std::fmt::Formatter, print_item: F) -> std::fmt::Result
where
    A: Array,
    F: Fn(&A, usize, &mut std::fmt::Formatter) -> std::fmt::Result,
{
    let head = std::cmp::min(10, array.len());

    for i in 0..head {
        if array.is_null(i) {
            writeln!(f, "  null,")?;
        } else {
            write!(f, "  ")?;
            print_item(array, i, f)?;
            writeln!(f, ",")?;
        }
    }
    if array.len() > 10 {
        if array.len() > 20 {
            writeln!(f, "  ...{} elements...,", array.len() - 20)?;
        }

        let tail = std::cmp::max(head, array.len() - 10);

        for i in tail..array.len() {
            if array.is_null(i) {
                writeln!(f, "  null,")?;
            } else {
                write!(f, "  ")?;
                print_item(array, i, f)?;
                writeln!(f, ",")?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cast::{as_union_array, downcast_array};
    use crate::downcast_run_array;
    use arrow_buffer::MutableBuffer;
    use arrow_schema::{Field, Fields, UnionFields, UnionMode};

    #[test]
    fn test_empty_primitive() {
        let array = new_empty_array(&DataType::Int32);
        let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a.len(), 0);
        let expected: &[i32] = &[];
        assert_eq!(a.values(), expected);
    }

    #[test]
    fn test_empty_variable_sized() {
        let array = new_empty_array(&DataType::Utf8);
        let a = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(a.len(), 0);
        assert_eq!(a.value_offsets()[0], 0i32);
    }

    #[test]
    fn test_empty_list_primitive() {
        let data_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, false)));
        let array = new_empty_array(&data_type);
        let a = array.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(a.len(), 0);
        assert_eq!(a.value_offsets()[0], 0i32);
    }

    #[test]
    fn test_null_boolean() {
        let array = new_null_array(&DataType::Boolean, 9);
        let a = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(a.len(), 9);
        for i in 0..9 {
            assert!(a.is_null(i));
        }
    }

    #[test]
    fn test_null_primitive() {
        let array = new_null_array(&DataType::Int32, 9);
        let a = array.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(a.len(), 9);
        for i in 0..9 {
            assert!(a.is_null(i));
        }
    }

    #[test]
    fn test_null_struct() {
        // It is possible to create a null struct containing a non-nullable child
        // see https://github.com/apache/arrow-rs/pull/3244 for details
        let struct_type = DataType::Struct(vec![Field::new("data", DataType::Int64, false)].into());
        let array = new_null_array(&struct_type, 9);

        let a = array.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(a.len(), 9);
        assert_eq!(a.column(0).len(), 9);
        for i in 0..9 {
            assert!(a.is_null(i));
        }

        // Make sure we can slice the resulting array.
        a.slice(0, 5);
    }

    #[test]
    fn test_null_variable_sized() {
        let array = new_null_array(&DataType::Utf8, 9);
        let a = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(a.len(), 9);
        assert_eq!(a.value_offsets()[9], 0i32);
        for i in 0..9 {
            assert!(a.is_null(i));
        }
    }

    #[test]
    fn test_null_list_primitive() {
        let data_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)));
        let array = new_null_array(&data_type, 9);
        let a = array.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(a.len(), 9);
        assert_eq!(a.value_offsets()[9], 0i32);
        for i in 0..9 {
            assert!(a.is_null(i));
        }
    }

    #[test]
    fn test_null_map() {
        let data_type = DataType::Map(
            Arc::new(Field::new(
                "entry",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int32, true),
                ])),
                false,
            )),
            false,
        );
        let array = new_null_array(&data_type, 9);
        let a = array.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(a.len(), 9);
        assert_eq!(a.value_offsets()[9], 0i32);
        for i in 0..9 {
            assert!(a.is_null(i));
        }
    }

    #[test]
    fn test_null_dictionary() {
        let values =
            vec![None, None, None, None, None, None, None, None, None] as Vec<Option<&str>>;

        let array: DictionaryArray<Int8Type> = values.into_iter().collect();
        let array = Arc::new(array) as ArrayRef;

        let null_array = new_null_array(array.data_type(), 9);
        assert_eq!(&array, &null_array);
        assert_eq!(
            array.to_data().buffers()[0].len(),
            null_array.to_data().buffers()[0].len()
        );
    }

    #[test]
    fn test_null_union() {
        for mode in [UnionMode::Sparse, UnionMode::Dense] {
            let data_type = DataType::Union(
                UnionFields::new(
                    vec![2, 1],
                    vec![
                        Field::new("foo", DataType::Int32, true),
                        Field::new("bar", DataType::Int64, true),
                    ],
                ),
                mode,
            );
            let array = new_null_array(&data_type, 4);

            let array = as_union_array(array.as_ref());
            assert_eq!(array.len(), 4);
            assert_eq!(array.null_count(), 0);
            assert_eq!(array.logical_null_count(), 4);

            for i in 0..4 {
                let a = array.value(i);
                assert_eq!(a.len(), 1);
                assert_eq!(a.null_count(), 1);
                assert_eq!(a.logical_null_count(), 1);
                assert!(a.is_null(0))
            }

            array.to_data().validate_full().unwrap();
        }
    }

    #[test]
    #[allow(unused_parens)]
    fn test_null_runs() {
        for r in [DataType::Int16, DataType::Int32, DataType::Int64] {
            let data_type = DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", r, false)),
                Arc::new(Field::new("values", DataType::Utf8, true)),
            );

            let array = new_null_array(&data_type, 4);
            let array = array.as_ref();

            downcast_run_array! {
                array => {
                    assert_eq!(array.len(), 4);
                    assert_eq!(array.null_count(), 0);
                    assert_eq!(array.logical_null_count(), 4);
                    assert_eq!(array.values().len(), 1);
                    assert_eq!(array.values().null_count(), 1);
                    assert_eq!(array.run_ends().len(), 4);
                    assert_eq!(array.run_ends().values(), &[4]);

                    let idx = array.get_physical_indices(&[0, 1, 2, 3]).unwrap();
                    assert_eq!(idx, &[0,0,0,0]);
                }
                d => unreachable!("{d}")
            }
        }
    }

    #[test]
    fn test_null_fixed_size_binary() {
        for size in [1, 2, 7] {
            let array = new_null_array(&DataType::FixedSizeBinary(size), 6);
            let array = array
                .as_ref()
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap();

            assert_eq!(array.len(), 6);
            assert_eq!(array.null_count(), 6);
            assert_eq!(array.logical_null_count(), 6);
            array.iter().for_each(|x| assert!(x.is_none()));
        }
    }

    #[test]
    fn test_memory_size_null() {
        let null_arr = NullArray::new(32);

        assert_eq!(0, null_arr.get_buffer_memory_size());
        assert_eq!(
            std::mem::size_of::<usize>(),
            null_arr.get_array_memory_size()
        );
    }

    #[test]
    fn test_memory_size_primitive() {
        let arr = PrimitiveArray::<Int64Type>::from_iter_values(0..128);
        let empty = PrimitiveArray::<Int64Type>::from(ArrayData::new_empty(arr.data_type()));

        // subtract empty array to avoid magic numbers for the size of additional fields
        assert_eq!(
            arr.get_array_memory_size() - empty.get_array_memory_size(),
            128 * std::mem::size_of::<i64>()
        );
    }

    #[test]
    fn test_memory_size_primitive_sliced() {
        let arr = PrimitiveArray::<Int64Type>::from_iter_values(0..128);
        let slice1 = arr.slice(0, 64);
        let slice2 = arr.slice(64, 64);

        // both slices report the full buffer memory usage, even though the buffers are shared
        assert_eq!(slice1.get_array_memory_size(), arr.get_array_memory_size());
        assert_eq!(slice2.get_array_memory_size(), arr.get_array_memory_size());
    }

    #[test]
    fn test_memory_size_primitive_nullable() {
        let arr: PrimitiveArray<Int64Type> = (0..128)
            .map(|i| if i % 20 == 0 { Some(i) } else { None })
            .collect();
        let empty_with_bitmap = PrimitiveArray::<Int64Type>::from(
            ArrayData::builder(arr.data_type().clone())
                .add_buffer(MutableBuffer::new(0).into())
                .null_bit_buffer(Some(MutableBuffer::new_null(0).into()))
                .build()
                .unwrap(),
        );

        // expected size is the size of the PrimitiveArray struct,
        // which includes the optional validity buffer
        // plus one buffer on the heap
        assert_eq!(
            std::mem::size_of::<PrimitiveArray<Int64Type>>(),
            empty_with_bitmap.get_array_memory_size()
        );

        // subtract empty array to avoid magic numbers for the size of additional fields
        // the size of the validity bitmap is rounded up to 64 bytes
        assert_eq!(
            arr.get_array_memory_size() - empty_with_bitmap.get_array_memory_size(),
            128 * std::mem::size_of::<i64>() + 64
        );
    }

    #[test]
    fn test_memory_size_dictionary() {
        let values = PrimitiveArray::<Int64Type>::from_iter_values(0..16);
        let keys = PrimitiveArray::<Int16Type>::from_iter_values(
            (0..256).map(|i| (i % values.len()) as i16),
        );

        let dict_data_type = DataType::Dictionary(
            Box::new(keys.data_type().clone()),
            Box::new(values.data_type().clone()),
        );
        let dict_data = keys
            .into_data()
            .into_builder()
            .data_type(dict_data_type)
            .child_data(vec![values.into_data()])
            .build()
            .unwrap();

        let empty_data = ArrayData::new_empty(&DataType::Dictionary(
            Box::new(DataType::Int16),
            Box::new(DataType::Int64),
        ));

        let arr = DictionaryArray::<Int16Type>::from(dict_data);
        let empty = DictionaryArray::<Int16Type>::from(empty_data);

        let expected_keys_size = 256 * std::mem::size_of::<i16>();
        assert_eq!(
            arr.keys().get_array_memory_size() - empty.keys().get_array_memory_size(),
            expected_keys_size
        );

        let expected_values_size = 16 * std::mem::size_of::<i64>();
        assert_eq!(
            arr.values().get_array_memory_size() - empty.values().get_array_memory_size(),
            expected_values_size
        );

        let expected_size = expected_keys_size + expected_values_size;
        assert_eq!(
            arr.get_array_memory_size() - empty.get_array_memory_size(),
            expected_size
        );
    }

    /// Test function that takes an &dyn Array
    fn compute_my_thing(arr: &dyn Array) -> bool {
        !arr.is_empty()
    }

    #[test]
    fn test_array_ref_as_array() {
        let arr: Int32Array = vec![1, 2, 3].into_iter().map(Some).collect();

        // works well!
        assert!(compute_my_thing(&arr));

        // Should also work when wrapped as an ArrayRef
        let arr: ArrayRef = Arc::new(arr);
        assert!(compute_my_thing(&arr));
        assert!(compute_my_thing(arr.as_ref()));
    }

    #[test]
    fn test_downcast_array() {
        let array: Int32Array = vec![1, 2, 3].into_iter().map(Some).collect();

        let boxed: ArrayRef = Arc::new(array);
        let array: Int32Array = downcast_array(&boxed);

        let expected: Int32Array = vec![1, 2, 3].into_iter().map(Some).collect();
        assert_eq!(array, expected);
    }
}
