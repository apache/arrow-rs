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
use arrow_buffer::{Buffer, MutableBuffer, ToByteSlice};
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

/// Trait for dealing with different types of array at runtime when the type of the
/// array is not known in advance.
pub trait Array: std::fmt::Debug + Send + Sync {
    /// Returns the array as [`Any`](std::any::Any) so that it can be
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

    /// Returns a reference to the underlying data of this array.
    fn data(&self) -> &ArrayData;

    /// Returns the underlying data of this array.
    fn into_data(self) -> ArrayData;

    /// Returns a reference-counted pointer to the underlying data of this array.
    fn data_ref(&self) -> &ArrayData {
        self.data()
    }

    /// Returns a reference to the [`DataType`](arrow_schema::DataType) of this array.
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
    fn data_type(&self) -> &DataType {
        self.data_ref().data_type()
    }

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
    /// assert_eq!(array_slice.as_ref(), &Int32Array::from(vec![2, 3, 4]));
    /// ```
    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        make_array(self.data_ref().slice(offset, length))
    }

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
    fn len(&self) -> usize {
        self.data_ref().len()
    }

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
    fn is_empty(&self) -> bool {
        self.data_ref().is_empty()
    }

    /// Returns the offset into the underlying data used by this array(-slice).
    /// Note that the underlying data can be shared by many arrays.
    /// This defaults to `0`.
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
    /// assert_eq!(array.offset(), 0);
    /// assert_eq!(array_slice.offset(), 1);
    /// ```
    fn offset(&self) -> usize {
        self.data_ref().offset()
    }

    /// Returns whether the element at `index` is null.
    /// When using this function on a slice, the index is relative to the slice.
    ///
    /// # Example:
    ///
    /// ```
    /// use arrow_array::{Array, Int32Array};
    ///
    /// let array = Int32Array::from(vec![Some(1), None]);
    ///
    /// assert_eq!(array.is_null(0), false);
    /// assert_eq!(array.is_null(1), true);
    /// ```
    fn is_null(&self, index: usize) -> bool {
        self.data_ref().is_null(index)
    }

    /// Returns whether the element at `index` is not null.
    /// When using this function on a slice, the index is relative to the slice.
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
        self.data_ref().is_valid(index)
    }

    /// Returns the total number of null values in this array.
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
        self.data_ref().null_count()
    }

    /// Returns the total number of bytes of memory pointed to by this array.
    /// The buffers store bytes in the Arrow memory format, and include the data as well as the validity map.
    fn get_buffer_memory_size(&self) -> usize {
        self.data_ref().get_buffer_memory_size()
    }

    /// Returns the total number of bytes of memory occupied physically by this array.
    /// This value will always be greater than returned by `get_buffer_memory_size()` and
    /// includes the overhead of the data structures that contain the pointers to the various buffers.
    fn get_array_memory_size(&self) -> usize {
        // both data.get_array_memory_size and size_of_val(self) include ArrayData fields,
        // to only count additional fields of this array substract size_of(ArrayData)
        self.data_ref().get_array_memory_size() + std::mem::size_of_val(self)
            - std::mem::size_of::<ArrayData>()
    }
}

/// A reference-counted reference to a generic `Array`.
pub type ArrayRef = Arc<dyn Array>;

/// Ergonomics: Allow use of an ArrayRef as an `&dyn Array`
impl Array for ArrayRef {
    fn as_any(&self) -> &dyn Any {
        self.as_ref().as_any()
    }

    fn data(&self) -> &ArrayData {
        self.as_ref().data()
    }

    fn into_data(self) -> ArrayData {
        self.data().clone()
    }

    fn data_ref(&self) -> &ArrayData {
        self.as_ref().data_ref()
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

    fn offset(&self) -> usize {
        self.as_ref().offset()
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

    fn get_buffer_memory_size(&self) -> usize {
        self.as_ref().get_buffer_memory_size()
    }

    fn get_array_memory_size(&self) -> usize {
        self.as_ref().get_array_memory_size()
    }
}

impl<'a, T: Array> Array for &'a T {
    fn as_any(&self) -> &dyn Any {
        T::as_any(self)
    }

    fn data(&self) -> &ArrayData {
        T::data(self)
    }

    fn into_data(self) -> ArrayData {
        self.data().clone()
    }

    fn data_ref(&self) -> &ArrayData {
        T::data_ref(self)
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

    fn is_null(&self, index: usize) -> bool {
        T::is_null(self, index)
    }

    fn is_valid(&self, index: usize) -> bool {
        T::is_valid(self, index)
    }

    fn null_count(&self) -> usize {
        T::null_count(self)
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
/// # Validity
///
/// An [`ArrayAccessor`] must always return a well-defined value for an index that is
/// within the bounds `0..Array::len`, including for null indexes where [`Array::is_null`] is true.
///
/// The value at null indexes is unspecified, and implementations must not rely on a specific
/// value such as [`Default::default`] being returned, however, it must not be undefined
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

impl PartialEq for dyn Array {
    fn eq(&self, other: &Self) -> bool {
        self.data().eq(other.data())
    }
}

impl<T: Array> PartialEq<T> for dyn Array {
    fn eq(&self, other: &T) -> bool {
        self.data().eq(other.data())
    }
}

impl PartialEq for NullArray {
    fn eq(&self, other: &NullArray) -> bool {
        self.data().eq(other.data())
    }
}

impl<T: ArrowPrimitiveType> PartialEq for PrimitiveArray<T> {
    fn eq(&self, other: &PrimitiveArray<T>) -> bool {
        self.data().eq(other.data())
    }
}

impl<K: ArrowPrimitiveType> PartialEq for DictionaryArray<K> {
    fn eq(&self, other: &Self) -> bool {
        self.data().eq(other.data())
    }
}

impl PartialEq for BooleanArray {
    fn eq(&self, other: &BooleanArray) -> bool {
        self.data().eq(other.data())
    }
}

impl<OffsetSize: OffsetSizeTrait> PartialEq for GenericStringArray<OffsetSize> {
    fn eq(&self, other: &Self) -> bool {
        self.data().eq(other.data())
    }
}

impl<OffsetSize: OffsetSizeTrait> PartialEq for GenericBinaryArray<OffsetSize> {
    fn eq(&self, other: &Self) -> bool {
        self.data().eq(other.data())
    }
}

impl PartialEq for FixedSizeBinaryArray {
    fn eq(&self, other: &Self) -> bool {
        self.data().eq(other.data())
    }
}

impl<OffsetSize: OffsetSizeTrait> PartialEq for GenericListArray<OffsetSize> {
    fn eq(&self, other: &Self) -> bool {
        self.data().eq(other.data())
    }
}

impl PartialEq for MapArray {
    fn eq(&self, other: &Self) -> bool {
        self.data().eq(other.data())
    }
}

impl PartialEq for FixedSizeListArray {
    fn eq(&self, other: &Self) -> bool {
        self.data().eq(other.data())
    }
}

impl PartialEq for StructArray {
    fn eq(&self, other: &Self) -> bool {
        self.data().eq(other.data())
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
        DataType::Time32(TimeUnit::Second) => {
            Arc::new(Time32SecondArray::from(data)) as ArrayRef
        }
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
        DataType::FixedSizeBinary(_) => {
            Arc::new(FixedSizeBinaryArray::from(data)) as ArrayRef
        }
        DataType::Utf8 => Arc::new(StringArray::from(data)) as ArrayRef,
        DataType::LargeUtf8 => Arc::new(LargeStringArray::from(data)) as ArrayRef,
        DataType::List(_) => Arc::new(ListArray::from(data)) as ArrayRef,
        DataType::LargeList(_) => Arc::new(LargeListArray::from(data)) as ArrayRef,
        DataType::Struct(_) => Arc::new(StructArray::from(data)) as ArrayRef,
        DataType::Map(_, _) => Arc::new(MapArray::from(data)) as ArrayRef,
        DataType::Union(_, _, _) => Arc::new(UnionArray::from(data)) as ArrayRef,
        DataType::FixedSizeList(_, _) => {
            Arc::new(FixedSizeListArray::from(data)) as ArrayRef
        }
        DataType::Dictionary(ref key_type, _) => match key_type.as_ref() {
            DataType::Int8 => {
                Arc::new(DictionaryArray::<Int8Type>::from(data)) as ArrayRef
            }
            DataType::Int16 => {
                Arc::new(DictionaryArray::<Int16Type>::from(data)) as ArrayRef
            }
            DataType::Int32 => {
                Arc::new(DictionaryArray::<Int32Type>::from(data)) as ArrayRef
            }
            DataType::Int64 => {
                Arc::new(DictionaryArray::<Int64Type>::from(data)) as ArrayRef
            }
            DataType::UInt8 => {
                Arc::new(DictionaryArray::<UInt8Type>::from(data)) as ArrayRef
            }
            DataType::UInt16 => {
                Arc::new(DictionaryArray::<UInt16Type>::from(data)) as ArrayRef
            }
            DataType::UInt32 => {
                Arc::new(DictionaryArray::<UInt32Type>::from(data)) as ArrayRef
            }
            DataType::UInt64 => {
                Arc::new(DictionaryArray::<UInt64Type>::from(data)) as ArrayRef
            }
            dt => panic!("Unexpected dictionary key type {:?}", dt),
        },
        DataType::Null => Arc::new(NullArray::from(data)) as ArrayRef,
        DataType::Decimal128(_, _) => Arc::new(Decimal128Array::from(data)) as ArrayRef,
        DataType::Decimal256(_, _) => Arc::new(Decimal256Array::from(data)) as ArrayRef,
        dt => panic!("Unexpected data type {:?}", dt),
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
    // context: https://github.com/apache/arrow/pull/9469#discussion_r574761687
    match data_type {
        DataType::Null => Arc::new(NullArray::new(length)),
        DataType::Boolean => {
            let null_buf: Buffer = MutableBuffer::new_null(length).into();
            make_array(unsafe {
                ArrayData::new_unchecked(
                    data_type.clone(),
                    length,
                    Some(length),
                    Some(null_buf.clone()),
                    0,
                    vec![null_buf],
                    vec![],
                )
            })
        }
        DataType::Int8 => new_null_sized_array::<Int8Type>(data_type, length),
        DataType::UInt8 => new_null_sized_array::<UInt8Type>(data_type, length),
        DataType::Int16 => new_null_sized_array::<Int16Type>(data_type, length),
        DataType::UInt16 => new_null_sized_array::<UInt16Type>(data_type, length),
        DataType::Float16 => new_null_sized_array::<Float16Type>(data_type, length),
        DataType::Int32 => new_null_sized_array::<Int32Type>(data_type, length),
        DataType::UInt32 => new_null_sized_array::<UInt32Type>(data_type, length),
        DataType::Float32 => new_null_sized_array::<Float32Type>(data_type, length),
        DataType::Date32 => new_null_sized_array::<Date32Type>(data_type, length),
        // expanding this into Date23{unit}Type results in needless branching
        DataType::Time32(_) => new_null_sized_array::<Int32Type>(data_type, length),
        DataType::Int64 => new_null_sized_array::<Int64Type>(data_type, length),
        DataType::UInt64 => new_null_sized_array::<UInt64Type>(data_type, length),
        DataType::Float64 => new_null_sized_array::<Float64Type>(data_type, length),
        DataType::Date64 => new_null_sized_array::<Date64Type>(data_type, length),
        // expanding this into Timestamp{unit}Type results in needless branching
        DataType::Timestamp(_, _) => new_null_sized_array::<Int64Type>(data_type, length),
        DataType::Time64(_) => new_null_sized_array::<Int64Type>(data_type, length),
        DataType::Duration(_) => new_null_sized_array::<Int64Type>(data_type, length),
        DataType::Interval(unit) => match unit {
            IntervalUnit::YearMonth => {
                new_null_sized_array::<IntervalYearMonthType>(data_type, length)
            }
            IntervalUnit::DayTime => {
                new_null_sized_array::<IntervalDayTimeType>(data_type, length)
            }
            IntervalUnit::MonthDayNano => {
                new_null_sized_array::<IntervalMonthDayNanoType>(data_type, length)
            }
        },
        DataType::FixedSizeBinary(value_len) => make_array(unsafe {
            ArrayData::new_unchecked(
                data_type.clone(),
                length,
                Some(length),
                Some(MutableBuffer::new_null(length).into()),
                0,
                vec![Buffer::from(vec![0u8; *value_len as usize * length])],
                vec![],
            )
        }),
        DataType::Binary | DataType::Utf8 => {
            new_null_binary_array::<i32>(data_type, length)
        }
        DataType::LargeBinary | DataType::LargeUtf8 => {
            new_null_binary_array::<i64>(data_type, length)
        }
        DataType::List(field) => {
            new_null_list_array::<i32>(data_type, field.data_type(), length)
        }
        DataType::LargeList(field) => {
            new_null_list_array::<i64>(data_type, field.data_type(), length)
        }
        DataType::FixedSizeList(field, value_len) => make_array(unsafe {
            ArrayData::new_unchecked(
                data_type.clone(),
                length,
                Some(length),
                Some(MutableBuffer::new_null(length).into()),
                0,
                vec![],
                vec![
                    new_null_array(field.data_type(), *value_len as usize * length)
                        .data()
                        .clone(),
                ],
            )
        }),
        DataType::Struct(fields) => {
            let fields: Vec<_> = fields
                .iter()
                .map(|field| (field.clone(), new_null_array(field.data_type(), length)))
                .collect();

            let null_buffer = MutableBuffer::new_null(length);
            Arc::new(StructArray::from((fields, null_buffer.into())))
        }
        DataType::Map(field, _keys_sorted) => {
            new_null_list_array::<i32>(data_type, field.data_type(), length)
        }
        DataType::Union(_, _, _) => {
            unimplemented!("Creating null Union array not yet supported")
        }
        DataType::Dictionary(key, value) => {
            let keys = new_null_array(key, length);
            let keys = keys.data();

            make_array(unsafe {
                ArrayData::new_unchecked(
                    data_type.clone(),
                    length,
                    Some(length),
                    keys.null_buffer().cloned(),
                    0,
                    keys.buffers().into(),
                    vec![new_empty_array(value.as_ref()).into_data()],
                )
            })
        }
        DataType::Decimal128(_, _) => {
            new_null_sized_decimal(data_type, length, std::mem::size_of::<i128>())
        }
        DataType::Decimal256(_, _) => new_null_sized_decimal(data_type, length, 32),
    }
}

#[inline]
fn new_null_list_array<OffsetSize: OffsetSizeTrait>(
    data_type: &DataType,
    child_data_type: &DataType,
    length: usize,
) -> ArrayRef {
    make_array(unsafe {
        ArrayData::new_unchecked(
            data_type.clone(),
            length,
            Some(length),
            Some(MutableBuffer::new_null(length).into()),
            0,
            vec![Buffer::from(
                vec![OffsetSize::zero(); length + 1].to_byte_slice(),
            )],
            vec![ArrayData::new_empty(child_data_type)],
        )
    })
}

#[inline]
fn new_null_binary_array<OffsetSize: OffsetSizeTrait>(
    data_type: &DataType,
    length: usize,
) -> ArrayRef {
    make_array(unsafe {
        ArrayData::new_unchecked(
            data_type.clone(),
            length,
            Some(length),
            Some(MutableBuffer::new_null(length).into()),
            0,
            vec![
                Buffer::from(vec![OffsetSize::zero(); length + 1].to_byte_slice()),
                MutableBuffer::new(0).into(),
            ],
            vec![],
        )
    })
}

#[inline]
fn new_null_sized_array<T: ArrowPrimitiveType>(
    data_type: &DataType,
    length: usize,
) -> ArrayRef {
    make_array(unsafe {
        ArrayData::new_unchecked(
            data_type.clone(),
            length,
            Some(length),
            Some(MutableBuffer::new_null(length).into()),
            0,
            vec![Buffer::from(vec![0u8; length * T::get_byte_width()])],
            vec![],
        )
    })
}

#[inline]
fn new_null_sized_decimal(
    data_type: &DataType,
    length: usize,
    byte_width: usize,
) -> ArrayRef {
    make_array(unsafe {
        ArrayData::new_unchecked(
            data_type.clone(),
            length,
            Some(length),
            Some(MutableBuffer::new_null(length).into()),
            0,
            vec![Buffer::from(vec![0u8; length * byte_width])],
            vec![],
        )
    })
}

// Helper function for printing potentially long arrays.
pub(crate) fn print_long_array<A, F>(
    array: &A,
    f: &mut std::fmt::Formatter,
    print_item: F,
) -> std::fmt::Result
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
    use crate::cast::downcast_array;
    use arrow_schema::Field;

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
        let data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, false)));
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
        let struct_type =
            DataType::Struct(vec![Field::new("data", DataType::Int64, false)]);
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
        let data_type =
            DataType::List(Box::new(Field::new("item", DataType::Int32, true)));
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
            Box::new(Field::new(
                "entry",
                DataType::Struct(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int32, true),
                ]),
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
        let values = vec![None, None, None, None, None, None, None, None, None]
            as Vec<Option<&str>>;

        let array: DictionaryArray<Int8Type> = values.into_iter().collect();
        let array = Arc::new(array) as ArrayRef;

        let null_array = new_null_array(array.data_type(), 9);
        assert_eq!(&array, &null_array);
        assert_eq!(
            array.data().buffers()[0].len(),
            null_array.data().buffers()[0].len()
        );
    }

    #[test]
    fn test_memory_size_null() {
        let null_arr = NullArray::new(32);

        assert_eq!(0, null_arr.get_buffer_memory_size());
        assert_eq!(
            std::mem::size_of::<NullArray>(),
            null_arr.get_array_memory_size()
        );
        assert_eq!(
            std::mem::size_of::<NullArray>(),
            std::mem::size_of::<ArrayData>(),
        );
    }

    #[test]
    fn test_memory_size_primitive() {
        let arr = PrimitiveArray::<Int64Type>::from_iter_values(0..128);
        let empty =
            PrimitiveArray::<Int64Type>::from(ArrayData::new_empty(arr.data_type()));

        // substract empty array to avoid magic numbers for the size of additional fields
        assert_eq!(
            arr.get_array_memory_size() - empty.get_array_memory_size(),
            128 * std::mem::size_of::<i64>()
        );
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
            std::mem::size_of::<PrimitiveArray<Int64Type>>()
                + std::mem::size_of::<Buffer>(),
            empty_with_bitmap.get_array_memory_size()
        );

        // substract empty array to avoid magic numbers for the size of additional fields
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

        let dict_data = ArrayData::builder(DataType::Dictionary(
            Box::new(keys.data_type().clone()),
            Box::new(values.data_type().clone()),
        ))
        .len(keys.len())
        .buffers(keys.data_ref().buffers().to_vec())
        .child_data(vec![ArrayData::builder(DataType::Int64)
            .len(values.len())
            .buffers(values.data_ref().buffers().to_vec())
            .build()
            .unwrap()])
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
