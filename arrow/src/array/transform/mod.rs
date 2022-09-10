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

use super::{
    data::{into_buffers, new_buffers},
    ArrayData, ArrayDataBuilder, OffsetSizeTrait,
};
use crate::{
    buffer::MutableBuffer,
    datatypes::DataType,
    error::{ArrowError, Result},
    util::bit_util,
};
use half::f16;
use std::mem;

mod boolean;
mod fixed_binary;
mod fixed_size_list;
mod list;
mod null;
mod primitive;
mod structure;
mod union;
mod utils;
mod variable_size;

type ExtendNullBits<'a> = Box<dyn Fn(&mut _MutableArrayData, usize, usize) + 'a>;
// function that extends `[start..start+len]` to the mutable array.
// this is dynamic because different data_types influence how buffers and children are extended.
type Extend<'a> = Box<dyn Fn(&mut _MutableArrayData, usize, usize, usize) + 'a>;

type ExtendNulls = Box<dyn Fn(&mut _MutableArrayData, usize)>;

/// A mutable [ArrayData] that knows how to freeze itself into an [ArrayData].
/// This is just a data container.
#[derive(Debug)]
struct _MutableArrayData<'a> {
    pub data_type: DataType,
    pub null_count: usize,

    pub len: usize,
    pub null_buffer: MutableBuffer,

    // arrow specification only allows up to 3 buffers (2 ignoring the nulls above).
    // Thus, we place them in the stack to avoid bound checks and greater data locality.
    pub buffer1: MutableBuffer,
    pub buffer2: MutableBuffer,
    pub child_data: Vec<MutableArrayData<'a>>,
}

impl<'a> _MutableArrayData<'a> {
    fn freeze(self, dictionary: Option<ArrayData>) -> ArrayDataBuilder {
        let buffers = into_buffers(&self.data_type, self.buffer1, self.buffer2);

        let child_data = match self.data_type {
            DataType::Dictionary(_, _) => vec![dictionary.unwrap()],
            _ => {
                let mut child_data = Vec::with_capacity(self.child_data.len());
                for child in self.child_data {
                    child_data.push(child.freeze());
                }
                child_data
            }
        };

        ArrayDataBuilder::new(self.data_type)
            .offset(0)
            .len(self.len)
            .null_count(self.null_count)
            .buffers(buffers)
            .child_data(child_data)
            .null_bit_buffer((self.null_count > 0).then(|| self.null_buffer.into()))
    }
}

fn build_extend_null_bits(array: &ArrayData, use_nulls: bool) -> ExtendNullBits {
    if let Some(bitmap) = array.null_bitmap() {
        let bytes = bitmap.bits.as_slice();
        Box::new(move |mutable, start, len| {
            utils::resize_for_bits(&mut mutable.null_buffer, mutable.len + len);
            mutable.null_count += crate::util::bit_mask::set_bits(
                mutable.null_buffer.as_slice_mut(),
                bytes,
                mutable.len,
                array.offset() + start,
                len,
            );
        })
    } else if use_nulls {
        Box::new(|mutable, _, len| {
            utils::resize_for_bits(&mut mutable.null_buffer, mutable.len + len);
            let write_data = mutable.null_buffer.as_slice_mut();
            let offset = mutable.len;
            (0..len).for_each(|i| {
                bit_util::set_bit(write_data, offset + i);
            });
        })
    } else {
        Box::new(|_, _, _| {})
    }
}

/// Struct to efficiently and interactively create an [ArrayData] from an existing [ArrayData] by
/// copying chunks.
/// The main use case of this struct is to perform unary operations to arrays of arbitrary types, such as `filter` and `take`.
/// # Example:
///
/// ```
/// use arrow::{array::{Int32Array, Array, MutableArrayData}};
///
/// let array = Int32Array::from(vec![1, 2, 3, 4, 5]);
/// let array = array.data();
/// // Create a new `MutableArrayData` from an array and with a capacity of 4.
/// // Capacity here is equivalent to `Vec::with_capacity`
/// let arrays = vec![array];
/// let mut mutable = MutableArrayData::new(arrays, false, 4);
/// mutable.extend(0, 1, 3); // extend from the slice [1..3], [2,3]
/// mutable.extend(0, 0, 3); // extend from the slice [0..3], [1,2,3]
/// // `.freeze()` to convert `MutableArrayData` into a `ArrayData`.
/// let new_array = Int32Array::from(mutable.freeze());
/// assert_eq!(Int32Array::from(vec![2, 3, 1, 2, 3]), new_array);
/// ```
pub struct MutableArrayData<'a> {
    #[allow(dead_code)]
    arrays: Vec<&'a ArrayData>,
    // The attributes in [_MutableArrayData] cannot be in [MutableArrayData] due to
    // mutability invariants (interior mutability):
    // [MutableArrayData] contains a function that can only mutate [_MutableArrayData], not
    // [MutableArrayData] itself
    data: _MutableArrayData<'a>,

    // the child data of the `Array` in Dictionary arrays.
    // This is not stored in `MutableArrayData` because these values constant and only needed
    // at the end, when freezing [_MutableArrayData].
    dictionary: Option<ArrayData>,

    // function used to extend values from arrays. This function's lifetime is bound to the array
    // because it reads values from it.
    extend_values: Vec<Extend<'a>>,
    // function used to extend nulls from arrays. This function's lifetime is bound to the array
    // because it reads nulls from it.
    extend_null_bits: Vec<ExtendNullBits<'a>>,

    // function used to extend nulls.
    // this is independent of the arrays and therefore has no lifetime.
    extend_nulls: ExtendNulls,
}

impl<'a> std::fmt::Debug for MutableArrayData<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // ignores the closures.
        f.debug_struct("MutableArrayData")
            .field("data", &self.data)
            .finish()
    }
}

/// Builds an extend that adds `offset` to the source primitive
/// Additionally validates that `max` fits into the
/// the underlying primitive returning None if not
fn build_extend_dictionary(
    array: &ArrayData,
    offset: usize,
    max: usize,
) -> Option<Extend> {
    use crate::datatypes::*;
    macro_rules! validate_and_build {
        ($dt: ty) => {{
            let _: $dt = max.try_into().ok()?;
            let offset: $dt = offset.try_into().ok()?;
            Some(primitive::build_extend_with_offset(array, offset))
        }};
    }
    match array.data_type() {
        DataType::Dictionary(child_data_type, _) => match child_data_type.as_ref() {
            DataType::UInt8 => validate_and_build!(u8),
            DataType::UInt16 => validate_and_build!(u16),
            DataType::UInt32 => validate_and_build!(u32),
            DataType::UInt64 => validate_and_build!(u64),
            DataType::Int8 => validate_and_build!(i8),
            DataType::Int16 => validate_and_build!(i16),
            DataType::Int32 => validate_and_build!(i32),
            DataType::Int64 => validate_and_build!(i64),
            _ => unreachable!(),
        },
        _ => None,
    }
}

fn build_extend(array: &ArrayData) -> Extend {
    use crate::datatypes::*;
    match array.data_type() {
        DataType::Decimal128(_, _) => primitive::build_extend::<i128>(array),
        DataType::Null => null::build_extend(array),
        DataType::Boolean => boolean::build_extend(array),
        DataType::UInt8 => primitive::build_extend::<u8>(array),
        DataType::UInt16 => primitive::build_extend::<u16>(array),
        DataType::UInt32 => primitive::build_extend::<u32>(array),
        DataType::UInt64 => primitive::build_extend::<u64>(array),
        DataType::Int8 => primitive::build_extend::<i8>(array),
        DataType::Int16 => primitive::build_extend::<i16>(array),
        DataType::Int32 => primitive::build_extend::<i32>(array),
        DataType::Int64 => primitive::build_extend::<i64>(array),
        DataType::Float32 => primitive::build_extend::<f32>(array),
        DataType::Float64 => primitive::build_extend::<f64>(array),
        DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => {
            primitive::build_extend::<i32>(array)
        }
        DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(IntervalUnit::DayTime) => {
            primitive::build_extend::<i64>(array)
        }
        DataType::Interval(IntervalUnit::MonthDayNano) => {
            primitive::build_extend::<i128>(array)
        }
        DataType::Utf8 | DataType::Binary => variable_size::build_extend::<i32>(array),
        DataType::LargeUtf8 | DataType::LargeBinary => {
            variable_size::build_extend::<i64>(array)
        }
        DataType::Map(_, _) | DataType::List(_) => list::build_extend::<i32>(array),
        DataType::LargeList(_) => list::build_extend::<i64>(array),
        DataType::Dictionary(_, _) => unreachable!("should use build_extend_dictionary"),
        DataType::Struct(_) => structure::build_extend(array),
        DataType::FixedSizeBinary(_) | DataType::Decimal256(_, _) => {
            fixed_binary::build_extend(array)
        }
        DataType::Float16 => primitive::build_extend::<f16>(array),
        DataType::FixedSizeList(_, _) => fixed_size_list::build_extend(array),
        DataType::Union(_, _, mode) => match mode {
            UnionMode::Sparse => union::build_extend_sparse(array),
            UnionMode::Dense => union::build_extend_dense(array),
        },
    }
}

fn build_extend_nulls(data_type: &DataType) -> ExtendNulls {
    use crate::datatypes::*;
    Box::new(match data_type {
        DataType::Decimal128(_, _) => primitive::extend_nulls::<i128>,
        DataType::Null => null::extend_nulls,
        DataType::Boolean => boolean::extend_nulls,
        DataType::UInt8 => primitive::extend_nulls::<u8>,
        DataType::UInt16 => primitive::extend_nulls::<u16>,
        DataType::UInt32 => primitive::extend_nulls::<u32>,
        DataType::UInt64 => primitive::extend_nulls::<u64>,
        DataType::Int8 => primitive::extend_nulls::<i8>,
        DataType::Int16 => primitive::extend_nulls::<i16>,
        DataType::Int32 => primitive::extend_nulls::<i32>,
        DataType::Int64 => primitive::extend_nulls::<i64>,
        DataType::Float32 => primitive::extend_nulls::<f32>,
        DataType::Float64 => primitive::extend_nulls::<f64>,
        DataType::Date32
        | DataType::Time32(_)
        | DataType::Interval(IntervalUnit::YearMonth) => primitive::extend_nulls::<i32>,
        DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(IntervalUnit::DayTime) => primitive::extend_nulls::<i64>,
        DataType::Interval(IntervalUnit::MonthDayNano) => primitive::extend_nulls::<i128>,
        DataType::Utf8 | DataType::Binary => variable_size::extend_nulls::<i32>,
        DataType::LargeUtf8 | DataType::LargeBinary => variable_size::extend_nulls::<i64>,
        DataType::Map(_, _) | DataType::List(_) => list::extend_nulls::<i32>,
        DataType::LargeList(_) => list::extend_nulls::<i64>,
        DataType::Dictionary(child_data_type, _) => match child_data_type.as_ref() {
            DataType::UInt8 => primitive::extend_nulls::<u8>,
            DataType::UInt16 => primitive::extend_nulls::<u16>,
            DataType::UInt32 => primitive::extend_nulls::<u32>,
            DataType::UInt64 => primitive::extend_nulls::<u64>,
            DataType::Int8 => primitive::extend_nulls::<i8>,
            DataType::Int16 => primitive::extend_nulls::<i16>,
            DataType::Int32 => primitive::extend_nulls::<i32>,
            DataType::Int64 => primitive::extend_nulls::<i64>,
            _ => unreachable!(),
        },
        DataType::Struct(_) => structure::extend_nulls,
        DataType::FixedSizeBinary(_) | DataType::Decimal256(_, _) => {
            fixed_binary::extend_nulls
        }
        DataType::Float16 => primitive::extend_nulls::<f16>,
        DataType::FixedSizeList(_, _) => fixed_size_list::extend_nulls,
        DataType::Union(_, _, mode) => match mode {
            UnionMode::Sparse => union::extend_nulls_sparse,
            UnionMode::Dense => union::extend_nulls_dense,
        },
    })
}

fn preallocate_offset_and_binary_buffer<Offset: OffsetSizeTrait>(
    capacity: usize,
    binary_size: usize,
) -> [MutableBuffer; 2] {
    // offsets
    let mut buffer = MutableBuffer::new((1 + capacity) * mem::size_of::<Offset>());
    // safety: `unsafe` code assumes that this buffer is initialized with one element
    buffer.push(Offset::zero());

    [
        buffer,
        MutableBuffer::new(binary_size * mem::size_of::<u8>()),
    ]
}

/// Define capacities of child data or data buffers.
#[derive(Debug, Clone)]
pub enum Capacities {
    /// Binary, Utf8 and LargeUtf8 data types
    /// Define
    /// * the capacity of the array offsets
    /// * the capacity of the binary/ str buffer
    Binary(usize, Option<usize>),
    /// List and LargeList data types
    /// Define
    /// * the capacity of the array offsets
    /// * the capacity of the child data
    List(usize, Option<Box<Capacities>>),
    /// Struct type
    /// * the capacity of the array
    /// * the capacities of the fields
    Struct(usize, Option<Vec<Capacities>>),
    /// Dictionary type
    /// * the capacity of the array/keys
    /// * the capacity of the values
    Dictionary(usize, Option<Box<Capacities>>),
    /// Don't preallocate inner buffers and rely on array growth strategy
    Array(usize),
}
impl<'a> MutableArrayData<'a> {
    /// returns a new [MutableArrayData] with capacity to `capacity` slots and specialized to create an
    /// [ArrayData] from multiple `arrays`.
    ///
    /// `use_nulls` is a flag used to optimize insertions. It should be `false` if the only source of nulls
    /// are the arrays themselves and `true` if the user plans to call [MutableArrayData::extend_nulls].
    /// In other words, if `use_nulls` is `false`, calling [MutableArrayData::extend_nulls] should not be used.
    pub fn new(arrays: Vec<&'a ArrayData>, use_nulls: bool, capacity: usize) -> Self {
        Self::with_capacities(arrays, use_nulls, Capacities::Array(capacity))
    }

    /// Similar to [MutableArrayData::new], but lets users define the preallocated capacities of the array.
    /// See also [MutableArrayData::new] for more information on the arguments.
    ///
    /// # Panic
    /// This function panics if the given `capacities` don't match the data type of `arrays`. Or when
    /// a [Capacities] variant is not yet supported.
    pub fn with_capacities(
        arrays: Vec<&'a ArrayData>,
        use_nulls: bool,
        capacities: Capacities,
    ) -> Self {
        let data_type = arrays[0].data_type();
        use crate::datatypes::*;

        // if any of the arrays has nulls, insertions from any array requires setting bits
        // as there is at least one array with nulls.
        let use_nulls = use_nulls | arrays.iter().any(|array| array.null_count() > 0);

        let mut array_capacity;

        let [buffer1, buffer2] = match (data_type, &capacities) {
            (
                DataType::LargeUtf8 | DataType::LargeBinary,
                Capacities::Binary(capacity, Some(value_cap)),
            ) => {
                array_capacity = *capacity;
                preallocate_offset_and_binary_buffer::<i64>(*capacity, *value_cap)
            }
            (
                DataType::Utf8 | DataType::Binary,
                Capacities::Binary(capacity, Some(value_cap)),
            ) => {
                array_capacity = *capacity;
                preallocate_offset_and_binary_buffer::<i32>(*capacity, *value_cap)
            }
            (_, Capacities::Array(capacity)) => {
                array_capacity = *capacity;
                new_buffers(data_type, *capacity)
            }
            (
                DataType::List(_) | DataType::LargeList(_),
                Capacities::List(capacity, _),
            ) => {
                array_capacity = *capacity;
                new_buffers(data_type, *capacity)
            }
            _ => panic!("Capacities: {:?} not yet supported", capacities),
        };

        let child_data = match &data_type {
            DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Null
            | DataType::Boolean
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Timestamp(_, _)
            | DataType::Utf8
            | DataType::Binary
            | DataType::LargeUtf8
            | DataType::LargeBinary
            | DataType::Interval(_)
            | DataType::FixedSizeBinary(_) => vec![],
            DataType::Map(_, _) | DataType::List(_) | DataType::LargeList(_) => {
                let childs = arrays
                    .iter()
                    .map(|array| &array.child_data()[0])
                    .collect::<Vec<_>>();

                let capacities = if let Capacities::List(capacity, ref child_capacities) =
                    capacities
                {
                    child_capacities
                        .clone()
                        .map(|c| *c)
                        .unwrap_or(Capacities::Array(capacity))
                } else {
                    Capacities::Array(array_capacity)
                };

                vec![MutableArrayData::with_capacities(
                    childs, use_nulls, capacities,
                )]
            }
            // the dictionary type just appends keys and clones the values.
            DataType::Dictionary(_, _) => vec![],
            DataType::Struct(fields) => match capacities {
                Capacities::Struct(capacity, Some(ref child_capacities)) => {
                    array_capacity = capacity;
                    (0..fields.len())
                        .zip(child_capacities)
                        .map(|(i, child_cap)| {
                            let child_arrays = arrays
                                .iter()
                                .map(|array| &array.child_data()[i])
                                .collect::<Vec<_>>();
                            MutableArrayData::with_capacities(
                                child_arrays,
                                use_nulls,
                                child_cap.clone(),
                            )
                        })
                        .collect::<Vec<_>>()
                }
                Capacities::Struct(capacity, None) => {
                    array_capacity = capacity;
                    (0..fields.len())
                        .map(|i| {
                            let child_arrays = arrays
                                .iter()
                                .map(|array| &array.child_data()[i])
                                .collect::<Vec<_>>();
                            MutableArrayData::new(child_arrays, use_nulls, capacity)
                        })
                        .collect::<Vec<_>>()
                }
                _ => (0..fields.len())
                    .map(|i| {
                        let child_arrays = arrays
                            .iter()
                            .map(|array| &array.child_data()[i])
                            .collect::<Vec<_>>();
                        MutableArrayData::new(child_arrays, use_nulls, array_capacity)
                    })
                    .collect::<Vec<_>>(),
            },
            DataType::FixedSizeList(_, _) => {
                let childs = arrays
                    .iter()
                    .map(|array| &array.child_data()[0])
                    .collect::<Vec<_>>();
                vec![MutableArrayData::new(childs, use_nulls, array_capacity)]
            }
            DataType::Union(fields, _, _) => (0..fields.len())
                .map(|i| {
                    let child_arrays = arrays
                        .iter()
                        .map(|array| &array.child_data()[i])
                        .collect::<Vec<_>>();
                    MutableArrayData::new(child_arrays, use_nulls, array_capacity)
                })
                .collect::<Vec<_>>(),
        };

        // Get the dictionary if any, and if it is a concatenation of multiple
        let (dictionary, dict_concat) = match &data_type {
            DataType::Dictionary(_, _) => {
                // If more than one dictionary, concatenate dictionaries together
                let dict_concat = !arrays
                    .windows(2)
                    .all(|a| a[0].child_data()[0].ptr_eq(&a[1].child_data()[0]));

                match dict_concat {
                    false => (Some(arrays[0].child_data()[0].clone()), false),
                    true => {
                        if let Capacities::Dictionary(_, _) = capacities {
                            panic!("dictionary capacity not yet supported")
                        }
                        let dictionaries: Vec<_> =
                            arrays.iter().map(|array| &array.child_data()[0]).collect();
                        let lengths: Vec<_> = dictionaries
                            .iter()
                            .map(|dictionary| dictionary.len())
                            .collect();
                        let capacity = lengths.iter().sum();

                        let mut mutable =
                            MutableArrayData::new(dictionaries, false, capacity);

                        for (i, len) in lengths.iter().enumerate() {
                            mutable.extend(i, 0, *len)
                        }

                        (Some(mutable.freeze()), true)
                    }
                }
            }
            _ => (None, false),
        };

        let extend_nulls = build_extend_nulls(data_type);

        let extend_null_bits = arrays
            .iter()
            .map(|array| build_extend_null_bits(array, use_nulls))
            .collect();

        let null_buffer = if use_nulls {
            let null_bytes = bit_util::ceil(array_capacity, 8);
            MutableBuffer::from_len_zeroed(null_bytes)
        } else {
            // create 0 capacity mutable buffer with the intention that it won't be used
            MutableBuffer::with_capacity(0)
        };

        let extend_values = match &data_type {
            DataType::Dictionary(_, _) => {
                let mut next_offset = 0;
                let extend_values: Result<Vec<_>> = arrays
                    .iter()
                    .map(|array| {
                        let offset = next_offset;
                        let dict_len = array.child_data()[0].len();

                        if dict_concat {
                            next_offset += dict_len;
                        }

                        build_extend_dictionary(array, offset, offset + dict_len)
                            .ok_or(ArrowError::DictionaryKeyOverflowError)
                    })
                    .collect();

                extend_values.expect("MutableArrayData::new is infallible")
            }
            _ => arrays.iter().map(|array| build_extend(array)).collect(),
        };

        let data = _MutableArrayData {
            data_type: data_type.clone(),
            len: 0,
            null_count: 0,
            null_buffer,
            buffer1,
            buffer2,
            child_data,
        };
        Self {
            arrays,
            data,
            dictionary,
            extend_values,
            extend_null_bits,
            extend_nulls,
        }
    }

    /// Extends this array with a chunk of its source arrays
    ///
    /// # Arguments
    /// * `index` - the index of array that you what to copy values from
    /// * `start` - the start index of the chunk (inclusive)
    /// * `end` - the end index of the chunk (exclusive)
    ///
    /// # Panic
    /// This function panics if there is an invalid index,
    /// i.e. `index` >= the number of source arrays
    /// or `end` > the length of the `index`th array
    pub fn extend(&mut self, index: usize, start: usize, end: usize) {
        let len = end - start;
        (self.extend_null_bits[index])(&mut self.data, start, len);
        (self.extend_values[index])(&mut self.data, index, start, len);
        self.data.len += len;
    }

    /// Extends this [MutableArrayData] with null elements, disregarding the bound arrays
    pub fn extend_nulls(&mut self, len: usize) {
        // TODO: null_buffer should probably be extended here as well
        // otherwise is_valid() could later panic
        // add test to confirm
        self.data.null_count += len;
        (self.extend_nulls)(&mut self.data, len);
        self.data.len += len;
    }

    /// Returns the current length
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len
    }

    /// Returns true if len is 0
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.len == 0
    }

    /// Returns the current null count
    #[inline]
    pub fn null_count(&self) -> usize {
        self.data.null_count
    }

    /// Creates a [ArrayData] from the pushed regions up to this point, consuming `self`.
    pub fn freeze(self) -> ArrayData {
        unsafe { self.data.freeze(self.dictionary).build_unchecked() }
    }

    /// Creates a [ArrayDataBuilder] from the pushed regions up to this point, consuming `self`.
    /// This is useful for extending the default behavior of MutableArrayData.
    pub fn into_builder(self) -> ArrayDataBuilder {
        self.data.freeze(self.dictionary)
    }
}

#[cfg(test)]
mod tests {
    use std::{convert::TryFrom, sync::Arc};

    use super::*;
    use crate::array::Decimal128Array;
    use crate::{
        array::{
            Array, ArrayData, ArrayRef, BooleanArray, DictionaryArray,
            FixedSizeBinaryArray, Int16Array, Int16Type, Int32Array, Int64Array,
            Int64Builder, ListBuilder, MapBuilder, NullArray, StringArray,
            StringDictionaryBuilder, StructArray, UInt8Array,
        },
        buffer::Buffer,
        datatypes::Field,
    };
    use crate::{
        array::{ListArray, StringBuilder},
        error::Result,
    };

    fn create_decimal_array(
        array: Vec<Option<i128>>,
        precision: u8,
        scale: u8,
    ) -> Decimal128Array {
        array
            .into_iter()
            .collect::<Decimal128Array>()
            .with_precision_and_scale(precision, scale)
            .unwrap()
    }

    #[test]
    #[cfg(not(feature = "force_validate"))]
    fn test_decimal() {
        let decimal_array =
            create_decimal_array(vec![Some(1), Some(2), None, Some(3)], 10, 3);
        let arrays = vec![Array::data(&decimal_array)];
        let mut a = MutableArrayData::new(arrays, true, 3);
        a.extend(0, 0, 3);
        a.extend(0, 2, 3);
        let result = a.freeze();
        let array = Decimal128Array::from(result);
        let expected = create_decimal_array(vec![Some(1), Some(2), None, None], 10, 3);
        assert_eq!(array, expected);
    }
    #[test]
    #[cfg(not(feature = "force_validate"))]
    fn test_decimal_offset() {
        let decimal_array =
            create_decimal_array(vec![Some(1), Some(2), None, Some(3)], 10, 3);
        let decimal_array = decimal_array.slice(1, 3); // 2, null, 3
        let arrays = vec![decimal_array.data()];
        let mut a = MutableArrayData::new(arrays, true, 2);
        a.extend(0, 0, 2); // 2, null
        let result = a.freeze();
        let array = Decimal128Array::from(result);
        let expected = create_decimal_array(vec![Some(2), None], 10, 3);
        assert_eq!(array, expected);
    }

    #[test]
    #[cfg(not(feature = "force_validate"))]
    fn test_decimal_null_offset_nulls() {
        let decimal_array =
            create_decimal_array(vec![Some(1), Some(2), None, Some(3)], 10, 3);
        let decimal_array = decimal_array.slice(1, 3); // 2, null, 3
        let arrays = vec![decimal_array.data()];
        let mut a = MutableArrayData::new(arrays, true, 2);
        a.extend(0, 0, 2); // 2, null
        a.extend_nulls(3); // 2, null, null, null, null
        a.extend(0, 1, 3); //2, null, null, null, null, null, 3
        let result = a.freeze();
        let array = Decimal128Array::from(result);
        let expected = create_decimal_array(
            vec![Some(2), None, None, None, None, None, Some(3)],
            10,
            3,
        );
        assert_eq!(array, expected);
    }

    /// tests extending from a primitive array w/ offset nor nulls
    #[test]
    fn test_primitive() {
        let b = UInt8Array::from(vec![Some(1), Some(2), Some(3)]);
        let arrays = vec![b.data()];
        let mut a = MutableArrayData::new(arrays, false, 3);
        a.extend(0, 0, 2);
        let result = a.freeze();
        let array = UInt8Array::from(result);
        let expected = UInt8Array::from(vec![Some(1), Some(2)]);
        assert_eq!(array, expected);
    }

    /// tests extending from a primitive array with offset w/ nulls
    #[test]
    fn test_primitive_offset() {
        let b = UInt8Array::from(vec![Some(1), Some(2), Some(3)]);
        let b = b.slice(1, 2);
        let arrays = vec![b.data()];
        let mut a = MutableArrayData::new(arrays, false, 2);
        a.extend(0, 0, 2);
        let result = a.freeze();
        let array = UInt8Array::from(result);
        let expected = UInt8Array::from(vec![Some(2), Some(3)]);
        assert_eq!(array, expected);
    }

    /// tests extending from a primitive array with offset and nulls
    #[test]
    fn test_primitive_null_offset() {
        let b = UInt8Array::from(vec![Some(1), None, Some(3)]);
        let b = b.slice(1, 2);
        let arrays = vec![b.data()];
        let mut a = MutableArrayData::new(arrays, false, 2);
        a.extend(0, 0, 2);
        let result = a.freeze();
        let array = UInt8Array::from(result);
        let expected = UInt8Array::from(vec![None, Some(3)]);
        assert_eq!(array, expected);
    }

    #[test]
    fn test_primitive_null_offset_nulls() {
        let b = UInt8Array::from(vec![Some(1), Some(2), Some(3)]);
        let b = b.slice(1, 2);
        let arrays = vec![b.data()];
        let mut a = MutableArrayData::new(arrays, true, 2);
        a.extend(0, 0, 2);
        a.extend_nulls(3);
        a.extend(0, 1, 2);
        let result = a.freeze();
        let array = UInt8Array::from(result);
        let expected =
            UInt8Array::from(vec![Some(2), Some(3), None, None, None, Some(3)]);
        assert_eq!(array, expected);
    }

    #[test]
    fn test_list_null_offset() {
        let int_builder = Int64Builder::with_capacity(24);
        let mut builder = ListBuilder::<Int64Builder>::new(int_builder);
        builder.values().append_slice(&[1, 2, 3]);
        builder.append(true);
        builder.values().append_slice(&[4, 5]);
        builder.append(true);
        builder.values().append_slice(&[6, 7, 8]);
        builder.append(true);
        let array = builder.finish();
        let arrays = vec![array.data()];

        let mut mutable = MutableArrayData::new(arrays, false, 0);
        mutable.extend(0, 0, 1);

        let result = mutable.freeze();
        let array = ListArray::from(result);

        let int_builder = Int64Builder::with_capacity(24);
        let mut builder = ListBuilder::<Int64Builder>::new(int_builder);
        builder.values().append_slice(&[1, 2, 3]);
        builder.append(true);
        let expected = builder.finish();

        assert_eq!(array, expected);
    }

    /// tests extending from a variable-sized (strings and binary) array w/ offset with nulls
    #[test]
    fn test_variable_sized_nulls() {
        let array = StringArray::from(vec![Some("a"), Some("bc"), None, Some("defh")]);
        let arrays = vec![array.data()];

        let mut mutable = MutableArrayData::new(arrays, false, 0);

        mutable.extend(0, 1, 3);

        let result = mutable.freeze();
        let result = StringArray::from(result);

        let expected = StringArray::from(vec![Some("bc"), None]);
        assert_eq!(result, expected);
    }

    /// tests extending from a variable-sized (strings and binary) array
    /// with an offset and nulls
    #[test]
    fn test_variable_sized_offsets() {
        let array = StringArray::from(vec![Some("a"), Some("bc"), None, Some("defh")]);
        let array = array.slice(1, 3);

        let arrays = vec![array.data()];

        let mut mutable = MutableArrayData::new(arrays, false, 0);

        mutable.extend(0, 0, 3);

        let result = mutable.freeze();
        let result = StringArray::from(result);

        let expected = StringArray::from(vec![Some("bc"), None, Some("defh")]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_string_offsets() {
        let array = StringArray::from(vec![Some("a"), Some("bc"), None, Some("defh")]);
        let array = array.slice(1, 3);

        let arrays = vec![array.data()];

        let mut mutable = MutableArrayData::new(arrays, false, 0);

        mutable.extend(0, 0, 3);

        let result = mutable.freeze();
        let result = StringArray::from(result);

        let expected = StringArray::from(vec![Some("bc"), None, Some("defh")]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_multiple_with_nulls() {
        let array1 = StringArray::from(vec!["hello", "world"]);
        let array2 = StringArray::from(vec![Some("1"), None]);

        let arrays = vec![array1.data(), array2.data()];

        let mut mutable = MutableArrayData::new(arrays, false, 5);

        mutable.extend(0, 0, 2);
        mutable.extend(1, 0, 2);

        let result = mutable.freeze();
        let result = StringArray::from(result);

        let expected =
            StringArray::from(vec![Some("hello"), Some("world"), Some("1"), None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_string_null_offset_nulls() {
        let array = StringArray::from(vec![Some("a"), Some("bc"), None, Some("defh")]);
        let array = array.slice(1, 3);

        let arrays = vec![array.data()];

        let mut mutable = MutableArrayData::new(arrays, true, 0);

        mutable.extend(0, 1, 3);
        mutable.extend_nulls(1);

        let result = mutable.freeze();
        let result = StringArray::from(result);

        let expected = StringArray::from(vec![None, Some("defh"), None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_bool() {
        let array = BooleanArray::from(vec![Some(false), Some(true), None, Some(false)]);
        let arrays = vec![array.data()];

        let mut mutable = MutableArrayData::new(arrays, false, 0);

        mutable.extend(0, 1, 3);

        let result = mutable.freeze();
        let result = BooleanArray::from(result);

        let expected = BooleanArray::from(vec![Some(true), None]);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_null() {
        let array1 = NullArray::new(10);
        let array2 = NullArray::new(5);
        let arrays = vec![array1.data(), array2.data()];

        let mut mutable = MutableArrayData::new(arrays, false, 0);

        mutable.extend(0, 1, 3);
        mutable.extend(1, 0, 1);

        let result = mutable.freeze();
        let result = NullArray::from(result);

        let expected = NullArray::new(3);
        assert_eq!(result, expected);
    }

    fn create_dictionary_array(values: &[&str], keys: &[Option<&str>]) -> ArrayData {
        let values = StringArray::from(values.to_vec());
        let mut builder = StringDictionaryBuilder::<Int16Type>::new_with_dictionary(
            keys.len(),
            &values,
        )
        .unwrap();
        for key in keys {
            if let Some(v) = key {
                builder.append(v).unwrap();
            } else {
                builder.append_null()
            }
        }
        builder.finish().into_data()
    }

    #[test]
    fn test_dictionary() {
        // (a, b, c), (0, 1, 0, 2) => (a, b, a, c)
        let array = create_dictionary_array(
            &["a", "b", "c"],
            &[Some("a"), Some("b"), None, Some("c")],
        );
        let arrays = vec![&array];

        let mut mutable = MutableArrayData::new(arrays, false, 0);

        mutable.extend(0, 1, 3);

        let result = mutable.freeze();
        let result = DictionaryArray::from(result);

        let expected = Int16Array::from(vec![Some(1), None]);
        assert_eq!(result.keys(), &expected);
    }

    #[test]
    fn test_struct() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("joe"),
            None,
            None,
            Some("mark"),
            Some("doe"),
        ]));
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
        ]));

        let array =
            StructArray::try_from(vec![("f1", strings.clone()), ("f2", ints.clone())])
                .unwrap();
        let arrays = vec![array.data()];
        let mut mutable = MutableArrayData::new(arrays, false, 0);

        mutable.extend(0, 1, 3);
        let data = mutable.freeze();
        let array = StructArray::from(data);

        let expected = StructArray::try_from(vec![
            ("f1", strings.slice(1, 2)),
            ("f2", ints.slice(1, 2)),
        ])
        .unwrap();
        assert_eq!(array, expected)
    }

    #[test]
    fn test_struct_offset() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("joe"),
            None,
            None,
            Some("mark"),
            Some("doe"),
        ]));
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
        ]));

        let array =
            StructArray::try_from(vec![("f1", strings.clone()), ("f2", ints.clone())])
                .unwrap()
                .slice(1, 3);
        let arrays = vec![array.data()];
        let mut mutable = MutableArrayData::new(arrays, false, 0);

        mutable.extend(0, 1, 3);
        let data = mutable.freeze();
        let array = StructArray::from(data);

        let expected_strings: ArrayRef =
            Arc::new(StringArray::from(vec![None, Some("mark")]));
        let expected = StructArray::try_from(vec![
            ("f1", expected_strings),
            ("f2", ints.slice(2, 2)),
        ])
        .unwrap();

        assert_eq!(array, expected);
    }

    #[test]
    fn test_struct_nulls() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("joe"),
            None,
            None,
            Some("mark"),
            Some("doe"),
        ]));
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
        ]));

        let array =
            StructArray::try_from(vec![("f1", strings.clone()), ("f2", ints.clone())])
                .unwrap();
        let arrays = vec![array.data()];

        let mut mutable = MutableArrayData::new(arrays, false, 0);

        mutable.extend(0, 1, 3);
        let data = mutable.freeze();
        let array = StructArray::from(data);

        let expected_string = Arc::new(StringArray::from(vec![None, None])) as ArrayRef;
        let expected_int = Arc::new(Int32Array::from(vec![Some(2), None])) as ArrayRef;

        let expected =
            StructArray::try_from(vec![("f1", expected_string), ("f2", expected_int)])
                .unwrap();
        assert_eq!(array, expected)
    }

    #[test]
    fn test_struct_many() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![
            Some("joe"),
            None,
            None,
            Some("mark"),
            Some("doe"),
        ]));
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(2),
            None,
            Some(4),
            Some(5),
        ]));

        let array =
            StructArray::try_from(vec![("f1", strings.clone()), ("f2", ints.clone())])
                .unwrap();
        let arrays = vec![array.data(), array.data()];
        let mut mutable = MutableArrayData::new(arrays, false, 0);

        mutable.extend(0, 1, 3);
        mutable.extend(1, 0, 2);
        let data = mutable.freeze();
        let array = StructArray::from(data);

        let expected_string =
            Arc::new(StringArray::from(vec![None, None, Some("joe"), None])) as ArrayRef;
        let expected_int =
            Arc::new(Int32Array::from(vec![Some(2), None, Some(1), Some(2)])) as ArrayRef;

        let expected =
            StructArray::try_from(vec![("f1", expected_string), ("f2", expected_int)])
                .unwrap();
        assert_eq!(array, expected)
    }

    #[test]
    fn test_binary_fixed_sized_offsets() {
        let array = FixedSizeBinaryArray::try_from_iter(
            vec![vec![0, 0], vec![0, 1], vec![0, 2]].into_iter(),
        )
        .expect("Failed to create FixedSizeBinaryArray from iterable");
        let array = array.slice(1, 2);
        // = [[0, 1], [0, 2]] due to the offset = 1

        let arrays = vec![array.data()];

        let mut mutable = MutableArrayData::new(arrays, false, 0);

        mutable.extend(0, 1, 2);
        mutable.extend(0, 0, 1);

        let result = mutable.freeze();
        let result = FixedSizeBinaryArray::from(result);

        let expected =
            FixedSizeBinaryArray::try_from_iter(vec![vec![0, 2], vec![0, 1]].into_iter())
                .expect("Failed to create FixedSizeBinaryArray from iterable");
        assert_eq!(result, expected);
    }

    #[test]
    fn test_list_append() {
        let mut builder =
            ListBuilder::<Int64Builder>::new(Int64Builder::with_capacity(24));
        builder.values().append_slice(&[1, 2, 3]);
        builder.append(true);
        builder.values().append_slice(&[4, 5]);
        builder.append(true);
        builder.values().append_slice(&[6, 7, 8]);
        builder.values().append_slice(&[9, 10, 11]);
        builder.append(true);
        let a = builder.finish();

        let a_builder = Int64Builder::with_capacity(24);
        let mut a_builder = ListBuilder::<Int64Builder>::new(a_builder);
        a_builder.values().append_slice(&[12, 13]);
        a_builder.append(true);
        a_builder.append(true);
        a_builder.values().append_slice(&[14, 15]);
        a_builder.append(true);
        let b = a_builder.finish();

        let c = b.slice(1, 2);

        let mut mutable =
            MutableArrayData::new(vec![a.data(), b.data(), c.data()], false, 1);
        mutable.extend(0, 0, a.len());
        mutable.extend(1, 0, b.len());
        mutable.extend(2, 0, c.len());

        let finished = mutable.freeze();

        let expected_int_array = Int64Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            Some(9),
            Some(10),
            Some(11),
            // append first array
            Some(12),
            Some(13),
            Some(14),
            Some(15),
            // append second array
            Some(14),
            Some(15),
        ]);
        let list_value_offsets =
            Buffer::from_slice_ref(&[0i32, 3, 5, 11, 13, 13, 15, 15, 17]);
        let expected_list_data = ArrayData::try_new(
            DataType::List(Box::new(Field::new("item", DataType::Int64, true))),
            8,
            None,
            0,
            vec![list_value_offsets],
            vec![expected_int_array.into_data()],
        )
        .unwrap();
        assert_eq!(finished, expected_list_data);
    }

    #[test]
    fn test_list_nulls_append() -> Result<()> {
        let mut builder =
            ListBuilder::<Int64Builder>::new(Int64Builder::with_capacity(32));
        builder.values().append_slice(&[1, 2, 3]);
        builder.append(true);
        builder.values().append_slice(&[4, 5]);
        builder.append(true);
        builder.append(false);
        builder.values().append_slice(&[6, 7, 8]);
        builder.values().append_null();
        builder.values().append_null();
        builder.values().append_slice(&[9, 10, 11]);
        builder.append(true);
        let a = builder.finish();
        let a = a.data();

        let mut builder =
            ListBuilder::<Int64Builder>::new(Int64Builder::with_capacity(32));
        builder.values().append_slice(&[12, 13]);
        builder.append(true);
        builder.append(false);
        builder.append(true);
        builder.values().append_null();
        builder.values().append_null();
        builder.values().append_slice(&[14, 15]);
        builder.append(true);
        let b = builder.finish();
        let b = b.data();
        let c = b.slice(1, 2);
        let d = b.slice(2, 2);

        let mut mutable = MutableArrayData::new(vec![a, b, &c, &d], false, 10);

        mutable.extend(0, 0, a.len());
        mutable.extend(1, 0, b.len());
        mutable.extend(2, 0, c.len());
        mutable.extend(3, 0, d.len());
        let result = mutable.freeze();

        let expected_int_array = Int64Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            None,
            None,
            Some(9),
            Some(10),
            Some(11),
            // second array
            Some(12),
            Some(13),
            None,
            None,
            Some(14),
            Some(15),
            // slice(1, 2) results in no values added
            None,
            None,
            Some(14),
            Some(15),
        ]);
        let list_value_offsets =
            Buffer::from_slice_ref(&[0, 3, 5, 5, 13, 15, 15, 15, 19, 19, 19, 19, 23]);
        let expected_list_data = ArrayData::try_new(
            DataType::List(Box::new(Field::new("item", DataType::Int64, true))),
            12,
            Some(Buffer::from(&[0b11011011, 0b1110])),
            0,
            vec![list_value_offsets],
            vec![expected_int_array.into_data()],
        )
        .unwrap();
        assert_eq!(result, expected_list_data);

        Ok(())
    }

    #[test]
    fn test_list_append_with_capacities() {
        let mut builder =
            ListBuilder::<Int64Builder>::new(Int64Builder::with_capacity(24));
        builder.values().append_slice(&[1, 2, 3]);
        builder.append(true);
        builder.values().append_slice(&[4, 5]);
        builder.append(true);
        builder.values().append_slice(&[6, 7, 8]);
        builder.values().append_slice(&[9, 10, 11]);
        builder.append(true);
        let a = builder.finish();

        let a_builder = Int64Builder::with_capacity(24);
        let mut a_builder = ListBuilder::<Int64Builder>::new(a_builder);
        a_builder.values().append_slice(&[12, 13]);
        a_builder.append(true);
        a_builder.append(true);
        a_builder.values().append_slice(&[14, 15, 16, 17]);
        a_builder.append(true);
        let b = a_builder.finish();

        let mutable = MutableArrayData::with_capacities(
            vec![a.data(), b.data()],
            false,
            Capacities::List(6, Some(Box::new(Capacities::Array(17)))),
        );

        // capacities are rounded up to multiples of 64 by MutableBuffer
        assert_eq!(mutable.data.buffer1.capacity(), 64);
        assert_eq!(mutable.data.child_data[0].data.buffer1.capacity(), 192);
    }

    #[test]
    fn test_map_nulls_append() -> Result<()> {
        let mut builder = MapBuilder::<Int64Builder, Int64Builder>::new(
            None,
            Int64Builder::with_capacity(32),
            Int64Builder::with_capacity(32),
        );
        builder.keys().append_slice(&[1, 2, 3]);
        builder.values().append_slice(&[1, 2, 3]);
        builder.append(true).unwrap();
        builder.keys().append_slice(&[4, 5]);
        builder.values().append_slice(&[4, 5]);
        builder.append(true).unwrap();
        builder.append(false).unwrap();
        builder.keys().append_slice(&[6, 7, 8, 100, 101, 9, 10, 11]);
        builder.values().append_slice(&[6, 7, 8]);
        builder.values().append_null();
        builder.values().append_null();
        builder.values().append_slice(&[9, 10, 11]);
        builder.append(true).unwrap();

        let a = builder.finish();
        let a = a.data();

        let mut builder = MapBuilder::<Int64Builder, Int64Builder>::new(
            None,
            Int64Builder::with_capacity(32),
            Int64Builder::with_capacity(32),
        );

        builder.keys().append_slice(&[12, 13]);
        builder.values().append_slice(&[12, 13]);
        builder.append(true).unwrap();
        builder.append(false).unwrap();
        builder.append(true).unwrap();
        builder.keys().append_slice(&[100, 101, 14, 15]);
        builder.values().append_null();
        builder.values().append_null();
        builder.values().append_slice(&[14, 15]);
        builder.append(true).unwrap();

        let b = builder.finish();
        let b = b.data();
        let c = b.slice(1, 2);
        let d = b.slice(2, 2);

        let mut mutable = MutableArrayData::new(vec![a, b, &c, &d], false, 10);

        mutable.extend(0, 0, a.len());
        mutable.extend(1, 0, b.len());
        mutable.extend(2, 0, c.len());
        mutable.extend(3, 0, d.len());
        let result = mutable.freeze();

        let expected_key_array = Int64Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            Some(100),
            Some(101),
            Some(9),
            Some(10),
            Some(11),
            // second array
            Some(12),
            Some(13),
            Some(100),
            Some(101),
            Some(14),
            Some(15),
            // slice(1, 2) results in no values added
            Some(100),
            Some(101),
            Some(14),
            Some(15),
        ]);

        let expected_value_array = Int64Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7),
            Some(8),
            None,
            None,
            Some(9),
            Some(10),
            Some(11),
            // second array
            Some(12),
            Some(13),
            None,
            None,
            Some(14),
            Some(15),
            // slice(1, 2) results in no values added
            None,
            None,
            Some(14),
            Some(15),
        ]);

        let expected_entry_array = StructArray::from(vec![
            (
                Field::new("keys", DataType::Int64, false),
                Arc::new(expected_key_array) as ArrayRef,
            ),
            (
                Field::new("values", DataType::Int64, true),
                Arc::new(expected_value_array) as ArrayRef,
            ),
        ]);

        let map_offsets =
            Buffer::from_slice_ref(&[0, 3, 5, 5, 13, 15, 15, 15, 19, 19, 19, 19, 23]);

        let expected_list_data = ArrayData::try_new(
            DataType::Map(
                Box::new(Field::new(
                    "entries",
                    DataType::Struct(vec![
                        Field::new("keys", DataType::Int64, false),
                        Field::new("values", DataType::Int64, true),
                    ]),
                    false,
                )),
                false,
            ),
            12,
            Some(Buffer::from(&[0b11011011, 0b1110])),
            0,
            vec![map_offsets],
            vec![expected_entry_array.into_data()],
        )
        .unwrap();
        assert_eq!(result, expected_list_data);

        Ok(())
    }

    #[test]
    fn test_list_of_strings_append() -> Result<()> {
        // [["alpha", "beta", None]]
        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.values().append_value("Hello");
        builder.values().append_value("Arrow");
        builder.values().append_null();
        builder.append(true);
        let a = builder.finish();

        // [["alpha", "beta"], [None], ["gamma", "delta", None]]
        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.values().append_value("alpha");
        builder.values().append_value("beta");
        builder.append(true);
        builder.values().append_null();
        builder.append(true);
        builder.values().append_value("gamma");
        builder.values().append_value("delta");
        builder.values().append_null();
        builder.append(true);
        let b = builder.finish();

        let mut mutable = MutableArrayData::new(vec![a.data(), b.data()], false, 10);

        mutable.extend(0, 0, a.len());
        mutable.extend(1, 0, b.len());
        mutable.extend(1, 1, 3);
        mutable.extend(1, 0, 0);
        let result = mutable.freeze();

        let expected_string_array = StringArray::from(vec![
            // extend a[0..a.len()]
            // a[0]
            Some("Hello"),
            Some("Arrow"),
            None,
            // extend b[0..b.len()]
            // b[0]
            Some("alpha"),
            Some("beta"),
            // b[1]
            None,
            // b[2]
            Some("gamma"),
            Some("delta"),
            None,
            // extend b[1..3]
            // b[1]
            None,
            // b[2]
            Some("gamma"),
            Some("delta"),
            None,
            // extend b[0..0]
        ]);
        let list_value_offsets = Buffer::from_slice_ref(&[0, 3, 5, 6, 9, 10, 13]);
        let expected_list_data = ArrayData::try_new(
            DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
            6,
            None,
            0,
            vec![list_value_offsets],
            vec![expected_string_array.into_data()],
        )
        .unwrap();
        assert_eq!(result, expected_list_data);
        Ok(())
    }

    #[test]
    fn test_fixed_size_binary_append() {
        let a = vec![Some(vec![1, 2]), Some(vec![3, 4]), Some(vec![5, 6])];
        let a = FixedSizeBinaryArray::try_from_sparse_iter(a.into_iter())
            .expect("Failed to create FixedSizeBinaryArray from iterable");

        let b = vec![
            None,
            Some(vec![7, 8]),
            Some(vec![9, 10]),
            None,
            Some(vec![13, 14]),
            None,
        ];
        let b = FixedSizeBinaryArray::try_from_sparse_iter(b.into_iter())
            .expect("Failed to create FixedSizeBinaryArray from iterable");

        let mut mutable = MutableArrayData::new(vec![a.data(), b.data()], false, 10);

        mutable.extend(0, 0, a.len());
        mutable.extend(1, 0, b.len());
        mutable.extend(1, 1, 4);
        mutable.extend(1, 2, 3);
        mutable.extend(1, 5, 5);
        let result = mutable.freeze();

        let expected = vec![
            // a
            Some(vec![1, 2]),
            Some(vec![3, 4]),
            Some(vec![5, 6]),
            // b
            None,
            Some(vec![7, 8]),
            Some(vec![9, 10]),
            None,
            Some(vec![13, 14]),
            None,
            // b[1..4]
            Some(vec![7, 8]),
            Some(vec![9, 10]),
            None,
            // b[2..3]
            Some(vec![9, 10]),
            // b[4..4]
        ];
        let expected = FixedSizeBinaryArray::try_from_sparse_iter(expected.into_iter())
            .expect("Failed to create FixedSizeBinaryArray from iterable");
        assert_eq!(&result, expected.data());
    }

    /*
    // this is an old test used on a meanwhile removed dead code
    // that is still useful when `MutableArrayData` supports fixed-size lists.
    #[test]
    fn test_fixed_size_list_append() -> Result<()> {
        let int_builder = UInt16Builder::new(64);
        let mut builder = FixedSizeListBuilder::<UInt16Builder>::new(int_builder, 2);
        builder.values().append_slice(&[1, 2])?;
        builder.append(true)?;
        builder.values().append_slice(&[3, 4])?;
        builder.append(false)?;
        builder.values().append_slice(&[5, 6])?;
        builder.append(true)?;

        let a_builder = UInt16Builder::new(64);
        let mut a_builder = FixedSizeListBuilder::<UInt16Builder>::new(a_builder, 2);
        a_builder.values().append_slice(&[7, 8])?;
        a_builder.append(true)?;
        a_builder.values().append_slice(&[9, 10])?;
        a_builder.append(true)?;
        a_builder.values().append_slice(&[11, 12])?;
        a_builder.append(false)?;
        a_builder.values().append_slice(&[13, 14])?;
        a_builder.append(true)?;
        a_builder.values().append_null()?;
        a_builder.values().append_null()?;
        a_builder.append(true)?;
        let a = a_builder.finish();

        // append array
        builder.append_data(&[
            a.data(),
            a.slice(1, 3).data(),
            a.slice(2, 1).data(),
            a.slice(5, 0).data(),
        ])?;
        let finished = builder.finish();

        let expected_int_array = UInt16Array::from(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            // append first array
            Some(7),
            Some(8),
            Some(9),
            Some(10),
            Some(11),
            Some(12),
            Some(13),
            Some(14),
            None,
            None,
            // append slice(1, 3)
            Some(9),
            Some(10),
            Some(11),
            Some(12),
            Some(13),
            Some(14),
            // append slice(2, 1)
            Some(11),
            Some(12),
        ]);
        let expected_list_data = ArrayData::new(
            DataType::FixedSizeList(
                Box::new(Field::new("item", DataType::UInt16, true)),
                2,
            ),
            12,
            None,
            None,
            0,
            vec![],
            vec![expected_int_array.data()],
        );
        let expected_list =
            FixedSizeListArray::from(Arc::new(expected_list_data) as ArrayData);
        assert_eq!(&expected_list.values(), &finished.values());
        assert_eq!(expected_list.len(), finished.len());

        Ok(())
    }
    */
}
