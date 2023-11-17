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
    ArrayData, ArrayDataBuilder,
};
use crate::bit_mask::set_bits;
use arrow_buffer::buffer::{BooleanBuffer, NullBuffer};
use arrow_buffer::{bit_util, i256, ArrowNativeType, MutableBuffer};
use arrow_schema::{ArrowError, DataType, IntervalUnit, UnionMode};
use half::f16;
use num::Integer;
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
    pub null_buffer: Option<MutableBuffer>,

    // arrow specification only allows up to 3 buffers (2 ignoring the nulls above).
    // Thus, we place them in the stack to avoid bound checks and greater data locality.
    pub buffer1: MutableBuffer,
    pub buffer2: MutableBuffer,
    pub child_data: Vec<MutableArrayData<'a>>,
}

impl<'a> _MutableArrayData<'a> {
    fn null_buffer(&mut self) -> &mut MutableBuffer {
        self.null_buffer
            .as_mut()
            .expect("MutableArrayData not nullable")
    }

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

        let nulls = self
            .null_buffer
            .map(|nulls| {
                let bools = BooleanBuffer::new(nulls.into(), 0, self.len);
                unsafe { NullBuffer::new_unchecked(bools, self.null_count) }
            })
            .filter(|n| n.null_count() > 0);

        ArrayDataBuilder::new(self.data_type)
            .offset(0)
            .len(self.len)
            .nulls(nulls)
            .buffers(buffers)
            .child_data(child_data)
    }
}

fn build_extend_null_bits(array: &ArrayData, use_nulls: bool) -> ExtendNullBits {
    if let Some(nulls) = array.nulls() {
        let bytes = nulls.validity();
        Box::new(move |mutable, start, len| {
            let mutable_len = mutable.len;
            let out = mutable.null_buffer();
            utils::resize_for_bits(out, mutable_len + len);
            mutable.null_count += set_bits(
                out.as_slice_mut(),
                bytes,
                mutable_len,
                nulls.offset() + start,
                len,
            );
        })
    } else if use_nulls {
        Box::new(|mutable, _, len| {
            let mutable_len = mutable.len;
            let out = mutable.null_buffer();
            utils::resize_for_bits(out, mutable_len + len);
            let write_data = out.as_slice_mut();
            (0..len).for_each(|i| {
                bit_util::set_bit(write_data, mutable_len + i);
            });
        })
    } else {
        Box::new(|_, _, _| {})
    }
}

/// Struct to efficiently and interactively create an [ArrayData] from an existing [ArrayData] by
/// copying chunks.
///
/// The main use case of this struct is to perform unary operations to arrays of arbitrary types,
/// such as `filter` and `take`.
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
fn build_extend_dictionary(array: &ArrayData, offset: usize, max: usize) -> Option<Extend> {
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
    match array.data_type() {
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
        DataType::Date32 | DataType::Time32(_) | DataType::Interval(IntervalUnit::YearMonth) => {
            primitive::build_extend::<i32>(array)
        }
        DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(IntervalUnit::DayTime) => primitive::build_extend::<i64>(array),
        DataType::Interval(IntervalUnit::MonthDayNano) => primitive::build_extend::<i128>(array),
        DataType::Decimal128(_, _) => primitive::build_extend::<i128>(array),
        DataType::Decimal256(_, _) => primitive::build_extend::<i256>(array),
        DataType::Utf8 | DataType::Binary => variable_size::build_extend::<i32>(array),
        DataType::LargeUtf8 | DataType::LargeBinary => variable_size::build_extend::<i64>(array),
        DataType::Map(_, _) | DataType::List(_) => list::build_extend::<i32>(array),
        DataType::LargeList(_) => list::build_extend::<i64>(array),
        DataType::Dictionary(_, _) => unreachable!("should use build_extend_dictionary"),
        DataType::Struct(_) => structure::build_extend(array),
        DataType::FixedSizeBinary(_) => fixed_binary::build_extend(array),
        DataType::Float16 => primitive::build_extend::<f16>(array),
        DataType::FixedSizeList(_, _) => fixed_size_list::build_extend(array),
        DataType::Union(_, mode) => match mode {
            UnionMode::Sparse => union::build_extend_sparse(array),
            UnionMode::Dense => union::build_extend_dense(array),
        },
        DataType::RunEndEncoded(_, _) => todo!(),
    }
}

fn build_extend_nulls(data_type: &DataType) -> ExtendNulls {
    Box::new(match data_type {
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
        DataType::Date32 | DataType::Time32(_) | DataType::Interval(IntervalUnit::YearMonth) => {
            primitive::extend_nulls::<i32>
        }
        DataType::Date64
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(IntervalUnit::DayTime) => primitive::extend_nulls::<i64>,
        DataType::Interval(IntervalUnit::MonthDayNano) => primitive::extend_nulls::<i128>,
        DataType::Decimal128(_, _) => primitive::extend_nulls::<i128>,
        DataType::Decimal256(_, _) => primitive::extend_nulls::<i256>,
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
        DataType::FixedSizeBinary(_) => fixed_binary::extend_nulls,
        DataType::Float16 => primitive::extend_nulls::<f16>,
        DataType::FixedSizeList(_, _) => fixed_size_list::extend_nulls,
        DataType::Union(_, mode) => match mode {
            UnionMode::Sparse => union::extend_nulls_sparse,
            UnionMode::Dense => union::extend_nulls_dense,
        },
        DataType::RunEndEncoded(_, _) => todo!(),
    })
}

fn preallocate_offset_and_binary_buffer<Offset: ArrowNativeType + Integer>(
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

        for a in arrays.iter().skip(1) {
            assert_eq!(
                data_type,
                a.data_type(),
                "Arrays with inconsistent types passed to MutableArrayData"
            )
        }

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
            (DataType::Utf8 | DataType::Binary, Capacities::Binary(capacity, Some(value_cap))) => {
                array_capacity = *capacity;
                preallocate_offset_and_binary_buffer::<i32>(*capacity, *value_cap)
            }
            (_, Capacities::Array(capacity)) => {
                array_capacity = *capacity;
                new_buffers(data_type, *capacity)
            }
            (DataType::List(_) | DataType::LargeList(_), Capacities::List(capacity, _)) => {
                array_capacity = *capacity;
                new_buffers(data_type, *capacity)
            }
            _ => panic!("Capacities: {capacities:?} not yet supported"),
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
                let children = arrays
                    .iter()
                    .map(|array| &array.child_data()[0])
                    .collect::<Vec<_>>();

                let capacities =
                    if let Capacities::List(capacity, ref child_capacities) = capacities {
                        child_capacities
                            .clone()
                            .map(|c| *c)
                            .unwrap_or(Capacities::Array(capacity))
                    } else {
                        Capacities::Array(array_capacity)
                    };

                vec![MutableArrayData::with_capacities(
                    children, use_nulls, capacities,
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
            DataType::RunEndEncoded(_, _) => {
                let run_ends_child = arrays
                    .iter()
                    .map(|array| &array.child_data()[0])
                    .collect::<Vec<_>>();
                let value_child = arrays
                    .iter()
                    .map(|array| &array.child_data()[1])
                    .collect::<Vec<_>>();
                vec![
                    MutableArrayData::new(run_ends_child, false, array_capacity),
                    MutableArrayData::new(value_child, use_nulls, array_capacity),
                ]
            }
            DataType::FixedSizeList(_, _) => {
                let children = arrays
                    .iter()
                    .map(|array| &array.child_data()[0])
                    .collect::<Vec<_>>();
                vec![MutableArrayData::new(children, use_nulls, array_capacity)]
            }
            DataType::Union(fields, _) => (0..fields.len())
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

                        let mut mutable = MutableArrayData::new(dictionaries, false, capacity);

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

        let null_buffer = use_nulls.then(|| {
            let null_bytes = bit_util::ceil(array_capacity, 8);
            MutableBuffer::from_len_zeroed(null_bytes)
        });

        let extend_values = match &data_type {
            DataType::Dictionary(_, _) => {
                let mut next_offset = 0;
                let extend_values: Result<Vec<_>, _> = arrays
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
    ///
    /// # Panics
    ///
    /// Panics if [`MutableArrayData`] not created with `use_nulls` or nullable source arrays
    ///
    pub fn extend_nulls(&mut self, len: usize) {
        self.data.len += len;
        let bit_len = bit_util::ceil(self.data.len, 8);
        let nulls = self.data.null_buffer();
        nulls.resize(bit_len, 0);
        self.data.null_count += len;
        (self.extend_nulls)(&mut self.data, len);
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

// See arrow/tests/array_transform.rs for tests of transform functionality

#[cfg(test)]
mod test {
    use super::*;
    use arrow_schema::Field;
    use std::sync::Arc;

    #[test]
    fn test_list_append_with_capacities() {
        let array = ArrayData::new_empty(&DataType::List(Arc::new(Field::new(
            "element",
            DataType::Int64,
            false,
        ))));

        let mutable = MutableArrayData::with_capacities(
            vec![&array],
            false,
            Capacities::List(6, Some(Box::new(Capacities::Array(17)))),
        );

        // capacities are rounded up to multiples of 64 by MutableBuffer
        assert_eq!(mutable.data.buffer1.capacity(), 64);
        assert_eq!(mutable.data.child_data[0].data.buffer1.capacity(), 192);
    }
}
