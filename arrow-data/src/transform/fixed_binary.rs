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

use super::{Extend, ExtendNullBits, SpecializedMutableArrayData, _MutableArrayData};
use crate::data::new_buffers;
use crate::transform::utils::build_extend_null_bits;
use crate::{ArrayData, ArrayDataBuilder};
use arrow_buffer::{bit_util, BooleanBuffer, MutableBuffer, NullBuffer};
use arrow_schema::DataType;


// TODO - remove
#[deprecated]
pub(super) fn build_extend(array: &ArrayData) -> Extend {
    let size = match array.data_type() {
        DataType::FixedSizeBinary(i) => *i as usize,
        _ => unreachable!(),
    };

    let values = &array.buffers()[0].as_slice()[array.offset() * size..];
    Box::new(
        move |mutable: &mut _MutableArrayData, _, start: usize, len: usize| {
            let buffer = &mut mutable.buffer1;
            buffer.extend_from_slice(&values[start * size..(start + len) * size]);
        },
    )
}

// TODO - remove
#[deprecated]
pub(super) fn extend_nulls(mutable: &mut _MutableArrayData, len: usize) {
    let size = match mutable.data_type {
        DataType::FixedSizeBinary(i) => i as usize,
        _ => unreachable!(),
    };

    let values_buffer = &mut mutable.buffer1;
    values_buffer.extend_zeros(len * size);
}



/// Efficiently create an [ArrayData] from one or more existing [ArrayData]s by
/// copying chunks.
///
/// The main use case of this struct is to perform unary operations to arrays of
/// arbitrary types, such as `filter` and `take`.
///
/// # Example
/// ```
/// use arrow_buffer::Buffer;
/// use arrow_data::ArrayData;
/// use arrow_data::transform::{MutableArrayData, SpecializedMutableArrayData};
/// use arrow_schema::DataType;
/// fn i32_array(values: &[i32]) -> ArrayData {
///   ArrayData::try_new(DataType::Int32, 5, None, 0, vec![Buffer::from_slice_ref(values)], vec![]).unwrap()
/// }
/// let arr1  = i32_array(&[1, 2, 3, 4, 5]);
/// let arr2  = i32_array(&[6, 7, 8, 9, 10]);
/// // Create a mutable array for copying values from arr1 and arr2, with a capacity for 6 elements
/// let capacity = 3 * std::mem::size_of::<i32>();
/// let mut mutable = MutableArrayData::new(vec![&arr1, &arr2], false, 10);
/// // Copy the first 3 elements from arr1
/// mutable.extend(0, 0, 3);
/// // Copy the last 3 elements from arr2
/// mutable.extend(1, 2, 4);
/// // Complete the MutableArrayData into a new ArrayData
/// let frozen = mutable.freeze();
/// assert_eq!(frozen, i32_array(&[1, 2, 3, 8, 9, 10]));
/// ```
pub struct FixedSizeBinaryMutableArrayData<'a> {
    /// Input arrays: the data being read FROM.
    ///
    /// Note this is "dead code" because all actual references to the arrays are
    /// stored in closures for extending values and nulls.
    #[allow(dead_code)]
    arrays: Vec<&'a ArrayData>,

    /// In progress output array: The data being written TO
    ///
    /// Note these fields are in a separate struct, [_MutableArrayData], as they
    /// cannot be in [crate::transform::MutableArrayData] itself due to mutability invariants (interior
    /// mutability): [crate::transform::MutableArrayData] contains a function that can only mutate
    /// [_MutableArrayData], not [crate::transform::MutableArrayData] itself
    data: _MutableArrayData<'a>,

    /// function used to extend the output array with nulls from input arrays.
    ///
    /// This function's lifetime is bound to the input arrays because it reads
    /// nulls from it.
    extend_null_bits: Vec<ExtendNullBits<'a>>,
}

impl<'a> std::fmt::Debug for FixedSizeBinaryMutableArrayData<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        // ignores the closures.
        f.debug_struct("FixedSizeBinaryMutableArrayData")
            .field("data", &self.data)
            .finish()
    }
}


impl<'a> FixedSizeBinaryMutableArrayData<'a> {
    // function that extends `[start..start+len]` to the mutable array.
    fn extend_values(&mut self, array_index: usize, start: usize, len: usize) {
        let array = self.arrays[array_index];

        // TODO - can we avoid this by saving the size in the struct? as we know all data types are the same
        let size = match array.data_type() {
            DataType::FixedSizeBinary(i) => *i as usize,
            _ => unreachable!(),
        };


        let values = &array.buffers()[0].as_slice()[array.offset() * size..];
        let buffer = &mut self.data.buffer1;
        buffer.extend_from_slice(&values[start * size..(start + len) * size]);
    }

    /// Similar to [crate::transform::MutableArrayData::new], but lets users define the
    /// preallocated capacities of the array with more granularity.
    ///
    /// See [crate::transform::MutableArrayData::new] for more information on the arguments.
    ///
    /// # Panics
    ///
    /// This function panics if the given `capacities` don't match the data type
    /// of `arrays`. Or when a [crate::transform::Capacities] variant is not yet supported.
    pub fn with_capacities(
        arrays: Vec<&'a ArrayData>,
        use_nulls: bool,
        capacity: usize,
    ) -> Self {
        // TODO - assert fixed size binary
        let data_type = arrays[0].data_type();

        for a in arrays.iter() {
            assert_eq!(
                data_type,
                a.data_type(),
                "Arrays with inconsistent types passed to FixedBinaryMutableArrayData"
            )
        }


        // if any of the arrays has nulls, insertions from any array requires setting bits
        // as there is at least one array with nulls.
        let use_nulls = use_nulls | arrays.iter().any(|array| array.null_count() > 0);

        let mut array_capacity;

        let [buffer1, buffer2] = {
            array_capacity = *capacity;
            new_buffers(data_type, *capacity)
        };

        let extend_null_bits = arrays
            .iter()
            .map(|array| build_extend_null_bits(array, use_nulls))
            .collect();

        let null_buffer = use_nulls.then(|| {
            let null_bytes = bit_util::ceil(array_capacity, 8);
            MutableBuffer::from_len_zeroed(null_bytes)
        });

        let data = _MutableArrayData {
            data_type: data_type.clone(),
            len: 0,
            null_count: 0,
            null_buffer,
            buffer1,
            buffer2,
            child_data: vec![],
        };
        Self {
            arrays,
            data,
            extend_null_bits,
        }
    }
}

impl<'a> SpecializedMutableArrayData<'a> for FixedSizeBinaryMutableArrayData<'a> {
    /// Returns a new [crate::transform::MutableArrayData] with capacity to `capacity` slots and
    /// specialized to create an [ArrayData] from multiple `arrays`.
    ///
    /// # Arguments
    /// * `arrays` - the source arrays to copy from
    /// * `use_nulls` - a flag used to optimize insertions
    ///   - `false` if the only source of nulls are the arrays themselves
    ///   - `true` if the user plans to call [crate::transform::MutableArrayData::extend_nulls].
    /// * capacity - the preallocated capacity of the output array, in bytes
    ///
    /// Thus, if `use_nulls` is `false`, calling
    /// [crate::transform::MutableArrayData::extend_nulls] should not be used.
    fn new(arrays: Vec<&'a ArrayData>, use_nulls: bool, capacity: usize) -> Self {
        Self::with_capacities(arrays, use_nulls, capacity)
    }

    /// Extends the in progress array with a region of the input arrays
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
    fn extend(&mut self, index: usize, start: usize, end: usize) {
        let len = end - start;
        (self.extend_null_bits[index])(&mut self.data, start, len);
        self.extend_values(index, start, len);
        self.data.len += len;
    }

    /// Extends the in progress array with null elements, ignoring the input arrays.
    ///
    /// # Panics
    ///
    /// Panics if [`crate::transform::MutableArrayData`] not created with `use_nulls` or nullable source arrays
    fn extend_nulls(&mut self, len: usize) {
        self.data.len += len;
        let bit_len = bit_util::ceil(self.data.len, 8);
        let nulls = self.data.null_buffer();
        nulls.resize(bit_len, 0);
        self.data.null_count += len;

        // TODO - can we avoid this by saving the size in the struct? as we know all data types are the same
        let size = match self.data.data_type {
            DataType::FixedSizeBinary(i) => i as usize,
            _ => unreachable!(),
        };

        let values_buffer = &mut self.data.buffer1;
        values_buffer.extend_zeros(len * size);
    }

    /// Returns the current length
    #[inline]
    fn len(&self) -> usize {
        self.data.len
    }

    /// Returns true if len is 0
    #[inline]
    fn is_empty(&self) -> bool {
        self.data.len == 0
    }

    /// Returns the current null count
    #[inline]
    fn null_count(&self) -> usize {
        self.data.null_count
    }

    /// Consume self and returns the in progress array as [`ArrayDataBuilder`].
    ///
    /// This is useful for extending the default behavior of MutableArrayData.
    fn into_builder(self) -> ArrayDataBuilder {
        let data = self.data;

        let buffers = vec![];

        let child_data = data.child_data.into_iter().map(|x| x.freeze()).collect();

        let nulls = data
            .null_buffer
            .map(|nulls| {
                let bools = BooleanBuffer::new(nulls.into(), 0, data.len);
                unsafe { NullBuffer::new_unchecked(bools, data.null_count) }
            })
            .filter(|n| n.null_count() > 0);

        ArrayDataBuilder::new(data.data_type)
            .offset(0)
            .len(data.len)
            .nulls(nulls)
            .buffers(buffers)
            .child_data(child_data)
    }
}

