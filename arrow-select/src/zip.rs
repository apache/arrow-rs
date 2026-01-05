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

//! [`zip`]: Combine values from two arrays based on boolean mask

use crate::filter::{SlicesIterator, prep_null_mask_filter};
use arrow_array::cast::AsArray;
use arrow_array::types::{
    BinaryType, BinaryViewType, ByteArrayType, ByteViewType, LargeBinaryType, LargeUtf8Type,
    StringViewType, Utf8Type,
};
use arrow_array::*;
use arrow_buffer::{
    BooleanBuffer, Buffer, MutableBuffer, NullBuffer, OffsetBuffer, OffsetBufferBuilder,
    ScalarBuffer, ToByteSlice,
};
use arrow_data::transform::MutableArrayData;
use arrow_data::{ArrayData, ByteView};
use arrow_schema::{ArrowError, DataType};
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::Not;
use std::sync::Arc;

/// Zip two arrays by some boolean mask.
///
/// - Where `mask` is `true`, values of `truthy` are taken
/// - Where `mask` is `false` or `NULL`, values of `falsy` are taken
///
/// # Example: `zip` two arrays
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{ArrayRef, BooleanArray, Int32Array};
/// # use arrow_select::zip::zip;
/// // mask: [true, true, false, NULL, true]
/// let mask = BooleanArray::from(vec![
///   Some(true), Some(true), Some(false), None, Some(true)
/// ]);
/// // truthy array: [1, NULL, 3, 4, 5]
/// let truthy = Int32Array::from(vec![
///   Some(1), None, Some(3), Some(4), Some(5)
/// ]);
/// // falsy array: [10, 20, 30, 40, 50]
/// let falsy = Int32Array::from(vec![
///   Some(10), Some(20), Some(30), Some(40), Some(50)
/// ]);
/// // zip with this mask select the first, second and last value from `truthy`
/// // and the third and fourth value from `falsy`
/// let result = zip(&mask, &truthy, &falsy).unwrap();
/// // Expected: [1, NULL, 30, 40, 5]
/// let expected: ArrayRef = Arc::new(Int32Array::from(vec![
///   Some(1), None, Some(30), Some(40), Some(5)
/// ]));
/// assert_eq!(&result, &expected);
/// ```
///
/// # Example: `zip` and array with a scalar
///
/// Use `zip` to replace certain values in an array with a scalar
///
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{ArrayRef, BooleanArray, Int32Array};
/// # use arrow_select::zip::zip;
/// // mask: [true, true, false, NULL, true]
/// let mask = BooleanArray::from(vec![
///   Some(true), Some(true), Some(false), None, Some(true)
/// ]);
/// //  array: [1, NULL, 3, 4, 5]
/// let arr = Int32Array::from(vec![
///   Some(1), None, Some(3), Some(4), Some(5)
/// ]);
/// // scalar: 42
/// let scalar = Int32Array::new_scalar(42);
/// // zip the array with the  mask select the first, second and last value from `arr`
/// // and fill the third and fourth value with the scalar 42
/// let result = zip(&mask, &arr, &scalar).unwrap();
/// // Expected: [1, NULL, 42, 42, 5]
/// let expected: ArrayRef = Arc::new(Int32Array::from(vec![
///   Some(1), None, Some(42), Some(42), Some(5)
/// ]));
/// assert_eq!(&result, &expected);
/// ```
pub fn zip(
    mask: &BooleanArray,
    truthy: &dyn Datum,
    falsy: &dyn Datum,
) -> Result<ArrayRef, ArrowError> {
    let (truthy_array, truthy_is_scalar) = truthy.get();
    let (falsy_array, falsy_is_scalar) = falsy.get();

    if falsy_is_scalar && truthy_is_scalar {
        let zipper = ScalarZipper::try_new(truthy, falsy)?;
        return zipper.zip_impl.create_output(mask);
    }

    let truthy = truthy_array;
    let falsy = falsy_array;

    if truthy.data_type() != falsy.data_type() {
        return Err(ArrowError::InvalidArgumentError(
            "arguments need to have the same data type".into(),
        ));
    }

    if truthy_is_scalar && truthy.len() != 1 {
        return Err(ArrowError::InvalidArgumentError(
            "scalar arrays must have 1 element".into(),
        ));
    }
    if !truthy_is_scalar && truthy.len() != mask.len() {
        return Err(ArrowError::InvalidArgumentError(
            "all arrays should have the same length".into(),
        ));
    }
    if falsy_is_scalar && falsy.len() != 1 {
        return Err(ArrowError::InvalidArgumentError(
            "scalar arrays must have 1 element".into(),
        ));
    }
    if !falsy_is_scalar && falsy.len() != mask.len() {
        return Err(ArrowError::InvalidArgumentError(
            "all arrays should have the same length".into(),
        ));
    }

    let falsy = falsy.to_data();
    let truthy = truthy.to_data();

    zip_impl(mask, &truthy, truthy_is_scalar, &falsy, falsy_is_scalar)
}

fn zip_impl(
    mask: &BooleanArray,
    truthy: &ArrayData,
    truthy_is_scalar: bool,
    falsy: &ArrayData,
    falsy_is_scalar: bool,
) -> Result<ArrayRef, ArrowError> {
    let mut mutable = MutableArrayData::new(vec![truthy, falsy], false, truthy.len());

    // the SlicesIterator slices only the true values. So the gaps left by this iterator we need to
    // fill with falsy values

    // keep track of how much is filled
    let mut filled = 0;

    let mask_buffer = maybe_prep_null_mask_filter(mask);
    SlicesIterator::from(&mask_buffer).for_each(|(start, end)| {
        // the gap needs to be filled with falsy values
        if start > filled {
            if falsy_is_scalar {
                for _ in filled..start {
                    // Copy the first item from the 'falsy' array into the output buffer.
                    mutable.extend(1, 0, 1);
                }
            } else {
                mutable.extend(1, filled, start);
            }
        }
        // fill with truthy values
        if truthy_is_scalar {
            for _ in start..end {
                // Copy the first item from the 'truthy' array into the output buffer.
                mutable.extend(0, 0, 1);
            }
        } else {
            mutable.extend(0, start, end);
        }
        filled = end;
    });
    // the remaining part is falsy
    if filled < mask.len() {
        if falsy_is_scalar {
            for _ in filled..mask.len() {
                // Copy the first item from the 'falsy' array into the output buffer.
                mutable.extend(1, 0, 1);
            }
        } else {
            mutable.extend(1, filled, mask.len());
        }
    }

    let data = mutable.freeze();
    Ok(make_array(data))
}

/// Zipper for 2 scalars
///
/// Useful for using in `IF <expr> THEN <scalar> ELSE <scalar> END` expressions
///
/// # Example
/// ```
/// # use std::sync::Arc;
/// # use arrow_array::{ArrayRef, BooleanArray, Int32Array, Scalar, cast::AsArray, types::Int32Type};
///
/// # use arrow_select::zip::ScalarZipper;
/// let scalar_truthy = Scalar::new(Int32Array::from_value(42, 1));
/// let scalar_falsy = Scalar::new(Int32Array::from_value(123, 1));
/// let zipper = ScalarZipper::try_new(&scalar_truthy, &scalar_falsy).unwrap();
///
/// // Later when we have a boolean mask
/// let mask = BooleanArray::from(vec![true, false, true, false, true]);
/// let result = zipper.zip(&mask).unwrap();
/// let actual = result.as_primitive::<Int32Type>();
/// let expected = Int32Array::from(vec![Some(42), Some(123), Some(42), Some(123), Some(42)]);
/// ```
///
#[derive(Debug, Clone)]
pub struct ScalarZipper {
    zip_impl: Arc<dyn ZipImpl>,
}

impl ScalarZipper {
    /// Try to create a new ScalarZipper from two scalar Datum
    ///
    /// # Errors
    /// returns error if:
    /// - the two Datum have different data types
    /// - either Datum is not a scalar (or has more than 1 element)
    ///
    pub fn try_new(truthy: &dyn Datum, falsy: &dyn Datum) -> Result<Self, ArrowError> {
        let (truthy, truthy_is_scalar) = truthy.get();
        let (falsy, falsy_is_scalar) = falsy.get();

        if truthy.data_type() != falsy.data_type() {
            return Err(ArrowError::InvalidArgumentError(
                "arguments need to have the same data type".into(),
            ));
        }

        if !truthy_is_scalar {
            return Err(ArrowError::InvalidArgumentError(
                "only scalar arrays are supported".into(),
            ));
        }

        if !falsy_is_scalar {
            return Err(ArrowError::InvalidArgumentError(
                "only scalar arrays are supported".into(),
            ));
        }

        if truthy.len() != 1 {
            return Err(ArrowError::InvalidArgumentError(
                "scalar arrays must have 1 element".into(),
            ));
        }
        if falsy.len() != 1 {
            return Err(ArrowError::InvalidArgumentError(
                "scalar arrays must have 1 element".into(),
            ));
        }

        macro_rules! primitive_size_helper {
            ($t:ty) => {
                Arc::new(PrimitiveScalarImpl::<$t>::new(truthy, falsy)) as Arc<dyn ZipImpl>
            };
        }

        let zip_impl = downcast_primitive! {
            truthy.data_type() => (primitive_size_helper),
            DataType::Utf8 => {
                Arc::new(BytesScalarImpl::<Utf8Type>::new(truthy, falsy)) as Arc<dyn ZipImpl>
            },
            DataType::LargeUtf8 => {
                Arc::new(BytesScalarImpl::<LargeUtf8Type>::new(truthy, falsy)) as Arc<dyn ZipImpl>
            },
            DataType::Binary => {
                Arc::new(BytesScalarImpl::<BinaryType>::new(truthy, falsy)) as Arc<dyn ZipImpl>
            },
            DataType::LargeBinary => {
                Arc::new(BytesScalarImpl::<LargeBinaryType>::new(truthy, falsy)) as Arc<dyn ZipImpl>
            },
            DataType::Utf8View => {
                Arc::new(ByteViewScalarImpl::<StringViewType>::new(truthy, falsy)) as Arc<dyn ZipImpl>
            },
            DataType::BinaryView => {
                Arc::new(ByteViewScalarImpl::<BinaryViewType>::new(truthy, falsy)) as Arc<dyn ZipImpl>
            },
            _ => {
                Arc::new(FallbackImpl::new(truthy, falsy)) as Arc<dyn ZipImpl>
            },
        };

        Ok(Self { zip_impl })
    }

    /// Creating output array based on input boolean array and the two scalar values the zipper was created with
    /// See struct level documentation for examples.
    pub fn zip(&self, mask: &BooleanArray) -> Result<ArrayRef, ArrowError> {
        self.zip_impl.create_output(mask)
    }
}

/// Impl for creating output array based on a mask
trait ZipImpl: Debug + Send + Sync {
    /// Creating output array based on input boolean array
    fn create_output(&self, input: &BooleanArray) -> Result<ArrayRef, ArrowError>;
}

#[derive(Debug, PartialEq)]
struct FallbackImpl {
    truthy: ArrayData,
    falsy: ArrayData,
}

impl FallbackImpl {
    fn new(left: &dyn Array, right: &dyn Array) -> Self {
        Self {
            truthy: left.to_data(),
            falsy: right.to_data(),
        }
    }
}

impl ZipImpl for FallbackImpl {
    fn create_output(&self, predicate: &BooleanArray) -> Result<ArrayRef, ArrowError> {
        zip_impl(predicate, &self.truthy, true, &self.falsy, true)
    }
}

struct PrimitiveScalarImpl<T: ArrowPrimitiveType> {
    data_type: DataType,
    truthy: Option<T::Native>,
    falsy: Option<T::Native>,
}

impl<T: ArrowPrimitiveType> Debug for PrimitiveScalarImpl<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrimitiveScalarImpl")
            .field("data_type", &self.data_type)
            .field("truthy", &self.truthy)
            .field("falsy", &self.falsy)
            .finish()
    }
}

impl<T: ArrowPrimitiveType> PrimitiveScalarImpl<T> {
    fn new(truthy: &dyn Array, falsy: &dyn Array) -> Self {
        Self {
            data_type: truthy.data_type().clone(),
            truthy: Self::get_value_from_scalar(truthy),
            falsy: Self::get_value_from_scalar(falsy),
        }
    }

    fn get_value_from_scalar(scalar: &dyn Array) -> Option<T::Native> {
        if scalar.is_null(0) {
            None
        } else {
            let value = scalar.as_primitive::<T>().value(0);

            Some(value)
        }
    }

    /// return an output array that has
    /// `value` in all locations where predicate is true
    /// `null` otherwise
    fn get_scalar_and_null_buffer_for_single_non_nullable(
        predicate: BooleanBuffer,
        value: T::Native,
    ) -> (Vec<T::Native>, Option<NullBuffer>) {
        let result_len = predicate.len();
        let nulls = NullBuffer::new(predicate);
        let scalars = vec![value; result_len];

        (scalars, Some(nulls))
    }
}

impl<T: ArrowPrimitiveType> ZipImpl for PrimitiveScalarImpl<T> {
    fn create_output(&self, predicate: &BooleanArray) -> Result<ArrayRef, ArrowError> {
        let result_len = predicate.len();
        // Nulls are treated as false
        let predicate = maybe_prep_null_mask_filter(predicate);

        let (scalars, nulls): (Vec<T::Native>, Option<NullBuffer>) = match (self.truthy, self.falsy)
        {
            (Some(truthy_val), Some(falsy_val)) => {
                let scalars: Vec<T::Native> = predicate
                    .iter()
                    .map(|b| if b { truthy_val } else { falsy_val })
                    .collect();

                (scalars, None)
            }
            (Some(truthy_val), None) => {
                // If a value is true we need the TRUTHY and the null buffer will have 1 (meaning not null)
                // If a value is false we need the FALSY and the null buffer will have 0 (meaning null)

                Self::get_scalar_and_null_buffer_for_single_non_nullable(predicate, truthy_val)
            }
            (None, Some(falsy_val)) => {
                // Flipping the boolean buffer as we want the opposite of the TRUE case
                //
                // if the condition is true we want null so we need to NOT the value so we get 0 (meaning null)
                // if the condition is false we want the FALSY value so we need to NOT the value so we get 1 (meaning not null)
                let predicate = predicate.not();

                Self::get_scalar_and_null_buffer_for_single_non_nullable(predicate, falsy_val)
            }
            (None, None) => {
                // All values are null
                let nulls = NullBuffer::new_null(result_len);
                let scalars = vec![T::default_value(); result_len];

                (scalars, Some(nulls))
            }
        };

        let scalars = ScalarBuffer::<T::Native>::from(scalars);
        let output = PrimitiveArray::<T>::try_new(scalars, nulls)?;

        // Keep decimal precisions, scales or timestamps timezones
        let output = output.with_data_type(self.data_type.clone());

        Ok(Arc::new(output))
    }
}

#[derive(PartialEq, Hash)]
struct BytesScalarImpl<T: ByteArrayType> {
    truthy: Option<Vec<u8>>,
    falsy: Option<Vec<u8>>,
    phantom: PhantomData<T>,
}

impl<T: ByteArrayType> Debug for BytesScalarImpl<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BytesScalarImpl")
            .field("truthy", &self.truthy)
            .field("falsy", &self.falsy)
            .finish()
    }
}

impl<T: ByteArrayType> BytesScalarImpl<T> {
    fn new(truthy_value: &dyn Array, falsy_value: &dyn Array) -> Self {
        Self {
            truthy: Self::get_value_from_scalar(truthy_value),
            falsy: Self::get_value_from_scalar(falsy_value),
            phantom: PhantomData,
        }
    }

    fn get_value_from_scalar(scalar: &dyn Array) -> Option<Vec<u8>> {
        if scalar.is_null(0) {
            None
        } else {
            let bytes: &[u8] = scalar.as_bytes::<T>().value(0).as_ref();

            Some(bytes.to_vec())
        }
    }

    /// return an output array that has
    /// `value` in all locations where predicate is true
    /// `null` otherwise
    fn get_scalar_and_null_buffer_for_single_non_nullable(
        predicate: BooleanBuffer,
        value: &[u8],
    ) -> (Buffer, OffsetBuffer<T::Offset>, Option<NullBuffer>) {
        let value_length = value.len();

        let number_of_true = predicate.count_set_bits();

        // Fast path for all nulls
        if number_of_true == 0 {
            // All values are null
            let nulls = NullBuffer::new_null(predicate.len());

            return (
                // Empty bytes
                Buffer::from(&[]),
                // All nulls so all lengths are 0
                OffsetBuffer::<T::Offset>::new_zeroed(predicate.len()),
                Some(nulls),
            );
        }

        let offsets = OffsetBuffer::<T::Offset>::from_lengths(
            predicate.iter().map(|b| if b { value_length } else { 0 }),
        );

        let mut bytes = MutableBuffer::with_capacity(0);
        bytes.repeat_slice_n_times(value, number_of_true);

        let bytes = Buffer::from(bytes);

        // If a value is true we need the TRUTHY and the null buffer will have 1 (meaning not null)
        // If a value is false we need the FALSY and the null buffer will have 0 (meaning null)
        let nulls = NullBuffer::new(predicate);

        (bytes, offsets, Some(nulls))
    }

    /// Create a [`Buffer`] where `value` slice is repeated `number_of_values` times
    /// and [`OffsetBuffer`] where there are `number_of_values` lengths, and all equals to `value` length
    fn get_bytes_and_offset_for_all_same_value(
        number_of_values: usize,
        value: &[u8],
    ) -> (Buffer, OffsetBuffer<T::Offset>) {
        let value_length = value.len();

        let offsets =
            OffsetBuffer::<T::Offset>::from_repeated_length(value_length, number_of_values);

        let mut bytes = MutableBuffer::with_capacity(0);
        bytes.repeat_slice_n_times(value, number_of_values);
        let bytes = Buffer::from(bytes);

        (bytes, offsets)
    }

    fn create_output_on_non_nulls(
        predicate: &BooleanBuffer,
        truthy_val: &[u8],
        falsy_val: &[u8],
    ) -> (Buffer, OffsetBuffer<<T as ByteArrayType>::Offset>) {
        let true_count = predicate.count_set_bits();

        match true_count {
            0 => {
                // All values are falsy

                let (bytes, offsets) =
                    Self::get_bytes_and_offset_for_all_same_value(predicate.len(), falsy_val);

                return (bytes, offsets);
            }
            n if n == predicate.len() => {
                // All values are truthy
                let (bytes, offsets) =
                    Self::get_bytes_and_offset_for_all_same_value(predicate.len(), truthy_val);

                return (bytes, offsets);
            }

            _ => {
                // Fallback
            }
        }

        let total_number_of_bytes =
            true_count * truthy_val.len() + (predicate.len() - true_count) * falsy_val.len();
        let mut mutable = MutableBuffer::with_capacity(total_number_of_bytes);
        let mut offset_buffer_builder = OffsetBufferBuilder::<T::Offset>::new(predicate.len());

        // keep track of how much is filled
        let mut filled = 0;

        let truthy_len = truthy_val.len();
        let falsy_len = falsy_val.len();

        SlicesIterator::from(predicate).for_each(|(start, end)| {
            // the gap needs to be filled with falsy values
            if start > filled {
                let false_repeat_count = start - filled;
                // Push false value `repeat_count` times
                mutable.repeat_slice_n_times(falsy_val, false_repeat_count);

                for _ in 0..false_repeat_count {
                    offset_buffer_builder.push_length(falsy_len)
                }
            }

            let true_repeat_count = end - start;
            // fill with truthy values
            mutable.repeat_slice_n_times(truthy_val, true_repeat_count);

            for _ in 0..true_repeat_count {
                offset_buffer_builder.push_length(truthy_len)
            }
            filled = end;
        });
        // the remaining part is falsy
        if filled < predicate.len() {
            let false_repeat_count = predicate.len() - filled;
            // Copy the first item from the 'falsy' array into the output buffer.
            mutable.repeat_slice_n_times(falsy_val, false_repeat_count);

            for _ in 0..false_repeat_count {
                offset_buffer_builder.push_length(falsy_len)
            }
        }

        (mutable.into(), offset_buffer_builder.finish())
    }
}

impl<T: ByteArrayType> ZipImpl for BytesScalarImpl<T> {
    fn create_output(&self, predicate: &BooleanArray) -> Result<ArrayRef, ArrowError> {
        let result_len = predicate.len();
        // Nulls are treated as false
        let predicate = maybe_prep_null_mask_filter(predicate);

        let (bytes, offsets, nulls): (Buffer, OffsetBuffer<T::Offset>, Option<NullBuffer>) =
            match (self.truthy.as_deref(), self.falsy.as_deref()) {
                (Some(truthy_val), Some(falsy_val)) => {
                    let (bytes, offsets) =
                        Self::create_output_on_non_nulls(&predicate, truthy_val, falsy_val);

                    (bytes, offsets, None)
                }
                (Some(truthy_val), None) => {
                    Self::get_scalar_and_null_buffer_for_single_non_nullable(predicate, truthy_val)
                }
                (None, Some(falsy_val)) => {
                    // Flipping the boolean buffer as we want the opposite of the TRUE case
                    //
                    // if the condition is true we want null so we need to NOT the value so we get 0 (meaning null)
                    // if the condition is false we want the FALSE value so we need to NOT the value so we get 1 (meaning not null)
                    let predicate = predicate.not();
                    Self::get_scalar_and_null_buffer_for_single_non_nullable(predicate, falsy_val)
                }
                (None, None) => {
                    // All values are null
                    let nulls = NullBuffer::new_null(result_len);

                    (
                        // Empty bytes
                        Buffer::from(&[]),
                        // All nulls so all lengths are 0
                        OffsetBuffer::<T::Offset>::new_zeroed(predicate.len()),
                        Some(nulls),
                    )
                }
            };

        let output = unsafe {
            // Safety: the values are based on valid inputs
            // and `try_new` is expensive for strings as it validate that the input is valid utf8
            GenericByteArray::<T>::new_unchecked(offsets, bytes, nulls)
        };

        Ok(Arc::new(output))
    }
}

fn maybe_prep_null_mask_filter(predicate: &BooleanArray) -> BooleanBuffer {
    // Nulls are treated as false
    if predicate.null_count() == 0 {
        predicate.values().clone()
    } else {
        let cleaned = prep_null_mask_filter(predicate);
        let (boolean_buffer, _) = cleaned.into_parts();
        boolean_buffer
    }
}

struct ByteViewScalarImpl<T: ByteViewType> {
    truthy_view: Option<u128>,
    truthy_buffers: Vec<Buffer>,
    falsy_view: Option<u128>,
    falsy_buffers: Vec<Buffer>,
    phantom: PhantomData<T>,
}

impl<T: ByteViewType> ByteViewScalarImpl<T> {
    fn new(truthy: &dyn Array, falsy: &dyn Array) -> Self {
        let (truthy_view, truthy_buffers) = Self::get_value_from_scalar(truthy);
        let (falsy_view, falsy_buffers) = Self::get_value_from_scalar(falsy);
        Self {
            truthy_view,
            truthy_buffers,
            falsy_view,
            falsy_buffers,
            phantom: PhantomData,
        }
    }

    fn get_value_from_scalar(scalar: &dyn Array) -> (Option<u128>, Vec<Buffer>) {
        if scalar.is_null(0) {
            (None, vec![])
        } else {
            let (views, buffers, _) = scalar.as_byte_view::<T>().clone().into_parts();
            (views.first().copied(), buffers)
        }
    }

    fn get_views_for_single_non_nullable(
        predicate: BooleanBuffer,
        value: u128,
        buffers: Vec<Buffer>,
    ) -> (ScalarBuffer<u128>, Vec<Buffer>, Option<NullBuffer>) {
        let number_of_true = predicate.count_set_bits();
        let number_of_values = predicate.len();

        // Fast path for all nulls
        if number_of_true == 0 {
            // All values are null
            return (
                vec![0; number_of_values].into(),
                vec![],
                Some(NullBuffer::new_null(number_of_values)),
            );
        }
        let bytes = vec![value; number_of_values];

        // If value is true and we want to handle the TRUTHY case, the null buffer will have 1 (meaning not null)
        // If value is false and we want to handle the FALSY case, the null buffer will have 0 (meaning null)
        let nulls = NullBuffer::new(predicate);
        (bytes.into(), buffers, Some(nulls))
    }

    fn get_views_for_non_nullable(
        predicate: BooleanBuffer,
        result_len: usize,
        truthy_view: u128,
        truthy_buffers: Vec<Buffer>,
        falsy_view: u128,
        falsy_buffers: Vec<Buffer>,
    ) -> (ScalarBuffer<u128>, Vec<Buffer>, Option<NullBuffer>) {
        let true_count = predicate.count_set_bits();
        match true_count {
            0 => {
                // all values are falsy
                (vec![falsy_view; result_len].into(), falsy_buffers, None)
            }
            n if n == predicate.len() => {
                // all values are truthy
                (vec![truthy_view; result_len].into(), truthy_buffers, None)
            }
            _ => {
                let true_count = predicate.count_set_bits();
                let mut buffers: Vec<Buffer> = truthy_buffers.to_vec();

                // If the falsy buffers are empty, we can use the falsy view as it is, because the value
                // is completely inlined. Otherwise, we have non-inlined values in the buffer, and we need
                // to recalculate the falsy view
                let view_falsy = if falsy_buffers.is_empty() {
                    falsy_view
                } else {
                    let byte_view_falsy = ByteView::from(falsy_view);
                    let new_index_falsy_buffers =
                        buffers.len() as u32 + byte_view_falsy.buffer_index;
                    buffers.extend(falsy_buffers);
                    let byte_view_falsy =
                        byte_view_falsy.with_buffer_index(new_index_falsy_buffers);
                    byte_view_falsy.as_u128()
                };

                let total_number_of_bytes = true_count * 16 + (predicate.len() - true_count) * 16;
                let mut mutable = MutableBuffer::new(total_number_of_bytes);
                let mut filled = 0;

                SlicesIterator::from(&predicate).for_each(|(start, end)| {
                    if start > filled {
                        let false_repeat_count = start - filled;
                        mutable
                            .repeat_slice_n_times(view_falsy.to_byte_slice(), false_repeat_count);
                    }
                    let true_repeat_count = end - start;
                    mutable.repeat_slice_n_times(truthy_view.to_byte_slice(), true_repeat_count);
                    filled = end;
                });

                if filled < predicate.len() {
                    let false_repeat_count = predicate.len() - filled;
                    mutable.repeat_slice_n_times(view_falsy.to_byte_slice(), false_repeat_count);
                }

                let bytes = Buffer::from(mutable);
                (bytes.into(), buffers, None)
            }
        }
    }
}

impl<T: ByteViewType> Debug for ByteViewScalarImpl<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ByteViewScalarImpl")
            .field("truthy", &self.truthy_view)
            .field("falsy", &self.falsy_view)
            .finish()
    }
}

impl<T: ByteViewType> ZipImpl for ByteViewScalarImpl<T> {
    fn create_output(&self, predicate: &BooleanArray) -> Result<ArrayRef, ArrowError> {
        let result_len = predicate.len();
        // Nulls are treated as false
        let predicate = maybe_prep_null_mask_filter(predicate);

        let (views, buffers, nulls) = match (self.truthy_view, self.falsy_view) {
            (Some(truthy), Some(falsy)) => Self::get_views_for_non_nullable(
                predicate,
                result_len,
                truthy,
                self.truthy_buffers.clone(),
                falsy,
                self.falsy_buffers.clone(),
            ),
            (Some(truthy), None) => Self::get_views_for_single_non_nullable(
                predicate,
                truthy,
                self.truthy_buffers.clone(),
            ),
            (None, Some(falsy)) => {
                let predicate = predicate.not();
                Self::get_views_for_single_non_nullable(
                    predicate,
                    falsy,
                    self.falsy_buffers.clone(),
                )
            }
            (None, None) => {
                // All values are null
                (
                    vec![0; result_len].into(),
                    vec![],
                    Some(NullBuffer::new_null(result_len)),
                )
            }
        };

        let result = unsafe { GenericByteViewArray::<T>::new_unchecked(views, buffers, nulls) };
        Ok(Arc::new(result))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow_array::types::Int32Type;

    #[test]
    fn test_zip_kernel_one() {
        let a = Int32Array::from(vec![Some(5), None, Some(7), None, Some(1)]);
        let b = Int32Array::from(vec![None, Some(3), Some(6), Some(7), Some(3)]);
        let mask = BooleanArray::from(vec![true, true, false, false, true]);
        let out = zip(&mask, &a, &b).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![Some(5), None, Some(6), Some(7), Some(1)]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_two() {
        let a = Int32Array::from(vec![Some(5), None, Some(7), None, Some(1)]);
        let b = Int32Array::from(vec![None, Some(3), Some(6), Some(7), Some(3)]);
        let mask = BooleanArray::from(vec![false, false, true, true, false]);
        let out = zip(&mask, &a, &b).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![None, Some(3), Some(7), None, Some(3)]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_falsy_1() {
        let a = Int32Array::from(vec![Some(5), None, Some(7), None, Some(1)]);

        let fallback = Scalar::new(Int32Array::from_value(42, 1));

        let mask = BooleanArray::from(vec![true, true, false, false, true]);
        let out = zip(&mask, &a, &fallback).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![Some(5), None, Some(42), Some(42), Some(1)]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_falsy_2() {
        let a = Int32Array::from(vec![Some(5), None, Some(7), None, Some(1)]);

        let fallback = Scalar::new(Int32Array::from_value(42, 1));

        let mask = BooleanArray::from(vec![false, false, true, true, false]);
        let out = zip(&mask, &a, &fallback).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![Some(42), Some(42), Some(7), None, Some(42)]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_truthy_1() {
        let a = Int32Array::from(vec![Some(5), None, Some(7), None, Some(1)]);

        let fallback = Scalar::new(Int32Array::from_value(42, 1));

        let mask = BooleanArray::from(vec![true, true, false, false, true]);
        let out = zip(&mask, &fallback, &a).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![Some(42), Some(42), Some(7), None, Some(42)]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_truthy_2() {
        let a = Int32Array::from(vec![Some(5), None, Some(7), None, Some(1)]);

        let fallback = Scalar::new(Int32Array::from_value(42, 1));

        let mask = BooleanArray::from(vec![false, false, true, true, false]);
        let out = zip(&mask, &fallback, &a).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![Some(5), None, Some(42), Some(42), Some(1)]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_both_mask_ends_with_true() {
        let scalar_truthy = Scalar::new(Int32Array::from_value(42, 1));
        let scalar_falsy = Scalar::new(Int32Array::from_value(123, 1));

        let mask = BooleanArray::from(vec![true, true, false, false, true]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![Some(42), Some(42), Some(123), Some(123), Some(42)]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_both_mask_ends_with_false() {
        let scalar_truthy = Scalar::new(Int32Array::from_value(42, 1));
        let scalar_falsy = Scalar::new(Int32Array::from_value(123, 1));

        let mask = BooleanArray::from(vec![true, true, false, true, false, false]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![
            Some(42),
            Some(42),
            Some(123),
            Some(42),
            Some(123),
            Some(123),
        ]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_primitive_scalar_none_1() {
        let scalar_truthy = Scalar::new(Int32Array::from_value(42, 1));
        let scalar_falsy = Scalar::new(Int32Array::new_null(1));

        let mask = BooleanArray::from(vec![true, true, false, false, true]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![Some(42), Some(42), None, None, Some(42)]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_primitive_scalar_none_2() {
        let scalar_truthy = Scalar::new(Int32Array::from_value(42, 1));
        let scalar_falsy = Scalar::new(Int32Array::new_null(1));

        let mask = BooleanArray::from(vec![false, false, true, true, false]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![None, None, Some(42), Some(42), None]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_primitive_scalar_both_null() {
        let scalar_truthy = Scalar::new(Int32Array::new_null(1));
        let scalar_falsy = Scalar::new(Int32Array::new_null(1));

        let mask = BooleanArray::from(vec![false, false, true, true, false]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![None, None, None, None, None]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_primitive_array_with_nulls_is_mask_should_be_treated_as_false() {
        let truthy = Int32Array::from_iter_values(vec![1, 2, 3, 4, 5, 6]);
        let falsy = Int32Array::from_iter_values(vec![7, 8, 9, 10, 11, 12]);

        let mask = {
            let booleans = BooleanBuffer::from(vec![true, true, false, true, false, false]);
            let nulls = NullBuffer::from(vec![
                true, true, true,
                false, // null treated as false even though in the original mask it was true
                true, true,
            ]);
            BooleanArray::new(booleans, Some(nulls))
        };
        let out = zip(&mask, &truthy, &falsy).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![
            Some(1),
            Some(2),
            Some(9),
            Some(10), // true in mask but null
            Some(11),
            Some(12),
        ]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_primitive_scalar_with_boolean_array_mask_with_nulls_should_be_treated_as_false()
     {
        let scalar_truthy = Scalar::new(Int32Array::from_value(42, 1));
        let scalar_falsy = Scalar::new(Int32Array::from_value(123, 1));

        let mask = {
            let booleans = BooleanBuffer::from(vec![true, true, false, true, false, false]);
            let nulls = NullBuffer::from(vec![
                true, true, true,
                false, // null treated as false even though in the original mask it was true
                true, true,
            ]);
            BooleanArray::new(booleans, Some(nulls))
        };
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![
            Some(42),
            Some(42),
            Some(123),
            Some(123), // true in mask but null
            Some(123),
            Some(123),
        ]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_string_array_with_nulls_is_mask_should_be_treated_as_false() {
        let truthy = StringArray::from_iter_values(vec!["1", "2", "3", "4", "5", "6"]);
        let falsy = StringArray::from_iter_values(vec!["7", "8", "9", "10", "11", "12"]);

        let mask = {
            let booleans = BooleanBuffer::from(vec![true, true, false, true, false, false]);
            let nulls = NullBuffer::from(vec![
                true, true, true,
                false, // null treated as false even though in the original mask it was true
                true, true,
            ]);
            BooleanArray::new(booleans, Some(nulls))
        };
        let out = zip(&mask, &truthy, &falsy).unwrap();
        let actual = out.as_string::<i32>();
        let expected = StringArray::from_iter_values(vec![
            "1", "2", "9", "10", // true in mask but null
            "11", "12",
        ]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_large_string_scalar_with_boolean_array_mask_with_nulls_should_be_treated_as_false()
     {
        let scalar_truthy = Scalar::new(LargeStringArray::from_iter_values(["test"]));
        let scalar_falsy = Scalar::new(LargeStringArray::from_iter_values(["something else"]));

        let mask = {
            let booleans = BooleanBuffer::from(vec![true, true, false, true, false, false]);
            let nulls = NullBuffer::from(vec![
                true, true, true,
                false, // null treated as false even though in the original mask it was true
                true, true,
            ]);
            BooleanArray::new(booleans, Some(nulls))
        };
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_any().downcast_ref::<LargeStringArray>().unwrap();
        let expected = LargeStringArray::from_iter(vec![
            Some("test"),
            Some("test"),
            Some("something else"),
            Some("something else"), // true in mask but null
            Some("something else"),
            Some("something else"),
        ]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_bytes_scalar_none_1() {
        let scalar_truthy = Scalar::new(StringArray::from_iter_values(["hello"]));
        let scalar_falsy = Scalar::new(StringArray::new_null(1));

        let mask = BooleanArray::from(vec![true, true, false, false, true]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_any().downcast_ref::<StringArray>().unwrap();
        let expected = StringArray::from_iter(vec![
            Some("hello"),
            Some("hello"),
            None,
            None,
            Some("hello"),
        ]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_bytes_scalar_none_2() {
        let scalar_truthy = Scalar::new(StringArray::new_null(1));
        let scalar_falsy = Scalar::new(StringArray::from_iter_values(["hello"]));

        let mask = BooleanArray::from(vec![true, true, false, false, true]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_any().downcast_ref::<StringArray>().unwrap();
        let expected = StringArray::from_iter(vec![None, None, Some("hello"), Some("hello"), None]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_bytes_scalar_both() {
        let scalar_truthy = Scalar::new(StringArray::from_iter_values(["test"]));
        let scalar_falsy = Scalar::new(StringArray::from_iter_values(["something else"]));

        // mask ends with false
        let mask = BooleanArray::from(vec![true, true, false, true, false, false]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_any().downcast_ref::<StringArray>().unwrap();
        let expected = StringArray::from_iter(vec![
            Some("test"),
            Some("test"),
            Some("something else"),
            Some("test"),
            Some("something else"),
            Some("something else"),
        ]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_scalar_bytes_only_taking_one_side() {
        let mask_len = 5;
        let all_true_mask = BooleanArray::from(vec![true; mask_len]);
        let all_false_mask = BooleanArray::from(vec![false; mask_len]);

        let null_scalar = Scalar::new(StringArray::new_null(1));
        let non_null_scalar_1 = Scalar::new(StringArray::from_iter_values(["test"]));
        let non_null_scalar_2 = Scalar::new(StringArray::from_iter_values(["something else"]));

        {
            // 1. Test where left is null and right is non-null
            //    and mask is all true
            let out = zip(&all_true_mask, &null_scalar, &non_null_scalar_1).unwrap();
            let actual = out.as_string::<i32>();
            let expected = StringArray::from_iter(std::iter::repeat_n(None::<&str>, mask_len));
            assert_eq!(actual, &expected);
        }

        {
            // 2. Test where left is null and right is non-null
            //    and mask is all false
            let out = zip(&all_false_mask, &null_scalar, &non_null_scalar_1).unwrap();
            let actual = out.as_string::<i32>();
            let expected = StringArray::from_iter(std::iter::repeat_n(Some("test"), mask_len));
            assert_eq!(actual, &expected);
        }

        {
            // 3. Test where left is non-null and right is null
            //    and mask is all true
            let out = zip(&all_true_mask, &non_null_scalar_1, &null_scalar).unwrap();
            let actual = out.as_string::<i32>();
            let expected = StringArray::from_iter(std::iter::repeat_n(Some("test"), mask_len));
            assert_eq!(actual, &expected);
        }

        {
            // 4. Test where left is non-null and right is null
            //    and mask is all false
            let out = zip(&all_false_mask, &non_null_scalar_1, &null_scalar).unwrap();
            let actual = out.as_string::<i32>();
            let expected = StringArray::from_iter(std::iter::repeat_n(None::<&str>, mask_len));
            assert_eq!(actual, &expected);
        }

        {
            // 5. Test where both left and right are not null
            //    and mask is all true
            let out = zip(&all_true_mask, &non_null_scalar_1, &non_null_scalar_2).unwrap();
            let actual = out.as_string::<i32>();
            let expected = StringArray::from_iter(std::iter::repeat_n(Some("test"), mask_len));
            assert_eq!(actual, &expected);
        }

        {
            // 6. Test where both left and right are not null
            //    and mask is all false
            let out = zip(&all_false_mask, &non_null_scalar_1, &non_null_scalar_2).unwrap();
            let actual = out.as_string::<i32>();
            let expected =
                StringArray::from_iter(std::iter::repeat_n(Some("something else"), mask_len));
            assert_eq!(actual, &expected);
        }

        {
            // 7. Test where both left and right are null
            //    and mask is random
            let mask = BooleanArray::from(vec![true, false, true, false, true]);
            let out = zip(&mask, &null_scalar, &null_scalar).unwrap();
            let actual = out.as_string::<i32>();
            let expected = StringArray::from_iter(std::iter::repeat_n(None::<&str>, mask_len));
            assert_eq!(actual, &expected);
        }
    }

    #[test]
    fn test_scalar_zipper() {
        let scalar_truthy = Scalar::new(Int32Array::from_value(42, 1));
        let scalar_falsy = Scalar::new(Int32Array::from_value(123, 1));

        let mask = BooleanArray::from(vec![false, false, true, true, false]);

        let scalar_zipper = ScalarZipper::try_new(&scalar_truthy, &scalar_falsy).unwrap();
        let out = scalar_zipper.zip(&mask).unwrap();
        let actual = out.as_primitive::<Int32Type>();
        let expected = Int32Array::from(vec![Some(123), Some(123), Some(42), Some(42), Some(123)]);
        assert_eq!(actual, &expected);

        // test with different mask length as well
        let mask = BooleanArray::from(vec![true, false, true]);
        let out = scalar_zipper.zip(&mask).unwrap();
        let actual = out.as_primitive::<Int32Type>();
        let expected = Int32Array::from(vec![Some(42), Some(123), Some(42)]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_strings() {
        let scalar_truthy = Scalar::new(StringArray::from(vec!["hello"]));
        let scalar_falsy = Scalar::new(StringArray::from(vec!["world"]));

        let mask = BooleanArray::from(vec![true, false, true, false, true]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_string::<i32>();
        let expected = StringArray::from(vec![
            Some("hello"),
            Some("world"),
            Some("hello"),
            Some("world"),
            Some("hello"),
        ]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_binary() {
        let truthy_bytes: &[u8] = b"\xFF\xFE\xFD";
        let falsy_bytes: &[u8] = b"world";
        let scalar_truthy = Scalar::new(BinaryArray::from_iter_values(
            // Non valid UTF8 bytes
            vec![truthy_bytes],
        ));
        let scalar_falsy = Scalar::new(BinaryArray::from_iter_values(vec![falsy_bytes]));

        let mask = BooleanArray::from(vec![true, false, true, false, true]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_binary::<i32>();
        let expected = BinaryArray::from(vec![
            Some(truthy_bytes),
            Some(falsy_bytes),
            Some(truthy_bytes),
            Some(falsy_bytes),
            Some(truthy_bytes),
        ]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_large_binary() {
        let truthy_bytes: &[u8] = b"hey";
        let falsy_bytes: &[u8] = b"world";
        let scalar_truthy = Scalar::new(LargeBinaryArray::from_iter_values(vec![truthy_bytes]));
        let scalar_falsy = Scalar::new(LargeBinaryArray::from_iter_values(vec![falsy_bytes]));

        let mask = BooleanArray::from(vec![true, false, true, false, true]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_binary::<i64>();
        let expected = LargeBinaryArray::from(vec![
            Some(truthy_bytes),
            Some(falsy_bytes),
            Some(truthy_bytes),
            Some(falsy_bytes),
            Some(truthy_bytes),
        ]);
        assert_eq!(actual, &expected);
    }

    // Test to ensure that the precision and scale are kept when zipping Decimal128 data
    #[test]
    fn test_zip_decimal_with_custom_precision_and_scale() {
        let arr = Decimal128Array::from_iter_values([12345, 456, 7890, -123223423432432])
            .with_precision_and_scale(20, 2)
            .unwrap();

        let arr: ArrayRef = Arc::new(arr);

        let scalar_1 = Scalar::new(arr.slice(0, 1));
        let scalar_2 = Scalar::new(arr.slice(1, 1));
        let null_scalar = Scalar::new(new_null_array(arr.data_type(), 1));
        let array_1: ArrayRef = arr.slice(0, 2);
        let array_2: ArrayRef = arr.slice(2, 2);

        test_zip_output_data_types_for_input(scalar_1, scalar_2, null_scalar, array_1, array_2);
    }

    // Test to ensure that the timezone is kept when zipping TimestampArray data
    #[test]
    fn test_zip_timestamp_with_timezone() {
        let arr = TimestampSecondArray::from(vec![0, 1000, 2000, 4000])
            .with_timezone("+01:00".to_string());

        let arr: ArrayRef = Arc::new(arr);

        let scalar_1 = Scalar::new(arr.slice(0, 1));
        let scalar_2 = Scalar::new(arr.slice(1, 1));
        let null_scalar = Scalar::new(new_null_array(arr.data_type(), 1));
        let array_1: ArrayRef = arr.slice(0, 2);
        let array_2: ArrayRef = arr.slice(2, 2);

        test_zip_output_data_types_for_input(scalar_1, scalar_2, null_scalar, array_1, array_2);
    }

    fn test_zip_output_data_types_for_input(
        scalar_1: Scalar<ArrayRef>,
        scalar_2: Scalar<ArrayRef>,
        null_scalar: Scalar<ArrayRef>,
        array_1: ArrayRef,
        array_2: ArrayRef,
    ) {
        // non null Scalar vs non null Scalar
        test_zip_output_data_type(&scalar_1, &scalar_2, 10);

        // null Scalar vs non-null Scalar (and vice versa)
        test_zip_output_data_type(&null_scalar, &scalar_1, 10);
        test_zip_output_data_type(&scalar_1, &null_scalar, 10);

        // non-null Scalar and array (and vice versa)
        test_zip_output_data_type(&array_1.as_ref(), &scalar_1, array_1.len());
        test_zip_output_data_type(&scalar_1, &array_1.as_ref(), array_1.len());

        // Array and null scalar (and vice versa)
        test_zip_output_data_type(&array_1.as_ref(), &null_scalar, array_1.len());

        test_zip_output_data_type(&null_scalar, &array_1.as_ref(), array_1.len());

        // Both arrays
        test_zip_output_data_type(&array_1.as_ref(), &array_2.as_ref(), array_1.len());
    }

    fn test_zip_output_data_type(truthy: &dyn Datum, falsy: &dyn Datum, mask_length: usize) {
        let expected_data_type = truthy.get().0.data_type().clone();
        assert_eq!(&expected_data_type, falsy.get().0.data_type());

        // Try different masks to test different paths
        let mask_all_true = BooleanArray::from(vec![true; mask_length]);
        let mask_all_false = BooleanArray::from(vec![false; mask_length]);
        let mask_some_true_and_false =
            BooleanArray::from((0..mask_length).map(|i| i % 2 == 0).collect::<Vec<bool>>());

        for mask in [&mask_all_true, &mask_all_false, &mask_some_true_and_false] {
            let out = zip(mask, truthy, falsy).unwrap();
            assert_eq!(out.data_type(), &expected_data_type);
        }
    }

    #[test]
    fn zip_scalar_fallback_impl() {
        let truthy_list_item_scalar = Some(vec![Some(1), None, Some(3)]);
        let truthy_list_array_scalar =
            Scalar::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                truthy_list_item_scalar.clone(),
            ]));
        let falsy_list_item_scalar = Some(vec![None, Some(2), Some(4)]);
        let falsy_list_array_scalar =
            Scalar::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                falsy_list_item_scalar.clone(),
            ]));
        let mask = BooleanArray::from(vec![true, false, true, false, false, true, false]);
        let out = zip(&mask, &truthy_list_array_scalar, &falsy_list_array_scalar).unwrap();
        let actual = out.as_list::<i32>();

        let expected = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            truthy_list_item_scalar.clone(),
            falsy_list_item_scalar.clone(),
            truthy_list_item_scalar.clone(),
            falsy_list_item_scalar.clone(),
            falsy_list_item_scalar.clone(),
            truthy_list_item_scalar.clone(),
            falsy_list_item_scalar.clone(),
        ]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_strings_array_view() {
        let scalar_truthy = Scalar::new(StringViewArray::from(vec!["hello"]));
        let scalar_falsy = Scalar::new(StringViewArray::from(vec!["world"]));

        let mask = BooleanArray::from(vec![true, false, true, false]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_string_view();
        let expected = StringViewArray::from(vec![
            Some("hello"),
            Some("world"),
            Some("hello"),
            Some("world"),
        ]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_binary_array_view() {
        let scalar_truthy = Scalar::new(BinaryViewArray::from_iter_values(vec![b"hello"]));
        let scalar_falsy = Scalar::new(BinaryViewArray::from_iter_values(vec![b"world"]));

        let mask = BooleanArray::from(vec![true, false]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_byte_view();
        let expected = BinaryViewArray::from_iter_values(vec![b"hello", b"world"]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_strings_array_view_with_nulls() {
        let scalar_truthy = Scalar::new(StringViewArray::from_iter_values(["hello"]));
        let scalar_falsy = Scalar::new(StringViewArray::new_null(1));

        let mask = BooleanArray::from(vec![true, true, false, false, true]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_any().downcast_ref::<StringViewArray>().unwrap();
        let expected = StringViewArray::from_iter(vec![
            Some("hello"),
            Some("hello"),
            None,
            None,
            Some("hello"),
        ]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_strings_array_view_all_true_null() {
        let scalar_truthy = Scalar::new(StringViewArray::new_null(1));
        let scalar_falsy = Scalar::new(StringViewArray::new_null(1));
        let mask = BooleanArray::from(vec![true, true]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_any().downcast_ref::<StringViewArray>().unwrap();
        let expected = StringViewArray::from_iter(vec![None::<String>, None]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_strings_array_view_all_false_null() {
        let scalar_truthy = Scalar::new(StringViewArray::new_null(1));
        let scalar_falsy = Scalar::new(StringViewArray::new_null(1));
        let mask = BooleanArray::from(vec![false, false]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_any().downcast_ref::<StringViewArray>().unwrap();
        let expected = StringViewArray::from_iter(vec![None::<String>, None]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_string_array_view_all_true() {
        let scalar_truthy = Scalar::new(StringViewArray::from(vec!["hello"]));
        let scalar_falsy = Scalar::new(StringViewArray::from(vec!["world"]));

        let mask = BooleanArray::from(vec![true, true]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_string_view();
        let expected = StringViewArray::from(vec![Some("hello"), Some("hello")]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_string_array_view_all_false() {
        let scalar_truthy = Scalar::new(StringViewArray::from(vec!["hello"]));
        let scalar_falsy = Scalar::new(StringViewArray::from(vec!["world"]));

        let mask = BooleanArray::from(vec![false, false]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_string_view();
        let expected = StringViewArray::from(vec![Some("world"), Some("world")]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_strings_large_strings() {
        let scalar_truthy = Scalar::new(StringViewArray::from(vec!["longer than 12 bytes"]));
        let scalar_falsy = Scalar::new(StringViewArray::from(vec!["another longer than 12 bytes"]));

        let mask = BooleanArray::from(vec![true, false]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_string_view();
        let expected = StringViewArray::from(vec![
            Some("longer than 12 bytes"),
            Some("another longer than 12 bytes"),
        ]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_strings_array_view_large_short_strings() {
        let scalar_truthy = Scalar::new(StringViewArray::from(vec!["hello"]));
        let scalar_falsy = Scalar::new(StringViewArray::from(vec!["longer than 12 bytes"]));

        let mask = BooleanArray::from(vec![true, false, true, false]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_string_view();
        let expected = StringViewArray::from(vec![
            Some("hello"),
            Some("longer than 12 bytes"),
            Some("hello"),
            Some("longer than 12 bytes"),
        ]);
        assert_eq!(actual, &expected);
    }
    #[test]
    fn test_zip_kernel_scalar_strings_array_view_large_all_true() {
        let scalar_truthy = Scalar::new(StringViewArray::from(vec!["longer than 12 bytes"]));
        let scalar_falsy = Scalar::new(StringViewArray::from(vec!["another longer than 12 bytes"]));

        let mask = BooleanArray::from(vec![true, true]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_string_view();
        let expected = StringViewArray::from(vec![
            Some("longer than 12 bytes"),
            Some("longer than 12 bytes"),
        ]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_strings_array_view_large_all_false() {
        let scalar_truthy = Scalar::new(StringViewArray::from(vec!["longer than 12 bytes"]));
        let scalar_falsy = Scalar::new(StringViewArray::from(vec!["another longer than 12 bytes"]));

        let mask = BooleanArray::from(vec![false, false]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_string_view();
        let expected = StringViewArray::from(vec![
            Some("another longer than 12 bytes"),
            Some("another longer than 12 bytes"),
        ]);
        assert_eq!(actual, &expected);
    }
}
