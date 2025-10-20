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

use crate::filter::SlicesIterator;
use arrow_array::cast::AsArray;
use arrow_array::types::{BinaryType, ByteArrayType, LargeBinaryType, LargeUtf8Type, Utf8Type};
use arrow_array::*;
use arrow_buffer::{
    BooleanBuffer, Buffer, MutableBuffer, NullBuffer, OffsetBuffer, OffsetBufferBuilder,
    ScalarBuffer,
};
use arrow_data::ArrayData;
use arrow_data::transform::MutableArrayData;
use arrow_schema::{ArrowError, DataType};
use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::{BitAnd, Not};
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

    SlicesIterator::new(mask).for_each(|(start, end)| {
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
            _ => {
                Arc::new(FallbackImpl::new(truthy, falsy)) as Arc<dyn ZipImpl>
            },
        };

        Ok(Self { zip_impl })
    }
}

/// Impl for creating output array based on input boolean array
trait ZipImpl: Debug {
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
        zip_impl(predicate, &self.truthy, false, &self.falsy, false)
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
}

impl<T: ArrowPrimitiveType> PrimitiveScalarImpl<T> {
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
        let predicate = combine_nulls_and_false(predicate);

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

    fn get_bytes_and_offset_for_all_same_value(
        predicate: &BooleanBuffer,
        value: &[u8],
    ) -> (Buffer, OffsetBuffer<T::Offset>) {
        let value_length = value.len();

        let offsets =
            OffsetBuffer::<T::Offset>::from_repeated_length(value_length, predicate.len());

        let mut bytes = MutableBuffer::with_capacity(0);
        bytes.repeat_slice_n_times(value, predicate.len());
        let bytes = Buffer::from(bytes);

        (bytes, offsets)
    }
}

impl<T: ByteArrayType> ZipImpl for BytesScalarImpl<T> {
    fn create_output(&self, predicate: &BooleanArray) -> Result<ArrayRef, ArrowError> {
        let result_len = predicate.len();
        // Nulls are treated as false
        let predicate = combine_nulls_and_false(predicate);

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

impl<T: ByteArrayType> BytesScalarImpl<T> {
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
                    Self::get_bytes_and_offset_for_all_same_value(predicate, falsy_val);

                return (bytes, offsets);
            }
            n if n == predicate.len() => {
                // All values are truthy
                let (bytes, offsets) =
                    Self::get_bytes_and_offset_for_all_same_value(predicate, truthy_val);

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

fn combine_nulls_and_false(predicate: &BooleanArray) -> BooleanBuffer {
    if let Some(nulls) = predicate.nulls().filter(|n| n.null_count() > 0) {
        predicate.values().bitand(
            // nulls are represented as 0 (false) in the values buffer
            nulls.inner(),
        )
    } else {
        predicate.values().clone()
    }
}

#[cfg(test)]
mod test {
    use super::*;

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
    fn test_zip_kernel_scalar_both() {
        let scalar_truthy = Scalar::new(Int32Array::from_value(42, 1));
        let scalar_falsy = Scalar::new(Int32Array::from_value(123, 1));

        let mask = BooleanArray::from(vec![true, true, false, false, true]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![Some(42), Some(42), Some(123), Some(123), Some(42)]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_none_1() {
        let scalar_truthy = Scalar::new(Int32Array::from_value(42, 1));
        let scalar_falsy = Scalar::new(Int32Array::new_null(1));

        let mask = BooleanArray::from(vec![true, true, false, false, true]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![Some(42), Some(42), None, None, Some(42)]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_zip_kernel_scalar_none_2() {
        let scalar_truthy = Scalar::new(Int32Array::from_value(42, 1));
        let scalar_falsy = Scalar::new(Int32Array::new_null(1));

        let mask = BooleanArray::from(vec![false, false, true, true, false]);
        let out = zip(&mask, &scalar_truthy, &scalar_falsy).unwrap();
        let actual = out.as_any().downcast_ref::<Int32Array>().unwrap();
        let expected = Int32Array::from(vec![None, None, Some(42), Some(42), None]);
        assert_eq!(actual, &expected);
    }
}
