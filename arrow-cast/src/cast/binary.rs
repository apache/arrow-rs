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

//! Cast support for binary, fixed-size binary and byte-view arrays.

use arrow_array::{builder::*, cast::*, types::*, *};
use arrow_buffer::{Buffer, OffsetBuffer};
use arrow_data::{ArrayData, ByteView};
use arrow_schema::{ArrowError, DataType};
use num_traits::{NumCast, ToPrimitive};
use std::sync::Arc;

use super::CastOptions;

pub(crate) fn cast_numeric_to_binary<FROM: ArrowPrimitiveType, O: OffsetSizeTrait>(
    array: &dyn Array,
) -> Result<ArrayRef, ArrowError> {
    let array = array.as_primitive::<FROM>();
    let size = std::mem::size_of::<FROM::Native>();
    let offsets = OffsetBuffer::from_repeated_length(size, array.len());
    Ok(Arc::new(GenericBinaryArray::<O>::try_new(
        offsets,
        array.values().inner().clone(),
        array.nulls().cloned(),
    )?))
}

/// Helper function to cast from one `BinaryArray` or 'LargeBinaryArray' to 'FixedSizeBinaryArray'.
pub(crate) fn cast_binary_to_fixed_size_binary<O: OffsetSizeTrait>(
    array: &dyn Array,
    byte_width: i32,
    cast_options: &CastOptions,
) -> Result<ArrayRef, ArrowError> {
    let array = array.as_binary::<O>();
    let mut builder = FixedSizeBinaryBuilder::with_capacity(array.len(), byte_width);

    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
        } else {
            match builder.append_value(array.value(i)) {
                Ok(()) => {}
                Err(e) => match cast_options.safe {
                    true => builder.append_null(),
                    false => return Err(e),
                },
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Helper function to cast from 'FixedSizeBinaryArray' to one `BinaryArray` or 'LargeBinaryArray'.
/// If the target one is too large for the source array it will return an Error.
pub(crate) fn cast_fixed_size_binary_to_binary<O: OffsetSizeTrait>(
    array: &dyn Array,
    byte_width: i32,
) -> Result<ArrayRef, ArrowError> {
    let array = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();

    let offsets: i128 = byte_width as i128 * array.len() as i128;

    let is_binary = matches!(GenericBinaryType::<O>::DATA_TYPE, DataType::Binary);
    if is_binary && offsets > i32::MAX as i128 {
        return Err(ArrowError::ComputeError(
            "FixedSizeBinary array too large to cast to Binary array".to_string(),
        ));
    } else if !is_binary && offsets > i64::MAX as i128 {
        return Err(ArrowError::ComputeError(
            "FixedSizeBinary array too large to cast to LargeBinary array".to_string(),
        ));
    }

    let mut builder = GenericBinaryBuilder::<O>::with_capacity(array.len(), array.len());

    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(array.value(i));
        }
    }

    Ok(Arc::new(builder.finish()))
}

pub(crate) fn cast_fixed_size_binary_to_binary_view(
    array: &dyn Array,
    _byte_width: i32,
) -> Result<ArrayRef, ArrowError> {
    let array = array
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap();

    let mut builder = BinaryViewBuilder::with_capacity(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(array.value(i));
        }
    }

    Ok(Arc::new(builder.finish()))
}

/// Helper function to cast from one `ByteArrayType` to another and vice versa.
/// If the target one (e.g., `LargeUtf8`) is too large for the source array it will return an Error.
pub(crate) fn cast_byte_container<FROM, TO>(array: &dyn Array) -> Result<ArrayRef, ArrowError>
where
    FROM: ByteArrayType,
    TO: ByteArrayType<Native = FROM::Native>,
    FROM::Offset: OffsetSizeTrait + ToPrimitive,
    TO::Offset: OffsetSizeTrait + NumCast,
{
    let data = array.to_data();
    assert_eq!(data.data_type(), &FROM::DATA_TYPE);
    let str_values_buf = data.buffers()[1].clone();
    let offsets = data.buffers()[0].typed_data::<FROM::Offset>();

    let mut cast_offsets = Vec::<TO::Offset>::with_capacity(offsets.len());
    offsets
        .iter()
        .try_for_each::<_, Result<_, ArrowError>>(|offset| {
            let offset =
                <<TO as ByteArrayType>::Offset as NumCast>::from(*offset).ok_or_else(|| {
                    ArrowError::ComputeError(format!(
                        "{}{} array too large to cast to {}{} array",
                        FROM::Offset::PREFIX,
                        FROM::PREFIX,
                        TO::Offset::PREFIX,
                        TO::PREFIX
                    ))
                })?;
            cast_offsets.push(offset);
            Ok(())
        })?;

    let offset_buffer = Buffer::from_vec(cast_offsets);

    let dtype = TO::DATA_TYPE;

    let builder = ArrayData::builder(dtype)
        .offset(array.offset())
        .len(array.len())
        .add_buffer(offset_buffer)
        .add_buffer(str_values_buf)
        .nulls(data.nulls().cloned());

    let array_data = unsafe { builder.build_unchecked() };

    Ok(Arc::new(GenericByteArray::<TO>::from(array_data)))
}

/// Helper function to cast from one `ByteViewType` array to `ByteArrayType` array.
pub(crate) fn cast_view_to_byte<FROM, TO>(array: &dyn Array) -> Result<ArrayRef, ArrowError>
where
    FROM: ByteViewType,
    TO: ByteArrayType,
    FROM::Native: AsRef<TO::Native>,
{
    let data = array.to_data();
    let view_array = GenericByteViewArray::<FROM>::from(data);

    let len = view_array.len();
    let bytes = view_array
        .views()
        .iter()
        .map(|v| ByteView::from(*v).length as usize)
        .sum::<usize>();

    let mut byte_array_builder = GenericByteBuilder::<TO>::with_capacity(len, bytes);

    for val in &view_array {
        byte_array_builder.append_option(val);
    }

    Ok(Arc::new(byte_array_builder.finish()))
}
