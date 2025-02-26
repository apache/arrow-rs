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

//! Defines take_in kernel for [Array] [ArrayBuilder]
use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::*;
use arrow_schema::{ArrowError, DataType};
use builder::{
    ArrayBuilder, GenericBinaryBuilder, GenericByteViewBuilder, GenericStringBuilder,
    PrimitiveBuilder,
};

use crate::take::check_bounds;
use crate::take::TakeOptions;
use crate::take::ToIndices;

/// Take values at `indices` from `values` and append them to `builder`.
pub fn take_in(
    values: &dyn Array,
    builder: &mut dyn ArrayBuilder,
    indices: &dyn Array,
    options: Option<TakeOptions>,
) -> Result<(), ArrowError> {
    let options = options.unwrap_or_default();
    macro_rules! helper {
        ($t:ty, $values:expr, $builder:expr, $indices:expr, $options:expr) => {{
            let indices = indices.as_primitive::<$t>();
            if $options.check_bounds {
                check_bounds($values.len(), indices)?;
            }
            let indices = indices.to_indices();
            Ok(take_in_impl($values, $builder, &indices))
        }};
    }
    downcast_integer! {
        indices.data_type() => (helper, values, builder, indices, options),
        d => Err(ArrowError::InvalidArgumentError(format!("Take only supported for integers, got {d:?}")))
    }
}

/// Take values at `indices` from `batch` and append them to `builder`.
pub fn take_in_batch(
    batch: &RecordBatch,
    builder: &mut RecordBatchBuilder,
    indices: &dyn Array,
    options: Option<TakeOptions>,
) -> Result<(), ArrowError> {
    let columns = batch.columns();
    let builders = builder.builders_mut();

    for (column, builder) in columns.into_iter().zip(builders.into_iter()) {
        take_in(column, builder.as_mut(), indices, options.clone())?
    }

    Ok(())
}

fn take_in_primitive<IndexType: ArrowPrimitiveType>(
    values: &dyn Array,
    builder: &mut dyn ArrayBuilder,
    indices: &PrimitiveArray<IndexType>,
) {
    macro_rules! helper {
        ($t: ty) => {{
            builder
                .as_any_mut()
                .downcast_mut::<PrimitiveBuilder<$t>>()
                .expect("builder does not match array type")
                .take_in(values.as_primitive::<$t>(), indices);
        }};
    }

    downcast_primitive! {
        values.data_type() => (helper),
        _ => unreachable!()
    }
}

fn take_in_impl<IndexType>(
    values: &dyn Array,
    builder: &mut dyn ArrayBuilder,
    indices: &PrimitiveArray<IndexType>,
) where
    IndexType: ArrowPrimitiveType,
{
    downcast_primitive_array! {
        values => {
            take_in_primitive(values, builder, indices)
        },
        DataType::Utf8 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<GenericStringBuilder<i32>>()
                .expect("builder does not match array type");
            builder.take_in(values.as_string::<i32>(), indices);
        }
        DataType::LargeUtf8 => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<GenericStringBuilder<i64>>()
                .expect("builder does not match array type");
            builder.take_in(values.as_string::<i64>(), indices);
        }
        DataType::Utf8View => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<GenericByteViewBuilder<StringViewType>>()
                .expect("builder does not match array type");
            builder.take_in(values.as_string_view(), indices);
        }
        DataType::Binary => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<GenericBinaryBuilder<i32>>()
                .expect("builder does not match array type");
            builder.take_in(values.as_binary::<i32>(), indices);
        }
        DataType::LargeBinary => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<GenericBinaryBuilder<i64>>()
                .expect("builder does not match array type");
            builder.take_in(values.as_binary::<i64>(), indices);
        }
        DataType::BinaryView => {
            let builder = builder
                .as_any_mut()
                .downcast_mut::<GenericByteViewBuilder<BinaryViewType>>()
                .expect("builder does not match array type");
            builder.take_in(values.as_binary_view(), indices);
        }
        t => unimplemented!("TakeIn not supported for data type {:?}", t)
    }
}
