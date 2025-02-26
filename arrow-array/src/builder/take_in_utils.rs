use arrow_buffer::{ArrowNativeType, BufferBuilder, NullBuffer, NullBufferBuilder, ScalarBuffer};

use crate::{Array, ArrowPrimitiveType, PrimitiveArray};

pub(crate) fn take_in_nulls<I>(
    null_buffer_builder: &mut NullBufferBuilder,
    array_nulls: Option<&NullBuffer>,
    indices: &PrimitiveArray<I>,
) where
    I: ArrowPrimitiveType,
{
    let array_nulls = array_nulls.filter(|n| n.null_count() > 0);
    let indices_nulls = indices.nulls().filter(|n| n.null_count() > 0);

    match (array_nulls, indices_nulls) {
        (None, None) => null_buffer_builder.append_n_non_nulls(indices.len()),
        (None, Some(indices_nulls)) => null_buffer_builder.append_buffer(&indices_nulls),
        (Some(array_nulls), None) => {
            let iter = indices
                .values()
                .iter()
                .map(|idx| array_nulls.is_valid(idx.as_usize()));
            null_buffer_builder.append_iter(iter);
        }
        (Some(array_nulls), Some(_indices_nulls)) => {
            let iter = indices.iter().map(|idx| {
                idx.map(|idx| array_nulls.is_valid(idx.as_usize()))
                    .unwrap_or(false)
            });
            null_buffer_builder.append_iter(iter);
        }
    }
}

pub(crate) fn take_in_native<T, I>(
    values_builder: &mut BufferBuilder<T>,
    array: &ScalarBuffer<T>,
    indices: &PrimitiveArray<I>,
) where
    T: ArrowNativeType,
    I: ArrowPrimitiveType,
{
    values_builder.reserve(indices.len());
    if indices.null_count() > 0 {
        values_builder.extend(
            indices
                .values()
                .iter()
                .map(|index| array.get(index.as_usize()).cloned().unwrap_or(T::default())),
        )
    } else {
        values_builder.extend(indices.values().iter().map(|index| array[index.as_usize()]));
    }
}
