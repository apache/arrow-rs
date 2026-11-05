use crate::ArrayData;
use arrow_buffer::ArrowNativeType;
use std::mem::size_of;
use std::ops::Add;

use super::{_MutableArrayData, Extend};

pub(super) fn build_extend<T: ArrowNativeType>(array: &ArrayData) -> Extend<'_> {
    let values = array.buffer::<T>(0);
    Box::new(
        move |mutable: &mut _MutableArrayData, _, start: usize, len: usize| {
            mutable
                .buffer1
                .extend_from_slice(&values[start..start + len]);
            Ok(())
        },
    )
}

pub(super) fn build_extend_with_offset<T>(array: &ArrayData, offset: T) -> Extend<'_>
where
    T: ArrowNativeType + Add<Output = T>,
{
    let values = array.buffer::<T>(0);
    Box::new(
        move |mutable: &mut _MutableArrayData, _, start: usize, len: usize| {
            mutable
                .buffer1
                .extend(values[start..start + len].iter().map(|x| *x + offset));
            Ok(())
        },
    )
}

pub(super) fn extend_nulls<T: ArrowNativeType>(
    mutable: &mut _MutableArrayData,
    len: usize,
) -> Result<(), arrow_schema::ArrowError> {
    mutable
        .buffer1
        .try_extend_zeros(len * size_of::<T>())
        .map_err(|e| arrow_schema::ArrowError::MemoryError(e.to_string()))?;
    Ok(())
}
