use super::{
    _MutableArrayData, Extend,
    utils::{get_last_offset, try_extend_offsets},
};
use crate::ArrayData;
use arrow_buffer::ArrowNativeType;
use arrow_schema::ArrowError;
use num_integer::Integer;
use num_traits::CheckedAdd;

pub(super) fn build_extend<T: ArrowNativeType + Integer + CheckedAdd>(
    array: &ArrayData,
) -> Extend<'_> {
    let offsets = array.buffer::<T>(0);
    Box::new(
        move |mutable: &mut _MutableArrayData, index: usize, start: usize, len: usize| {
            let offset_buffer = &mut mutable.buffer1;

            // this is safe due to how offset is built. See details on `get_last_offset`
            let last_offset: T = unsafe { get_last_offset(offset_buffer) };

            // offsets
            try_extend_offsets::<T>(offset_buffer, last_offset, &offsets[start..start + len + 1])?;

            mutable.child_data[0].try_extend(
                index,
                offsets[start].as_usize(),
                offsets[start + len].as_usize(),
            )
        },
    )
}

pub(super) fn extend_nulls<T: ArrowNativeType>(
    mutable: &mut _MutableArrayData,
    len: usize,
) -> Result<(), ArrowError> {
    let offset_buffer = &mut mutable.buffer1;

    // this is safe due to how offset is built. See details on `get_last_offset`
    let last_offset: T = unsafe { get_last_offset(offset_buffer) };

    (0..len).for_each(|_| offset_buffer.push(last_offset));
    Ok(())
}
