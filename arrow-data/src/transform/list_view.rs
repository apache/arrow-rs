use crate::ArrayData;
use crate::transform::_MutableArrayData;
use crate::transform::utils::get_last_value_or_default;
use arrow_buffer::ArrowNativeType;
use num_integer::Integer;
use num_traits::CheckedAdd;

pub(super) fn build_extend<T: ArrowNativeType + Integer + CheckedAdd>(
    array: &ArrayData,
) -> crate::transform::Extend<'_> {
    let offsets = array.buffer::<T>(0);
    let sizes = array.buffer::<T>(1);
    Box::new(
        move |mutable: &mut _MutableArrayData, _index: usize, start: usize, len: usize| {
            let offset_buffer = &mut mutable.buffer1;
            let sizes_buffer = &mut mutable.buffer2;

            for &offset in &offsets[start..start + len] {
                offset_buffer.push(offset);
            }

            // sizes
            for &size in &sizes[start..start + len] {
                sizes_buffer.push(size);
            }

            // the beauty of views is that we don't need to copy child_data, we just splat
            // the offsets and sizes.
        },
    )
}

pub(super) fn extend_nulls<T: ArrowNativeType>(mutable: &mut _MutableArrayData, len: usize) {
    let offset_buffer = &mut mutable.buffer1;
    let sizes_buffer = &mut mutable.buffer2;

    let last_offset: T = get_last_value_or_default(offset_buffer);
    let last_size: T = get_last_value_or_default(sizes_buffer);

    (0..len).for_each(|_| offset_buffer.push(last_offset));
    (0..len).for_each(|_| sizes_buffer.push(last_size));
}
