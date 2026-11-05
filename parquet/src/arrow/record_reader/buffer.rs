use crate::arrow::buffer::bit_util::iter_set_bits_rev;
use crate::errors::Result;

/// A buffer that supports padding with nulls
pub trait ValuesBuffer {
    /// Create a new buffer with capacity for at least `capacity` logical values
    fn with_capacity(capacity: usize) -> Self;

    /// Reserve capacity for `additional` more logical values.
    ///
    /// Callers should use this once the level decoders have determined how
    /// many output slots are actually needed for the next read.
    fn reserve_exact(&mut self, additional: usize);

    /// If a column contains nulls, more level data may be read than value data, as null
    /// values are not encoded. Therefore, first the levels data is read, the null count
    /// determined, and then the corresponding number of values read to a [`ValuesBuffer`].
    ///
    /// It is then necessary to move this values data into positions that correspond to
    /// the non-null level positions. This is what this method does.
    ///
    /// It is provided with:
    ///
    /// - `read_offset` - the offset in [`ValuesBuffer`] to start null padding from
    /// - `values_read` - the number of values read
    /// - `levels_read` - the number of levels read
    /// - `valid_mask` - a packed mask of valid levels
    ///
    /// Returns an error if the inputs are inconsistent, for example because the
    /// decoded data was corrupt. This must not panic on such input.
    fn pad_nulls(
        &mut self,
        read_offset: usize,
        values_read: usize,
        levels_read: usize,
        valid_mask: &[u8],
    ) -> Result<()>;
}

impl<T: Copy + Default> ValuesBuffer for Vec<T> {
    fn with_capacity(capacity: usize) -> Self {
        Vec::with_capacity(capacity)
    }

    fn reserve_exact(&mut self, additional: usize) {
        Vec::reserve_exact(self, additional)
    }

    fn pad_nulls(
        &mut self,
        read_offset: usize,
        values_read: usize,
        levels_read: usize,
        valid_mask: &[u8],
    ) -> Result<()> {
        self.resize(read_offset + levels_read, T::default());

        let values_range = read_offset..read_offset + values_read;
        for (value_pos, level_pos) in values_range.rev().zip(iter_set_bits_rev(valid_mask)) {
            debug_assert!(level_pos >= value_pos);
            if level_pos <= value_pos {
                break;
            }
            self[level_pos] = self[value_pos];
        }
        Ok(())
    }
}
