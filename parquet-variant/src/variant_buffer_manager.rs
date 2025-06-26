use arrow_schema::ArrowError;

pub trait VariantBufferManager {
    /// Returns the slice where the variant metadata needs to be written to. This method may be
    /// called several times during the construction of a new `metadata` field in a variant. The
    /// implementation must make sure that on every call, all the data written to the metadata
    /// buffer so far are preserved.
    fn borrow_metadata_buffer(&mut self) -> &mut [u8];

    /// Returns the slice where value needs to be written to. This method may be called several
    /// times during the construction of a new `value` field in a variant. The implementation must
    /// make sure that on every call, all the data written to the value buffer so far are preserved.
    fn borrow_value_buffer(&mut self) -> &mut [u8];

    /// Ensures that the next call to `borrow_metadata_buffer` returns a slice having at least
    /// `size` bytes. Also ensures that the value metadata written so far are persisted - this means
    /// that if `borrow_metadata_buffer` is to return a new buffer from the next call onwards, the
    /// new buffer must have the contents of the old metadata buffer.
    fn ensure_metadata_buffer_size(&mut self, size: usize) -> Result<(), ArrowError>;

    /// Ensures that the next call to `borrow_value_buffer` returns a slice having at least `size`
    /// bytes. Also ensures that the value bytes written so far are persisted - this means that
    /// if `borrow_value_buffer` is to return a new buffer from the next call onwards, the new
    /// buffer must have the contents of the old value buffer.
    fn ensure_value_buffer_size(&mut self, size: usize) -> Result<(), ArrowError>;
}

pub struct SampleVecBasedVariantBufferManager {
    pub value_buffer: Vec<u8>,
    pub metadata_buffer: Vec<u8>,
}

impl VariantBufferManager for SampleVecBasedVariantBufferManager {
    #[inline(always)]
    fn borrow_value_buffer(&mut self) -> &mut [u8] {
        self.value_buffer.as_mut_slice()
    }

    fn ensure_value_buffer_size(&mut self, size: usize) -> Result<(), ArrowError> {
        let cur_len = self.value_buffer.len();
        if size > cur_len {
            // Reallocate larger buffer
            self.value_buffer.resize(size, 0);
        }
        Ok(())
    }

    #[inline(always)]
    fn borrow_metadata_buffer(&mut self) -> &mut [u8] {
        self.metadata_buffer.as_mut_slice()
    }

    fn ensure_metadata_buffer_size(&mut self, size: usize) -> Result<(), ArrowError> {
        let cur_len = self.metadata_buffer.len();
        if size > cur_len {
            // Reallocate larger buffer
            self.metadata_buffer.resize(size, 0);
        }
        Ok(())
    }
}
