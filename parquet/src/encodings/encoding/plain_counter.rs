use crate::basic::Type;
use crate::data_type::private::ParquetValueType;
use crate::schema::types::ColumnDescriptor;

/// A helper to estimate the size of plain encoding of the values
/// that were written to the dictionary encoder.
///
/// This is used to enhance the dictionary fallback heuristic with the logic
/// that the writer should fall back to the plain encoding when at a certain point,
/// e.g. after encoding the first batch, the total size of unencoded data
/// is calculated as smaller than `(encodedSize + dictionarySize)`.
pub struct PlainDataSizeCounter {
    raw_data_byte_size: usize,
    // Cached type length to improve performance for fixed-length types.
    type_length: usize,
}

impl PlainDataSizeCounter {
    pub fn new(desc: &ColumnDescriptor) -> Self {
        Self {
            raw_data_byte_size: 0,
            type_length: desc.type_length() as usize,
        }
    }

    /// Updates the counter with the given slice.
    pub fn update<T: ParquetValueType>(&mut self, values: &[T]) {
        let raw_size = match T::PHYSICAL_TYPE {
            Type::BOOLEAN => 1 * values.len(),
            Type::INT32 | Type::FLOAT => 4 * values.len(),
            Type::INT64 | Type::DOUBLE => 8 * values.len(),
            Type::INT96 => 12 * values.len(),
            Type::BYTE_ARRAY => {
                // For variable-length types, the length prefix and the actual data are are encoded.
                values
                    .iter()
                    .map(|value| {
                        let (base_size, num_elements) = value.dict_encoding_size();
                        base_size + num_elements
                    })
                    .sum()
            }
            Type::FIXED_LEN_BYTE_ARRAY => self.type_length * values.len(),
        };
        self.raw_data_byte_size = self.raw_data_byte_size.saturating_add(raw_size);
    }

    /// Like `update`, but specialized for byte array data exposed by Arrow
    /// array accessors.
    #[cfg(feature = "arrow")]
    #[inline]
    pub fn update_byte_array(&mut self, value: &[u8]) {
        // For variable-length types, the length prefix and the actual data are are encoded.
        let raw_size = std::mem::size_of::<u32>() + value.len();
        self.raw_data_byte_size = self.raw_data_byte_size.saturating_add(raw_size);
    }

    /// Returns the total size in bytes of values passed to `update` as if they were written
    /// in plain encoding, without a dictionary.
    #[inline]
    pub fn uncompressed_data_size(&self) -> usize {
        self.raw_data_byte_size
    }
}
