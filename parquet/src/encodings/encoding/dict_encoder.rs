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

// ----------------------------------------------------------------------
// Dictionary encoding

use bytes::Bytes;

use crate::basic::{Encoding, Type};
use crate::data_type::private::ParquetValueType;
use crate::data_type::{DataType, FixedLenByteArray, Int96};
use crate::encodings::encoding::{Encoder, PlainEncoder};
use crate::encodings::rle::RleEncoder;
use crate::errors::Result;
use crate::schema::types::ColumnDescPtr;
use crate::util::bit_util::num_required_bits;
use crate::util::interner::{Interner, Storage};

#[derive(Debug)]
pub struct KeyStorage<T: ParquetValueType> {
    uniques: Vec<T>,

    /// size of unique values (keys) in the dictionary, in bytes.
    size_in_bytes: usize,

    type_length: usize,
}

impl<T: ParquetValueType> Storage for KeyStorage<T> {
    type Key = u64;
    type Value = T;

    #[inline]
    fn get(&self, idx: Self::Key) -> &Self::Value {
        &self.uniques[idx as usize]
    }

    fn push(&mut self, value: &Self::Value) -> Self::Key {
        let (base_size, _) = value.dict_encoding_size();
        let unique_size = if T::PHYSICAL_TYPE == Type::FIXED_LEN_BYTE_ARRAY {
            self.type_length
        } else {
            base_size
        };
        self.size_in_bytes += unique_size;

        let key = self.uniques.len() as u64;
        self.uniques.push(value.clone());
        key
    }

    fn estimated_memory_size(&self) -> usize {
        let uniques_heap_bytes = if T::PHYSICAL_TYPE == Type::FIXED_LEN_BYTE_ARRAY {
            self.type_length * self.uniques.len()
        } else {
            0
        };
        self.uniques.capacity() * std::mem::size_of::<T>() + uniques_heap_bytes
    }
}

pub trait DictionaryStorage<T>: Send {
    fn new(desc: &ColumnDescPtr) -> Self;
    fn intern(&mut self, value: &T) -> Result<u64>;
    fn intern_bytes(&mut self, bytes: &[u8], make: impl FnOnce() -> T) -> Result<u64>;
    fn len_and_size(&self) -> (usize, usize);
    /// Visit each distinct physical value's bytes without materializing typed wrappers.
    fn visit_uniques(&self, visit: impl FnMut(&[u8]));
    fn estimated_memory_size(&self) -> usize;
    fn write_dict<D: DataType<T = T>>(&self) -> Result<Bytes>;
    fn into_dict<D: DataType<T = T>>(self) -> Result<Bytes>
    where
        Self: Sized,
    {
        self.write_dict::<D>()
    }
}

type TypedDictionaryStorage<T> = Interner<KeyStorage<T>>;

impl<T: ParquetValueType> DictionaryStorage<T> for TypedDictionaryStorage<T> {
    fn new(desc: &ColumnDescPtr) -> Self {
        Interner::new(KeyStorage {
            uniques: vec![],
            size_in_bytes: 0,
            type_length: desc.type_length() as usize,
        })
    }

    #[inline]
    fn intern(&mut self, value: &T) -> Result<u64> {
        Ok(Interner::intern(self, value))
    }

    #[inline(always)]
    fn intern_bytes(&mut self, bytes: &[u8], make: impl FnOnce() -> T) -> Result<u64> {
        Ok(Interner::intern_bytes(self, bytes, make))
    }

    fn len_and_size(&self) -> (usize, usize) {
        (self.storage().uniques.len(), self.storage().size_in_bytes)
    }

    fn visit_uniques(&self, mut visit: impl FnMut(&[u8])) {
        for value in &self.storage().uniques {
            visit(value.as_bytes());
        }
    }

    fn estimated_memory_size(&self) -> usize {
        Interner::estimated_memory_size(self)
    }

    fn write_dict<D: DataType<T = T>>(&self) -> Result<Bytes> {
        let mut plain_encoder = PlainEncoder::<D>::new();
        plain_encoder.put(&self.storage().uniques)?;
        plain_encoder.flush_buffer()
    }
}

/// Selects the unique-value storage for a physical value type.
pub trait DictionaryValue: Sized {
    type Storage: DictionaryStorage<Self>;
}

macro_rules! typed_dictionary_value {
    ($($ty:ty),+ $(,)?) => {
        $(impl DictionaryValue for $ty {
            type Storage = TypedDictionaryStorage<$ty>;
        })+
    };
}

typed_dictionary_value!(bool, i32, i64, Int96, f32, f64, FixedLenByteArray);

/// The buffered dictionary indices of one column chunk's data page.
///
/// Input appends one index per value to `indices`.
#[derive(Debug, Default)]
pub(crate) struct RunIndexBuffer {
    /// The buffered indices, one per value (the dense path).
    indices: Vec<u64>,
}

impl RunIndexBuffer {
    /// Append one dense index.
    #[inline]
    pub(crate) fn push(&mut self, index: u64) {
        self.indices.push(index);
    }

    /// Reserve capacity for `additional` more dense indices.
    pub(crate) fn reserve(&mut self, additional: usize) {
        self.indices.reserve(additional);
    }

    /// Logical value count buffered.
    pub(crate) fn num_values(&self) -> usize {
        self.indices.len()
    }

    /// Serialize and reset the buffered indices as an `RLE_DICTIONARY` data
    /// page: the bit width in the first byte followed by every index. Shared by
    /// both dictionary encoders so the page framing lives in one place.
    pub(crate) fn write_indices(&mut self, bit_width: u8) -> Result<Bytes> {
        // One byte for the bit width and the encoded indices.
        let mut buffer =
            Vec::with_capacity(1 + RleEncoder::max_buffer_size(bit_width, self.indices.len()));
        buffer.push(bit_width);
        let mut encoder = RleEncoder::new_from_buf(bit_width, buffer);
        encoder.put_batch(&self.indices);
        self.indices.clear();
        Ok(encoder.consume().into())
    }

    /// Heap footprint of the buffered indices.
    pub(crate) fn estimated_memory_size(&self) -> usize {
        self.indices.capacity() * std::mem::size_of::<u64>()
    }
}

/// Dictionary encoder.
/// The dictionary encoding builds a dictionary of values encountered in a given column.
/// The dictionary page is written first, before the data pages of the column chunk.
///
/// Dictionary page format: the entries in the dictionary - in dictionary order -
/// using the plain encoding.
///
/// Data page format: the bit width used to encode the entry ids stored as 1 byte
/// (max bit width = 32), followed by the values encoded using RLE/Bit packed described
/// above (with the given bit width).
pub struct DictEncoder<T: DataType> {
    dictionary: <T::T as DictionaryValue>::Storage,

    /// The buffered data-page indices.
    indices: RunIndexBuffer,
}

impl<T: DataType> DictEncoder<T> {
    /// Creates new dictionary encoder.
    pub fn new(desc: ColumnDescPtr) -> Self {
        Self {
            dictionary: DictionaryStorage::new(&desc),
            indices: RunIndexBuffer::default(),
        }
    }

    /// Returns true if dictionary entries are sorted, false otherwise.
    pub fn is_sorted(&self) -> bool {
        // Sorting is not supported currently.
        false
    }

    /// Returns number of unique values (keys) in the dictionary.
    pub fn num_entries(&self) -> usize {
        self.dictionary.len_and_size().0
    }

    /// Visits the bytes of each distinct physical value in dictionary order.
    ///
    /// Values are borrowed directly from the physical storage. In particular,
    /// byte-array dictionaries do not allocate per-value wrappers or copy their page.
    pub fn visit_uniques(&self, visit: impl FnMut(&[u8])) {
        self.dictionary.visit_uniques(visit);
    }

    /// Returns size of unique values (keys) in the dictionary, in bytes.
    pub fn dict_encoded_size(&self) -> usize {
        self.dictionary.len_and_size().1
    }

    /// Writes out the dictionary values with PLAIN encoding in a byte buffer, and return
    /// the result.
    #[cfg(any(test, feature = "experimental"))]
    pub fn write_dict(&self) -> Result<Bytes> {
        self.dictionary.write_dict::<T>()
    }

    /// Consumes this encoder and returns its PLAIN-encoded dictionary page.
    pub(crate) fn into_dict_page(self) -> Result<Bytes> {
        self.dictionary.into_dict::<T>()
    }

    /// Writes out the dictionary values with RLE encoding in a byte buffer, and return
    /// the result.
    pub fn write_indices(&mut self) -> Result<Bytes> {
        self.indices.write_indices(self.bit_width())
    }

    /// Intern `value` and append its dictionary index to the data page.
    #[inline]
    pub(crate) fn put_one(&mut self, value: &T::T) -> Result<()> {
        self.indices.push(self.dictionary.intern(value)?);
        Ok(())
    }

    #[inline]
    fn bit_width(&self) -> u8 {
        num_required_bits(self.num_entries().saturating_sub(1) as u64)
    }
}

impl<T: DataType> Encoder<T> for DictEncoder<T> {
    fn put(&mut self, values: &[T::T]) -> Result<()> {
        self.indices.reserve(values.len());
        for i in values {
            self.put_one(i)?;
        }
        Ok(())
    }

    // Performance Note:
    // As far as can be seen these functions are rarely called and as such we can hint to the
    // compiler that they dont need to be folded into hot locations in the final output.
    fn encoding(&self) -> Encoding {
        Encoding::PLAIN_DICTIONARY
    }

    /// Returns an estimate of the data page size in bytes
    ///
    /// This includes:
    /// <already_written_encoded_byte_size> + <estimated_encoded_size_of_unflushed_bytes>
    fn estimated_data_encoded_size(&self) -> usize {
        let bit_width = self.bit_width();
        1 + RleEncoder::max_buffer_size(bit_width, self.indices.num_values())
    }

    fn flush_buffer(&mut self) -> Result<Bytes> {
        self.write_indices()
    }

    /// Returns the estimated total memory usage
    ///
    /// For this encoder, the indices are unencoded bytes (refer to [`Self::write_indices`]).
    fn estimated_memory_size(&self) -> usize {
        self.dictionary.estimated_memory_size() + self.indices.estimated_memory_size()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    use crate::data_type::{
        ByteArray, ByteArrayType, FixedLenByteArray, FixedLenByteArrayType, Int32Type,
    };
    use crate::encodings::encoding::Encoder;
    use crate::schema::types::{ColumnDescriptor, ColumnPath, Type as SchemaType};

    fn make_col_desc<T: DataType>() -> ColumnDescPtr {
        make_col_desc_with_length::<T>(-1)
    }

    fn make_col_desc_with_length<T: DataType>(type_length: i32) -> ColumnDescPtr {
        let ty = SchemaType::primitive_type_builder("col", T::get_physical_type())
            .with_length(type_length)
            .build()
            .unwrap();
        Arc::new(ColumnDescriptor::new(
            Arc::new(ty),
            0,
            0,
            ColumnPath::new(vec![]),
        ))
    }

    #[test]
    fn dictionary_storage_contract() {
        use crate::data_type::ByteArray;
        let desc = make_col_desc::<ByteArrayType>();
        let mut storage: <ByteArray as DictionaryValue>::Storage = DictionaryStorage::new(&desc);
        assert_eq!(
            DictionaryStorage::intern_bytes(&mut storage, b"", || panic!("must borrow bytes"))
                .unwrap(),
            0
        );
        assert_eq!(
            DictionaryStorage::intern_bytes(&mut storage, b"abc", || panic!("must borrow bytes"))
                .unwrap(),
            1
        );
        assert_eq!(
            DictionaryStorage::intern_bytes(&mut storage, b"abc", || panic!("must borrow bytes"))
                .unwrap(),
            1
        );
        assert_eq!(
            DictionaryStorage::intern(&mut storage, &ByteArray::from("z")).unwrap(),
            2
        );
        let expected = b"\0\0\0\0\x03\0\0\0abc\x01\0\0\0z";
        assert_eq!(storage.len_and_size(), (3, expected.len()));
        assert!(DictionaryStorage::estimated_memory_size(&storage) >= expected.len());
        assert_eq!(
            storage.write_dict::<ByteArrayType>().unwrap().as_ref(),
            expected
        );
        assert_eq!(
            storage.into_dict::<ByteArrayType>().unwrap().as_ref(),
            expected
        );

        let mut encoder = DictEncoder::<ByteArrayType>::new(desc);
        encoder
            .put(&[
                ByteArray::from(""),
                ByteArray::from("abc"),
                ByteArray::from("abc"),
            ])
            .unwrap();
        assert_eq!(encoder.num_entries(), 2);
        let mut visited = Vec::new();
        encoder.visit_uniques(|bytes| visited.push(bytes.to_vec()));
        assert_eq!(visited, [b"".to_vec(), b"abc".to_vec()]);
        assert!(!encoder.flush_buffer().unwrap().is_empty());
        assert_eq!(encoder.indices.num_values(), 0);
        encoder.put(&[ByteArray::from("z")]).unwrap();
        assert_eq!(encoder.num_entries(), 3);
        encoder.flush_buffer().unwrap();
        assert_eq!(encoder.into_dict_page().unwrap().as_ref(), expected);
    }

    #[test]
    fn test_estimated_memory_size_primitive_with_duplicates() {
        let mut encoder = DictEncoder::<Int32Type>::new(make_col_desc::<Int32Type>());
        let empty_size = encoder.estimated_memory_size();

        // 3 distinct values, repeated to produce 9 indices total.
        encoder.put(&[1, 2, 3, 1, 2, 3, 1, 2, 3]).unwrap();

        let size = encoder.estimated_memory_size();

        // Must account for the 3 unique dictionary entries.
        let dict_entry_size = 3 * std::mem::size_of::<i32>();
        assert!(
            size >= empty_size + dict_entry_size,
            "memory size {size} should grow by at least the dict storage ({dict_entry_size} bytes)"
        );

        // Must also account for the 9 buffered indices.
        let indices_size = 9 * std::mem::size_of::<u64>();
        assert!(
            size >= empty_size + dict_entry_size + indices_size,
            "memory size {size} should include indices ({indices_size} bytes)"
        );
    }

    #[test]
    fn test_estimated_memory_size_primitive_all_distinct() {
        let mut encoder = DictEncoder::<Int32Type>::new(make_col_desc::<Int32Type>());
        let empty_size = encoder.estimated_memory_size();

        let values: Vec<i32> = (0..100).collect();
        encoder.put(&values).unwrap();

        let size = encoder.estimated_memory_size();

        // Must account for the 100 unique dictionary entries.
        let dict_entry_size = 100 * std::mem::size_of::<i32>();
        assert!(
            size >= empty_size + dict_entry_size,
            "memory size {size} should grow by at least the dict storage ({dict_entry_size} bytes)"
        );

        // Must also account for the 100 buffered indices.
        let indices_size = 100 * std::mem::size_of::<u64>();
        assert!(
            size >= empty_size + dict_entry_size + indices_size,
            "memory size {size} should include indices ({indices_size} bytes)"
        );
    }

    #[test]
    fn test_estimated_memory_size_byte_array_with_duplicates() {
        let mut encoder = DictEncoder::<ByteArrayType>::new(make_col_desc::<ByteArrayType>());
        let empty_size = encoder.estimated_memory_size();

        // 3 distinct byte strings ("foo", "bar", "baz" — 3 bytes each), repeated to produce
        // 9 indices total.
        let vals: Vec<ByteArray> = [
            "foo", "bar", "baz", "foo", "bar", "baz", "foo", "bar", "baz",
        ]
        .iter()
        .map(|s| ByteArray::from(*s))
        .collect();
        encoder.put(&vals).unwrap();

        let size = encoder.estimated_memory_size();

        // Must account for the 3 unique dictionary entries, including their heap-allocated bytes.
        let dict_entry_size = 3 * std::mem::size_of::<ByteArray>() + 3 * 3; // 3 values × 3 bytes each
        assert!(
            size >= empty_size + dict_entry_size,
            "memory size {size} should grow by at least the dict storage ({dict_entry_size} bytes)"
        );

        // Must also account for the 9 buffered indices.
        let indices_size = 9 * std::mem::size_of::<u64>();
        assert!(
            size >= empty_size + dict_entry_size + indices_size,
            "memory size {size} should include indices ({indices_size} bytes)"
        );
    }

    #[test]
    fn test_estimated_memory_size_byte_array_all_distinct() {
        let mut encoder = DictEncoder::<ByteArrayType>::new(make_col_desc::<ByteArrayType>());
        let empty_size = encoder.estimated_memory_size();

        // 100 distinct values: "0".."9" (1 byte each) and "10".."99" (2 bytes each).
        let values: Vec<ByteArray> = (0..100_u32)
            .map(|i| ByteArray::from(i.to_string().into_bytes()))
            .collect();
        let bytes_total: usize = values.iter().map(|v| v.len()).sum(); // 10×1 + 90×2 = 190
        encoder.put(&values).unwrap();

        let size = encoder.estimated_memory_size();

        // Must account for the 100 unique dictionary entries, including their heap-allocated bytes.
        let dict_entry_size = 100 * std::mem::size_of::<ByteArray>() + bytes_total;
        assert!(
            size >= empty_size + dict_entry_size,
            "memory size {size} should grow by at least the dict storage ({dict_entry_size} bytes)"
        );

        // Must also account for the 100 buffered indices.
        let indices_size = 100 * std::mem::size_of::<u64>();
        assert!(
            size >= empty_size + dict_entry_size + indices_size,
            "memory size {size} should include indices ({indices_size} bytes)"
        );
    }

    #[test]
    fn test_estimated_memory_size_fixed_len_byte_array_with_duplicates() {
        const TYPE_LEN: usize = 3;
        let mut encoder = DictEncoder::<FixedLenByteArrayType>::new(make_col_desc_with_length::<
            FixedLenByteArrayType,
        >(TYPE_LEN as i32));
        let empty_size = encoder.estimated_memory_size();

        // 3 distinct 3-byte values, repeated to produce 9 indices total.
        let vals = [
            b"foo", b"bar", b"baz", b"foo", b"bar", b"baz", b"foo", b"bar", b"baz",
        ]
        .iter()
        .map(|b| FixedLenByteArray::from(b.to_vec()))
        .collect::<Vec<_>>();
        encoder.put(&vals).unwrap();

        let size = encoder.estimated_memory_size();

        // Must account for the 3 unique dictionary entries: struct overhead plus the
        // fixed-length bytes allocated per entry.
        let dict_entry_size = 3 * std::mem::size_of::<FixedLenByteArray>() + 3 * TYPE_LEN;
        assert!(
            size >= empty_size + dict_entry_size,
            "memory size {size} should grow by at least the dict storage ({dict_entry_size} bytes)"
        );

        // Must also account for the 9 buffered indices.
        let indices_size = 9 * std::mem::size_of::<u64>();
        assert!(
            size >= empty_size + dict_entry_size + indices_size,
            "memory size {size} should include indices ({indices_size} bytes)"
        );
    }

    #[test]
    fn test_estimated_memory_size_fixed_len_byte_array_all_distinct() {
        const TYPE_LEN: usize = 3;
        let mut encoder = DictEncoder::<FixedLenByteArrayType>::new(make_col_desc_with_length::<
            FixedLenByteArrayType,
        >(TYPE_LEN as i32));
        let empty_size = encoder.estimated_memory_size();

        // 100 distinct 3-byte values: zero-padded big-endian u8 indices.
        let values = (0..100_u8)
            .map(|i| FixedLenByteArray::from(vec![0u8, 0u8, i]))
            .collect::<Vec<_>>();
        encoder.put(&values).unwrap();

        let size = encoder.estimated_memory_size();

        // Must account for the 100 unique dictionary entries: struct overhead plus the
        // fixed-length bytes allocated per entry.
        let dict_entry_size = 100 * std::mem::size_of::<FixedLenByteArray>() + 100 * TYPE_LEN;
        assert!(
            size >= empty_size + dict_entry_size,
            "memory size {size} should grow by at least the dict storage ({dict_entry_size} bytes)"
        );

        // Must also account for the 100 buffered indices.
        let indices_size = 100 * std::mem::size_of::<u64>();
        assert!(
            size >= empty_size + dict_entry_size + indices_size,
            "memory size {size} should include indices ({indices_size} bytes)"
        );
    }

    #[test]
    fn test_estimated_memory_size_includes_interner_dedup_table() {
        // The dedup `HashTable` in `Interner` is preallocated with
        // `DEFAULT_DEDUP_CAPACITY` slots at construction, independent of any
        // values pushed.
        let encoder = DictEncoder::<Int32Type>::new(make_col_desc::<Int32Type>());

        let size = encoder.estimated_memory_size();

        assert!(
            size > 0,
            "memory size should include the preallocated dedup hash table"
        );
    }

    #[test]
    fn test_estimated_memory_size_accounts_for_indices_capacity() {
        // Exercises the `indices.capacity()` (not `.len()`) accounting.
        // After a flush, `indices` is cleared but its capacity is retained; pushing a
        // smaller batch afterwards leaves capacity strictly greater than length.
        let mut encoder = DictEncoder::<Int32Type>::new(make_col_desc::<Int32Type>());

        let big: Vec<i32> = vec![0; 64];
        encoder.put(&big).unwrap();
        let _ = encoder.flush_buffer().unwrap();

        let flushed_size = encoder.estimated_memory_size();

        // Push a single value — indices.len() == 1 but indices.capacity() >= 64.
        // No change on the key storage since the value is already interned.
        encoder.put(&[0]).unwrap();

        let size = encoder.estimated_memory_size();

        assert_eq!(
            size, flushed_size,
            "memory size should include retained indices capacity",
        );
    }

    #[test]
    fn test_estimated_memory_size_accounts_for_uniques_capacity() {
        let mut encoder = DictEncoder::<Int32Type>::new(make_col_desc::<Int32Type>());

        let values: Vec<i32> = (0..64).collect();
        encoder.put(&values).unwrap();
        // Flush indices so they don't mask the uniques accounting in the lower bound.
        let _ = encoder.flush_buffer().unwrap();

        let size1 = encoder.estimated_memory_size();

        // Push more values to trigger uniques capacity growth.
        // The pre-allocated dedup hash table is unlikely to be resized.
        let values: Vec<i32> = (64..128).collect();
        encoder.put(&values).unwrap();
        // Flush indices so they don't mask the uniques accounting in the lower bound.
        let _ = encoder.flush_buffer().unwrap();

        let size2 = encoder.estimated_memory_size();

        let min_uniques_bytes = 64 * std::mem::size_of::<i32>();
        assert!(
            size2 >= size1 + min_uniques_bytes,
            "memory size {size2} should grow from {size1} by allocated uniques capacity \
             (at least {min_uniques_bytes} bytes)"
        );
    }
}
