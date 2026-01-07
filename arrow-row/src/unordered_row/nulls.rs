use crate::unordered_row::fixed::split_off;
use arrow_buffer::bit_chunk_iterator::BitChunkIterator;
use arrow_buffer::{bit_util, NullBuffer, NullBufferBuilder};
use std::iter::{Chain, Once};

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
#[repr(u8)]
enum MetadataEncodingType {
    None = 0,
    FullByte = 1,
    SingleBit = 2,
}

impl MetadataEncodingType {
    #[inline]
    fn is_known_to_be_all_valid(&self, byte: u8) -> bool {
        match self {
            // No metadata so unknown
            MetadataEncodingType::None => false,
            MetadataEncodingType::FullByte => byte == u8::MAX,
            MetadataEncodingType::SingleBit => (byte & 1) != 0,
        }
    }
}

impl From<u8> for MetadataEncodingType {
    // Always inline to make sure that converting to MetadataEncodingType is hopefully
    // done at compile time
    #[inline(always)]
    fn from(value: u8) -> Self {
        match value {
            0 => Self::None,
            1 => Self::FullByte,
            2 => Self::SingleBit,
            _ => unreachable!("invalid metadata type: {value}"),
        }
    }
}

fn get_metadata_encoding_type(number_of_columns: usize) -> MetadataEncodingType {
    // If we have less than 8 columns having a metadata bit is unnecessary as we can compare the value to u8::MAX
    if number_of_columns <= 8 {
        return MetadataEncodingType::None;
    }

    // If we have a multiple of 8 columns, we will use an extra byte for metadata to avoid bit ops
    if number_of_columns % 8 == 0 {
        return MetadataEncodingType::FullByte;
    }

    MetadataEncodingType::SingleBit
}

#[inline(always)]
pub(crate) fn get_number_of_bytes_for_nulls(number_of_columns: usize) -> usize {
    get_number_of_bytes_for_nulls_from_metadata(
        get_metadata_encoding_type(number_of_columns),
        number_of_columns,
    )
}

#[inline(always)]
fn get_number_of_bytes_for_nulls_from_metadata(
    metadata: MetadataEncodingType,
    number_of_columns: usize,
) -> usize {
    match metadata {
        MetadataEncodingType::None => bit_util::ceil(number_of_columns, 8),
        MetadataEncodingType::FullByte => 1 + bit_util::ceil(number_of_columns, 8),
        MetadataEncodingType::SingleBit => bit_util::ceil(1 + number_of_columns, 8),
    }
}

/// Get bytes to use when all columns are valid
#[inline]
fn get_all_valid_bytes(number_of_columns: usize) -> Vec<u8> {
    let metadata_type = get_metadata_encoding_type(number_of_columns);

    let number_of_bytes =
        get_number_of_bytes_for_nulls_from_metadata(metadata_type, number_of_columns);

    // Unused bit are set as well for simplicity, there is no benefit in setting them to 0
    vec![u8::MAX; number_of_bytes]
}

fn encode_nulls_to_slice<const METADATA_TYPE: u8>(
    mut output: &mut [u8],
    merge_iters: &mut [MergeIter],
) {
    let metadata_type = MetadataEncodingType::from(METADATA_TYPE);

    let mut are_all_valid = true;

    for (mut index, merge_iter) in merge_iters.iter_mut().enumerate() {
        if metadata_type == MetadataEncodingType::FullByte {
            // Skip the initial byte
            index += 1;
        }

        let byte = merge_iter.next().unwrap();
        // Unused bytes are set to u8::MAX as well
        are_all_valid = are_all_valid && byte == u8::MAX;
        output[index] = byte;
    }

    match metadata_type {
        MetadataEncodingType::None => {}
        MetadataEncodingType::FullByte => {
            // as we have the metadata bit
            output[0] = if are_all_valid { u8::MAX } else { 0 };
        }
        MetadataEncodingType::SingleBit => {
            if are_all_valid {
                output[0] |= 1;
            } else {
                output[0] &= !1;
            }
        }
    }
}

struct MergeIter<'a> {
    inner: [Option<Chain<BitChunkIterator<'a>, Once<u64>>>; 8],
    current: [u64; 8],
    bit_index: usize,
    number_of_bits_remaining: usize,
}

impl MergeIter<'_> {
    fn new(nulls: &[Option<&NullBuffer>], len: usize) -> Self {
        assert!(
            nulls.len() <= 8,
            "MergeIter only supports up to 8 null buffers"
        );
        assert_ne!(nulls.len(), 0, "Must have columns nulls to encode");
        assert_ne!(len, 0, "Must have columns with data to encode");
        assert!(
            nulls.iter().all(|n| n.is_none_or(|n| n.len() == len)),
            "All null buffers must have the same length as the data"
        );

        let normalized_iterators = nulls
            .iter()
            .map(|n| match n {
                None => None,
                Some(null_buffer) => Some(null_buffer.inner().bit_chunks()),
            })
            .map(|n| {
                n.map(|bit_chunks| {
                    bit_chunks
                        .iter()
                        .chain(std::iter::once(bit_chunks.remainder_bits()))
                })
            })
            .collect::<Vec<_>>();

        let mut inner = [None; 8];
        for (i, it) in normalized_iterators.into_iter().enumerate() {
            inner[i] = it;
        }

        let mut current = [0; 8].map(|iter| {
            match iter {
                None => u64::MAX,
                Some(mut it) => {
                    // We already asserted that length cannot be 0
                    it.next().unwrap()
                }
            }
        });

        MergeIter {
            inner,
            current,
            bit_index: 0,
            number_of_bits_remaining: len,
        }
    }

    fn advance_to_next_iter(&mut self) {
        assert_ne!(
            self.number_of_bits_remaining, 0,
            "Should have at least one u64 remaining"
        );

        self.inner
            .iter_mut()
            .zip(self.current.iter_mut())
            .for_each(|(inner, current)| {
                match inner {
                    None => {
                        // We don't modify current for None iterators, so it should already match u64::MAX
                        assert_eq!(current, &u64::MAX);
                    }
                    Some(inner) => {
                        *current = inner.next().unwrap();
                    }
                }
            });

        // Reset bit index to start over
        self.bit_index = 0;
    }
}

impl<'a> Iterator for MergeIter<'a> {
    type Item = u8;

    fn next(&mut self) -> Option<Self::Item> {
        if self.number_of_bits_remaining == 0 {
            return None;
        }

        if self.bit_index > 63 {
            self.advance_to_next_iter();
        }

        let item = fetch_and_shift(self.current, self.bit_index);

        self.bit_index += 1;

        Some(item)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            self.number_of_bits_remaining,
            Some(self.number_of_bits_remaining),
        )
    }
}

impl ExactSizeIterator for MergeIter<'_> {
    fn len(&self) -> usize {
        self.number_of_bits_remaining
    }
}

/// Decode single row nulls
fn decode_nulls_from_slice(bitpacked: &[u8], length: usize) -> Vec<bool> {
    let number_of_bytes = bit_util::ceil(length, 8);
    let mut result = vec![false; length];

    let mut index = 0;

    for byte_index in 0..(number_of_bytes - 1) {
        let byte = bitpacked[byte_index];
        for bit_index in 0..8 {
            let overall_index = byte_index * 8 + bit_index;
            if overall_index >= length {
                break;
            }
            let is_valid = (byte & (1 << bit_index)) != 0;
            result[index] = is_valid;
            index += 1;
        }
    }

    for bit_index in 0..(length % 8) {
        let byte = bitpacked[number_of_bytes - 1];
        let is_valid = (byte & (1 << bit_index)) != 0;
        result[index] = is_valid;
        index += 1;
    }

    result
}

// Naive implementation of encoding nulls
pub(crate) fn encode_nulls_naive(
    data: &mut [u8],
    offsets: &mut [usize],
    mut nulls: Vec<Option<&NullBuffer>>,
) {
    assert_ne!(nulls.len(), 0, "Must have columns nulls to encode");

    // Replace all Null buffers with no nulls with None for normalization
    nulls.iter_mut().for_each(|n| {
        if n.is_some_and(|n| n.null_count() == 0) {
            *n = None;
        }
    });

    // Fast path, if all valid
    if nulls.iter().all(|n| n.is_none()) {
        encode_all_valid(data, offsets, nulls.len());
        return;
    }

    let mut merge_iters: Vec<MergeIter> = vec![];

    match get_metadata_encoding_type(nulls.len()) {
        MetadataEncodingType::None => {
            {
                let mut left_nulls = nulls.as_mut_slice();
                while !left_nulls.is_empty() {
                    let (current_chunk, next_slice) =
                        left_nulls.split_at_mut(std::cmp::min(8, left_nulls.len()));
                    let merge_iter = MergeIter::new(current_chunk, data.len());
                    merge_iters.push(merge_iter);
                    left_nulls = next_slice;
                }
            }

            encode_slice_with_metadata_const::<{ MetadataEncodingType::None as u8 }>(
                data,
                offsets,
                merge_iters,
                nulls.len(),
            );
        }
        MetadataEncodingType::FullByte => {
            {
                let mut left_nulls = nulls.as_mut_slice();
                while !left_nulls.is_empty() {
                    let (current_chunk, next_slice) =
                        left_nulls.split_at_mut(std::cmp::min(8, left_nulls.len()));
                    let merge_iter = MergeIter::new(current_chunk, data.len());
                    merge_iters.push(merge_iter);
                    left_nulls = next_slice;
                }
            }

            encode_slice_with_metadata_const::<{ MetadataEncodingType::FullByte as u8 }>(
                data,
                offsets,
                merge_iters,
                nulls.len(),
            );
        }
        MetadataEncodingType::SingleBit => {
            {
                let take = std::cmp::min(7, nulls.len());
                let mut left_nulls = nulls.as_mut_slice();
                let (current_chunk, next_slice) = left_nulls.split_at_mut(take);
                left_nulls = next_slice;

                // First None to reserve space for the metadata bit
                let mut first_byte = vec![None];
                first_byte.extend(current_chunk);
                let merge_iter = MergeIter::new(current_chunk, data.len());
                merge_iters.push(merge_iter);

                while !left_nulls.is_empty() {
                    let (current_chunk, next_slice) =
                        left_nulls.split_at_mut(std::cmp::min(8, left_nulls.len()));
                    let merge_iter = MergeIter::new(current_chunk, data.len());
                    merge_iters.push(merge_iter);
                    left_nulls = next_slice;
                }
            }

            encode_slice_with_metadata_const::<{ MetadataEncodingType::SingleBit as u8 }>(
                data,
                offsets,
                merge_iters,
                nulls.len(),
            );
        }
    }
}

fn encode_slice_with_metadata_const<const METADATA_TYPE: u8>(
    data: &mut [u8],
    offsets: &mut [usize],
    mut merge_iters: Vec<MergeIter>,
    number_of_columns: usize,
) {
    let number_of_bytes = {
        let metadata_type = MetadataEncodingType::from(METADATA_TYPE);
        assert_eq!(
            metadata_type,
            get_metadata_encoding_type(number_of_columns),
            "metadata type mismatch"
        );

        get_number_of_bytes_for_nulls_from_metadata(metadata_type, number_of_columns)
    };
    for offset in offsets.iter_mut().skip(1) {
        encode_nulls_to_slice::<METADATA_TYPE>(&mut data[*offset..], merge_iters.as_mut_slice());
        *offset += number_of_bytes;
    }
}

// Optimized implementation when all columns don't have nulls in them
fn encode_all_valid(data: &mut [u8], offsets: &mut [usize], null_bits: usize) {
    assert_ne!(null_bits, 0, "Number of null bits must be greater than 0");
    let bytes_to_copy = get_all_valid_bytes(null_bits);
    let number_of_bytes = bytes_to_copy.len();

    for offset in offsets.iter_mut().skip(1) {
        data[*offset..*offset + number_of_bytes].copy_from_slice(&bytes_to_copy);
        *offset += number_of_bytes;
    }
}

/// Decodes packed nulls from rows
///
/// TODO - maybe have a function to only do for 8 nulls and then we avoid slicing and maybe we could shift each bit by the position of the column and then shift again to the
pub(crate) fn decode_packed_nulls_in_rows(
    rows: &mut [&[u8]],
    number_of_columns: usize,
) -> Vec<Option<NullBuffer>> {
    match get_metadata_encoding_type(number_of_columns) {
        MetadataEncodingType::None => decode_packed_nulls_in_rows_with_metadata_type::<
            { MetadataEncodingType::None as u8 },
        >(rows, number_of_columns),
        MetadataEncodingType::FullByte => decode_packed_nulls_in_rows_with_metadata_type::<
            { MetadataEncodingType::FullByte as u8 },
        >(rows, number_of_columns),
        MetadataEncodingType::SingleBit => decode_packed_nulls_in_rows_with_metadata_type::<
            { MetadataEncodingType::SingleBit as u8 },
        >(rows, number_of_columns),
    }
}

/// Decodes packed nulls from rows
///
/// TODO - maybe have a function to only do for 8 nulls and then we avoid slicing and maybe we could shift each bit by the position of the column and then shift again to the
pub fn decode_packed_nulls_in_rows_with_metadata_type<const METADATA_TYPE: u8>(
    rows: &mut [&[u8]],
    number_of_columns: usize,
) -> Vec<Option<NullBuffer>> {
    let metadata_type = MetadataEncodingType::from(METADATA_TYPE);
    assert_eq!(
        metadata_type,
        get_metadata_encoding_type(number_of_columns),
        "metadata type mismatch"
    );

    let number_of_rows = rows.len();
    let mut builders = vec![NullBufferBuilder::new(number_of_rows); number_of_columns];
    let number_of_bytes =
        get_number_of_bytes_for_nulls_from_metadata(metadata_type, number_of_columns);

    let unset_metadata_bit = if metadata_type == MetadataEncodingType::SingleBit {
        // All bits are set except the first one
        0b1111_1110
    } else {
        u8::MAX
    };

    for row in rows.iter_mut() {
        let mut null_bytes = split_off(row, number_of_bytes);
        let known_to_be_all_valid = metadata_type.is_known_to_be_all_valid(null_bytes[0]);

        if known_to_be_all_valid {
            builders.iter_mut().for_each(|b| b.append(true));
            continue;
        }

        let mut builders_slice = builders.as_mut_slice();

        match metadata_type {
            MetadataEncodingType::None => {}
            MetadataEncodingType::FullByte => {
                // Skip the first byte
                null_bytes = &mut null_bytes[1..];
            }
            MetadataEncodingType::SingleBit => {
                // Adding this assertion as the implementation assume that
                assert_ne!(
                    null_bytes.len(),
                    1,
                    "Must have more bytes when using single bit metadata"
                );

                let byte_builders;
                (byte_builders, builders_slice) = builders_slice.split_at_mut(8);

                decode_to_builder::<
                    // Has metadata bit as we are in the first byte
                    true,
                >(
                    null_bytes[0],
                    // Because we already asserted that there are null bits, we need to check with the metadata bit unset
                    unset_metadata_bit,
                    byte_builders,
                );

                null_bytes = &mut null_bytes[1..];
            }
        }

        for &byte in &null_bytes[..null_bytes.len() - 1] {
            let byte_builders;
            (byte_builders, builders_slice) = builders_slice.split_at_mut(8);
            // No metadata bit in this byte as we already handled that
            decode_to_builder::<false>(byte, u8::MAX, byte_builders);
        }

        // No metadata bit in this byte as we already handled that
        decode_to_builder::<false>(null_bytes[null_bytes.len() - 1], u8::MAX, builders_slice);
    }

    // Finalize null buffers
    builders
        .into_iter()
        .map(|mut builder| builder.finish())
        .collect()
}

fn decode_to_builder<const HAS_METADATA_BIT: bool>(
    null_byte: u8,
    all_valid_byte: u8,
    byte_builders: &mut [NullBufferBuilder],
) {
    // assert to verify that we won't shift by too many bits
    assert!(byte_builders.len() <= if HAS_METADATA_BIT { 7 } else { 8 });

    // The all valid should account the metadata bit if has.
    //
    // No all null condition as it is not that column that all the columns are nulls, I think
    // so avoid adding a condition in the hot path
    if null_byte == all_valid_byte {
        // All valid
        byte_builders.iter_mut().for_each(|b| b.append(true));
    } else {
        for (mut bit_index, builder) in byte_builders.iter_mut().enumerate() {
            if HAS_METADATA_BIT {
                bit_index += 1;
            }
            let is_valid = (null_byte & (1 << bit_index)) != 0;
            builder.append(is_valid);
        }
    }
}

/// Create a bit packed from 8 u64 items at bit index
///
/// This is carefully done to be vectorized
pub fn fetch_and_shift(bitpacked: [u64; 8], bit_index: usize) -> u8 {
    // Each bit should be shift by bit_index

    const SHIFT: [u8; 8] = [0, 1, 2, 3, 4, 5, 6, 7];

    // single value logic:
    // shift bitpacked by bit_index and mask with 1
    // then shift by the corresponding SHIFT value
    // to make it in the correct position
    // and then OR with the rest of the items.

    // Not doing manual loop as it will not be vectorized
    let a = bitpacked
        .iter()
        .map(|&item| ((item >> bit_index) & 1) as u8)
        .zip(SHIFT)
        .map(|(item, shift)| item << shift)
        // Collecting as the fold break the vectorization
        .collect::<Vec<_>>();

    a.into_iter().fold(0, |acc, item| acc | item)
}
