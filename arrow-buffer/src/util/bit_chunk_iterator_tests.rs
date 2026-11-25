// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Tests for `bit_chunk_iterator`, which build their inputs with [`Buffer`]
use rand::distr::uniform::UniformSampler;
use rand::distr::uniform::UniformUsize;
use rand::prelude::*;
use rand::rng;

use crate::buffer::Buffer;
use crate::util::bit_chunk_iterator::UnalignedBitChunk;
use crate::util::bit_util::get_bit;

#[test]
fn test_iter_aligned() {
    let input: &[u8] = &[0, 1, 2, 3, 4, 5, 6, 7];
    let buffer: Buffer = Buffer::from(input);

    let bitchunks = buffer.bit_chunks(0, 64);
    let result = bitchunks.into_iter().collect::<Vec<_>>();

    assert_eq!(vec![0x0706050403020100], result);
}

#[test]
fn test_iter_unaligned() {
    let input: &[u8] = &[
        0b00000000, 0b00000001, 0b00000010, 0b00000100, 0b00001000, 0b00010000, 0b00100000,
        0b01000000, 0b11111111,
    ];
    let buffer: Buffer = Buffer::from(input);

    let bitchunks = buffer.bit_chunks(4, 64);

    assert_eq!(0, bitchunks.remainder_len());
    assert_eq!(0, bitchunks.remainder_bits());

    let result = bitchunks.into_iter().collect::<Vec<_>>();

    assert_eq!(
        vec![0b1111010000000010000000010000000010000000010000000010000000010000],
        result
    );
}

#[test]
fn test_iter_unaligned_remainder_1_byte() {
    let input: &[u8] = &[
        0b00000000, 0b00000001, 0b00000010, 0b00000100, 0b00001000, 0b00010000, 0b00100000,
        0b01000000, 0b11111111,
    ];
    let buffer: Buffer = Buffer::from(input);

    let bitchunks = buffer.bit_chunks(4, 66);

    assert_eq!(2, bitchunks.remainder_len());
    assert_eq!(0b00000011, bitchunks.remainder_bits());

    let result = bitchunks.into_iter().collect::<Vec<_>>();

    assert_eq!(
        vec![0b1111010000000010000000010000000010000000010000000010000000010000],
        result
    );
}

#[test]
fn test_iter_unaligned_remainder_bits_across_bytes() {
    let input: &[u8] = &[0b00111111, 0b11111100];
    let buffer: Buffer = Buffer::from(input);

    // remainder contains bits from both bytes
    // result should be the highest 2 bits from first byte followed by lowest 5 bits of second bytes
    let bitchunks = buffer.bit_chunks(6, 7);

    assert_eq!(7, bitchunks.remainder_len());
    assert_eq!(0b1110000, bitchunks.remainder_bits());
}

#[test]
fn test_iter_unaligned_remainder_bits_large() {
    let input: &[u8] = &[
        0b11111111, 0b00000000, 0b11111111, 0b00000000, 0b11111111, 0b00000000, 0b11111111,
        0b00000000, 0b11111111,
    ];
    let buffer: Buffer = Buffer::from(input);

    let bitchunks = buffer.bit_chunks(2, 63);

    assert_eq!(63, bitchunks.remainder_len());
    assert_eq!(
        0b100_0000_0011_1111_1100_0000_0011_1111_1100_0000_0011_1111_1100_0000_0011_1111,
        bitchunks.remainder_bits()
    );
}

#[test]
fn test_chunk_aligned() {
    let input: Vec<u8> = (0..24).collect();
    let buffer = Buffer::from(input);

    let bitchunks = buffer.bit_chunks(0, 24 * 8);
    assert_eq!(3, bitchunks.chunk_len());
    assert_eq!(0x0706050403020100, bitchunks.chunk(0));
    assert_eq!(0x0f0e0d0c0b0a0908, bitchunks.chunk(1));
    assert_eq!(0x1716151413121110, bitchunks.chunk(2));
}

#[test]
fn test_chunk_matches_iter() {
    // 26 bytes cover three complete chunks at every offset below, with
    // the last one needing the byte after its eight when the bit offset
    // is not zero
    let input: Vec<u8> = (0..26_u8).map(|i| i.wrapping_mul(13)).collect();
    let buffer = Buffer::from(input);

    // Bit offsets within a byte and across one, lengths ending on and
    // off a word boundary
    for offset in (0..8).chain([8, 13]) {
        for len in [64, 65, 128, 130, 191, 192] {
            let bitchunks = buffer.bit_chunks(offset, len);
            assert_eq!(len / 64, bitchunks.chunk_len());
            for index in 0..bitchunks.chunk_len() {
                assert_eq!(
                    bitchunks.iter().nth(index),
                    Some(bitchunks.chunk(index)),
                    "offset {offset} len {len} chunk {index}"
                );
            }
        }
    }
}

#[test]
fn test_bit_chunk_iterator_nth() {
    let input: Vec<u8> = (0..26_u8).map(|i| i.wrapping_mul(13)).collect();
    let buffer = Buffer::from(input);
    for offset in [0, 3] {
        let chunks = buffer.bit_chunks(offset, 192);
        let all: Vec<u64> = chunks.iter().collect();
        // From every position, skip to every chunk and one past the end
        for start in 0..=all.len() {
            for n in 0..=all.len() + 1 {
                let mut iter = chunks.iter();
                iter.by_ref().take(start).for_each(drop);
                assert_eq!(iter.nth(n), all.get(start + n).copied());
                assert_eq!(iter.len(), all.len().saturating_sub(start + n + 1));
                assert_eq!(iter.next(), all.get(start + n + 1).copied());
            }
        }
        let mut iter = chunks.iter();
        iter.next();
        assert_eq!(iter.nth(usize::MAX), None);
        assert_eq!(iter.len(), 0);
    }
}

#[test]
#[should_panic(expected = "chunk index out of bounds")]
fn test_chunk_out_of_bounds() {
    let buffer = Buffer::from(vec![0xFF_u8; 16]);
    let bitchunks = buffer.bit_chunks(0, 128);
    assert_eq!(2, bitchunks.chunk_len());
    bitchunks.chunk(2);
}

#[test]
#[should_panic(expected = "chunk index out of bounds")]
fn test_chunk_out_of_bounds_no_complete_chunk() {
    let buffer = Buffer::from(vec![0xFF_u8; 8]);
    let bitchunks = buffer.bit_chunks(1, 63);
    assert_eq!(0, bitchunks.chunk_len());
    bitchunks.chunk(0);
}

#[test]
fn test_iter_remainder_out_of_bounds() {
    // allocating a full page should trigger a fault when reading out of bounds
    const ALLOC_SIZE: usize = 4 * 1024;
    let input = vec![0xFF_u8; ALLOC_SIZE];

    let buffer: Buffer = Buffer::from_vec(input);

    let bitchunks = buffer.bit_chunks(57, ALLOC_SIZE * 8 - 57);

    assert_eq!(u64::MAX, bitchunks.iter().last().unwrap());
    assert_eq!(0x7F, bitchunks.remainder_bits());
}

#[test]
#[should_panic(expected = "offset + len out of bounds")]
fn test_out_of_bound_should_panic_length_is_more_than_buffer_length() {
    const ALLOC_SIZE: usize = 4 * 1024;
    let input = vec![0xFF_u8; ALLOC_SIZE];

    let buffer: Buffer = Buffer::from_vec(input);

    // We are reading more than exists in the buffer
    buffer.bit_chunks(0, (ALLOC_SIZE + 1) * 8);
}

#[test]
#[should_panic(expected = "offset + len out of bounds")]
fn test_out_of_bound_should_panic_length_is_more_than_buffer_length_but_not_when_not_using_ceil() {
    const ALLOC_SIZE: usize = 4 * 1024;
    let input = vec![0xFF_u8; ALLOC_SIZE];

    let buffer: Buffer = Buffer::from_vec(input);

    // We are reading more than exists in the buffer
    buffer.bit_chunks(0, (ALLOC_SIZE * 8) + 1);
}

#[test]
#[should_panic(expected = "offset + len out of bounds")]
fn test_out_of_bound_should_panic_when_offset_is_not_zero_and_length_is_the_entire_buffer_length() {
    const ALLOC_SIZE: usize = 4 * 1024;
    let input = vec![0xFF_u8; ALLOC_SIZE];

    let buffer: Buffer = Buffer::from_vec(input);

    // We are reading more than exists in the buffer
    buffer.bit_chunks(8, ALLOC_SIZE * 8);
}

#[test]
#[should_panic(expected = "offset + len out of bounds")]
fn test_out_of_bound_should_panic_when_offset_is_not_zero_and_length_is_the_entire_buffer_length_with_ceil()
 {
    const ALLOC_SIZE: usize = 4 * 1024;
    let input = vec![0xFF_u8; ALLOC_SIZE];

    let buffer: Buffer = Buffer::from_vec(input);

    // We are reading more than exists in the buffer
    buffer.bit_chunks(1, ALLOC_SIZE * 8);
}

#[test]
#[should_panic(expected = "offset + len out of bounds")]
fn test_out_of_bound_should_panic_when_offset_and_length_overflow() {
    let buffer = Buffer::from(vec![0xFF_u8; 8]);
    buffer.bit_chunks(1, usize::MAX);
}

#[test]
fn test_unaligned_bit_chunk_iterator() {
    let buffer = Buffer::from(&[0xFF; 5]);
    let unaligned = UnalignedBitChunk::new(buffer.as_slice(), 0, 40);

    assert!(unaligned.chunks().is_empty()); // Less than 128 elements
    assert_eq!(unaligned.lead_padding(), 0);
    assert_eq!(unaligned.trailing_padding(), 24);
    // 24x 1 bit then 40x 0 bits
    assert_eq!(
        unaligned.prefix(),
        Some(0b0000000000000000000000001111111111111111111111111111111111111111)
    );
    assert_eq!(unaligned.suffix(), None);

    let buffer = buffer.slice(1);
    let unaligned = UnalignedBitChunk::new(buffer.as_slice(), 0, 32);

    assert!(unaligned.chunks().is_empty()); // Less than 128 elements
    assert_eq!(unaligned.lead_padding(), 0);
    assert_eq!(unaligned.trailing_padding(), 32);
    // 32x 1 bit then 32x 0 bits
    assert_eq!(
        unaligned.prefix(),
        Some(0b0000000000000000000000000000000011111111111111111111111111111111)
    );
    assert_eq!(unaligned.suffix(), None);

    let unaligned = UnalignedBitChunk::new(buffer.as_slice(), 5, 27);

    assert!(unaligned.chunks().is_empty()); // Less than 128 elements
    assert_eq!(unaligned.lead_padding(), 5); // 5 % 8 == 5
    assert_eq!(unaligned.trailing_padding(), 32);
    // 5x 0 bit, 27x 1 bit then 32x 0 bits
    assert_eq!(
        unaligned.prefix(),
        Some(0b0000000000000000000000000000000011111111111111111111111111100000)
    );
    assert_eq!(unaligned.suffix(), None);

    let unaligned = UnalignedBitChunk::new(buffer.as_slice(), 12, 20);

    assert!(unaligned.chunks().is_empty()); // Less than 128 elements
    assert_eq!(unaligned.lead_padding(), 4); // 12 % 8 == 4
    assert_eq!(unaligned.trailing_padding(), 40);
    // 4x 0 bit, 20x 1 bit then 40x 0 bits
    assert_eq!(
        unaligned.prefix(),
        Some(0b0000000000000000000000000000000000000000111111111111111111110000)
    );
    assert_eq!(unaligned.suffix(), None);

    let buffer = Buffer::from(&[0xFF; 14]);

    // Verify buffer alignment
    let (prefix, aligned, suffix) = unsafe { buffer.as_slice().align_to::<u64>() };
    assert_eq!(prefix.len(), 0);
    assert_eq!(aligned.len(), 1);
    assert_eq!(suffix.len(), 6);

    let unaligned = UnalignedBitChunk::new(buffer.as_slice(), 0, 112);

    assert!(unaligned.chunks().is_empty()); // Less than 128 elements
    assert_eq!(unaligned.lead_padding(), 0); // No offset and buffer aligned on 64-bit boundary
    assert_eq!(unaligned.trailing_padding(), 16);
    assert_eq!(unaligned.prefix(), Some(u64::MAX));
    assert_eq!(unaligned.suffix(), Some((1 << 48) - 1));

    let buffer = Buffer::from(&[0xFF; 16]);

    // Verify buffer alignment
    let (prefix, aligned, suffix) = unsafe { buffer.as_slice().align_to::<u64>() };
    assert_eq!(prefix.len(), 0);
    assert_eq!(aligned.len(), 2);
    assert_eq!(suffix.len(), 0);

    let unaligned = UnalignedBitChunk::new(buffer.as_slice(), 0, 128);

    assert_eq!(unaligned.prefix(), Some(u64::MAX));
    assert_eq!(unaligned.suffix(), Some(u64::MAX));
    assert!(unaligned.chunks().is_empty()); // Exactly 128 elements

    let buffer = Buffer::from(&[0xFF; 64]);

    // Verify buffer alignment
    let (prefix, aligned, suffix) = unsafe { buffer.as_slice().align_to::<u64>() };
    assert_eq!(prefix.len(), 0);
    assert_eq!(aligned.len(), 8);
    assert_eq!(suffix.len(), 0);

    let unaligned = UnalignedBitChunk::new(buffer.as_slice(), 0, 512);

    // Buffer is completely aligned and larger than 128 elements -> all in chunks array
    assert_eq!(unaligned.suffix(), None);
    assert_eq!(unaligned.prefix(), None);
    assert_eq!(unaligned.chunks(), [u64::MAX; 8].as_slice());
    assert_eq!(unaligned.lead_padding(), 0);
    assert_eq!(unaligned.trailing_padding(), 0);

    let buffer = buffer.slice(1); // Offset buffer 1 byte off 64-bit alignment

    // Verify buffer alignment
    let (prefix, aligned, suffix) = unsafe { buffer.as_slice().align_to::<u64>() };
    assert_eq!(prefix.len(), 7);
    assert_eq!(aligned.len(), 7);
    assert_eq!(suffix.len(), 0);

    let unaligned = UnalignedBitChunk::new(buffer.as_slice(), 0, 504);

    // Need a prefix with 1 byte of lead padding to bring the buffer into alignment
    assert_eq!(unaligned.prefix(), Some(u64::MAX - 0xFF));
    assert_eq!(unaligned.suffix(), None);
    assert_eq!(unaligned.chunks(), [u64::MAX; 7].as_slice());
    assert_eq!(unaligned.lead_padding(), 8);
    assert_eq!(unaligned.trailing_padding(), 0);

    let unaligned = UnalignedBitChunk::new(buffer.as_slice(), 17, 300);

    // Out of 64-bit alignment by 8 bits from buffer, and 17 bits from provided offset
    //   => need 8 + 17 = 25 bits of lead padding + 39 bits in prefix
    //
    // This leaves 300 - 17 = 261 bits remaining
    //   => 4x 64-bit aligned 64-bit chunks + 5 remaining bits
    //   => trailing padding of 59 bits
    assert_eq!(unaligned.lead_padding(), 25);
    assert_eq!(unaligned.trailing_padding(), 59);
    assert_eq!(unaligned.prefix(), Some(u64::MAX - (1 << 25) + 1));
    assert_eq!(unaligned.suffix(), Some(0b11111));
    assert_eq!(unaligned.chunks(), [u64::MAX; 4].as_slice());

    let unaligned = UnalignedBitChunk::new(buffer.as_slice(), 17, 0);

    assert_eq!(unaligned.prefix(), None);
    assert_eq!(unaligned.suffix(), None);
    assert!(unaligned.chunks().is_empty());
    assert_eq!(unaligned.lead_padding(), 0);
    assert_eq!(unaligned.trailing_padding(), 0);

    let unaligned = UnalignedBitChunk::new(buffer.as_slice(), 17, 1);

    assert_eq!(unaligned.prefix(), Some(2));
    assert_eq!(unaligned.suffix(), None);
    assert!(unaligned.chunks().is_empty());
    assert_eq!(unaligned.lead_padding(), 1);
    assert_eq!(unaligned.trailing_padding(), 62);
}

#[test]
fn test_unaligned_bit_chunk_count_ones() {
    // Sizes in bytes around the 64-byte block and the 2047-block flush in
    // `count_ones_words`. With all-ones input, this fails if the flush comes too
    // late and a u16 lane counter overflows. Random input (`None`) fails if a
    // lane counts the wrong bytes, which uniform input hides.
    let sizes = [
        0, 1, 7, 8, 63, 64, 65, 72, 136, 131_008, 131_072, 131_136, 131_207,
    ];
    let mut rng = StdRng::seed_from_u64(42);
    for size in sizes {
        for byte in [Some(0xFF_u8), Some(0xA5), Some(0x00), None] {
            let bytes = match byte {
                Some(byte) => vec![byte; size],
                None => (0..size).map(|_| rng.random()).collect(),
            };
            let buffer = Buffer::from(bytes);
            let bits = size * 8;
            for offset in [0, 3.min(bits), 64.min(bits)] {
                let len = bits - offset;
                let unaligned = UnalignedBitChunk::new(buffer.as_slice(), offset, len);
                let expected = (offset..bits)
                    .filter(|i| get_bit(buffer.as_slice(), *i))
                    .count();
                assert_eq!(
                    unaligned.count_ones(),
                    expected,
                    "{size} {byte:#x?} {offset}"
                );
            }
        }
    }
}

#[test]
fn fuzz_unaligned_bit_chunk_iterator() {
    let mut rng = rng();

    let uusize = UniformUsize::new(usize::MIN, usize::MAX).unwrap();
    for _ in 0..100 {
        let mask_len = rng.random_range(0..1024);
        let bools: Vec<_> = std::iter::from_fn(|| Some(rng.random()))
            .take(mask_len)
            .collect();

        let buffer = Buffer::from_iter(bools.iter().copied());

        let max_offset = 64.min(mask_len);
        let offset = uusize.sample(&mut rng).checked_rem(max_offset).unwrap_or(0);

        let max_truncate = 128.min(mask_len - offset);
        let truncate = uusize
            .sample(&mut rng)
            .checked_rem(max_truncate)
            .unwrap_or(0);

        let unaligned =
            UnalignedBitChunk::new(buffer.as_slice(), offset, mask_len - offset - truncate);

        let bool_slice = &bools[offset..mask_len - truncate];

        let count = unaligned.count_ones();
        let expected_count = bool_slice.iter().filter(|x| **x).count();

        assert_eq!(count, expected_count);

        let collected: Vec<u64> = unaligned.iter().collect();

        let get_bit = |idx: usize| -> bool {
            let padded_index = idx + unaligned.lead_padding();
            let byte_idx = padded_index / 64;
            let bit_idx = padded_index % 64;
            (collected[byte_idx] & (1 << bit_idx)) != 0
        };

        for (idx, b) in bool_slice.iter().enumerate() {
            assert_eq!(*b, get_bit(idx))
        }
    }
}
