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

//! PFOR decoder. See [`crate::encodings::pfor`] for the wire format.

use std::marker::PhantomData;

use bytes::Bytes;

use super::Decoder;
use crate::basic::Encoding;
use crate::data_type::DataType;
use crate::encodings::pfor::{
    DEFAULT_LOG_VECTOR_SIZE, HEADER_SIZE, OFFSET_SIZE, PACKING_MODE_FOR_BIT_PACK, POSITION_SIZE,
    PforHeader, PforInt, PforVectorInfo, bytes_for_bits, read_offset, validate_offsets,
};
use crate::errors::{ParquetError, Result};
use crate::util::bit_util::BitReader;

/// Decoder for [`Encoding::PFOR`].
///
/// A page is decoded one vector at a time into a buffer of the decoder's own, and callers are
/// served out of that buffer. Decoding a whole vector at once is what the format asks for -- the
/// residuals are bit-packed as one run and the exceptions patched over the result -- and buffering
/// it is what lets [`Decoder::get`] stop mid-vector and resume where it left off.
pub struct PforDecoder<T: DataType> {
    /// The page, from its header on. `None` until [`Decoder::set_data`].
    data: Option<Bytes>,
    /// Header of the page being decoded.
    header: PforHeader,
    /// The vector currently being served, decoded in full.
    vector: Vec<T::T>,
    /// Position in [`Self::vector`] of the next value to hand out.
    vector_pos: usize,
    /// Index of the next vector to decode.
    next_vector: usize,
    /// Values of the page not yet handed out.
    values_left: usize,
    _phantom: PhantomData<T>,
}

impl<T: DataType> Default for PforDecoder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: DataType> PforDecoder<T> {
    /// Creates a decoder with no page set.
    pub fn new() -> Self {
        Self {
            data: None,
            header: PforHeader {
                packing_mode: PACKING_MODE_FOR_BIT_PACK,
                log_vector_size: DEFAULT_LOG_VECTOR_SIZE,
                value_byte_width: 0,
                num_elements: 0,
            },
            vector: Vec::new(),
            vector_pos: 0,
            next_vector: 0,
            values_left: 0,
            _phantom: PhantomData,
        }
    }
}

impl<T: DataType> PforDecoder<T>
where
    T::T: PforInt,
{
    /// Number of elements vector `index` of the current page holds.
    ///
    /// Every vector but the last is full; the last holds whatever remains.
    fn elements_in_vector(&self, index: usize) -> usize {
        let vector_size = self.header.vector_size();
        let start = index * vector_size;
        std::cmp::min(vector_size, self.header.num_elements as usize - start)
    }

    /// Decode vector `index` into the decoder's buffer, replacing whatever it held.
    ///
    /// The work runs in the order the format lays the vector out: locate and check it, then
    /// residuals, then exceptions, then -- for a differenced vector -- the running sum.
    fn decode_vector(&mut self, index: usize) -> Result<()> {
        let data = self
            .data
            .as_ref()
            .ok_or_else(|| general_err!("PFOR decoder has no data"))?;
        let num_elements = self.elements_in_vector(index);

        // Offsets count from the start of the offset array, which follows the header.
        let payload = &data[HEADER_SIZE..];
        let vector_at = read_offset(payload, index);
        let src = &payload[vector_at..];

        let layout = VectorLayout::read(src, num_elements)?;
        let info = layout.info;
        let frame = info.frame_of_reference;

        self.vector.clear();
        self.vector.resize(num_elements, T::T::default());

        // A constant vector, which is the whole of what it stores.
        if info.bit_width == 0 && info.num_exceptions == 0 {
            if info.is_delta {
                step_by(&mut self.vector, layout.start_value, frame);
            } else {
                self.vector.fill(frame);
            }
            return Ok(());
        }

        if info.bit_width > 0 {
            // Slice out of the page rather than out of `src`: BitReader wants an owned `Bytes`.
            let packed_at = HEADER_SIZE + vector_at + layout.packed_at;
            let packed = data.slice(packed_at..packed_at + layout.packed_bytes);
            unpack_residuals(&mut self.vector, packed, info.bit_width, frame)?;
        } else {
            // Width zero with exceptions: every unpatched slot is the frame itself.
            self.vector.fill(frame);
        }

        if info.num_exceptions > 0 {
            patch_exceptions(
                &mut self.vector,
                &src[layout.positions_at..layout.values_at],
                &src[layout.values_at..layout.values_end],
            )?;
        }

        // In a differenced vector, everything above produced differences. Sum them.
        //
        // This has to come after the patch: an exception in a differenced vector is a difference
        // too, and summing before patching would carry the placeholder zero into every value that
        // follows.
        if info.is_delta {
            accumulate(&mut self.vector, layout.start_value);
        }

        Ok(())
    }

    /// Make sure [`Self::vector`] has a value to hand out, decoding the next vector if not.
    ///
    /// Returns false once the page is exhausted.
    fn ensure_vector(&mut self) -> Result<bool> {
        while self.vector_pos >= self.vector.len() {
            if self.next_vector >= self.header.num_vectors() {
                return Ok(false);
            }
            let index = self.next_vector;
            self.next_vector += 1;
            self.decode_vector(index)?;
            self.vector_pos = 0;
        }
        Ok(true)
    }
}

impl<T: DataType> Decoder<T> for PforDecoder<T>
where
    T::T: PforInt,
{
    fn set_data(&mut self, data: Bytes, num_values: usize) -> Result<()> {
        let header = PforHeader::read::<T::T>(&data)?;

        // The page carries its own count, and that is the count the vectors were laid out from, so
        // it is what the decoder reads. `num_values` is the level count when a V1 page does not
        // give a value count of its own, and that includes nulls, which PFOR does not store -- so
        // the two are equal only for a required column. What cannot happen either way is a page
        // claiming more values than the levels leave room for.
        if header.num_elements as usize > num_values {
            return Err(general_err!(
                "PFOR header element count {} exceeds the {} values the page has room for",
                header.num_elements,
                num_values
            ));
        }

        let num_vectors = header.num_vectors();
        let offset_array_size = num_vectors * OFFSET_SIZE;
        if HEADER_SIZE + offset_array_size > data.len() {
            return Err(general_err!(
                "PFOR offset array for {} vectors does not fit in {} bytes",
                num_vectors,
                data.len()
            ));
        }
        let payload_size = data.len() - HEADER_SIZE;
        validate_offsets(&data[HEADER_SIZE..], num_vectors, payload_size)?;

        self.values_left = header.num_elements as usize;
        self.header = header;
        self.data = Some(data);
        self.vector.clear();
        self.vector_pos = 0;
        self.next_vector = 0;
        Ok(())
    }

    fn get(&mut self, buffer: &mut [T::T]) -> Result<usize> {
        let mut written = 0;
        while written < buffer.len() {
            if !self.ensure_vector()? {
                break;
            }
            let available = self.vector.len() - self.vector_pos;
            let take = std::cmp::min(available, buffer.len() - written);
            buffer[written..written + take]
                .clone_from_slice(&self.vector[self.vector_pos..self.vector_pos + take]);
            self.vector_pos += take;
            written += take;
        }
        self.values_left -= written;
        Ok(written)
    }

    fn values_left(&self) -> usize {
        self.values_left
    }

    fn encoding(&self) -> Encoding {
        Encoding::PFOR
    }

    fn skip(&mut self, num_values: usize) -> Result<usize> {
        let mut skipped = 0;
        while skipped < num_values {
            // A vector nobody has read into can be skipped without decoding it at all, which is
            // the whole reason the offset array is in the page.
            if self.vector_pos >= self.vector.len() {
                if self.next_vector >= self.header.num_vectors() {
                    break;
                }
                let whole = self.elements_in_vector(self.next_vector);
                if skipped + whole <= num_values {
                    self.next_vector += 1;
                    self.vector.clear();
                    self.vector_pos = 0;
                    skipped += whole;
                    continue;
                }
            }
            if !self.ensure_vector()? {
                break;
            }
            let available = self.vector.len() - self.vector_pos;
            let take = std::cmp::min(available, num_values - skipped);
            self.vector_pos += take;
            skipped += take;
        }
        self.values_left -= skipped;
        Ok(skipped)
    }
}

/// Where the parts of one encoded vector sit, once their sizes have been checked.
///
/// Every field below is sized by numbers that came off the wire, so they are checked here --
/// against the vector's element count and against the bytes that remain -- before anything reads
/// through them. Offsets are relative to the start of the vector, not of the page.
#[derive(Debug, Clone, Copy)]
struct VectorLayout<V> {
    info: PforVectorInfo<V>,
    /// First value of a differenced vector, zero otherwise.
    start_value: V,
    packed_at: usize,
    packed_bytes: usize,
    positions_at: usize,
    values_at: usize,
    values_end: usize,
}

impl<V: PforInt> VectorLayout<V> {
    fn read(src: &[u8], num_elements: usize) -> Result<Self> {
        let info = PforVectorInfo::<V>::read(src)?;
        let info_bytes = info.stored_bytes();
        if info_bytes > src.len() {
            return Err(general_err!(
                "PFOR delta vector needs {} bytes of metadata but only {} remain",
                info_bytes,
                src.len()
            ));
        }

        // A differenced vector stores its own first value, which is what lets it decode without
        // the vector before it -- the property the mode exists for.
        let start_value = if info.is_delta {
            V::read_le(&src[V::INFO_SIZE..])
        } else {
            V::default()
        };

        if info.num_exceptions as usize > num_elements {
            return Err(general_err!(
                "PFOR vector has {} exceptions but only {} elements",
                info.num_exceptions,
                num_elements
            ));
        }

        let num_exceptions = info.num_exceptions as usize;
        let packed_bytes = bytes_for_bits(num_elements * info.bit_width as usize);
        let positions_at = info_bytes + packed_bytes;
        let values_at = positions_at + num_exceptions * POSITION_SIZE;
        let values_end = values_at + num_exceptions * V::BYTE_WIDTH;
        if values_end > src.len() {
            return Err(general_err!(
                "PFOR vector needs {} bytes but only {} remain",
                values_end,
                src.len()
            ));
        }

        Ok(Self {
            info,
            start_value,
            packed_at: info_bytes,
            packed_bytes,
            positions_at,
            values_at,
            values_end,
        })
    }
}

/// Unpack the residuals into `out` and add the frame back.
///
/// The add is modular in the unsigned domain, so the bits the unpacker wrote are the signed values
/// once the frame is on them, exactly as the encoder produced them.
///
/// TODO: fuse the frame add into the unpacking kernel. Paired per column against the C++
/// implementation of this format on 29 shared int32 columns, this decoder runs at 0.37x of it
/// (0.32x-0.70x, slower on all 29), while `DELTA_BINARY_PACKED` in this crate runs at 1.55x of
/// the same C++ benchmark on 29 of 29 -- so the gap is this inner loop, not the machine or the
/// harness. Two candidates, not yet separated: the C++ side adds the frame inside its vectorized
/// unpack kernel where this makes a second pass over the whole vector, and `unpack32` here is
/// scalar code left to the autovectorizer. A fused variant would settle how much of the 2.7x is
/// the extra pass.
fn unpack_residuals<V: PforInt>(
    out: &mut [V],
    packed: Bytes,
    bit_width: u8,
    frame: V,
) -> Result<()> {
    let mut reader = BitReader::new(packed);
    let unpacked = reader.get_batch(out, bit_width as usize);
    if unpacked != out.len() {
        return Err(general_err!(
            "PFOR vector unpacked {} of {} values",
            unpacked,
            out.len()
        ));
    }
    let frame_bits = frame.to_bits();
    if frame_bits != 0 {
        for slot in out {
            *slot = V::from_bits(slot.to_bits().wrapping_add(frame_bits));
        }
    }
    Ok(())
}

/// Patch the exceptions over `out`, taking their positions and values from the two arrays that
/// close the vector.
///
/// An exception carries whatever the packed stream carries: a value in a plain vector, a difference
/// in a differenced one.
fn patch_exceptions<V: PforInt>(out: &mut [V], positions: &[u8], values: &[u8]) -> Result<()> {
    let num_exceptions = positions.len() / POSITION_SIZE;

    // Every position indexes the vector, so one past its end would be an out-of-bounds write. Take
    // the maximum first: a reduction still vectorizes, where a bounds check inside the patch loop
    // would not.
    let mut max_position = 0u16;
    for i in 0..num_exceptions {
        let at = i * POSITION_SIZE;
        let position = u16::from_le_bytes([positions[at], positions[at + 1]]);
        max_position = max_position.max(position);
    }
    if max_position as usize >= out.len() {
        return Err(general_err!(
            "PFOR exception position {} is outside a vector of {} elements",
            max_position,
            out.len()
        ));
    }

    for i in 0..num_exceptions {
        let at = i * POSITION_SIZE;
        let position = u16::from_le_bytes([positions[at], positions[at + 1]]) as usize;
        out[position] = V::read_le(&values[i * V::BYTE_WIDTH..]);
    }
    Ok(())
}

/// Turn the differences in `out` into values, starting from `start`.
///
/// The sum runs in the unsigned domain for the same reason the frame add does.
fn accumulate<V: PforInt>(out: &mut [V], start: V) {
    let mut acc = start.to_bits();
    for slot in out {
        acc = acc.wrapping_add(slot.to_bits());
        *slot = V::from_bits(acc);
    }
}

/// Fill `out` with the sequence that steps from `start` by `step`.
///
/// This is a differenced vector whose every difference is the frame. Slot 0 always holds a
/// difference of zero, so a frame other than zero cannot occur here, but the running sum
/// reconstructs the sequence either way rather than assuming it.
fn step_by<V: PforInt>(out: &mut [V], start: V, step: V) {
    let step_bits = step.to_bits();
    let mut acc = start.to_bits();
    for (i, slot) in out.iter_mut().enumerate() {
        if i > 0 {
            acc = acc.wrapping_add(step_bits);
        }
        *slot = V::from_bits(acc);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data_type::{Int32Type, Int64Type};

    /// Decode a whole page, checking the value count as well as the values.
    fn decode<T: DataType>(page: &[u8], num_values: usize) -> Result<Vec<T::T>>
    where
        T::T: PforInt,
    {
        let mut decoder = PforDecoder::<T>::new();
        decoder.set_data(Bytes::copy_from_slice(page), num_values)?;
        let mut out = vec![T::T::default(); num_values];
        let read = decoder.get(&mut out)?;
        out.truncate(read);
        assert_eq!(decoder.values_left(), 0);
        Ok(out)
    }

    /// Assemble a page out of literal vector bodies, filling in the offset array.
    ///
    /// The offsets are the one part of a page that cannot be written by hand without recomputing
    /// them on every edit; the vector bodies below are literal bytes, so what is being tested is
    /// still the format and not our encoder.
    fn page(
        log_vector_size: u8,
        value_byte_width: u8,
        num_elements: i32,
        vectors: &[Vec<u8>],
    ) -> Vec<u8> {
        let mut out = vec![PACKING_MODE_FOR_BIT_PACK, log_vector_size, value_byte_width];
        out.extend_from_slice(&num_elements.to_le_bytes());
        let offset_array_at = out.len();
        out.resize(offset_array_at + vectors.len() * OFFSET_SIZE, 0);
        for (v, body) in vectors.iter().enumerate() {
            let offset = (out.len() - offset_array_at) as u32;
            let at = offset_array_at + v * OFFSET_SIZE;
            out[at..at + OFFSET_SIZE].copy_from_slice(&offset.to_le_bytes());
            out.extend_from_slice(body);
        }
        out
    }

    // The four phases of `decode_vector` are exercised through whole pages below. These tests
    // reach them directly, which is the only way to cover an argument no encoder of ours produces.

    #[test]
    fn test_patch_exceptions_writes_positions_and_values() {
        let mut out = vec![0i32; 4];
        let positions = [1u16, 3]
            .iter()
            .flat_map(|p| p.to_le_bytes())
            .collect::<Vec<_>>();
        let values = [-7i32, 9]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect::<Vec<_>>();
        patch_exceptions(&mut out, &positions, &values).unwrap();
        assert_eq!(out, vec![0, -7, 0, 9]);
    }

    #[test]
    fn test_patch_exceptions_rejects_a_position_one_past_the_end() {
        let mut out = vec![0i32; 4];
        let err = patch_exceptions(&mut out, &4u16.to_le_bytes(), &0i32.to_le_bytes()).unwrap_err();
        assert!(
            err.to_string().contains("outside a vector of 4 elements"),
            "{err}"
        );
    }

    #[test]
    fn test_accumulate_sums_differences_and_wraps_at_the_type_width() {
        let mut out = vec![0i32, 5, -2, 7];
        accumulate(&mut out, 10);
        assert_eq!(out, vec![10, 15, 13, 20]);

        // A sum past the type's range wraps, matching what the encoder differenced.
        let mut out = vec![0i32, 1];
        accumulate(&mut out, i32::MAX);
        assert_eq!(out, vec![i32::MAX, i32::MIN]);
    }

    #[test]
    fn test_step_by_reconstructs_a_constant_delta_vector() {
        let mut out = vec![0i64; 4];
        step_by(&mut out, 100, 7);
        assert_eq!(out, vec![100, 107, 114, 121]);

        // The legal case: every difference is zero, so the vector is constant.
        let mut out = vec![0i64; 3];
        step_by(&mut out, -5, 0);
        assert_eq!(out, vec![-5, -5, -5]);
    }

    #[test]
    fn test_unpack_residuals_adds_the_frame_back() {
        // Four 4-bit residuals 1, 2, 3, 4 over a frame of 1000.
        let packed = Bytes::from(vec![0x21u8, 0x43]);
        let mut out = vec![0i32; 4];
        unpack_residuals(&mut out, packed, 4, 1000).unwrap();
        assert_eq!(out, vec![1001, 1002, 1003, 1004]);
    }

    #[test]
    fn test_unpack_residuals_rejects_a_buffer_that_runs_short() {
        let mut out = vec![0i32; 8];
        let err = unpack_residuals(&mut out, Bytes::from(vec![0u8]), 4, 0).unwrap_err();
        assert!(err.to_string().contains("unpacked 2 of 8 values"), "{err}");
    }

    #[test]
    fn test_decode_golden_page_byte_for_byte() {
        // One page written out in full, so that a change to the layout has to change this test.
        // Two vectors of eight: the first packs 100..=107 at three bits over a frame of 100, the
        // second is a pair of sevens and so packs at width zero.
        #[rustfmt::skip]
        let golden = vec![
            // header: bit-packed mode, 2^3 values per vector, 4-byte values, 10 elements
            0x00, 0x03, 0x04, 0x0A, 0x00, 0x00, 0x00,
            // offsets: the first vector starts just past the 8-byte array, the second 10 bytes on
            0x08, 0x00, 0x00, 0x00,
            0x12, 0x00, 0x00, 0x00,
            // vector 0 info: frame 100, width 3, no exceptions
            0x64, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00,
            // vector 0 residuals 0..=7 at 3 bits, least significant bit first
            0x88, 0xC6, 0xFA,
            // vector 1 info: frame 7, width 0, no exceptions -- and so no residuals at all
            0x07, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert_eq!(
            decode::<Int32Type>(&golden, 10).unwrap(),
            vec![100, 101, 102, 103, 104, 105, 106, 107, 7, 7]
        );
    }

    #[test]
    fn test_decode_exceptions_are_patched() {
        // 10..=16 pack at three bits over a frame of 10; 1000 does not, so it travels as an
        // exception and its packed slot holds a zero.
        #[rustfmt::skip]
        let vector = vec![
            0x0A, 0x00, 0x00, 0x00, 0x03, 0x01, 0x00, // frame 10, width 3, one exception
            0x88, 0xC6, 0x1A,                         // residuals 0..=6 then the placeholder
            0x07, 0x00,                               // the exception is at position 7
            0xE8, 0x03, 0x00, 0x00,                   // and its value is 1000
        ];
        assert_eq!(
            decode::<Int32Type>(&page(3, 4, 8, &[vector]), 8).unwrap(),
            vec![10, 11, 12, 13, 14, 15, 16, 1000]
        );
    }

    #[test]
    fn test_decode_below_frame_values_are_patched() {
        // The frame does not have to be the minimum. Here it is 100 and one value sits below it,
        // where the residual wraps and fails the width test from the other side -- which is what
        // lets one window cover a cluster with outliers on both sides of it.
        #[rustfmt::skip]
        let vector = vec![
            0x64, 0x00, 0x00, 0x00, 0x02, 0x02, 0x00, // frame 100, width 2, two exceptions
            0x24,                                     // residuals 0, 1, 2, and one placeholder
            0x00, 0x00,                               // position 0
            0x03, 0x00,                               // position 3
            0xC6, 0xFF, 0xFF, 0xFF,                   // -58
            0xE8, 0x03, 0x00, 0x00,                   // 1000
        ];
        assert_eq!(
            decode::<Int32Type>(&page(3, 4, 4, &[vector]), 4).unwrap(),
            vec![-58, 101, 102, 1000]
        );
    }

    #[test]
    fn test_decode_delta_vector() {
        // The differencing flag in bit 7 of the width byte, a start value between the info and the
        // residuals, and d[0] == 0.
        #[rustfmt::skip]
        let vector = vec![
            0x00, 0x00, 0x00, 0x00, 0x82, 0x00, 0x00, // frame 0, width 2 with the delta flag
            0xE8, 0x03, 0x00, 0x00,                   // start value 1000
            0xA8, 0xAA,                               // 0 then seven twos, at 2 bits each
        ];
        assert_eq!(
            decode::<Int32Type>(&page(3, 4, 8, &[vector]), 8).unwrap(),
            vec![1000, 1002, 1004, 1006, 1008, 1010, 1012, 1014]
        );
    }

    #[test]
    fn test_decode_delta_vector_patches_before_summing() {
        // An exception in a differenced vector holds a difference, and the sum has to run after the
        // patch: summing first would carry the placeholder zero into every value after it.
        #[rustfmt::skip]
        let vector = vec![
            0x00, 0x00, 0x00, 0x00, 0x81, 0x01, 0x00, // frame 0, width 1 with the delta flag
            0x0A, 0x00, 0x00, 0x00,                   // start value 10
            0x0A,                                     // differences 0, 1, 0 (a placeholder), 1
            0x02, 0x00,                               // the placeholder is at position 2
            0xF4, 0x01, 0x00, 0x00,                   // and the difference there is really 500
        ];
        assert_eq!(
            decode::<Int32Type>(&page(3, 4, 4, &[vector]), 4).unwrap(),
            vec![10, 11, 511, 512]
        );
    }

    #[test]
    fn test_decode_delta_vector_at_width_zero() {
        // A constant difference needs no residuals at all: the values are the start value plus the
        // frame, stepped. That skip is the decoder's fast path, so it gets a page of its own.
        #[rustfmt::skip]
        let vector = vec![
            0x03, 0x00, 0x00, 0x00, 0x80, 0x00, 0x00, // frame 3, width 0 with the delta flag
            0x05, 0x00, 0x00, 0x00,                   // start value 5
        ];
        assert_eq!(
            decode::<Int32Type>(&page(3, 4, 4, &[vector]), 4).unwrap(),
            vec![5, 8, 11, 14]
        );
    }

    #[test]
    fn test_decode_int64_at_full_width() {
        // Width 64 needs all seven bits of the width field, and a six-bit field would read it as a
        // constant vector. At that width the residuals are just little-endian words, which is what
        // makes the page writable by hand.
        let frame = i64::MIN;
        let values = vec![i64::MIN, i64::MAX, -1, 0, 1, 2, 3, 4];
        let mut vector = frame.to_le_bytes().to_vec();
        vector.push(64);
        vector.extend_from_slice(&0u16.to_le_bytes());
        for value in &values {
            vector.extend_from_slice(&(value.wrapping_sub(frame) as u64).to_le_bytes());
        }
        assert_eq!(
            decode::<Int64Type>(&page(3, 8, 8, &[vector]), 8).unwrap(),
            values
        );
    }

    #[test]
    fn test_decode_empty_page() {
        // No values still has to be a readable page: a reader loads the header before it knows how
        // many values the page holds.
        let empty = page(3, 4, 0, &[]);
        assert_eq!(empty.len(), HEADER_SIZE);
        assert!(decode::<Int32Type>(&empty, 0).unwrap().is_empty());
    }

    #[test]
    fn test_get_stops_and_resumes_mid_vector() {
        // Vectors of eight, read three at a time, so every read but the first starts part-way into
        // a buffered vector and one of them spans a vector boundary.
        let expected: Vec<i32> = (0..20).collect();
        let page = encoded_page::<Int32Type>(&expected, 8);

        let mut decoder = PforDecoder::<Int32Type>::new();
        decoder.set_data(page, expected.len()).unwrap();

        let mut got = Vec::new();
        let mut buffer = [0i32; 3];
        loop {
            let read = decoder.get(&mut buffer).unwrap();
            if read == 0 {
                break;
            }
            got.extend_from_slice(&buffer[..read]);
            assert_eq!(decoder.values_left(), expected.len() - got.len());
        }
        assert_eq!(got, expected);
    }

    #[test]
    fn test_skip_whole_and_partial_vectors() {
        let expected: Vec<i32> = (0..40).collect();

        // A skip landing exactly on a vector boundary, which decodes nothing at all.
        let mut decoder = PforDecoder::<Int32Type>::new();
        decoder
            .set_data(encoded_page::<Int32Type>(&expected, 8), 40)
            .unwrap();
        assert_eq!(decoder.skip(16).unwrap(), 16);
        assert_eq!(decoder.values_left(), 24);
        let mut out = vec![0i32; 24];
        assert_eq!(decoder.get(&mut out).unwrap(), 24);
        assert_eq!(out, expected[16..]);

        // A skip stopping part-way into a vector, then a read that continues from there.
        let mut decoder = PforDecoder::<Int32Type>::new();
        decoder
            .set_data(encoded_page::<Int32Type>(&expected, 8), 40)
            .unwrap();
        assert_eq!(decoder.skip(19).unwrap(), 19);
        let mut out = vec![0i32; 5];
        assert_eq!(decoder.get(&mut out).unwrap(), 5);
        assert_eq!(out, expected[19..24]);

        // Interleaved reads and skips.
        let mut decoder = PforDecoder::<Int32Type>::new();
        decoder
            .set_data(encoded_page::<Int32Type>(&expected, 8), 40)
            .unwrap();
        let mut out = vec![0i32; 3];
        decoder.get(&mut out).unwrap();
        assert_eq!(out, expected[..3]);
        assert_eq!(decoder.skip(30).unwrap(), 30);
        let mut out = vec![0i32; 7];
        assert_eq!(decoder.get(&mut out).unwrap(), 7);
        assert_eq!(out, expected[33..]);

        // A skip past the end reports what it could actually skip.
        let mut decoder = PforDecoder::<Int32Type>::new();
        decoder
            .set_data(encoded_page::<Int32Type>(&expected, 8), 40)
            .unwrap();
        assert_eq!(decoder.skip(100).unwrap(), 40);
        assert_eq!(decoder.values_left(), 0);
        assert_eq!(decoder.get(&mut out).unwrap(), 0);
    }

    #[test]
    fn test_set_data_rejects_a_header_it_cannot_read() {
        let mut decoder = PforDecoder::<Int32Type>::new();
        // Shorter than a header.
        assert!(decoder.set_data(Bytes::from(vec![0u8; 3]), 0).is_err());
        // An INT64 page handed to an INT32 column.
        let page = page(3, 8, 0, &[]);
        assert!(decoder.set_data(Bytes::from(page), 0).is_err());
    }

    #[test]
    fn test_set_data_rejects_more_elements_than_the_page_has_room_for() {
        // The header claims eight values where the caller knows of four. Believing it would hand
        // out values built from whatever follows.
        let mut page = page(3, 4, 8, &[vec![0u8; 7]]);
        page[3] = 8;
        let mut decoder = PforDecoder::<Int32Type>::new();
        assert!(decoder.set_data(Bytes::from(page), 4).is_err());
    }

    #[test]
    fn test_set_data_rejects_a_truncated_offset_array() {
        // Two vectors' worth of header, and not enough page left to hold their offsets.
        let mut page = page(3, 4, 16, &[vec![0u8; 7], vec![0u8; 7]]);
        page.truncate(HEADER_SIZE + OFFSET_SIZE);
        let mut decoder = PforDecoder::<Int32Type>::new();
        assert!(decoder.set_data(Bytes::from(page), 16).is_err());
    }

    #[test]
    fn test_set_data_rejects_a_broken_offset_chain() {
        let good = page(3, 4, 16, &[vec![0u8; 7], vec![0u8; 7]]);

        // The second vector starting inside the first.
        let mut broken = good.clone();
        broken[HEADER_SIZE + OFFSET_SIZE..HEADER_SIZE + 2 * OFFSET_SIZE]
            .copy_from_slice(&4u32.to_le_bytes());
        let mut decoder = PforDecoder::<Int32Type>::new();
        assert!(decoder.set_data(Bytes::from(broken), 16).is_err());

        // The first vector not landing just past the offset array.
        let mut broken = good.clone();
        broken[HEADER_SIZE..HEADER_SIZE + OFFSET_SIZE].copy_from_slice(&99u32.to_le_bytes());
        let mut decoder = PforDecoder::<Int32Type>::new();
        assert!(decoder.set_data(Bytes::from(broken), 16).is_err());

        // An offset past the end of the payload.
        let mut broken = good;
        broken[HEADER_SIZE + OFFSET_SIZE..HEADER_SIZE + 2 * OFFSET_SIZE]
            .copy_from_slice(&10_000u32.to_le_bytes());
        let mut decoder = PforDecoder::<Int32Type>::new();
        assert!(decoder.set_data(Bytes::from(broken), 16).is_err());
    }

    #[test]
    fn test_decode_rejects_a_width_the_type_cannot_hold() {
        // Width 33 on an INT32 column. Nothing packs to it, and honouring it would read past the
        // vector.
        let mut vector = vec![0u8; 7];
        vector[4] = 33;
        vector.extend_from_slice(&[0u8; 64]);
        let err = decode::<Int32Type>(&page(3, 4, 8, &[vector]), 8).unwrap_err();
        assert!(err.to_string().contains("bit_width"), "{err}");
    }

    #[test]
    fn test_decode_rejects_an_exception_position_outside_the_vector() {
        // Position 8 in a vector of eight: one past the end, and so a write past the end of the
        // decoded buffer.
        #[rustfmt::skip]
        let vector = vec![
            0x00, 0x00, 0x00, 0x00, 0x03, 0x01, 0x00, // width 3, one exception
            0x00, 0x00, 0x00,                         // eight residuals at 3 bits
            0x08, 0x00,                               // position 8, which does not exist
            0x00, 0x00, 0x00, 0x00,
        ];
        let err = decode::<Int32Type>(&page(3, 4, 8, &[vector]), 8).unwrap_err();
        assert!(err.to_string().contains("outside a vector"), "{err}");
    }

    #[test]
    fn test_decode_rejects_a_vector_that_runs_off_the_page() {
        // The info block promises eight 32-bit residuals and the page ends after three bytes of
        // them. Every section a vector claims is bounded against what is left of the page.
        #[rustfmt::skip]
        let vector = vec![
            0x00, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, // width 32
            0x00, 0x00, 0x00,
        ];
        assert!(decode::<Int32Type>(&page(3, 4, 8, &[vector]), 8).is_err());

        // The same for the exception sections, which sit after the residuals.
        #[rustfmt::skip]
        let vector = vec![
            0x00, 0x00, 0x00, 0x00, 0x03, 0x02, 0x00, // width 3, two exceptions
            0x00, 0x00, 0x00,                         // the residuals, and then nothing
            0x00, 0x00,
        ];
        assert!(decode::<Int32Type>(&page(3, 4, 8, &[vector]), 8).is_err());
    }

    #[test]
    fn test_decode_rejects_a_truncated_info_block() {
        assert!(decode::<Int32Type>(&page(3, 4, 8, &[vec![0u8; 6]]), 8).is_err());
    }

    /// Encode `values` with the Rust encoder, for the tests that are about reading rather than
    /// about the byte layout.
    fn encoded_page<T: DataType>(values: &[T::T], vector_size: usize) -> Bytes
    where
        T::T: PforInt,
    {
        use crate::encodings::encoding::{Encoder, PforEncoder};
        let mut encoder = PforEncoder::<T>::new()
            .with_vector_size(vector_size)
            .unwrap();
        encoder.put(values).unwrap();
        encoder.flush_buffer().unwrap()
    }
}
