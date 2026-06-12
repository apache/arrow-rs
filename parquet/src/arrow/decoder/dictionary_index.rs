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

use bytes::Bytes;

use crate::encodings::rle::RleDecoder;
use crate::errors::Result;

/// Decoder for `Encoding::RLE_DICTIONARY` indices
pub struct DictIndexDecoder {
    /// Decoder for the dictionary offsets array
    decoder: RleDecoder,

    /// We want to decode the offsets in chunks so we will maintain an internal buffer of decoded
    /// offsets
    index_buf: Box<[i32; 1024]>,
    /// Current length of `index_buf`
    index_buf_len: usize,
    /// Current offset into `index_buf`. If `index_buf_offset` == `index_buf_len` then we've consumed
    /// the entire buffer and need to decode another chunk of offsets.
    index_offset: usize,

    /// This is a maximum as the null count is not always known, e.g. value data from
    /// a v1 data page
    max_remaining_values: usize,
}

impl DictIndexDecoder {
    /// Create a new [`DictIndexDecoder`] with the provided data page, the number of levels
    /// associated with this data page, and the number of non-null values (if known)
    pub fn new(data: Bytes, num_levels: usize, num_values: Option<usize>) -> Result<Self> {
        let bit_width = data[0];
        let mut decoder = RleDecoder::new(bit_width);
        decoder.set_data(data.slice(1..))?;

        Ok(Self {
            decoder,
            index_buf: Box::new([0; 1024]),
            index_buf_len: 0,
            index_offset: 0,
            max_remaining_values: num_values.unwrap_or(num_levels),
        })
    }

    /// Read up to `len` values, returning the number of values read
    /// and calling `f` with each decoded dictionary index
    ///
    /// Will short-circuit and return on error
    #[inline(always)]
    pub fn read<F: FnMut(&[i32]) -> Result<()>>(&mut self, len: usize, mut f: F) -> Result<usize> {
        let total_to_read = len.min(self.max_remaining_values);

        let mut values_read = 0;

        let index_buf = self.index_buf.as_mut();
        while values_read < total_to_read {
            if self.index_offset == self.index_buf_len {
                // We've consumed the entire index buffer so we need to reload it before proceeding
                let read = self.decoder.get_batch(index_buf)?;
                if read == 0 {
                    break;
                }
                self.index_buf_len = read;
                self.index_offset = 0;
            }

            let available = self.index_buf_len - self.index_offset;
            let n = available.min(total_to_read - values_read);

            f(&index_buf[self.index_offset..self.index_offset + n])?;

            self.index_offset += n;
            values_read += n;
        }
        self.max_remaining_values -= values_read;

        Ok(values_read)
    }

    /// Read up to `len` values directly into `output`, gathering through `dict`
    /// in a single pass. Avoids expanding RLE runs into the index buffer and
    /// writes through a `MaybeUninit` slice so the caller does not need to
    /// zero-initialise output capacity.
    pub fn read_with_dict<T: Clone>(
        &mut self,
        len: usize,
        dict: &[T],
        output: &mut [std::mem::MaybeUninit<T>],
    ) -> Result<usize> {
        use crate::errors::ParquetError;
        let total_to_read = len.min(self.max_remaining_values);
        let mut values_read = 0;

        // Drain any leftover indices buffered from a prior `read` call before
        // switching to the direct-gather path. Uses the same CHUNK=16 +
        // max-reduction pattern as `RleDecoder::get_batch_with_dict`.
        let leftover = self.index_buf_len - self.index_offset;
        if leftover > 0 {
            let n = leftover.min(total_to_read);
            let keys = &self.index_buf[self.index_offset..self.index_offset + n];
            let out = &mut output[..n];
            let dict_len = dict.len();
            let dict_len_u32 = dict_len as u32;

            const CHUNK: usize = 16;
            let mut out_chunks = out.chunks_exact_mut(CHUNK);
            let mut key_chunks = keys.chunks_exact(CHUNK);
            for (out_chunk, key_chunk) in out_chunks.by_ref().zip(key_chunks.by_ref()) {
                let max_key = key_chunk.iter().fold(0u32, |acc, &k| acc.max(k as u32));
                if max_key >= dict_len_u32 {
                    return Err(ParquetError::General(format!(
                        "dictionary index out of bounds: the len is {dict_len} but the index is {max_key}"
                    )));
                }
                for (dst, &k) in out_chunk.iter_mut().zip(key_chunk.iter()) {
                    // SAFETY: bounds checked above.
                    dst.write(unsafe { dict.get_unchecked(k as usize) }.clone());
                }
            }
            for (dst, &k) in out_chunks
                .into_remainder()
                .iter_mut()
                .zip(key_chunks.remainder().iter())
            {
                let idx = k as usize;
                if idx >= dict_len {
                    return Err(ParquetError::General(format!(
                        "dictionary index out of bounds: the len is {dict_len} but the index is {idx}"
                    )));
                }
                // SAFETY: bounds checked above.
                dst.write(unsafe { dict.get_unchecked(idx) }.clone());
            }

            self.index_offset += n;
            values_read += n;
        }

        if values_read < total_to_read {
            let got = self.decoder.get_batch_with_dict(
                dict,
                &mut output[values_read..total_to_read],
                total_to_read - values_read,
            )?;
            values_read += got;
        }

        self.max_remaining_values -= values_read;
        Ok(values_read)
    }

    /// Skip up to `to_skip` values, returning the number of values skipped
    pub fn skip(&mut self, to_skip: usize) -> Result<usize> {
        let to_skip = to_skip.min(self.max_remaining_values);

        let mut values_skip = 0;
        while values_skip < to_skip {
            if self.index_offset == self.index_buf_len {
                // Instead of reloading the buffer, just skip in the decoder
                let skip = self.decoder.skip(to_skip - values_skip)?;

                if skip == 0 {
                    break;
                }

                self.max_remaining_values -= skip;
                values_skip += skip;
            } else {
                // We still have indices buffered, so skip within the buffer
                let skip = (to_skip - values_skip).min(self.index_buf_len - self.index_offset);

                self.index_offset += skip;
                self.max_remaining_values -= skip;
                values_skip += skip;
            }
        }
        Ok(values_skip)
    }
}
