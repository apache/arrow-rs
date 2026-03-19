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

use crate::encodings::rle::{RleDecodedBatch, RleDecoder};
use crate::errors::Result;

/// Decoder for `Encoding::RLE_DICTIONARY` indices
pub struct DictIndexDecoder {
    /// Decoder for the dictionary offsets array
    decoder: RleDecoder,

    /// We want to decode the offsets in chunks so we will maintain an internal buffer of decoded
    /// offsets
    index_buf: Vec<i32>,
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
        let max_remaining = num_values.unwrap_or(num_levels);

        Ok(Self {
            decoder,
            index_buf: vec![0; max_remaining.min(1024)],
            index_buf_len: 0,
            index_offset: 0,
            max_remaining_values: max_remaining,
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

    /// Decode indices and gather views directly into `output`.
    ///
    /// For RLE runs, fills output with the repeated view directly (no index buffer needed).
    /// For bit-packed runs, decodes indices to a stack-local buffer and gathers immediately.
    pub fn read_gather_views(
        &mut self,
        len: usize,
        dict_views: &[u128],
        output: &mut Vec<u128>,
        base_buffer_idx: u32,
    ) -> Result<usize> {
        let to_read = len.min(self.max_remaining_values);
        let mut values_read = 0;

        // Flush any buffered indices from previous reads
        if self.index_offset < self.index_buf_len {
            let n = (self.index_buf_len - self.index_offset).min(to_read);
            let keys = &self.index_buf[self.index_offset..self.index_offset + n];
            Self::gather_views(keys, dict_views, output, base_buffer_idx);
            self.index_offset += n;
            self.max_remaining_values -= n;
            values_read += n;
        }

        if values_read < to_read {
            let read = self.decoder.get_batch_direct(to_read - values_read, |batch| {
                match batch {
                    RleDecodedBatch::Rle { index, count } => {
                        let view = dict_views[index as usize];
                        let view = if base_buffer_idx == 0 || (view as u32) <= 12 {
                            view
                        } else {
                            let mut bv = arrow_data::ByteView::from(view);
                            bv.buffer_index += base_buffer_idx;
                            bv.into()
                        };
                        output.extend(std::iter::repeat_n(view, count));
                    }
                    RleDecodedBatch::BitPacked(keys) => {
                        Self::gather_views(keys, dict_views, output, base_buffer_idx);
                    }
                }
            })?;
            self.max_remaining_values -= read;
            values_read += read;
        }

        Ok(values_read)
    }

    fn gather_views(
        keys: &[i32],
        dict_views: &[u128],
        output: &mut Vec<u128>,
        base_buffer_idx: u32,
    ) {
        // Clamp index to valid range to prevent UB on corrupt data.
        // This is branchless (cmp+csel on ARM) and avoids bounds checks in the hot loop.
        let max_idx = dict_views.len() - 1;
        if base_buffer_idx == 0 {
            output.extend(keys.iter().map(|k| unsafe {
                *dict_views.get_unchecked((*k as usize).min(max_idx))
            }));
        } else {
            output.extend(keys.iter().map(|k| {
                let view = unsafe { *dict_views.get_unchecked((*k as usize).min(max_idx)) };
                if (view as u32) <= 12 {
                    view
                } else {
                    let mut bv = arrow_data::ByteView::from(view);
                    bv.buffer_index += base_buffer_idx;
                    bv.into()
                }
            }));
        }
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
