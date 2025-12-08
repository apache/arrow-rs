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

use crate::data_type::Int32Type;
use crate::encodings::decoding::{Decoder, DeltaBitPackDecoder};
use crate::errors::{ParquetError, Result};

/// Decoder for `Encoding::DELTA_BYTE_ARRAY`
pub struct DeltaByteArrayDecoder {
    prefix_lengths: Vec<i32>,
    suffix_lengths: Vec<i32>,
    data: Bytes,
    length_offset: usize,
    data_offset: usize,
    last_value: Vec<u8>,
}

impl DeltaByteArrayDecoder {
    /// Create a new [`DeltaByteArrayDecoder`] with the provided data page
    pub fn new(data: Bytes) -> Result<Self> {
        let mut prefix = DeltaBitPackDecoder::<Int32Type>::new();
        prefix.set_data(data.clone(), 0)?;

        let num_prefix = prefix.values_left();
        let mut prefix_lengths = vec![0; num_prefix];
        assert_eq!(prefix.get(&mut prefix_lengths)?, num_prefix);

        let mut suffix = DeltaBitPackDecoder::<Int32Type>::new();
        suffix.set_data(data.slice(prefix.get_offset()..), 0)?;

        let num_suffix = suffix.values_left();
        let mut suffix_lengths = vec![0; num_suffix];
        assert_eq!(suffix.get(&mut suffix_lengths)?, num_suffix);

        if num_prefix != num_suffix {
            return Err(general_err!(format!(
                "inconsistent DELTA_BYTE_ARRAY lengths, prefixes: {num_prefix}, suffixes: {num_suffix}"
            )));
        }

        assert_eq!(prefix_lengths.len(), suffix_lengths.len());

        Ok(Self {
            prefix_lengths,
            suffix_lengths,
            data,
            length_offset: 0,
            data_offset: prefix.get_offset() + suffix.get_offset(),
            last_value: vec![],
        })
    }

    /// Returns the number of values remaining
    pub fn remaining(&self) -> usize {
        self.prefix_lengths.len() - self.length_offset
    }

    /// Read up to `len` values, returning the number of values read
    /// and calling `f` with each decoded byte slice
    ///
    /// Will short-circuit and return on error
    pub fn read<F>(&mut self, len: usize, mut f: F) -> Result<usize>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        let to_read = len.min(self.remaining());

        let length_range = self.length_offset..self.length_offset + to_read;
        let iter = self.prefix_lengths[length_range.clone()]
            .iter()
            .zip(&self.suffix_lengths[length_range]);

        let data = self.data.as_ref();

        for (prefix_length, suffix_length) in iter {
            let prefix_length = *prefix_length as usize;
            let suffix_length = *suffix_length as usize;

            if self.data_offset + suffix_length > self.data.len() {
                return Err(ParquetError::EOF("eof decoding byte array".into()));
            }

            self.last_value.truncate(prefix_length);
            self.last_value
                .extend_from_slice(&data[self.data_offset..self.data_offset + suffix_length]);
            f(&self.last_value)?;

            self.data_offset += suffix_length;
        }

        self.length_offset += to_read;
        Ok(to_read)
    }

    /// Skip up to `to_skip` values, returning the number of values skipped
    pub fn skip(&mut self, to_skip: usize) -> Result<usize> {
        let to_skip = to_skip.min(self.prefix_lengths.len() - self.length_offset);

        let length_range = self.length_offset..self.length_offset + to_skip;
        let iter = self.prefix_lengths[length_range.clone()]
            .iter()
            .zip(&self.suffix_lengths[length_range]);

        let data = self.data.as_ref();

        for (prefix_length, suffix_length) in iter {
            let prefix_length = *prefix_length as usize;
            let suffix_length = *suffix_length as usize;

            if self.data_offset + suffix_length > self.data.len() {
                return Err(ParquetError::EOF("eof decoding byte array".into()));
            }

            self.last_value.truncate(prefix_length);
            self.last_value
                .extend_from_slice(&data[self.data_offset..self.data_offset + suffix_length]);
            self.data_offset += suffix_length;
        }
        self.length_offset += to_skip;
        Ok(to_skip)
    }
}
