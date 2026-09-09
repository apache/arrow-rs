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

//! Boolean native batches and encoder adapters.

use super::PlainEncoderImpl as PlainEncoder;
use super::*;

#[derive(Debug, Clone, Copy)]
pub(super) enum BoolBatchSelection<'a> {
    /// An unpacked `&[bool]` slice from the low-level `write_batch` path: one byte per
    /// value, no bit-packing. The bits are packed lazily when the encoder pulls them
    /// (`put_indexed_packed`), so the slice path is allocation-free.
    Unpacked { values: &'a [bool] },
}

/// Borrowed unpacked Boolean values, packed lazily by the encoder.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BoolBatch<'a> {
    pub(super) selection: BoolBatchSelection<'a>,
}

impl<'a> BoolBatch<'a> {
    /// Wrap an unpacked `&[bool]` run — the slice `write_batch` path. Bits are packed
    /// lazily when the encoder pulls them (`put_indexed_packed`), so this is
    /// allocation-free.
    pub(crate) fn from_bool_slice(values: &'a [bool]) -> Self {
        Self {
            selection: BoolBatchSelection::Unpacked { values },
        }
    }

    pub(crate) fn len(self) -> usize {
        match self.selection {
            BoolBatchSelection::Unpacked { values } => values.len(),
        }
    }

    /// Resolve the selection and yield each selected bit.
    #[inline]
    pub(super) fn for_each(self, mut f: impl FnMut(bool)) {
        match self.selection {
            BoolBatchSelection::Unpacked { values } => {
                for &b in values {
                    f(b);
                }
            }
        }
    }

    #[inline]
    fn put_indexed_packed(self, bit_writer: &mut BitWriter) {
        // Stream the selected bits once, packing them LSB-first into words.
        let mut word: u64 = 0;
        let mut bits: usize = 0;
        self.for_each(|b| {
            word |= (b as u64) << bits;
            bits += 1;
            if bits == 64 {
                bit_writer.put_value(word, 64);
                word = 0;
                bits = 0;
            }
        });
        if bits > 0 {
            bit_writer.put_value(word, bits);
        }
    }

    pub(crate) fn true_count(self) -> usize {
        match self.selection {
            // Unpacked (slice path): no bitmap to popcount, so count directly.
            BoolBatchSelection::Unpacked { values } => values.iter().filter(|b| **b).count(),
        }
    }
}
/// Encoder extension for packed boolean values.
#[doc(hidden)]
pub(crate) trait BoolEncoder: Encoder<BoolType> {
    fn put_bool_batch(&mut self, _values: BoolBatch<'_>) -> Result<()> {
        Err(general_err!(
            "Packed boolean values are not supported by this encoder"
        ))
    }
}

impl<D: DataType<T = bool>> EncodingFamily<D> for BoolEncodingFamily {
    fn try_new(
        _dict_supported: bool,
        fallback_encoding: Encoding,
        descr: &ColumnDescPtr,
    ) -> Result<Self> {
        Self::from_encoding(fallback_encoding, descr)
    }

    fn flush_data_page(&mut self) -> Result<(Bytes, Encoding)> {
        let buf = <Self as Encoder<BoolType>>::flush_buffer(self)?;
        let encoding = <Self as Encoder<BoolType>>::encoding(self);
        Ok((buf, encoding))
    }

    fn data_page_size(&self) -> usize {
        <Self as Encoder<BoolType>>::estimated_data_encoded_size(self)
    }

    fn memory_size(&self) -> usize {
        <Self as Encoder<BoolType>>::estimated_memory_size(self)
    }
}

impl BoolEncoder for BoolEncodingFamily {
    fn put_bool_batch(&mut self, values: BoolBatch<'_>) -> Result<()> {
        match self {
            Self::Plain(e) => e.put_bool_batch(values),
            Self::Rle(e) => e.put_bool_batch(values),
        }
    }
}
impl BoolEncodingFamily {
    fn from_encoding(encoding: Encoding, _descr: &ColumnDescPtr) -> Result<Self> {
        match encoding {
            Encoding::PLAIN => Ok(Self::Plain(PlainEncoder::new())),
            Encoding::RLE => Ok(Self::Rle(RleValueEncoder::new())),
            e => Err(unsupported_column_encoding(e, Type::BOOLEAN)),
        }
    }
}

impl BoolEncoder for PlainEncoder<BoolType> {
    #[inline]
    fn put_bool_batch(&mut self, values: BoolBatch<'_>) -> Result<()> {
        values.put_indexed_packed(&mut self.bit_writer);
        Ok(())
    }
}

impl BoolEncoder for RleValueEncoder<BoolType> {
    #[inline(never)]
    fn put_bool_batch(&mut self, values: BoolBatch<'_>) -> Result<()> {
        let rle_encoder = self.encoder.get_or_insert_with(|| {
            let mut buffer = Vec::with_capacity(DEFAULT_RLE_BUFFER_LEN);
            // Reserve space for length
            buffer.extend_from_slice(&[0; 4]);
            RleEncoder::new_from_buf(1, buffer)
        });

        values.for_each(|b| rle_encoder.put(b as u64));
        Ok(())
    }
}
