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
    /// (`put_indexed_packed`), so the slice path is allocation-free and shares the
    /// boolean [`BatchSink`](crate::column::value_batch::BatchSink) with Arrow input
    /// in every build.
    Unpacked { values: &'a [bool] },
    /// A directly addressable packed Arrow range. Cache this compact shape at
    /// construction so the statistics and encoder passes do not repeatedly
    /// interrogate a comparatively large physical-index selection.
    #[cfg(feature = "arrow")]
    Dense { bit_offset: usize, len: usize },
    #[cfg(feature = "arrow")]
    Sparse {
        bit_offset: usize,
        indices: &'a [usize],
    },
    /// A recursively lowered Arrow selection. Flat dictionaries use scalar
    /// traversal to avoid span-coalescing overhead for alternating keys; other
    /// selections use spans for packed copies, popcounts, and repeated runs.
    #[cfg(feature = "arrow")]
    Physical {
        bit_offset: usize,
        selection: PhysicalValueSelection<'a>,
        scalar: bool,
    },
}

#[cfg(feature = "arrow")]
#[inline]
fn put_repeated_bit(bit_writer: &mut BitWriter, bit: bool, count: usize) {
    let word = if bit { u64::MAX } else { 0 };
    let mut remaining = count;
    while remaining >= 64 {
        bit_writer.put_value(word, 64);
        remaining -= 64;
    }
    if remaining != 0 {
        let tail = if bit { (1_u64 << remaining) - 1 } else { 0 };
        bit_writer.put_value(tail, remaining);
    }
}

/// Borrowed packed boolean values.
///
/// Bits are addressed in the same least-significant-bit-first order used by
/// Arrow boolean buffers and Parquet boolean encodings.
#[derive(Debug, Clone, Copy)]
pub(crate) struct BoolBatch<'a> {
    /// Backing bit buffer for the packed (Arrow) selections. Unused by
    /// [`BoolBatchSelection::Unpacked`], hence the non-`arrow` dead-code allow.
    #[cfg_attr(not(feature = "arrow"), allow(dead_code))]
    bytes: &'a [u8],
    pub(super) selection: BoolBatchSelection<'a>,
}

impl<'a> BoolBatch<'a> {
    /// Wrap an unpacked `&[bool]` run — the slice `write_batch` path. Bits are packed
    /// lazily when the encoder pulls them (`put_indexed_packed`), so this is
    /// allocation-free and drives through the same boolean
    /// [`BatchSink`](crate::column::value_batch::BatchSink) as Arrow input.
    pub(crate) fn from_bool_slice(values: &'a [bool]) -> Self {
        Self {
            bytes: &[],
            selection: BoolBatchSelection::Unpacked { values },
        }
    }

    #[cfg(feature = "arrow")]
    pub(crate) fn new_physical(
        bytes: &'a [u8],
        bit_offset: usize,
        selection: PhysicalValueSelection<'a>,
    ) -> Self {
        let selection = match selection.direct_physical_range() {
            Some(range) => BoolBatchSelection::Dense {
                bit_offset: bit_offset + range.start,
                len: range.len(),
            },
            None => match selection.unmapped_selection() {
                Some(ValueSelectionRef::Sparse(indices)) => BoolBatchSelection::Sparse {
                    bit_offset,
                    indices,
                },
                _ => BoolBatchSelection::Physical {
                    bit_offset,
                    selection,
                    scalar: selection.has_dictionary_mapping() && !selection.is_grouped(),
                },
            },
        };
        Self { bytes, selection }
    }

    pub(crate) fn len(self) -> usize {
        match self.selection {
            #[cfg(feature = "arrow")]
            BoolBatchSelection::Dense { len, .. } => len,
            #[cfg(feature = "arrow")]
            BoolBatchSelection::Sparse { indices, .. } => indices.len(),
            #[cfg(feature = "arrow")]
            BoolBatchSelection::Physical { selection, .. } => selection.len(),
            BoolBatchSelection::Unpacked { values } => values.len(),
        }
    }

    /// Resolve the selection and yield each selected bit. `Unpacked` is placed
    /// last to keep the Arrow arms grouped together.
    #[inline]
    pub(super) fn for_each(self, mut f: impl FnMut(bool)) {
        match self.selection {
            #[cfg(feature = "arrow")]
            BoolBatchSelection::Dense { bit_offset, len } => {
                for index in 0..len {
                    f(get_bit(self.bytes, bit_offset + index));
                }
            }
            #[cfg(feature = "arrow")]
            BoolBatchSelection::Sparse {
                bit_offset,
                indices,
            } => {
                for &index in indices {
                    f(get_bit(self.bytes, bit_offset + index));
                }
            }
            #[cfg(feature = "arrow")]
            BoolBatchSelection::Physical {
                bit_offset,
                selection,
                ..
            } => {
                let bytes = self.bytes;
                let _ = selection.try_for_each_index(|index| -> Result<(), ()> {
                    f(get_bit(bytes, bit_offset + index));
                    Ok(())
                });
            }
            BoolBatchSelection::Unpacked { values } => {
                for &b in values {
                    f(b);
                }
            }
        }
    }

    /// Yield source-provided run groups. Run-end input can produce multi-bit
    /// groups; every other selection emits one group per bit. Adjacent groups
    /// may contain the same value because grouping follows storage, not value
    /// equality.
    #[inline]
    pub(super) fn for_each_run_group(self, mut f: impl FnMut(bool, usize)) {
        match self.selection {
            #[cfg(feature = "arrow")]
            BoolBatchSelection::Physical {
                bit_offset,
                selection,
                ..
            } if selection.is_grouped() => {
                let bytes = self.bytes;
                let _ = selection.try_for_each_index_group(|index, count| -> Result<(), ()> {
                    f(get_bit(bytes, bit_offset + index), count);
                    Ok(())
                });
            }
            _ => self.for_each(|b| f(b, 1)),
        }
    }

    #[inline]
    fn put_indexed_packed(self, bit_writer: &mut BitWriter) {
        #[cfg(feature = "arrow")]
        match self.selection {
            BoolBatchSelection::Dense { bit_offset, len } => {
                bit_writer.put_bits(self.bytes, bit_offset, len);
                return;
            }
            BoolBatchSelection::Physical {
                bit_offset,
                selection,
                scalar: false,
            } => {
                let _: Result<(), ()> = selection.try_for_each_span(|span| {
                    match span {
                        PhysicalValueSpan::Range { start, len } => {
                            bit_writer.put_bits(self.bytes, bit_offset + start, len);
                        }
                        PhysicalValueSpan::Repeat { index, count } => {
                            let bit = get_bit(self.bytes, bit_offset + index);
                            put_repeated_bit(bit_writer, bit, count);
                        }
                        PhysicalValueSpan::Gather(indices) => {
                            for &index in indices {
                                bit_writer.put_bits(self.bytes, bit_offset + index, 1);
                            }
                        }
                    }
                    Ok(())
                });
                return;
            }
            _ => {}
        }
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
            #[cfg(feature = "arrow")]
            BoolBatchSelection::Dense { bit_offset, len } => {
                UnalignedBitChunk::new(self.bytes, bit_offset, len).count_ones()
            }
            #[cfg(feature = "arrow")]
            BoolBatchSelection::Sparse {
                bit_offset,
                indices,
            } => indices
                .iter()
                .filter(|&&index| get_bit(self.bytes, bit_offset + index))
                .count(),
            #[cfg(feature = "arrow")]
            BoolBatchSelection::Physical { scalar: true, .. } => {
                let mut count = 0;
                self.for_each(|value| count += usize::from(value));
                count
            }
            #[cfg(feature = "arrow")]
            BoolBatchSelection::Physical {
                bit_offset,
                selection,
                scalar: false,
            } => {
                let mut count = 0;
                let _: Result<(), ()> = selection.try_for_each_span(|span| {
                    match span {
                        PhysicalValueSpan::Range { start, len } => {
                            count += UnalignedBitChunk::new(self.bytes, bit_offset + start, len)
                                .count_ones();
                        }
                        PhysicalValueSpan::Repeat {
                            index,
                            count: repeated,
                        } => {
                            count += repeated * get_bit(self.bytes, bit_offset + index) as usize;
                        }
                        PhysicalValueSpan::Gather(indices) => {
                            count += indices
                                .iter()
                                .filter(|&&index| get_bit(self.bytes, bit_offset + index))
                                .count();
                        }
                    }
                    Ok(())
                });
                count
            }
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

        // Run-end input emits one O(1) put per selected run group; every other
        // selection emits per-bit puts.
        values.for_each_run_group(|b, n| rle_encoder.put_run(b as u64, n));
        Ok(())
    }
}
