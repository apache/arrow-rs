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

use crate::errors::{ParquetError, Result};
use crate::file::properties::CdcOptions;
use crate::schema::types::ColumnDescriptor;

use super::Chunk;
use super::cdc_generated::{GEARHASH_TABLE, NUM_GEARHASH_TABLES};

/// CDC (Content-Defined Chunking) divides data into variable-sized chunks based on
/// content rather than fixed-size boundaries.
///
/// For example, given this sequence of values in a column:
///
/// ```text
/// File1:    [1,2,3,   4,5,6,   7,8,9]
///            chunk1   chunk2   chunk3
/// ```
///
/// If a value is inserted between 3 and 4:
///
/// ```text
/// File2:    [1,2,3,0,   4,5,6,   7,8,9]
///            new-chunk  chunk2   chunk3
/// ```
///
/// The chunking process adjusts to maintain stable boundaries across data modifications.
/// Each chunk defines a new parquet data page which is contiguously written to the file.
/// Since each page is compressed independently, the files' contents look like:
///
/// ```text
/// File1:    [Page1][Page2][Page3]...
/// File2:    [Page4][Page2][Page3]...
/// ```
///
/// When uploaded to a content-addressable storage (CAS) system, the CAS splits the byte
/// stream into content-defined blobs with unique identifiers. Identical blobs are stored
/// only once, so Page2 and Page3 are deduplicated across File1 and File2.
///
/// ## Implementation
///
/// Only the parquet writer needs to be aware of content-defined chunking; the reader is
/// unaffected. Each parquet column writer holds a `ContentDefinedChunker` instance
/// depending on the writer's properties. The chunker's state is maintained across the
/// entire column without being reset between pages and row groups.
///
/// This implements a [FastCDC]-inspired algorithm using gear hashing. The input data is
/// fed byte-by-byte into a rolling hash; when the hash matches a predefined mask, a new
/// chunk boundary candidate is recorded. To reduce the exponential variance of chunk
/// sizes inherent in a single gear hash, the algorithm requires **8 consecutive mask
/// matches** — each against a different pre-computed gear hash table — before committing
/// to a boundary. This [central-limit-theorem normalization] makes the chunk size
/// distribution approximately normal between `min_chunk_size` and `max_chunk_size`.
///
/// The chunker receives the record-shredded column data (def_levels, rep_levels, values)
/// and iterates over the (def_level, rep_level, value) triplets while adjusting the
/// column-global rolling hash. Whenever the rolling hash matches, the chunker creates a
/// new chunk. For nested data (lists, maps, structs) chunk boundaries are restricted to
/// top-level record boundaries (`rep_level == 0`) so that a nested row is never split
/// across chunks.
///
/// Note that boundaries are deterministically calculated exclusively based on the data
/// itself, so the same data always produces the same chunks given the same configuration.
///
/// Ported from the C++ implementation in apache/arrow#45360
/// (`cpp/src/parquet/chunker_internal.cc`).
///
/// [FastCDC]: https://www.usenix.org/conference/atc16/technical-sessions/presentation/xia
/// [central-limit-theorem normalization]: https://www.cidrdb.org/cidr2023/papers/p43-low.pdf
#[derive(Debug)]
pub(crate) struct ContentDefinedChunker {
    /// Maximum definition level for this column.
    max_def_level: i16,
    /// Maximum repetition level for this column.
    max_rep_level: i16,
    /// Definition level at the nearest REPEATED ancestor.
    repeated_ancestor_def_level: i16,

    /// Minimum chunk size in bytes.
    /// The rolling hash will not be updated until this size is reached for each chunk.
    /// All data sent through the hash function counts towards the chunk size, including
    /// definition and repetition levels if present.
    min_chunk_size: i64,
    /// Maximum chunk size in bytes.
    /// A new chunk is created whenever the chunk size exceeds this value. The chunk size
    /// distribution approximates a normal distribution between `min_chunk_size` and
    /// `max_chunk_size`. Note that the parquet writer has a related `data_pagesize`
    /// property that controls the maximum size of a parquet data page after encoding.
    /// While setting `data_pagesize` smaller than `max_chunk_size` doesn't affect
    /// chunking effectiveness, it results in more small parquet data pages.
    max_chunk_size: i64,
    /// Mask for matching against the rolling hash.
    rolling_hash_mask: u64,

    /// Rolling hash state, never reset — initialized once for the entire column.
    rolling_hash: u64,
    /// Whether the rolling hash has matched the mask since the last chunk boundary.
    has_matched: bool,
    /// Current run count for the central-limit-theorem normalization.
    nth_run: usize,
    /// Current chunk size in bytes.
    chunk_size: i64,
}

impl ContentDefinedChunker {
    pub fn new(desc: &ColumnDescriptor, options: &CdcOptions) -> Result<Self> {
        let rolling_hash_mask = Self::calculate_mask(
            options.min_chunk_size as i64,
            options.max_chunk_size as i64,
            options.norm_level,
        )?;
        Ok(Self {
            max_def_level: desc.max_def_level(),
            max_rep_level: desc.max_rep_level(),
            repeated_ancestor_def_level: desc.repeated_ancestor_def_level(),
            min_chunk_size: options.min_chunk_size as i64,
            max_chunk_size: options.max_chunk_size as i64,
            rolling_hash_mask,
            rolling_hash: 0,
            has_matched: false,
            nth_run: 0,
            chunk_size: 0,
        })
    }

    /// Calculate the mask used to determine chunk boundaries from the rolling hash.
    ///
    /// The mask is calculated so that the expected chunk size distribution approximates
    /// a normal distribution between min and max chunk sizes.
    fn calculate_mask(min_chunk_size: i64, max_chunk_size: i64, norm_level: i32) -> Result<u64> {
        if min_chunk_size < 0 {
            return Err(ParquetError::General(
                "min_chunk_size must be non-negative".to_string(),
            ));
        }
        if max_chunk_size <= min_chunk_size {
            return Err(ParquetError::General(
                "max_chunk_size must be greater than min_chunk_size".to_string(),
            ));
        }

        let avg_chunk_size = (min_chunk_size + max_chunk_size) / 2;
        // Target size after subtracting the min-size skip window and dividing by the
        // number of hash tables (for central-limit-theorem normalization).
        let target_size = (avg_chunk_size - min_chunk_size) / NUM_GEARHASH_TABLES as i64;

        // floor(log2(target_size)) — equivalent to C++ NumRequiredBits(target_size) - 1
        let mask_bits = if target_size > 0 {
            63 - target_size.leading_zeros() as i32
        } else {
            0
        };

        let effective_bits = mask_bits - norm_level;

        if !(1..=63).contains(&effective_bits) {
            return Err(ParquetError::General(format!(
                "The number of bits in the CDC mask must be between 1 and 63, got {effective_bits}"
            )));
        }

        // Create the mask by setting the top `effective_bits` bits.
        Ok(u64::MAX << (64 - effective_bits))
    }

    /// Feed raw bytes into the rolling hash.
    ///
    /// The byte count always accumulates toward `chunk_size`, but the actual hash
    /// update is skipped until `min_chunk_size` has been reached. This "skip window"
    /// is the FastCDC optimization that prevents boundaries from appearing too early
    /// in a chunk.
    #[inline]
    fn roll(&mut self, bytes: &[u8]) {
        self.chunk_size += bytes.len() as i64;
        if self.chunk_size < self.min_chunk_size {
            return;
        }
        for &b in bytes {
            self.rolling_hash = self
                .rolling_hash
                .wrapping_shl(1)
                .wrapping_add(GEARHASH_TABLE[self.nth_run][b as usize]);
            self.has_matched =
                self.has_matched || ((self.rolling_hash & self.rolling_hash_mask) == 0);
        }
    }

    /// Feed exactly `N` bytes into the rolling hash (compile-time width).
    ///
    /// Like [`roll`](Self::roll), but the byte count is known at compile time,
    /// allowing the compiler to unroll the inner loop.
    #[inline(always)]
    fn roll_fixed<const N: usize>(&mut self, bytes: &[u8; N]) {
        self.chunk_size += N as i64;
        if self.chunk_size < self.min_chunk_size {
            return;
        }
        for j in 0..N {
            self.rolling_hash = self
                .rolling_hash
                .wrapping_shl(1)
                .wrapping_add(GEARHASH_TABLE[self.nth_run][bytes[j] as usize]);
            self.has_matched =
                self.has_matched || ((self.rolling_hash & self.rolling_hash_mask) == 0);
        }
    }

    /// Feed a definition or repetition level (i16) into the rolling hash.
    #[inline]
    fn roll_level(&mut self, level: i16) {
        self.roll_fixed(&level.to_le_bytes());
    }

    /// Check whether a new chunk boundary should be created.
    ///
    /// A boundary is created when **either** of two conditions holds:
    ///
    /// 1. **CLT normalization**: The rolling hash has matched the mask (`has_matched`)
    ///    *and* this is the 8th consecutive such match (`nth_run` reaches
    ///    `NUM_GEARHASH_TABLES`). Each match advances to the next gear hash table, so
    ///    8 independent matches are required. A single hash table would yield
    ///    exponentially distributed chunk sizes; requiring 8 independent matches
    ///    approximates a normal (Gaussian) distribution by the central limit theorem.
    ///
    /// 2. **Hard size limit**: `chunk_size` has reached `max_chunk_size`. This caps
    ///    chunk size even if the CLT normalization sequence has not completed.
    ///
    /// Note: when `max_chunk_size` forces a boundary, `nth_run` is **not** reset, so
    /// the CLT sequence continues from where it left off in the next chunk. This
    /// matches the C++ behavior.
    #[inline]
    fn need_new_chunk(&mut self) -> bool {
        if self.has_matched {
            self.has_matched = false;
            self.nth_run += 1;
            if self.nth_run >= NUM_GEARHASH_TABLES {
                self.nth_run = 0;
                self.chunk_size = 0;
                return true;
            }
        }
        if self.chunk_size >= self.max_chunk_size {
            self.chunk_size = 0;
            return true;
        }
        false
    }

    /// Compute chunk boundaries for the given column data.
    ///
    /// The chunking state is maintained across the entire column without being
    /// reset between pages and row groups. This enables the chunking process to
    /// be continued between different write calls.
    ///
    /// We go over the (def_level, rep_level, value) triplets one by one while
    /// adjusting the column-global rolling hash based on the triplet. Whenever
    /// the rolling hash matches a predefined mask it sets `has_matched` to true.
    ///
    /// After each triplet [`need_new_chunk`](Self::need_new_chunk) is called to
    /// evaluate if we need to create a new chunk.
    fn calculate<F>(
        &mut self,
        def_levels: Option<&[i16]>,
        rep_levels: Option<&[i16]>,
        num_levels: usize,
        mut roll_value: F,
    ) -> Vec<Chunk>
    where
        F: FnMut(&mut Self, usize),
    {
        let has_def_levels = self.max_def_level > 0;
        let has_rep_levels = self.max_rep_level > 0;

        let mut chunks = Vec::new();
        let mut prev_offset: usize = 0;
        let mut prev_value_offset: usize = 0;
        // Total number of values seen; for non-nested data this equals num_levels.
        let mut total_values: usize = num_levels;

        if !has_rep_levels && !has_def_levels {
            // Fastest path: non-nested, non-null data.
            for offset in 0..num_levels {
                roll_value(self, offset);
                if self.need_new_chunk() {
                    chunks.push(Chunk {
                        level_offset: prev_offset,
                        value_offset: prev_offset,
                        num_levels: offset - prev_offset,
                        num_values: offset - prev_offset,
                    });
                    prev_offset = offset;
                }
            }
            // Set the previous value offset to add the last chunk.
            prev_value_offset = prev_offset;
        } else if !has_rep_levels {
            // Non-nested data with nulls.
            let def_levels = def_levels.expect("def_levels required when max_def_level > 0");
            #[allow(clippy::needless_range_loop)]
            for offset in 0..num_levels {
                let def_level = def_levels[offset];
                self.roll_level(def_level);
                if def_level == self.max_def_level {
                    roll_value(self, offset);
                }
                if self.need_new_chunk() {
                    chunks.push(Chunk {
                        level_offset: prev_offset,
                        value_offset: prev_offset,
                        num_levels: offset - prev_offset,
                        num_values: offset - prev_offset,
                    });
                    prev_offset = offset;
                }
            }
            // Set the previous value offset to add the last chunk.
            prev_value_offset = prev_offset;
        } else {
            // Nested data with nulls.
            let def_levels = def_levels.expect("def_levels required for nested data");
            let rep_levels = rep_levels.expect("rep_levels required for nested data");
            let mut value_offset: usize = 0;

            for offset in 0..num_levels {
                let def_level = def_levels[offset];
                let rep_level = rep_levels[offset];

                self.roll_level(def_level);
                self.roll_level(rep_level);
                if def_level == self.max_def_level {
                    roll_value(self, value_offset);
                }

                if rep_level == 0 && self.need_new_chunk() {
                    // If we are at a record boundary and need a new chunk, create one.
                    let levels_to_write = offset - prev_offset;
                    if levels_to_write > 0 {
                        chunks.push(Chunk {
                            level_offset: prev_offset,
                            value_offset: prev_value_offset,
                            num_levels: levels_to_write,
                            num_values: value_offset - prev_value_offset,
                        });
                        prev_offset = offset;
                        prev_value_offset = value_offset;
                    }
                }
                if def_level >= self.repeated_ancestor_def_level {
                    // We only increment the value offset if we have a leaf value.
                    value_offset += 1;
                }
            }
            total_values = value_offset;
        }

        // Add the last chunk if we have any levels left.
        if prev_offset < num_levels {
            chunks.push(Chunk {
                level_offset: prev_offset,
                value_offset: prev_value_offset,
                num_levels: num_levels - prev_offset,
                num_values: total_values - prev_value_offset,
            });
        }

        #[cfg(debug_assertions)]
        self.validate_chunks(&chunks, num_levels, total_values);

        chunks
    }

    /// Compute CDC chunk boundaries by dispatching on the Arrow array's data type
    /// to feed value bytes into the rolling hash.
    #[cfg(feature = "arrow")]
    pub(crate) fn get_arrow_chunks(
        &mut self,
        def_levels: Option<&[i16]>,
        rep_levels: Option<&[i16]>,
        array: &dyn arrow_array::Array,
    ) -> Result<Vec<Chunk>> {
        use arrow_array::cast::AsArray;
        use arrow_schema::DataType;

        let num_levels = match def_levels {
            Some(def_levels) => def_levels.len(),
            None => array.len(),
        };

        macro_rules! fixed_width {
            ($N:literal) => {{
                let data = array.to_data();
                let buffer = data.buffers()[0].as_slice();
                let values = &buffer[data.offset() * $N..];
                self.calculate(def_levels, rep_levels, num_levels, |c, i| {
                    let offset = i * $N;
                    let slice = &values[offset..offset + $N];
                    c.roll_fixed::<$N>(slice.try_into().unwrap());
                })
            }};
        }

        macro_rules! binary_like {
            ($a:expr) => {{
                let a = $a;
                self.calculate(def_levels, rep_levels, num_levels, |c, i| {
                    c.roll(a.value(i).as_ref());
                })
            }};
        }

        let dtype = array.data_type();
        let chunks = match dtype {
            DataType::Null => self.calculate(def_levels, rep_levels, num_levels, |_, _| {}),
            DataType::Boolean => {
                let a = array.as_boolean();
                self.calculate(def_levels, rep_levels, num_levels, |c, i| {
                    c.roll_fixed(&[a.value(i) as u8]);
                })
            }
            DataType::Int8 | DataType::UInt8 => fixed_width!(1),
            DataType::Int16 | DataType::UInt16 | DataType::Float16 => fixed_width!(2),
            DataType::Int32
            | DataType::UInt32
            | DataType::Float32
            | DataType::Date32
            | DataType::Time32(_)
            | DataType::Interval(arrow_schema::IntervalUnit::YearMonth)
            | DataType::Decimal32(_, _) => fixed_width!(4),
            DataType::Int64
            | DataType::UInt64
            | DataType::Float64
            | DataType::Date64
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
            | DataType::Interval(arrow_schema::IntervalUnit::DayTime)
            | DataType::Decimal64(_, _) => fixed_width!(8),
            DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano)
            | DataType::Decimal128(_, _) => fixed_width!(16),
            DataType::Decimal256(_, _) => fixed_width!(32),
            DataType::FixedSizeBinary(_) => binary_like!(array.as_fixed_size_binary()),
            DataType::Binary => binary_like!(array.as_binary::<i32>()),
            DataType::LargeBinary => binary_like!(array.as_binary::<i64>()),
            DataType::Utf8 => binary_like!(array.as_string::<i32>()),
            DataType::LargeUtf8 => binary_like!(array.as_string::<i64>()),
            DataType::BinaryView => binary_like!(array.as_binary_view()),
            DataType::Utf8View => binary_like!(array.as_string_view()),
            DataType::Dictionary(_, _) => {
                let dict = array.as_any_dictionary();
                self.get_arrow_chunks(def_levels, rep_levels, dict.keys())?
            }
            _ => {
                return Err(ParquetError::General(format!(
                    "content-defined chunking is not supported for data type {dtype:?}",
                )));
            }
        };
        Ok(chunks)
    }

    #[cfg(debug_assertions)]
    fn validate_chunks(&self, chunks: &[Chunk], num_levels: usize, total_values: usize) {
        assert!(!chunks.is_empty(), "chunks must be non-empty");

        let first = &chunks[0];
        assert_eq!(first.level_offset, 0, "first chunk must start at level 0");
        assert_eq!(first.value_offset, 0, "first chunk must start at value 0");

        let mut sum_levels = first.num_levels;
        let mut sum_values = first.num_values;
        for i in 1..chunks.len() {
            let chunk = &chunks[i];
            let prev = &chunks[i - 1];
            assert!(chunk.num_levels > 0, "chunk must have levels");
            assert_eq!(
                chunk.level_offset,
                prev.level_offset + prev.num_levels,
                "level offsets must be contiguous"
            );
            assert_eq!(
                chunk.value_offset,
                prev.value_offset + prev.num_values,
                "value offsets must be contiguous"
            );
            sum_levels += chunk.num_levels;
            sum_values += chunk.num_values;
        }
        assert_eq!(sum_levels, num_levels, "chunks must cover all levels");
        assert_eq!(sum_values, total_values, "chunks must cover all values");

        let last = chunks.last().unwrap();
        assert_eq!(
            last.level_offset + last.num_levels,
            num_levels,
            "last chunk must end at num_levels"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::basic::Type as PhysicalType;
    use crate::schema::types::{ColumnPath, Type};
    use std::sync::Arc;

    fn make_desc(max_def_level: i16, max_rep_level: i16) -> ColumnDescriptor {
        let tp = Type::primitive_type_builder("col", PhysicalType::INT32)
            .build()
            .unwrap();
        ColumnDescriptor::new(
            Arc::new(tp),
            max_def_level,
            max_rep_level,
            ColumnPath::new(vec![]),
        )
    }

    #[test]
    fn test_calculate_mask_defaults() {
        let mask = ContentDefinedChunker::calculate_mask(256 * 1024, 1024 * 1024, 0).unwrap();
        // avg = 640 KiB, target = (640-256)*1024/8 = 49152, log2(49152) = 15
        // mask = u64::MAX << (64 - 15) = top 15 bits set
        let expected = u64::MAX << (64 - 15);
        assert_eq!(mask, expected);
    }

    #[test]
    fn test_calculate_mask_with_norm_level() {
        let mask = ContentDefinedChunker::calculate_mask(256 * 1024, 1024 * 1024, 1).unwrap();
        let expected = u64::MAX << (64 - 14);
        assert_eq!(mask, expected);
    }

    #[test]
    fn test_calculate_mask_invalid() {
        assert!(ContentDefinedChunker::calculate_mask(-1, 100, 0).is_err());
        assert!(ContentDefinedChunker::calculate_mask(100, 50, 0).is_err());
        assert!(ContentDefinedChunker::calculate_mask(100, 100, 0).is_err());
    }

    #[test]
    fn test_non_nested_non_null_single_chunk() {
        let options = CdcOptions {
            min_chunk_size: 8,
            max_chunk_size: 1024,
            norm_level: 0,
        };
        let mut chunker = ContentDefinedChunker::new(&make_desc(0, 0), &options).unwrap();

        // Write a small amount of data — should produce exactly 1 chunk.
        let num_values = 4;
        let chunks = chunker.calculate(None, None, num_values, |c, i| {
            c.roll_fixed::<4>(&(i as i32).to_le_bytes());
        });
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].level_offset, 0);
        assert_eq!(chunks[0].value_offset, 0);
        assert_eq!(chunks[0].num_levels, 4);
    }

    #[test]
    fn test_max_chunk_size_forces_boundary() {
        let options = CdcOptions {
            min_chunk_size: 256,
            max_chunk_size: 1024,
            norm_level: 0,
        };
        let mut chunker = ContentDefinedChunker::new(&make_desc(0, 0), &options).unwrap();

        // Write enough data to exceed max_chunk_size multiple times.
        // Each i32 = 4 bytes, max_chunk_size=1024, so ~256 values per chunk max.
        let num_values = 2000;
        let chunks = chunker.calculate(None, None, num_values, |c, i| {
            c.roll_fixed::<4>(&(i as i32).to_le_bytes());
        });

        // Should have multiple chunks
        assert!(chunks.len() > 1);

        // Verify contiguity
        let mut total_levels = 0;
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.level_offset, total_levels);
            if i < chunks.len() - 1 {
                assert!(chunk.num_levels > 0);
            }
            total_levels += chunk.num_levels;
        }
        assert_eq!(total_levels, num_values);
    }

    #[test]
    fn test_deterministic_chunks() {
        let options = CdcOptions {
            min_chunk_size: 4,
            max_chunk_size: 64,
            norm_level: 0,
        };

        let roll = |c: &mut ContentDefinedChunker, i: usize| {
            c.roll_fixed::<8>(&(i as i64).to_le_bytes());
        };

        let mut chunker1 = ContentDefinedChunker::new(&make_desc(0, 0), &options).unwrap();
        let chunks1 = chunker1.calculate(None, None, 200, roll);

        let mut chunker2 = ContentDefinedChunker::new(&make_desc(0, 0), &options).unwrap();
        let chunks2 = chunker2.calculate(None, None, 200, roll);

        assert_eq!(chunks1.len(), chunks2.len());
        for (a, b) in chunks1.iter().zip(chunks2.iter()) {
            assert_eq!(a.level_offset, b.level_offset);
            assert_eq!(a.value_offset, b.value_offset);
            assert_eq!(a.num_levels, b.num_levels);
        }
    }

    #[test]
    fn test_nullable_non_nested() {
        let options = CdcOptions {
            min_chunk_size: 4,
            max_chunk_size: 64,
            norm_level: 0,
        };
        let mut chunker = ContentDefinedChunker::new(&make_desc(1, 0), &options).unwrap();

        let num_levels = 20;
        // def_level=1 means non-null, def_level=0 means null
        let def_levels: Vec<i16> = (0..num_levels)
            .map(|i| if i % 3 == 0 { 0 } else { 1 })
            .collect();

        let chunks = chunker.calculate(Some(&def_levels), None, num_levels, |c, i| {
            c.roll_fixed::<4>(&(i as i32).to_le_bytes());
        });

        assert!(!chunks.is_empty());
        let total: usize = chunks.iter().map(|c| c.num_levels).sum();
        assert_eq!(total, num_levels);
    }
}

/// Integration tests that exercise CDC through the Arrow writer/reader roundtrip.
#[cfg(all(test, feature = "arrow"))]
mod arrow_tests {
    use std::borrow::Borrow;
    use std::sync::Arc;

    use arrow_array::builder::ListBuilder;
    use arrow_array::{ArrayRef, Float64Array, Int32Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use crate::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use crate::arrow::arrow_writer::ArrowWriter;
    use crate::file::properties::{CdcOptions, WriterProperties};
    use crate::file::reader::{FileReader, SerializedFileReader};

    // --- Constants ---

    const CDC_MIN_CHUNK_SIZE: usize = 4 * 1024;
    const CDC_MAX_CHUNK_SIZE: usize = 16 * 1024;
    const CDC_PART_SIZE: usize = 128 * 1024;
    const CDC_EDIT_SIZE: usize = 128;

    // --- Helpers ---

    /// Deterministic hash function matching the C++ test generator.
    fn test_hash(seed: u64, index: u64) -> u64 {
        let mut h = (index.wrapping_add(seed)).wrapping_mul(0xc4ceb9fe1a85ec53u64);
        h ^= h >> 33;
        h = h.wrapping_mul(0xff51afd7ed558ccdu64);
        h ^= h >> 33;
        h = h.wrapping_mul(0xc4ceb9fe1a85ec53u64);
        h ^= h >> 33;
        h
    }

    fn generate_i32_array(length: usize, seed: u64) -> Int32Array {
        (0..length)
            .map(|i| test_hash(seed, i as u64) as i32)
            .collect()
    }

    fn generate_nullable_i32_array(length: usize, seed: u64) -> Int32Array {
        (0..length)
            .map(|i| {
                let val = test_hash(seed, i as u64);
                if val % 10 == 0 {
                    None
                } else {
                    Some(val as i32)
                }
            })
            .collect()
    }

    fn generate_string_array(length: usize, seed: u64) -> StringArray {
        (0..length)
            .map(|i| {
                let val = test_hash(seed, i as u64);
                Some(format!("str_{val}"))
            })
            .collect()
    }

    fn write_batch_with_cdc(batch: &RecordBatch) -> Vec<u8> {
        let props = WriterProperties::builder()
            .set_content_defined_chunking(true)
            .build();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props)).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        buf
    }

    fn write_batch_without_cdc(batch: &RecordBatch) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        buf
    }

    fn read_batches(data: &[u8]) -> Vec<RecordBatch> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(data.to_vec()))
            .unwrap()
            .build()
            .unwrap();
        reader.collect::<std::result::Result<Vec<_>, _>>().unwrap()
    }

    fn get_data_page_bytes(data: &[u8]) -> Vec<Vec<u8>> {
        let reader = SerializedFileReader::new(bytes::Bytes::from(data.to_vec())).unwrap();
        let metadata = reader.metadata();
        let mut pages = Vec::new();
        for rg in 0..metadata.num_row_groups() {
            let rg_reader = reader.get_row_group(rg).unwrap();
            for col in 0..metadata.row_group(rg).num_columns() {
                let col_reader = rg_reader.get_column_page_reader(col).unwrap();
                for page in col_reader {
                    let page = page.unwrap();
                    pages.push(page.buffer().to_vec());
                }
            }
        }
        pages
    }

    fn write_with_cdc_options(
        batches: &[&RecordBatch],
        min_chunk_size: usize,
        max_chunk_size: usize,
        max_row_group_rows: Option<usize>,
    ) -> Vec<u8> {
        assert!(!batches.is_empty());
        let schema = batches[0].schema();
        let mut builder = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_cdc_options(CdcOptions {
                min_chunk_size,
                max_chunk_size,
                norm_level: 0,
            });
        if let Some(max_rows) = max_row_group_rows {
            builder = builder.set_max_row_group_row_count(Some(max_rows));
        }
        let props = builder.build();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.close().unwrap();
        buf
    }

    fn get_page_lengths(data: &[u8], column_index: usize) -> Vec<Vec<i64>> {
        let reader = SerializedFileReader::new(bytes::Bytes::from(data.to_vec())).unwrap();
        let metadata = reader.metadata();
        let mut result = Vec::new();
        for rg in 0..metadata.num_row_groups() {
            let rg_reader = reader.get_row_group(rg).unwrap();
            let col_reader = rg_reader.get_column_page_reader(column_index).unwrap();
            let mut lengths = Vec::new();
            for page in col_reader {
                let page = page.unwrap();
                if matches!(
                    page.page_type(),
                    crate::basic::PageType::DATA_PAGE | crate::basic::PageType::DATA_PAGE_V2
                ) {
                    lengths.push(page.num_values() as i64);
                }
            }
            result.push(lengths);
        }
        result
    }

    /// LCS-based diff between two sequences of page lengths (ported from C++).
    fn find_differences(first: &[i64], second: &[i64]) -> Vec<(Vec<i64>, Vec<i64>)> {
        let n = first.len();
        let m = second.len();
        let mut dp = vec![vec![0usize; m + 1]; n + 1];
        for i in 0..n {
            for j in 0..m {
                if first[i] == second[j] {
                    dp[i + 1][j + 1] = dp[i][j] + 1;
                } else {
                    dp[i + 1][j + 1] = dp[i + 1][j].max(dp[i][j + 1]);
                }
            }
        }
        // Backtrack to find common elements
        let mut common = Vec::new();
        let (mut i, mut j) = (n, m);
        while i > 0 && j > 0 {
            if first[i - 1] == second[j - 1] {
                common.push((i - 1, j - 1));
                i -= 1;
                j -= 1;
            } else if dp[i - 1][j] >= dp[i][j - 1] {
                i -= 1;
            } else {
                j -= 1;
            }
        }
        common.reverse();

        let mut result = Vec::new();
        let (mut last_i, mut last_j) = (0usize, 0usize);
        for (ci, cj) in &common {
            if *ci > last_i || *cj > last_j {
                result.push((first[last_i..*ci].to_vec(), second[last_j..*cj].to_vec()));
            }
            last_i = ci + 1;
            last_j = cj + 1;
        }
        if last_i < n || last_j < m {
            result.push((first[last_i..].to_vec(), second[last_j..].to_vec()));
        }
        result
    }

    fn make_i32_batch(length: usize, seed: u64) -> RecordBatch {
        let col: ArrayRef = Arc::new(generate_i32_array(length, seed));
        RecordBatch::try_from_iter(vec![("col", col)]).unwrap()
    }

    fn concat_batches(batches: impl IntoIterator<Item = impl Borrow<RecordBatch>>) -> RecordBatch {
        let batches: Vec<_> = batches.into_iter().collect();
        let schema = batches[0].borrow().schema();
        let batches = batches.iter().map(|b| b.borrow());
        arrow_select::concat::concat_batches(&schema, batches).unwrap()
    }

    fn i32_part_length() -> usize {
        CDC_PART_SIZE / 4
    }

    fn i32_edit_length() -> usize {
        CDC_EDIT_SIZE / 4
    }

    // --- Roundtrip tests ---

    #[test]
    fn test_cdc_roundtrip_i32() {
        let array: ArrayRef = Arc::new(Int32Array::from_iter(0..10_000));
        let batch = RecordBatch::try_from_iter(vec![("col", array)]).unwrap();

        let data = write_batch_with_cdc(&batch);
        let batches = read_batches(&data);
        let result = concat_batches(&batches);
        assert_eq!(batch, result);
    }

    #[test]
    fn test_cdc_roundtrip_string() {
        let values = (0..5_000).map(|i| Some(format!("value_{i}")));
        let array: ArrayRef = Arc::new(StringArray::from_iter(values));
        let batch = RecordBatch::try_from_iter(vec![("col", array)]).unwrap();

        let data = write_batch_with_cdc(&batch);
        let batches = read_batches(&data);
        let result = concat_batches(&batches);
        assert_eq!(batch, result);
    }

    #[test]
    fn test_cdc_roundtrip_large_binary() {
        let mut builder = arrow_array::builder::LargeBinaryBuilder::new();
        for i in 0..5_000u32 {
            builder.append_value(format!("value_{i}"));
        }
        let array: ArrayRef = Arc::new(builder.finish());
        let batch = RecordBatch::try_from_iter(vec![("col", array)]).unwrap();

        let data = write_batch_with_cdc(&batch);
        let batches = read_batches(&data);
        let result = concat_batches(&batches);
        assert_eq!(batch, result);
    }

    #[test]
    fn test_cdc_roundtrip_nullable() {
        let values = (0..10_000).map(|i| if i % 7 == 0 { None } else { Some(i) });
        let array: ArrayRef = Arc::new(Int32Array::from_iter(values));
        let batch = RecordBatch::try_from_iter(vec![("col", array)]).unwrap();

        let data = write_batch_with_cdc(&batch);
        let batches = read_batches(&data);
        let result = concat_batches(&batches);
        assert_eq!(batch, result);
    }

    #[test]
    fn test_cdc_deterministic() {
        let values = 0..10_000;
        let array: ArrayRef = Arc::new(Int32Array::from_iter(values));
        let batch = RecordBatch::try_from_iter(vec![("col", array)]).unwrap();

        let data1 = write_batch_with_cdc(&batch);
        let data2 = write_batch_with_cdc(&batch);
        assert_eq!(data1, data2, "CDC output must be deterministic");
    }

    #[test]
    fn test_cdc_produces_multiple_pages() {
        let values = 0..500_000;
        let array: ArrayRef = Arc::new(Int32Array::from_iter(values));
        let batch = RecordBatch::try_from_iter(vec![("col", array)]).unwrap();

        let cdc_data = write_batch_with_cdc(&batch);
        let no_cdc_data = write_batch_without_cdc(&batch);

        let cdc_pages = get_data_page_bytes(&cdc_data);
        let no_cdc_pages = get_data_page_bytes(&no_cdc_data);

        assert!(
            cdc_pages.len() > 1,
            "CDC should produce multiple pages, got {}",
            cdc_pages.len()
        );
        assert!(
            cdc_pages.len() >= no_cdc_pages.len(),
            "CDC pages {} should be >= non-CDC pages {}",
            cdc_pages.len(),
            no_cdc_pages.len()
        );
    }

    #[test]
    fn test_cdc_page_reuse_on_append() {
        let n = 500_000;
        let original_values = 0..n;
        let appended_values = 0..n + 100;
        let original: ArrayRef = Arc::new(Int32Array::from_iter(original_values));
        let appended: ArrayRef = Arc::new(Int32Array::from_iter(appended_values));

        let batch1 = RecordBatch::try_from_iter(vec![("col", original)]).unwrap();
        let batch2 = RecordBatch::try_from_iter(vec![("col", appended)]).unwrap();

        let pages1 = get_data_page_bytes(&write_batch_with_cdc(&batch1));
        let pages2 = get_data_page_bytes(&write_batch_with_cdc(&batch2));

        let reused = pages1.iter().filter(|p| pages2.contains(p)).count();
        assert!(
            reused > 0,
            "At least some pages should be reused after append, pages1={}, pages2={}",
            pages1.len(),
            pages2.len()
        );
    }

    #[test]
    fn test_cdc_state_persists_across_row_groups() {
        let n = 500_000i32;
        let all_data: ArrayRef = Arc::new(Int32Array::from_iter(0..n));
        let batch_all = RecordBatch::try_from_iter(vec![("col", all_data)]).unwrap();
        let schema = batch_all.schema();
        let data_one_rg = write_batch_with_cdc(&batch_all);

        let props = WriterProperties::builder()
            .set_content_defined_chunking(true)
            .set_max_row_group_row_count(Some(n as usize / 2))
            .build();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
        writer.write(&batch_all).unwrap();
        writer.close().unwrap();
        let data_two_rg = buf;

        let result1 = read_batches(&data_one_rg);
        let result2 = read_batches(&data_two_rg);
        let concat1 = concat_batches(&result1);
        let concat2 = concat_batches(&result2);
        assert_eq!(concat1, concat2);
    }

    #[test]
    fn test_cdc_roundtrip_dictionary() {
        let values = StringArray::from_iter_values((0..10_000).map(|i| format!("val_{}", i % 100)));
        let array: ArrayRef = Arc::new(
            arrow_cast::cast::cast(
                &values,
                &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            )
            .unwrap(),
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            array.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();

        let data = write_batch_with_cdc(&batch);
        let batches = read_batches(&data);
        let result = concat_batches(&batches);
        assert_eq!(batch.num_rows(), result.num_rows());
    }

    #[test]
    fn test_cdc_roundtrip_list() {
        let mut builder = ListBuilder::new(arrow_array::builder::Int32Builder::new());
        for i in 0..5_000 {
            for j in 0..(i % 5) {
                builder.values().append_value(i * 10 + j);
            }
            builder.append(true);
        }
        let list_array: ArrayRef = Arc::new(builder.finish());

        let batch = RecordBatch::try_from_iter(vec![("col", list_array)]).unwrap();

        let data = write_batch_with_cdc(&batch);
        let batches = read_batches(&data);
        let result = concat_batches(&batches);
        assert_eq!(batch, result);
    }

    #[test]
    fn test_cdc_roundtrip_multiple_columns() {
        let i32_array: ArrayRef = Arc::new(Int32Array::from_iter(0..10_000));
        let str_array: ArrayRef = Arc::new(StringArray::from_iter(
            (0..10_000).map(|i| Some(format!("s{i}"))),
        ));
        let f64_array: ArrayRef =
            Arc::new(Float64Array::from_iter((0..10_000).map(|i| i as f64 * 0.1)));

        let batch = RecordBatch::try_from_iter(vec![
            ("ints", i32_array),
            ("strings", str_array),
            ("floats", f64_array),
        ])
        .unwrap();

        let data = write_batch_with_cdc(&batch);
        let batches = read_batches(&data);
        let result = concat_batches(&batches);
        assert_eq!(batch, result);
    }

    // --- Page-level CDC tests ported from C++ chunker_internal_test.cc ---

    #[test]
    fn test_cdc_find_differences() {
        let diffs = find_differences(&[1, 2, 3, 4, 5], &[1, 7, 8, 4, 5]);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].0, vec![2, 3]);
        assert_eq!(diffs[0].1, vec![7, 8]);

        let diffs = find_differences(&[1, 2, 3], &[1, 2, 3, 4, 5]);
        assert_eq!(diffs.len(), 1);
        assert!(diffs[0].0.is_empty());
        assert_eq!(diffs[0].1, vec![4, 5]);

        let diffs = find_differences(&[], &[]);
        assert!(diffs.is_empty());
    }

    #[test]
    fn test_cdc_delete_once() {
        let part_len = i32_part_length();
        let edit_len = i32_edit_length();

        let part1 = make_i32_batch(part_len, 0);
        let edit = make_i32_batch(edit_len, 1);
        let part2 = make_i32_batch(part_len, 100);

        let base = concat_batches([&part1, &edit, &part2]);
        let modified = concat_batches([&part1, &part2]);

        let base_data =
            write_with_cdc_options(&[&base], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);
        let mod_data =
            write_with_cdc_options(&[&modified], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);

        // Verify roundtrip
        let base_result = read_batches(&base_data);
        let mod_result = read_batches(&mod_data);
        assert_eq!(concat_batches(&base_result), base);
        assert_eq!(concat_batches(&mod_result), modified);

        let base_pages = get_page_lengths(&base_data, 0);
        let mod_pages = get_page_lengths(&mod_data, 0);
        assert_eq!(base_pages.len(), 1);
        assert_eq!(mod_pages.len(), 1);

        let diffs = find_differences(&base_pages[0], &mod_pages[0]);
        assert_eq!(diffs.len(), 1, "Expected 1 diff, got {diffs:?}");
        let base_sum: i64 = diffs[0].0.iter().sum();
        let mod_sum: i64 = diffs[0].1.iter().sum();
        assert_eq!(
            base_sum - mod_sum,
            edit_len as i64,
            "Diff should account for deleted rows"
        );
    }

    #[test]
    fn test_cdc_insert_once() {
        let part_len = i32_part_length();
        let edit_len = i32_edit_length();

        let part1 = make_i32_batch(part_len, 0);
        let edit = make_i32_batch(edit_len, 1);
        let part2 = make_i32_batch(part_len, 100);

        let base = concat_batches([&part1, &part2]);
        let modified = concat_batches([&part1, &edit, &part2]);

        let base_data =
            write_with_cdc_options(&[&base], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);
        let mod_data =
            write_with_cdc_options(&[&modified], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);

        let mod_result = read_batches(&mod_data);
        assert_eq!(concat_batches(&mod_result), modified);

        let base_pages = get_page_lengths(&base_data, 0);
        let mod_pages = get_page_lengths(&mod_data, 0);
        assert_eq!(base_pages.len(), 1);
        assert_eq!(mod_pages.len(), 1);

        let diffs = find_differences(&base_pages[0], &mod_pages[0]);
        assert_eq!(diffs.len(), 1, "Expected 1 diff, got {diffs:?}");
        let base_sum: i64 = diffs[0].0.iter().sum();
        let mod_sum: i64 = diffs[0].1.iter().sum();
        assert_eq!(
            mod_sum - base_sum,
            edit_len as i64,
            "Diff should account for inserted rows"
        );
    }

    #[test]
    fn test_cdc_update_once() {
        let part_len = i32_part_length();
        let edit_len = i32_edit_length();

        let part1 = make_i32_batch(part_len, 0);
        let edit1 = make_i32_batch(edit_len, 1);
        let edit2 = make_i32_batch(edit_len, 2);
        let part2 = make_i32_batch(part_len, 100);

        let base = concat_batches([&part1, &edit1, &part2]);
        let modified = concat_batches([&part1, &edit2, &part2]);

        let base_data =
            write_with_cdc_options(&[&base], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);
        let mod_data =
            write_with_cdc_options(&[&modified], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);

        let base_pages = get_page_lengths(&base_data, 0);
        let mod_pages = get_page_lengths(&mod_data, 0);
        assert_eq!(base_pages.len(), 1);
        assert_eq!(mod_pages.len(), 1);

        let diffs = find_differences(&base_pages[0], &mod_pages[0]);
        assert!(diffs.len() <= 1, "Expected at most 1 diff, got {diffs:?}");
        for (left, right) in &diffs {
            let left_sum: i64 = left.iter().sum();
            let right_sum: i64 = right.iter().sum();
            assert_eq!(
                left_sum, right_sum,
                "Update should not change total row count"
            );
        }
    }

    #[test]
    fn test_cdc_update_twice() {
        let part_len = i32_part_length();
        let edit_len = i32_edit_length();

        let part1 = make_i32_batch(part_len, 0);
        let edit1_old = make_i32_batch(edit_len, 1);
        let edit1_new = make_i32_batch(edit_len, 2);
        let part2 = make_i32_batch(part_len, 100);
        let edit2_old = make_i32_batch(edit_len, 3);
        let edit2_new = make_i32_batch(edit_len, 4);
        let part3 = make_i32_batch(part_len, 200);

        let base = concat_batches([&part1, &edit1_old, &part2, &edit2_old, &part3]);
        let modified = concat_batches([&part1, &edit1_new, &part2, &edit2_new, &part3]);

        let base_data =
            write_with_cdc_options(&[&base], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);
        let mod_data =
            write_with_cdc_options(&[&modified], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);

        let base_pages = get_page_lengths(&base_data, 0);
        let mod_pages = get_page_lengths(&mod_data, 0);

        // A double update may produce 0, 1, or 2 diffs depending on whether the
        // edits shift CDC boundaries. What must always hold is that the total row
        // count within each diff region is unchanged (updates are row-count-neutral).
        let diffs = find_differences(&base_pages[0], &mod_pages[0]);
        assert!(diffs.len() <= 2, "Expected at most 2 diffs, got {diffs:?}");
        for (left, right) in &diffs {
            let left_sum: i64 = left.iter().sum();
            let right_sum: i64 = right.iter().sum();
            assert_eq!(
                left_sum, right_sum,
                "Each update diff should not change total row count"
            );
        }
    }

    /// Verifies that the `primitive_width` fallback in `get_cdc_chunks` (used for
    /// f64 and other fixed-width non-integer types) produces correct CDC boundaries.
    #[test]
    fn test_cdc_f64_column() {
        let part_len = CDC_PART_SIZE / 8; // 8 bytes per f64
        let edit_len = CDC_EDIT_SIZE / 8;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "col",
            DataType::Float64,
            false,
        )]));

        let make_batch = |len: usize, seed: u64| {
            let array: ArrayRef = Arc::new(
                (0..len)
                    .map(|i| test_hash(seed, i as u64) as f64)
                    .collect::<arrow_array::Float64Array>(),
            );
            RecordBatch::try_new(schema.clone(), vec![array]).unwrap()
        };

        let part1 = make_batch(part_len, 0);
        let edit = make_batch(edit_len, 1);
        let part2 = make_batch(part_len, 100);

        let base = concat_batches([&part1, &part2]);
        let modified = concat_batches([&part1, &edit, &part2]);

        let base_data =
            write_with_cdc_options(&[&base], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);
        let mod_data =
            write_with_cdc_options(&[&modified], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);

        let mod_result = read_batches(&mod_data);
        assert_eq!(concat_batches(&mod_result), modified);

        let base_pages = get_page_lengths(&base_data, 0);
        let mod_pages = get_page_lengths(&mod_data, 0);

        let diffs = find_differences(&base_pages[0], &mod_pages[0]);
        assert_eq!(
            diffs.len(),
            1,
            "Expected 1 diff for f64 insert, got {diffs:?}"
        );
        let mod_sum: i64 = diffs[0].1.iter().sum();
        let base_sum: i64 = diffs[0].0.iter().sum();
        assert_eq!(mod_sum - base_sum, edit_len as i64);
    }

    #[test]
    fn test_cdc_append() {
        let part_len = i32_part_length();
        let edit_len = i32_edit_length();

        let part1 = make_i32_batch(part_len, 0);
        let part2 = make_i32_batch(part_len, 100);
        let edit = make_i32_batch(edit_len, 1);

        let base = concat_batches([&part1, &part2]);
        let modified = concat_batches([&part1, &part2, &edit]);

        let base_data =
            write_with_cdc_options(&[&base], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);
        let mod_data =
            write_with_cdc_options(&[&modified], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);

        let base_pages = get_page_lengths(&base_data, 0);
        let mod_pages = get_page_lengths(&mod_data, 0);
        assert_eq!(base_pages.len(), 1);
        assert_eq!(mod_pages.len(), 1);

        let bp = &base_pages[0];
        let mp = &mod_pages[0];

        assert!(mp.len() >= bp.len());
        for i in 0..bp.len() - 1 {
            assert_eq!(bp[i], mp[i], "Page {i} should be identical");
        }
        assert!(
            mp[bp.len() - 1] >= bp[bp.len() - 1],
            "Last original page should be same or larger in modified"
        );
    }

    #[test]
    fn test_cdc_prepend() {
        let part_len = i32_part_length();
        let edit_len = i32_edit_length();

        let part1 = make_i32_batch(part_len, 0);
        let part2 = make_i32_batch(part_len, 100);
        let edit = make_i32_batch(edit_len, 1);

        let base = concat_batches([&part1, &part2]);
        let modified = concat_batches([&edit, &part1, &part2]);

        let base_data =
            write_with_cdc_options(&[&base], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);
        let mod_data =
            write_with_cdc_options(&[&modified], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);

        let base_pages = get_page_lengths(&base_data, 0);
        let mod_pages = get_page_lengths(&mod_data, 0);
        assert_eq!(base_pages.len(), 1);
        assert_eq!(mod_pages.len(), 1);

        assert!(mod_pages[0].len() >= base_pages[0].len());

        let diffs = find_differences(&base_pages[0], &mod_pages[0]);
        assert_eq!(diffs.len(), 1, "Expected 1 diff, got {diffs:?}");
        let base_sum: i64 = diffs[0].0.iter().sum();
        let mod_sum: i64 = diffs[0].1.iter().sum();
        assert_eq!(
            mod_sum - base_sum,
            edit_len as i64,
            "Diff should account for prepended rows"
        );
    }

    #[test]
    fn test_cdc_empty_table() {
        let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Int32, false)]));
        let empty = RecordBatch::new_empty(schema.clone());
        let data = write_with_cdc_options(&[&empty], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);

        let pages = get_page_lengths(&data, 0);
        assert!(pages.is_empty(), "Empty table should produce no row groups");

        let result = read_batches(&data);
        let total_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 0);
    }

    #[test]
    fn test_cdc_multiple_row_groups_insert() {
        let part_len = i32_part_length();
        let edit_len = i32_edit_length();
        let rg_rows = part_len / 2;

        let part1 = make_i32_batch(part_len, 0);
        let edit1 = make_i32_batch(edit_len, 1);
        let edit2 = make_i32_batch(edit_len, 3);
        let part2 = make_i32_batch(part_len, 100);
        let part3 = make_i32_batch(part_len, 200);

        let base = concat_batches([&part1, &edit1, &part2, &part3]);
        let modified = concat_batches([&part1, &edit1, &edit2, &part2, &part3]);

        let base_data = write_with_cdc_options(
            &[&base],
            CDC_MIN_CHUNK_SIZE,
            CDC_MAX_CHUNK_SIZE,
            Some(rg_rows),
        );
        let mod_data = write_with_cdc_options(
            &[&modified],
            CDC_MIN_CHUNK_SIZE,
            CDC_MAX_CHUNK_SIZE,
            Some(rg_rows),
        );

        let base_result = read_batches(&base_data);
        let mod_result = read_batches(&mod_data);
        assert_eq!(concat_batches(&base_result), base);
        assert_eq!(concat_batches(&mod_result), modified);

        let base_pages = get_page_lengths(&base_data, 0);
        let mod_pages = get_page_lengths(&mod_data, 0);

        assert!(base_pages.len() > 1);
        assert_eq!(base_pages.len(), mod_pages.len());

        assert_eq!(base_pages[0], mod_pages[0]);
        assert_eq!(base_pages[1], mod_pages[1]);
    }

    #[test]
    fn test_cdc_multiple_row_groups_append() {
        let part_len = i32_part_length();
        let edit_len = i32_edit_length();
        let rg_rows = part_len / 2;

        let part1 = make_i32_batch(part_len, 0);
        let edit1 = make_i32_batch(edit_len, 1);
        let part2 = make_i32_batch(part_len, 100);
        let part3 = make_i32_batch(part_len, 200);
        let edit2 = make_i32_batch(edit_len, 3);

        let base = concat_batches([&part1, &edit1, &part2, &part3]);
        let modified = concat_batches([&part1, &edit1, &part2, &part3, &edit2]);

        let base_data = write_with_cdc_options(
            &[&base],
            CDC_MIN_CHUNK_SIZE,
            CDC_MAX_CHUNK_SIZE,
            Some(rg_rows),
        );
        let mod_data = write_with_cdc_options(
            &[&modified],
            CDC_MIN_CHUNK_SIZE,
            CDC_MAX_CHUNK_SIZE,
            Some(rg_rows),
        );

        let base_pages = get_page_lengths(&base_data, 0);
        let mod_pages = get_page_lengths(&mod_data, 0);
        assert!(base_pages.len() > 1);
        assert_eq!(base_pages.len(), mod_pages.len());

        for i in 0..base_pages.len() - 1 {
            assert_eq!(
                base_pages[i], mod_pages[i],
                "Row group {i} pages should be identical"
            );
        }
    }

    #[test]
    fn test_cdc_nullable_column() {
        let part_len = i32_part_length();
        let edit_len = i32_edit_length();

        let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Int32, true)]));

        let make_batch = |len, seed| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(generate_nullable_i32_array(len, seed)) as _],
            )
            .unwrap()
        };

        let part1 = make_batch(part_len, 0);
        let edit = make_batch(edit_len, 1);
        let part2 = make_batch(part_len, 100);

        let base = concat_batches([&part1, &part2]);
        let modified = concat_batches([&part1, &edit, &part2]);

        let base_data =
            write_with_cdc_options(&[&base], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);
        let mod_data =
            write_with_cdc_options(&[&modified], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);

        let mod_result = read_batches(&mod_data);
        assert_eq!(concat_batches(&mod_result), modified);

        let base_pages = get_page_lengths(&base_data, 0);
        let mod_pages = get_page_lengths(&mod_data, 0);

        let diffs = find_differences(&base_pages[0], &mod_pages[0]);
        assert_eq!(diffs.len(), 1, "Expected 1 diff, got {diffs:?}");
        let mod_sum: i64 = diffs[0].1.iter().sum();
        let base_sum: i64 = diffs[0].0.iter().sum();
        assert_eq!(mod_sum - base_sum, edit_len as i64);
    }

    #[test]
    fn test_cdc_string_column() {
        let part_len = CDC_PART_SIZE / 16;
        let edit_len = CDC_EDIT_SIZE / 16;

        let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));

        let make_batch = |len, seed| {
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(generate_string_array(len, seed)) as _],
            )
            .unwrap()
        };

        let part1 = make_batch(part_len, 0);
        let edit = make_batch(edit_len, 1);
        let part2 = make_batch(part_len, 100);

        let base = concat_batches([&part1, &part2]);
        let modified = concat_batches([&part1, &edit, &part2]);

        let base_data =
            write_with_cdc_options(&[&base], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);
        let mod_data =
            write_with_cdc_options(&[&modified], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);

        let mod_result = read_batches(&mod_data);
        assert_eq!(concat_batches(&mod_result), modified);

        let base_pages = get_page_lengths(&base_data, 0);
        let mod_pages = get_page_lengths(&mod_data, 0);

        let diffs = find_differences(&base_pages[0], &mod_pages[0]);
        assert_eq!(
            diffs.len(),
            1,
            "Expected 1 diff for string insert, got {diffs:?}"
        );
        let mod_sum: i64 = diffs[0].1.iter().sum();
        let base_sum: i64 = diffs[0].0.iter().sum();
        assert_eq!(mod_sum - base_sum, edit_len as i64);
    }

    #[test]
    fn test_cdc_delete_twice() {
        let part_len = i32_part_length();
        let edit_len = i32_edit_length();

        let part1 = make_i32_batch(part_len, 0);
        let edit1 = make_i32_batch(edit_len, 1);
        let part2 = make_i32_batch(part_len, 100);
        let edit2 = make_i32_batch(edit_len, 2);
        let part3 = make_i32_batch(part_len, 200);

        let base = concat_batches([&part1, &edit1, &part2, &edit2, &part3]);
        let modified = concat_batches([&part1, &part2, &part3]);

        let base_data =
            write_with_cdc_options(&[&base], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);
        let mod_data =
            write_with_cdc_options(&[&modified], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);

        let base_pages = get_page_lengths(&base_data, 0);
        let mod_pages = get_page_lengths(&mod_data, 0);

        let diffs = find_differences(&base_pages[0], &mod_pages[0]);
        assert_eq!(
            diffs.len(),
            2,
            "Expected 2 diffs for double delete, got {diffs:?}"
        );
        for (left, right) in &diffs {
            let left_sum: i64 = left.iter().sum();
            let right_sum: i64 = right.iter().sum();
            assert_eq!(
                left_sum - right_sum,
                edit_len as i64,
                "Each diff should account for one deletion"
            );
        }
    }

    #[test]
    fn test_cdc_insert_twice() {
        let part_len = i32_part_length();
        let edit_len = i32_edit_length();

        let part1 = make_i32_batch(part_len, 0);
        let edit1 = make_i32_batch(edit_len, 1);
        let part2 = make_i32_batch(part_len, 100);
        let edit2 = make_i32_batch(edit_len, 2);
        let part3 = make_i32_batch(part_len, 200);

        let base = concat_batches([&part1, &part2, &part3]);
        let modified = concat_batches([&part1, &edit1, &part2, &edit2, &part3]);

        let base_data =
            write_with_cdc_options(&[&base], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);
        let mod_data =
            write_with_cdc_options(&[&modified], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);

        let base_pages = get_page_lengths(&base_data, 0);
        let mod_pages = get_page_lengths(&mod_data, 0);

        let diffs = find_differences(&base_pages[0], &mod_pages[0]);
        assert_eq!(
            diffs.len(),
            2,
            "Expected 2 diffs for double insert, got {diffs:?}"
        );
        for (left, right) in &diffs {
            let left_sum: i64 = left.iter().sum();
            let right_sum: i64 = right.iter().sum();
            assert_eq!(
                right_sum - left_sum,
                edit_len as i64,
                "Each diff should account for one insertion"
            );
        }
    }

    #[test]
    fn test_cdc_array_offsets() {
        // CDC boundaries are content-defined: once the gear hash converges (within
        // a few dozen bytes), both the full and the sliced stream find boundaries
        // at the same absolute content positions.  Slicing at offset=10 therefore
        // produces page lengths of the form:
        //
        //   non-offsetted: [n,    a, b, c, ...]
        //   offsetted:     [n-10, a, b, c, ...]
        //
        // Only the first page is shorter by `offset`; every subsequent page,
        // including the last, is identical.
        let n = i32_part_length(); // large enough to span many CDC pages
        let offset = 10usize;
        let full = make_i32_batch(n, 0);
        let sliced = full.slice(offset, n - offset);

        let full_data =
            write_with_cdc_options(&[&full], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);
        let sliced_data =
            write_with_cdc_options(&[&sliced], CDC_MIN_CHUNK_SIZE, CDC_MAX_CHUNK_SIZE, None);

        // Roundtrip correctness.
        let read = read_batches(&sliced_data);
        assert_eq!(sliced, concat_batches(&read));

        let full_pages = get_page_lengths(&full_data, 0);
        let sliced_pages = get_page_lengths(&sliced_data, 0);

        assert_eq!(full_pages.len(), 1, "expected single row group");
        assert_eq!(sliced_pages.len(), 1, "expected single row group");

        let fp = &full_pages[0];
        let sp = &sliced_pages[0];

        assert!(fp.len() > 1, "expected multiple CDC pages, got {fp:?}");
        assert_eq!(fp.len(), sp.len(), "page count must match");

        // First page is shorter by exactly `offset`.
        assert_eq!(
            fp[0] - sp[0],
            offset as i64,
            "sliced first page should be {offset} values shorter: full={fp:?} sliced={sp:?}"
        );

        // All remaining pages — including the last — are identical.
        assert_eq!(
            &fp[1..],
            &sp[1..],
            "pages after the first must be identical: full={fp:?} sliced={sp:?}"
        );
    }

    #[test]
    fn test_cdc_array_offsets_direct() {
        // Call get_arrow_chunks directly on the low-level chunker, bypassing the
        // Arrow writer pipeline.  The same self-synchronisation property holds:
        //
        //   non-offsetted chunks: [n,    a, b, c, ...]
        //   offsetted chunks:     [n-10, a, b, c, ...]
        //
        // Only the first chunk is shorter by `offset`; all subsequent chunks have
        // identical num_values.
        use crate::basic::Type as PhysicalType;
        use crate::schema::types::{ColumnDescriptor, ColumnPath, Type};

        let options = CdcOptions {
            min_chunk_size: CDC_MIN_CHUNK_SIZE,
            max_chunk_size: CDC_MAX_CHUNK_SIZE,
            norm_level: 0,
        };
        let desc = {
            let tp = Type::primitive_type_builder("col", PhysicalType::INT32)
                .build()
                .unwrap();
            ColumnDescriptor::new(Arc::new(tp), 0, 0, ColumnPath::new(vec![]))
        };

        let n = i32_part_length(); // large enough for multiple CDC chunks
        let offset = 10usize;

        // Non-offsetted: plain fresh array of n values.
        let array = generate_i32_array(n, 0);
        let mut chunker = super::ContentDefinedChunker::new(&desc, &options).unwrap();
        let chunks = chunker.get_arrow_chunks(None, None, &array).unwrap();

        // Offsetted: same backing buffer sliced by `offset` elements.
        let sliced = array.slice(offset, n - offset);
        let mut chunker2 = super::ContentDefinedChunker::new(&desc, &options).unwrap();
        let chunks2 = chunker2.get_arrow_chunks(None, None, &sliced).unwrap();

        let values: Vec<usize> = chunks.iter().map(|c| c.num_values).collect();
        let values2: Vec<usize> = chunks2.iter().map(|c| c.num_values).collect();

        assert!(values.len() > 1, "expected multiple chunks, got {values:?}");
        assert_eq!(values.len(), values2.len(), "chunk count must match");

        assert_eq!(
            values[0] - values2[0],
            offset,
            "offsetted first chunk should be {offset} values shorter: {values:?} vs {values2:?}"
        );
        assert_eq!(
            &values[1..],
            &values2[1..],
            "all chunks after the first must be identical: {values:?} vs {values2:?}"
        );
    }
}
