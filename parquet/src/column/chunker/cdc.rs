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

use super::CdcChunk;
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
    ) -> Vec<CdcChunk>
    where
        F: FnMut(&mut Self, usize),
    {
        let has_def_levels = self.max_def_level > 0;
        let has_rep_levels = self.max_rep_level > 0;

        let mut chunks = Vec::new();
        let mut prev_offset: usize = 0;
        let mut prev_value_offset: usize = 0;
        let mut value_offset: usize = 0;

        if !has_rep_levels && !has_def_levels {
            // Fastest path: non-nested, non-null data.
            // Every level corresponds to exactly one non-null value, so
            // value_offset == level_offset and num_values == num_levels.
            //
            // Example: required Int32, array = [10, 20, 30]
            //   level:         0   1   2
            //   value_offset:  0   1   2
            for offset in 0..num_levels {
                roll_value(self, offset);
                if self.need_new_chunk() {
                    chunks.push(CdcChunk {
                        level_offset: prev_offset,
                        num_levels: offset - prev_offset,
                        value_offset: prev_offset,
                        num_values: offset - prev_offset,
                    });
                    prev_offset = offset;
                }
            }
            prev_value_offset = prev_offset;
            value_offset = num_levels;
        } else if !has_rep_levels {
            // Non-nested data with nulls. value_offset only increments for
            // non-null values (def == max_def), so it diverges from the
            // level offset when nulls are present.
            //
            // Example: optional Int32, array = [1, null, 2, null, 3]
            //   def_levels:    [1, 0, 1, 0, 1]
            //   level:          0  1  2  3  4
            //   value_offset:   0     1     2  (only increments on def==1)
            let def_levels = def_levels.expect("def_levels required when max_def_level > 0");
            #[allow(clippy::needless_range_loop)]
            for offset in 0..num_levels {
                let def_level = def_levels[offset];
                self.roll_level(def_level);
                if def_level == self.max_def_level {
                    roll_value(self, offset);
                }
                // Check boundary before incrementing value_offset so that
                // num_values reflects only entries in the completed chunk.
                if self.need_new_chunk() {
                    chunks.push(CdcChunk {
                        level_offset: prev_offset,
                        num_levels: offset - prev_offset,
                        value_offset: prev_value_offset,
                        num_values: value_offset - prev_value_offset,
                    });
                    prev_offset = offset;
                    prev_value_offset = value_offset;
                }
                if def_level == self.max_def_level {
                    value_offset += 1;
                }
            }
        } else {
            // Nested data with nulls. Two counters are needed:
            //
            //   leaf_offset: index into the leaf values array for hashing,
            //     incremented for all leaf slots (def >= repeated_ancestor_def_level),
            //     including null elements.
            //
            //   value_offset: index into non_null_indices for chunk boundaries,
            //     incremented only for non-null leaf values (def == max_def_level).
            //
            // These diverge when nullable elements exist inside lists.
            //
            // Example: List<Int32?> with repeated_ancestor_def_level=2, max_def=3
            //   row 0: [1, null, 2]   (3 leaf slots, 2 non-null)
            //   row 1: [3]            (1 leaf slot, 1 non-null)
            //
            //   leaf array:    [1, null, 2, 3]
            //   def_levels:    [3,  2,   3, 3]
            //   rep_levels:    [0,  1,   1, 0]
            //
            //   level  def  leaf_offset  value_offset  action
            //   ─────  ───  ───────────  ────────────  ──────────────────────────
            //     0     3       0             0        roll_value(0), value++, leaf++
            //     1     2       1             1        leaf++ only (null element)
            //     2     3       2             1        roll_value(2), value++, leaf++
            //     3     3       3             2        roll_value(3), value++, leaf++
            //
            // roll_value(2) correctly indexes leaf array position 2 (value "2").
            // Using value_offset=1 would index position 1 (the null slot).
            //
            // Using value_offset for roll_value would hash the wrong array slot.
            let def_levels = def_levels.expect("def_levels required for nested data");
            let rep_levels = rep_levels.expect("rep_levels required for nested data");
            let mut leaf_offset: usize = 0;

            for offset in 0..num_levels {
                let def_level = def_levels[offset];
                let rep_level = rep_levels[offset];

                self.roll_level(def_level);
                self.roll_level(rep_level);
                if def_level == self.max_def_level {
                    roll_value(self, leaf_offset);
                }

                // Check boundary before incrementing value_offset so that
                // num_values reflects only entries in the completed chunk.
                if rep_level == 0 && self.need_new_chunk() {
                    let levels_to_write = offset - prev_offset;
                    if levels_to_write > 0 {
                        chunks.push(CdcChunk {
                            level_offset: prev_offset,
                            num_levels: levels_to_write,
                            value_offset: prev_value_offset,
                            num_values: value_offset - prev_value_offset,
                        });
                        prev_offset = offset;
                        prev_value_offset = value_offset;
                    }
                }
                if def_level == self.max_def_level {
                    value_offset += 1;
                }
                if def_level >= self.repeated_ancestor_def_level {
                    leaf_offset += 1;
                }
            }
        }

        // Add the last chunk if we have any levels left.
        if prev_offset < num_levels {
            chunks.push(CdcChunk {
                level_offset: prev_offset,
                num_levels: num_levels - prev_offset,
                value_offset: prev_value_offset,
                num_values: value_offset - prev_value_offset,
            });
        }

        #[cfg(debug_assertions)]
        self.validate_chunks(&chunks, num_levels, value_offset);

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
    ) -> Result<Vec<CdcChunk>> {
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
    fn validate_chunks(&self, chunks: &[CdcChunk], num_levels: usize, total_values: usize) {
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
            assert_eq!(a.num_levels, b.num_levels);
            assert_eq!(a.value_offset, b.value_offset);
            assert_eq!(a.num_values, b.num_values);
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
/// Ported from the C++ test suite in `chunker_internal_test.cc`.
#[cfg(all(test, feature = "arrow"))]
mod arrow_tests {
    use std::borrow::Borrow;
    use std::sync::Arc;

    use arrow::util::data_gen::create_random_batch;
    use arrow_array::cast::AsArray;
    use arrow_array::{Array, ArrayRef, BooleanArray, Int32Array, RecordBatch};
    use arrow_buffer::Buffer;
    use arrow_data::ArrayData;
    use arrow_schema::{DataType, Field, Fields, Schema};

    use crate::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use crate::arrow::arrow_writer::ArrowWriter;
    use crate::file::properties::{CdcOptions, WriterProperties};
    use crate::file::reader::{FileReader, SerializedFileReader};

    // --- Constants matching C++ TestCDCSingleRowGroup ---

    const CDC_MIN_CHUNK_SIZE: usize = 4 * 1024;
    const CDC_MAX_CHUNK_SIZE: usize = 16 * 1024;
    const CDC_PART_SIZE: usize = 128 * 1024;
    const CDC_EDIT_SIZE: usize = 128;
    const CDC_ROW_GROUP_LENGTH: usize = 1024 * 1024;

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

    /// Generate a deterministic array for any supported data type, matching C++ `GenerateArray`.
    fn generate_array(dtype: &DataType, nullable: bool, length: usize, seed: u64) -> ArrayRef {
        macro_rules! gen_primitive {
            ($array_type:ty, $cast:expr) => {{
                if nullable {
                    let arr: $array_type = (0..length)
                        .map(|i| {
                            let val = test_hash(seed, i as u64);
                            if val % 10 == 0 {
                                None
                            } else {
                                Some($cast(val))
                            }
                        })
                        .collect();
                    Arc::new(arr) as ArrayRef
                } else {
                    let arr: $array_type = (0..length)
                        .map(|i| Some($cast(test_hash(seed, i as u64))))
                        .collect();
                    Arc::new(arr) as ArrayRef
                }
            }};
        }

        match dtype {
            DataType::Boolean => {
                if nullable {
                    let arr: BooleanArray = (0..length)
                        .map(|i| {
                            let val = test_hash(seed, i as u64);
                            if val % 10 == 0 {
                                None
                            } else {
                                Some(val % 2 == 0)
                            }
                        })
                        .collect();
                    Arc::new(arr)
                } else {
                    let arr: BooleanArray = (0..length)
                        .map(|i| Some(test_hash(seed, i as u64) % 2 == 0))
                        .collect();
                    Arc::new(arr)
                }
            }
            DataType::Int32 => gen_primitive!(Int32Array, |v: u64| v as i32),
            DataType::Int64 => {
                gen_primitive!(arrow_array::Int64Array, |v: u64| v as i64)
            }
            DataType::Float64 => {
                gen_primitive!(arrow_array::Float64Array, |v: u64| (v % 100000) as f64
                    / 1000.0)
            }
            DataType::Utf8 => {
                let arr: arrow_array::StringArray = if nullable {
                    (0..length)
                        .map(|i| {
                            let val = test_hash(seed, i as u64);
                            if val % 10 == 0 {
                                None
                            } else {
                                Some(format!("str_{val}"))
                            }
                        })
                        .collect()
                } else {
                    (0..length)
                        .map(|i| Some(format!("str_{}", test_hash(seed, i as u64))))
                        .collect()
                };
                Arc::new(arr)
            }
            DataType::Binary => {
                let arr: arrow_array::BinaryArray = if nullable {
                    (0..length)
                        .map(|i| {
                            let val = test_hash(seed, i as u64);
                            if val % 10 == 0 {
                                None
                            } else {
                                Some(format!("bin_{val}").into_bytes())
                            }
                        })
                        .collect()
                } else {
                    (0..length)
                        .map(|i| Some(format!("bin_{}", test_hash(seed, i as u64)).into_bytes()))
                        .collect()
                };
                Arc::new(arr)
            }
            DataType::FixedSizeBinary(size) => {
                let size = *size;
                let mut builder = arrow_array::builder::FixedSizeBinaryBuilder::new(size);
                for i in 0..length {
                    let val = test_hash(seed, i as u64);
                    if nullable && val % 10 == 0 {
                        builder.append_null();
                    } else {
                        let s = format!("bin_{val}");
                        let bytes = s.as_bytes();
                        let mut buf = vec![0u8; size as usize];
                        let copy_len = bytes.len().min(size as usize);
                        buf[..copy_len].copy_from_slice(&bytes[..copy_len]);
                        builder.append_value(&buf).unwrap();
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Date32 => {
                gen_primitive!(arrow_array::Date32Array, |v: u64| v as i32)
            }
            DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => {
                gen_primitive!(arrow_array::TimestampNanosecondArray, |v: u64| v as i64)
            }
            _ => panic!("Unsupported test data type: {dtype:?}"),
        }
    }

    /// Generate a RecordBatch with the given schema, matching C++ `GenerateTable`.
    fn generate_table(schema: &Arc<Schema>, length: usize, seed: u64) -> RecordBatch {
        let arrays: Vec<ArrayRef> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, field)| {
                generate_array(
                    field.data_type(),
                    field.is_nullable(),
                    length,
                    seed + i as u64 * 10,
                )
            })
            .collect();
        RecordBatch::try_new(schema.clone(), arrays).unwrap()
    }

    /// Compute the CDC byte width for a data type, matching C++ `bytes_per_record`.
    /// Returns 0 for variable-length types.
    fn cdc_byte_width(dtype: &DataType) -> usize {
        match dtype {
            DataType::Boolean => 1,
            DataType::Int8 | DataType::UInt8 => 1,
            DataType::Int16 | DataType::UInt16 | DataType::Float16 => 2,
            DataType::Int32
            | DataType::UInt32
            | DataType::Float32
            | DataType::Date32
            | DataType::Time32(_) => 4,
            DataType::Int64
            | DataType::UInt64
            | DataType::Float64
            | DataType::Date64
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_) => 8,
            DataType::Decimal128(_, _) => 16,
            DataType::Decimal256(_, _) => 32,
            DataType::FixedSizeBinary(n) => *n as usize,
            _ => 0, // variable-length
        }
    }

    /// Compute bytes_per_record for determining part/edit lengths, matching C++.
    fn bytes_per_record(dtype: &DataType, nullable: bool) -> usize {
        let bw = cdc_byte_width(dtype);
        if bw > 0 {
            if nullable { bw + 2 } else { bw }
        } else {
            16 // variable-length fallback, matching C++
        }
    }

    /// Compute the CDC chunk size for an array slice, matching C++ `CalculateCdcSize`.
    fn calculate_cdc_size(array: &dyn Array, nullable: bool) -> i64 {
        let dtype = array.data_type();
        let bw = cdc_byte_width(dtype);
        let result = if bw > 0 {
            // Fixed-width: count only non-null values
            let valid_count = array.len() - array.null_count();
            (valid_count * bw) as i64
        } else {
            // Variable-length: sum of actual byte lengths
            match dtype {
                DataType::Utf8 => {
                    let a = array.as_string::<i32>();
                    (0..a.len())
                        .filter(|&i| a.is_valid(i))
                        .map(|i| a.value(i).len() as i64)
                        .sum()
                }
                DataType::Binary => {
                    let a = array.as_binary::<i32>();
                    (0..a.len())
                        .filter(|&i| a.is_valid(i))
                        .map(|i| a.value(i).len() as i64)
                        .sum()
                }
                DataType::LargeBinary => {
                    let a = array.as_binary::<i64>();
                    (0..a.len())
                        .filter(|&i| a.is_valid(i))
                        .map(|i| a.value(i).len() as i64)
                        .sum()
                }
                _ => panic!("CDC size calculation not implemented for {dtype:?}"),
            }
        };

        if nullable {
            // Add 2 bytes per element for definition levels
            result + array.len() as i64 * 2
        } else {
            result
        }
    }

    /// Page-level metadata for a single column within a row group.
    struct ColumnInfo {
        page_lengths: Vec<i64>,
        has_dictionary_page: bool,
    }

    /// Extract per-row-group column info from Parquet data.
    fn get_column_info(data: &[u8], column_index: usize) -> Vec<ColumnInfo> {
        let reader = SerializedFileReader::new(bytes::Bytes::from(data.to_vec())).unwrap();
        let metadata = reader.metadata();
        let mut result = Vec::new();
        for rg in 0..metadata.num_row_groups() {
            let rg_reader = reader.get_row_group(rg).unwrap();
            let col_reader = rg_reader.get_column_page_reader(column_index).unwrap();
            let mut info = ColumnInfo {
                page_lengths: Vec::new(),
                has_dictionary_page: false,
            };
            for page in col_reader {
                let page = page.unwrap();
                match page.page_type() {
                    crate::basic::PageType::DATA_PAGE | crate::basic::PageType::DATA_PAGE_V2 => {
                        info.page_lengths.push(page.num_values() as i64);
                    }
                    crate::basic::PageType::DICTIONARY_PAGE => {
                        info.has_dictionary_page = true;
                    }
                    _ => {}
                }
            }
            result.push(info);
        }
        result
    }

    /// Assert that CDC chunk sizes are within the expected range.
    /// Equivalent to C++ `AssertContentDefinedChunkSizes`.
    fn assert_cdc_chunk_sizes(
        array: &ArrayRef,
        info: &ColumnInfo,
        nullable: bool,
        min_chunk_size: usize,
        max_chunk_size: usize,
        expect_dictionary_page: bool,
    ) {
        // Boolean and FixedSizeBinary never produce dictionary pages (matching C++)
        let expect_dict = match array.data_type() {
            DataType::Boolean | DataType::FixedSizeBinary(_) => false,
            _ => expect_dictionary_page,
        };
        assert_eq!(
            info.has_dictionary_page,
            expect_dict,
            "dictionary page mismatch for {:?}",
            array.data_type()
        );

        let page_lengths = &info.page_lengths;
        assert!(
            page_lengths.len() > 1,
            "CDC should produce multiple pages, got {page_lengths:?}"
        );

        let bw = cdc_byte_width(array.data_type());
        // Only do exact CDC size validation for fixed-width and base binary-like types
        if bw > 0
            || matches!(
                array.data_type(),
                DataType::Utf8 | DataType::Binary | DataType::LargeBinary
            )
        {
            let mut offset = 0i64;
            for (i, &page_len) in page_lengths.iter().enumerate() {
                let slice = array.slice(offset as usize, page_len as usize);
                let cdc_size = calculate_cdc_size(slice.as_ref(), nullable);
                if i < page_lengths.len() - 1 {
                    assert!(
                        cdc_size >= min_chunk_size as i64,
                        "Page {i}: CDC size {cdc_size} < min {min_chunk_size}, pages={page_lengths:?}"
                    );
                }
                assert!(
                    cdc_size <= max_chunk_size as i64,
                    "Page {i}: CDC size {cdc_size} > max {max_chunk_size}, pages={page_lengths:?}"
                );
                offset += page_len;
            }
            assert_eq!(
                offset,
                array.len() as i64,
                "page lengths must sum to array length"
            );
        }
    }

    /// Write batches with CDC options and validate roundtrip.
    /// Matches C++ `WriteTableToBuffer`.
    fn write_with_cdc_options(
        batches: &[&RecordBatch],
        min_chunk_size: usize,
        max_chunk_size: usize,
        max_row_group_rows: Option<usize>,
        enable_dictionary: bool,
    ) -> Vec<u8> {
        assert!(!batches.is_empty());
        let schema = batches[0].schema();
        let mut builder = WriterProperties::builder()
            .set_dictionary_enabled(enable_dictionary)
            .set_content_defined_chunking(Some(CdcOptions {
                min_chunk_size,
                max_chunk_size,
                norm_level: 0,
            }));
        if let Some(max_rows) = max_row_group_rows {
            builder = builder.set_max_row_group_row_count(Some(max_rows));
        }
        let props = builder.build();
        let mut buf = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buf, schema.clone(), Some(props)).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.close().unwrap();

        // Roundtrip validation (matching C++ WriteTableToBuffer)
        let readback = read_batches(&buf);
        let original_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let readback_rows: usize = readback.iter().map(|b| b.num_rows()).sum();
        assert_eq!(original_rows, readback_rows, "Roundtrip row count mismatch");
        if original_rows > 0 {
            let original = concat_batches(batches.iter().copied());
            let roundtrip = concat_batches(&readback);
            assert_eq!(original, roundtrip, "Roundtrip validation failed");
        }

        buf
    }

    fn read_batches(data: &[u8]) -> Vec<RecordBatch> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(data.to_vec()))
            .unwrap()
            .build()
            .unwrap();
        reader.collect::<std::result::Result<Vec<_>, _>>().unwrap()
    }

    fn concat_batches(batches: impl IntoIterator<Item = impl Borrow<RecordBatch>>) -> RecordBatch {
        let batches: Vec<_> = batches.into_iter().collect();
        let schema = batches[0].borrow().schema();
        let batches = batches.iter().map(|b| b.borrow());
        arrow_select::concat::concat_batches(&schema, batches).unwrap()
    }

    /// LCS-based diff between two sequences of page lengths (ported from C++).
    /// Includes the merge-adjacent-diffs post-processing from C++.
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

        // Merge adjacent diffs (matching C++ post-processing)
        let mut merged: Vec<(Vec<i64>, Vec<i64>)> = Vec::new();
        for diff in result {
            if let Some(prev) = merged.last_mut() {
                if prev.0.is_empty() && diff.1.is_empty() {
                    prev.0 = diff.0;
                    continue;
                } else if prev.1.is_empty() && diff.0.is_empty() {
                    prev.1 = diff.1;
                    continue;
                }
            }
            merged.push(diff);
        }
        merged
    }

    /// Assert exact page length differences between original and modified files.
    /// Matches C++ `AssertPageLengthDifferences` (full version).
    fn assert_page_length_differences(
        original: &ColumnInfo,
        modified: &ColumnInfo,
        exact_equal_diffs: usize,
        exact_larger_diffs: usize,
        exact_smaller_diffs: usize,
        edit_length: i64,
    ) {
        let diffs = find_differences(&original.page_lengths, &modified.page_lengths);
        let expected = exact_equal_diffs + exact_larger_diffs + exact_smaller_diffs;

        if diffs.len() != expected {
            eprintln!("Original: {:?}", original.page_lengths);
            eprintln!("Modified: {:?}", modified.page_lengths);
            for d in &diffs {
                eprintln!("  Diff: {:?} vs {:?}", d.0, d.1);
            }
        }
        assert_eq!(
            diffs.len(),
            expected,
            "Expected {expected} diffs, got {}",
            diffs.len()
        );

        let (mut eq, mut larger, mut smaller) = (0usize, 0usize, 0usize);
        for (left, right) in &diffs {
            let left_sum: i64 = left.iter().sum();
            let right_sum: i64 = right.iter().sum();
            if left_sum == right_sum {
                eq += 1;
            } else if left_sum < right_sum {
                larger += 1;
                assert_eq!(
                    left_sum + edit_length,
                    right_sum,
                    "Larger diff mismatch: {left_sum} + {edit_length} != {right_sum}"
                );
            } else {
                smaller += 1;
                assert_eq!(
                    left_sum,
                    right_sum + edit_length,
                    "Smaller diff mismatch: {left_sum} != {right_sum} + {edit_length}"
                );
            }
        }

        assert_eq!(eq, exact_equal_diffs, "equal diffs count");
        assert_eq!(larger, exact_larger_diffs, "larger diffs count");
        assert_eq!(smaller, exact_smaller_diffs, "smaller diffs count");
    }

    /// Assert page length differences for update cases (simplified version).
    /// Matches C++ `AssertPageLengthDifferences` (max_equal_diffs overload).
    fn assert_page_length_differences_update(
        original: &ColumnInfo,
        modified: &ColumnInfo,
        max_equal_diffs: usize,
    ) {
        let diffs = find_differences(&original.page_lengths, &modified.page_lengths);
        assert!(
            diffs.len() <= max_equal_diffs,
            "Expected at most {max_equal_diffs} diffs, got {}",
            diffs.len()
        );
        for (left, right) in &diffs {
            let left_sum: i64 = left.iter().sum();
            let right_sum: i64 = right.iter().sum();
            assert_eq!(
                left_sum, right_sum,
                "Update diff should not change total row count"
            );
        }
    }

    // --- FindDifferences tests (ported from C++) ---

    #[test]
    fn test_find_differences_basic() {
        let diffs = find_differences(&[1, 2, 3, 4, 5], &[1, 7, 8, 4, 5]);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].0, vec![2, 3]);
        assert_eq!(diffs[0].1, vec![7, 8]);
    }

    #[test]
    fn test_find_differences_multiple() {
        let diffs = find_differences(&[1, 2, 3, 4, 5, 6, 7], &[1, 8, 9, 4, 10, 6, 11]);
        assert_eq!(diffs.len(), 3);
        assert_eq!(diffs[0].0, vec![2, 3]);
        assert_eq!(diffs[0].1, vec![8, 9]);
        assert_eq!(diffs[1].0, vec![5]);
        assert_eq!(diffs[1].1, vec![10]);
        assert_eq!(diffs[2].0, vec![7]);
        assert_eq!(diffs[2].1, vec![11]);
    }

    #[test]
    fn test_find_differences_different_lengths() {
        let diffs = find_differences(&[1, 2, 3], &[1, 2, 3, 4, 5]);
        assert_eq!(diffs.len(), 1);
        assert!(diffs[0].0.is_empty());
        assert_eq!(diffs[0].1, vec![4, 5]);
    }

    #[test]
    fn test_find_differences_empty() {
        let diffs = find_differences(&[], &[]);
        assert!(diffs.is_empty());
    }

    #[test]
    fn test_find_differences_changes_at_both_ends() {
        let diffs = find_differences(&[1, 2, 3, 4, 5, 6, 7, 8, 9], &[0, 0, 2, 3, 4, 5, 7, 7, 8]);
        assert_eq!(diffs.len(), 3);
        assert_eq!(diffs[0].0, vec![1]);
        assert_eq!(diffs[0].1, vec![0, 0]);
        assert_eq!(diffs[1].0, vec![6]);
        assert_eq!(diffs[1].1, vec![7]);
        assert_eq!(diffs[2].0, vec![9]);
        assert!(diffs[2].1.is_empty());
    }

    #[test]
    fn test_find_differences_additional() {
        let diffs = find_differences(
            &[445, 312, 393, 401, 410, 138, 558, 457],
            &[445, 312, 393, 393, 410, 138, 558, 457],
        );
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].0, vec![401]);
        assert_eq!(diffs[0].1, vec![393]);
    }

    // --- Parameterized single-row-group tests via macro ---

    macro_rules! cdc_single_rg_tests {
        ($mod_name:ident, $dtype:expr, $nullable:expr) => {
            mod $mod_name {
                use super::*;

                fn config() -> (DataType, bool, usize, usize) {
                    let dtype: DataType = $dtype;
                    let nullable: bool = $nullable;
                    let bpr = bytes_per_record(&dtype, nullable);
                    let part_length = CDC_PART_SIZE / bpr;
                    let edit_length = CDC_EDIT_SIZE / bpr;
                    (dtype, nullable, part_length, edit_length)
                }

                fn make_schema(dtype: &DataType, nullable: bool) -> Arc<Schema> {
                    Arc::new(Schema::new(vec![Field::new("f0", dtype.clone(), nullable)]))
                }

                #[test]
                fn delete_once() {
                    let (dtype, nullable, part_length, edit_length) = config();
                    let schema = make_schema(&dtype, nullable);

                    let part1 = generate_table(&schema, part_length, 0);
                    let part2 = generate_table(&schema, edit_length, 1);
                    let part3 = generate_table(&schema, part_length, part_length as u64);

                    let base = concat_batches([&part1, &part2, &part3]);
                    let modified = concat_batches([&part1, &part3]);

                    for enable_dictionary in [false, true] {
                        let base_data = write_with_cdc_options(
                            &[&base],
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            Some(CDC_ROW_GROUP_LENGTH),
                            enable_dictionary,
                        );
                        let mod_data = write_with_cdc_options(
                            &[&modified],
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            Some(CDC_ROW_GROUP_LENGTH),
                            enable_dictionary,
                        );

                        let base_info = get_column_info(&base_data, 0);
                        let mod_info = get_column_info(&mod_data, 0);
                        assert_eq!(base_info.len(), 1);
                        assert_eq!(mod_info.len(), 1);

                        assert_cdc_chunk_sizes(
                            &base.column(0).clone(),
                            &base_info[0],
                            nullable,
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            enable_dictionary,
                        );
                        assert_cdc_chunk_sizes(
                            &modified.column(0).clone(),
                            &mod_info[0],
                            nullable,
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            enable_dictionary,
                        );

                        assert_page_length_differences(
                            &base_info[0],
                            &mod_info[0],
                            0,
                            0,
                            1,
                            edit_length as i64,
                        );
                    }
                }

                #[test]
                fn delete_twice() {
                    let (dtype, nullable, part_length, edit_length) = config();
                    let schema = make_schema(&dtype, nullable);

                    let part1 = generate_table(&schema, part_length, 0);
                    let part2 = generate_table(&schema, edit_length, 1);
                    let part3 = generate_table(&schema, part_length, part_length as u64);
                    let part4 = generate_table(&schema, edit_length, 2);
                    let part5 = generate_table(&schema, part_length, 2 * part_length as u64);

                    let base = concat_batches([&part1, &part2, &part3, &part4, &part5]);
                    let modified = concat_batches([&part1, &part3, &part5]);

                    for enable_dictionary in [false, true] {
                        let base_data = write_with_cdc_options(
                            &[&base],
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            Some(CDC_ROW_GROUP_LENGTH),
                            enable_dictionary,
                        );
                        let mod_data = write_with_cdc_options(
                            &[&modified],
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            Some(CDC_ROW_GROUP_LENGTH),
                            enable_dictionary,
                        );

                        let base_info = get_column_info(&base_data, 0);
                        let mod_info = get_column_info(&mod_data, 0);
                        assert_eq!(base_info.len(), 1);
                        assert_eq!(mod_info.len(), 1);

                        assert_cdc_chunk_sizes(
                            &base.column(0).clone(),
                            &base_info[0],
                            nullable,
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            enable_dictionary,
                        );
                        assert_cdc_chunk_sizes(
                            &modified.column(0).clone(),
                            &mod_info[0],
                            nullable,
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            enable_dictionary,
                        );

                        assert_page_length_differences(
                            &base_info[0],
                            &mod_info[0],
                            0,
                            0,
                            2,
                            edit_length as i64,
                        );
                    }
                }

                #[test]
                fn insert_once() {
                    let (dtype, nullable, part_length, edit_length) = config();
                    let schema = make_schema(&dtype, nullable);

                    let part1 = generate_table(&schema, part_length, 0);
                    let part2 = generate_table(&schema, edit_length, 1);
                    let part3 = generate_table(&schema, part_length, part_length as u64);

                    let base = concat_batches([&part1, &part3]);
                    let modified = concat_batches([&part1, &part2, &part3]);

                    for enable_dictionary in [false, true] {
                        let base_data = write_with_cdc_options(
                            &[&base],
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            Some(CDC_ROW_GROUP_LENGTH),
                            enable_dictionary,
                        );
                        let mod_data = write_with_cdc_options(
                            &[&modified],
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            Some(CDC_ROW_GROUP_LENGTH),
                            enable_dictionary,
                        );

                        let base_info = get_column_info(&base_data, 0);
                        let mod_info = get_column_info(&mod_data, 0);
                        assert_eq!(base_info.len(), 1);
                        assert_eq!(mod_info.len(), 1);

                        assert_cdc_chunk_sizes(
                            &base.column(0).clone(),
                            &base_info[0],
                            nullable,
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            enable_dictionary,
                        );
                        assert_cdc_chunk_sizes(
                            &modified.column(0).clone(),
                            &mod_info[0],
                            nullable,
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            enable_dictionary,
                        );

                        assert_page_length_differences(
                            &base_info[0],
                            &mod_info[0],
                            0,
                            1,
                            0,
                            edit_length as i64,
                        );
                    }
                }

                #[test]
                fn insert_twice() {
                    let (dtype, nullable, part_length, edit_length) = config();
                    let schema = make_schema(&dtype, nullable);

                    let part1 = generate_table(&schema, part_length, 0);
                    let part2 = generate_table(&schema, edit_length, 1);
                    let part3 = generate_table(&schema, part_length, part_length as u64);
                    let part4 = generate_table(&schema, edit_length, 2);
                    let part5 = generate_table(&schema, part_length, 2 * part_length as u64);

                    let base = concat_batches([&part1, &part3, &part5]);
                    let modified = concat_batches([&part1, &part2, &part3, &part4, &part5]);

                    for enable_dictionary in [false, true] {
                        let base_data = write_with_cdc_options(
                            &[&base],
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            Some(CDC_ROW_GROUP_LENGTH),
                            enable_dictionary,
                        );
                        let mod_data = write_with_cdc_options(
                            &[&modified],
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            Some(CDC_ROW_GROUP_LENGTH),
                            enable_dictionary,
                        );

                        let base_info = get_column_info(&base_data, 0);
                        let mod_info = get_column_info(&mod_data, 0);
                        assert_eq!(base_info.len(), 1);
                        assert_eq!(mod_info.len(), 1);

                        assert_cdc_chunk_sizes(
                            &base.column(0).clone(),
                            &base_info[0],
                            nullable,
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            enable_dictionary,
                        );
                        assert_cdc_chunk_sizes(
                            &modified.column(0).clone(),
                            &mod_info[0],
                            nullable,
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            enable_dictionary,
                        );

                        assert_page_length_differences(
                            &base_info[0],
                            &mod_info[0],
                            0,
                            2,
                            0,
                            edit_length as i64,
                        );
                    }
                }

                #[test]
                fn update_once() {
                    let (dtype, nullable, part_length, edit_length) = config();
                    let schema = make_schema(&dtype, nullable);

                    let part1 = generate_table(&schema, part_length, 0);
                    let part2 = generate_table(&schema, edit_length, 1);
                    let part3 = generate_table(&schema, part_length, part_length as u64);
                    let part4 = generate_table(&schema, edit_length, 2);

                    let base = concat_batches([&part1, &part2, &part3]);
                    let modified = concat_batches([&part1, &part4, &part3]);

                    for enable_dictionary in [false, true] {
                        let base_data = write_with_cdc_options(
                            &[&base],
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            Some(CDC_ROW_GROUP_LENGTH),
                            enable_dictionary,
                        );
                        let mod_data = write_with_cdc_options(
                            &[&modified],
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            Some(CDC_ROW_GROUP_LENGTH),
                            enable_dictionary,
                        );

                        let base_info = get_column_info(&base_data, 0);
                        let mod_info = get_column_info(&mod_data, 0);
                        assert_eq!(base_info.len(), 1);
                        assert_eq!(mod_info.len(), 1);

                        assert_cdc_chunk_sizes(
                            &base.column(0).clone(),
                            &base_info[0],
                            nullable,
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            enable_dictionary,
                        );
                        assert_cdc_chunk_sizes(
                            &modified.column(0).clone(),
                            &mod_info[0],
                            nullable,
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            enable_dictionary,
                        );

                        assert_page_length_differences_update(&base_info[0], &mod_info[0], 1);
                    }
                }

                #[test]
                fn update_twice() {
                    let (dtype, nullable, part_length, edit_length) = config();
                    let schema = make_schema(&dtype, nullable);

                    let part1 = generate_table(&schema, part_length, 0);
                    let part2 = generate_table(&schema, edit_length, 1);
                    let part3 = generate_table(&schema, part_length, part_length as u64);
                    let part4 = generate_table(&schema, edit_length, 2);
                    let part5 = generate_table(&schema, part_length, 2 * part_length as u64);
                    let part6 = generate_table(&schema, edit_length, 3);
                    let part7 = generate_table(&schema, edit_length, 4);

                    let base = concat_batches([&part1, &part2, &part3, &part4, &part5]);
                    let modified = concat_batches([&part1, &part6, &part3, &part7, &part5]);

                    for enable_dictionary in [false, true] {
                        let base_data = write_with_cdc_options(
                            &[&base],
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            Some(CDC_ROW_GROUP_LENGTH),
                            enable_dictionary,
                        );
                        let mod_data = write_with_cdc_options(
                            &[&modified],
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            Some(CDC_ROW_GROUP_LENGTH),
                            enable_dictionary,
                        );

                        let base_info = get_column_info(&base_data, 0);
                        let mod_info = get_column_info(&mod_data, 0);
                        assert_eq!(base_info.len(), 1);
                        assert_eq!(mod_info.len(), 1);

                        assert_cdc_chunk_sizes(
                            &base.column(0).clone(),
                            &base_info[0],
                            nullable,
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            enable_dictionary,
                        );
                        assert_cdc_chunk_sizes(
                            &modified.column(0).clone(),
                            &mod_info[0],
                            nullable,
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            enable_dictionary,
                        );

                        assert_page_length_differences_update(&base_info[0], &mod_info[0], 2);
                    }
                }

                #[test]
                fn prepend() {
                    let (dtype, nullable, part_length, edit_length) = config();
                    let schema = make_schema(&dtype, nullable);

                    let part1 = generate_table(&schema, part_length, 0);
                    let part2 = generate_table(&schema, edit_length, 1);
                    let part3 = generate_table(&schema, part_length, part_length as u64);
                    let part4 = generate_table(&schema, edit_length, 2);

                    let base = concat_batches([&part1, &part2, &part3]);
                    let modified = concat_batches([&part4, &part1, &part2, &part3]);

                    for enable_dictionary in [false, true] {
                        let base_data = write_with_cdc_options(
                            &[&base],
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            Some(CDC_ROW_GROUP_LENGTH),
                            enable_dictionary,
                        );
                        let mod_data = write_with_cdc_options(
                            &[&modified],
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            Some(CDC_ROW_GROUP_LENGTH),
                            enable_dictionary,
                        );

                        let base_info = get_column_info(&base_data, 0);
                        let mod_info = get_column_info(&mod_data, 0);
                        assert_eq!(base_info.len(), 1);
                        assert_eq!(mod_info.len(), 1);

                        assert_cdc_chunk_sizes(
                            &base.column(0).clone(),
                            &base_info[0],
                            nullable,
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            enable_dictionary,
                        );
                        assert_cdc_chunk_sizes(
                            &modified.column(0).clone(),
                            &mod_info[0],
                            nullable,
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            enable_dictionary,
                        );

                        assert!(
                            mod_info[0].page_lengths.len() >= base_info[0].page_lengths.len(),
                            "Modified should have same or more pages"
                        );

                        assert_page_length_differences(
                            &base_info[0],
                            &mod_info[0],
                            0,
                            1,
                            0,
                            edit_length as i64,
                        );
                    }
                }

                #[test]
                fn append() {
                    let (dtype, nullable, part_length, edit_length) = config();
                    let schema = make_schema(&dtype, nullable);

                    let part1 = generate_table(&schema, part_length, 0);
                    let part2 = generate_table(&schema, edit_length, 1);
                    let part3 = generate_table(&schema, part_length, part_length as u64);
                    let part4 = generate_table(&schema, edit_length, 2);

                    let base = concat_batches([&part1, &part2, &part3]);
                    let modified = concat_batches([&part1, &part2, &part3, &part4]);

                    for enable_dictionary in [false, true] {
                        let base_data = write_with_cdc_options(
                            &[&base],
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            Some(CDC_ROW_GROUP_LENGTH),
                            enable_dictionary,
                        );
                        let mod_data = write_with_cdc_options(
                            &[&modified],
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            Some(CDC_ROW_GROUP_LENGTH),
                            enable_dictionary,
                        );

                        let base_info = get_column_info(&base_data, 0);
                        let mod_info = get_column_info(&mod_data, 0);
                        assert_eq!(base_info.len(), 1);
                        assert_eq!(mod_info.len(), 1);

                        assert_cdc_chunk_sizes(
                            &base.column(0).clone(),
                            &base_info[0],
                            nullable,
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            enable_dictionary,
                        );
                        assert_cdc_chunk_sizes(
                            &modified.column(0).clone(),
                            &mod_info[0],
                            nullable,
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            enable_dictionary,
                        );

                        let bp = &base_info[0].page_lengths;
                        let mp = &mod_info[0].page_lengths;
                        assert!(mp.len() >= bp.len());
                        for i in 0..bp.len() - 1 {
                            assert_eq!(bp[i], mp[i], "Page {i} should be identical");
                        }
                        assert!(mp[bp.len() - 1] >= bp[bp.len() - 1]);
                    }
                }

                #[test]
                fn empty_table() {
                    let (dtype, nullable, _, _) = config();
                    let schema = make_schema(&dtype, nullable);

                    let empty = RecordBatch::new_empty(schema);
                    for enable_dictionary in [false, true] {
                        let data = write_with_cdc_options(
                            &[&empty],
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            Some(CDC_ROW_GROUP_LENGTH),
                            enable_dictionary,
                        );
                        let info = get_column_info(&data, 0);
                        // Empty table: either no row groups or one with no data pages
                        if !info.is_empty() {
                            assert!(info[0].page_lengths.is_empty());
                        }
                    }
                }

                #[test]
                fn array_offsets() {
                    let (dtype, nullable, part_length, edit_length) = config();
                    let schema = make_schema(&dtype, nullable);

                    let table = concat_batches([
                        &generate_table(&schema, part_length, 0),
                        &generate_table(&schema, edit_length, 1),
                        &generate_table(&schema, part_length, part_length as u64),
                    ]);

                    for offset in [0usize, 512, 1024] {
                        if offset >= table.num_rows() {
                            continue;
                        }
                        let sliced = table.slice(offset, table.num_rows() - offset);
                        let data = write_with_cdc_options(
                            &[&sliced],
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            Some(CDC_ROW_GROUP_LENGTH),
                            true,
                        );
                        let info = get_column_info(&data, 0);
                        assert_eq!(info.len(), 1);

                        // Verify CDC actually produced content-defined chunks
                        assert_cdc_chunk_sizes(
                            &sliced.column(0).clone(),
                            &info[0],
                            nullable,
                            CDC_MIN_CHUNK_SIZE,
                            CDC_MAX_CHUNK_SIZE,
                            true,
                        );
                    }
                }
            }
        };
    }

    // Instantiate for representative types matching C++ categories
    cdc_single_rg_tests!(cdc_bool_non_null, DataType::Boolean, false);
    cdc_single_rg_tests!(cdc_i32_non_null, DataType::Int32, false);
    cdc_single_rg_tests!(cdc_i64_nullable, DataType::Int64, true);
    cdc_single_rg_tests!(cdc_f64_nullable, DataType::Float64, true);
    cdc_single_rg_tests!(cdc_utf8_non_null, DataType::Utf8, false);
    cdc_single_rg_tests!(cdc_binary_nullable, DataType::Binary, true);
    cdc_single_rg_tests!(cdc_fsb16_nullable, DataType::FixedSizeBinary(16), true);
    cdc_single_rg_tests!(cdc_date32_non_null, DataType::Date32, false);
    cdc_single_rg_tests!(
        cdc_timestamp_nullable,
        DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None),
        true
    );

    // --- Multiple row group tests matching C++ TestCDCMultipleRowGroups ---

    mod cdc_multiple_row_groups {
        use super::*;

        const PART_LENGTH: usize = 128 * 1024;
        const EDIT_LENGTH: usize = 128;
        const ROW_GROUP_LENGTH: usize = 64 * 1024;

        fn schema() -> Arc<Schema> {
            Arc::new(Schema::new(vec![
                Field::new("int32", DataType::Int32, true),
                Field::new("float64", DataType::Float64, true),
                Field::new("bool", DataType::Boolean, false),
            ]))
        }

        #[test]
        fn insert_once() {
            let s = schema();
            let part1 = generate_table(&s, PART_LENGTH, 0);
            let part2 = generate_table(&s, PART_LENGTH, 2);
            let part3 = generate_table(&s, PART_LENGTH, 4);
            let edit1 = generate_table(&s, EDIT_LENGTH, 1);
            let edit2 = generate_table(&s, EDIT_LENGTH, 3);

            let base = concat_batches([&part1, &edit1, &part2, &part3]);
            let modified = concat_batches([&part1, &edit1, &edit2, &part2, &part3]);
            assert_eq!(modified.num_rows(), base.num_rows() + EDIT_LENGTH);

            let base_data = write_with_cdc_options(
                &[&base],
                CDC_MIN_CHUNK_SIZE,
                CDC_MAX_CHUNK_SIZE,
                Some(ROW_GROUP_LENGTH),
                false,
            );
            let mod_data = write_with_cdc_options(
                &[&modified],
                CDC_MIN_CHUNK_SIZE,
                CDC_MAX_CHUNK_SIZE,
                Some(ROW_GROUP_LENGTH),
                false,
            );

            for col in 0..s.fields().len() {
                let base_info = get_column_info(&base_data, col);
                let mod_info = get_column_info(&mod_data, col);

                assert_eq!(base_info.len(), 7, "expected 7 row groups for col {col}");
                assert_eq!(mod_info.len(), 7);

                // First two row groups should be identical
                assert_eq!(base_info[0].page_lengths, mod_info[0].page_lengths);
                assert_eq!(base_info[1].page_lengths, mod_info[1].page_lengths);

                // Middle row groups: 1 larger + 1 smaller diff
                for i in 2..mod_info.len() - 1 {
                    assert_page_length_differences(
                        &base_info[i],
                        &mod_info[i],
                        0,
                        1,
                        1,
                        EDIT_LENGTH as i64,
                    );
                }
                // Last row group: just larger
                assert_page_length_differences(
                    base_info.last().unwrap(),
                    mod_info.last().unwrap(),
                    0,
                    1,
                    0,
                    EDIT_LENGTH as i64,
                );
            }
        }

        #[test]
        fn delete_once() {
            let s = schema();
            let part1 = generate_table(&s, PART_LENGTH, 0);
            let part2 = generate_table(&s, PART_LENGTH, 2);
            let part3 = generate_table(&s, PART_LENGTH, 4);
            let edit1 = generate_table(&s, EDIT_LENGTH, 1);
            let edit2 = generate_table(&s, EDIT_LENGTH, 3);

            let base = concat_batches([&part1, &edit1, &part2, &part3, &edit2]);
            let modified = concat_batches([&part1, &part2, &part3, &edit2]);

            let base_data = write_with_cdc_options(
                &[&base],
                CDC_MIN_CHUNK_SIZE,
                CDC_MAX_CHUNK_SIZE,
                Some(ROW_GROUP_LENGTH),
                false,
            );
            let mod_data = write_with_cdc_options(
                &[&modified],
                CDC_MIN_CHUNK_SIZE,
                CDC_MAX_CHUNK_SIZE,
                Some(ROW_GROUP_LENGTH),
                false,
            );

            for col in 0..s.fields().len() {
                let base_info = get_column_info(&base_data, col);
                let mod_info = get_column_info(&mod_data, col);

                assert_eq!(base_info.len(), 7);
                assert_eq!(mod_info.len(), 7);

                assert_eq!(base_info[0].page_lengths, mod_info[0].page_lengths);
                assert_eq!(base_info[1].page_lengths, mod_info[1].page_lengths);

                for i in 2..mod_info.len() - 1 {
                    assert_page_length_differences(
                        &base_info[i],
                        &mod_info[i],
                        0,
                        1,
                        1,
                        EDIT_LENGTH as i64,
                    );
                }
                assert_page_length_differences(
                    base_info.last().unwrap(),
                    mod_info.last().unwrap(),
                    0,
                    0,
                    1,
                    EDIT_LENGTH as i64,
                );
            }
        }

        #[test]
        fn update_once() {
            let s = schema();
            let part1 = generate_table(&s, PART_LENGTH, 0);
            let part2 = generate_table(&s, PART_LENGTH, 2);
            let part3 = generate_table(&s, PART_LENGTH, 4);
            let edit1 = generate_table(&s, EDIT_LENGTH, 1);
            let edit2 = generate_table(&s, EDIT_LENGTH, 3);
            let edit3 = generate_table(&s, EDIT_LENGTH, 5);

            let base = concat_batches([&part1, &edit1, &part2, &part3, &edit2]);
            let modified = concat_batches([&part1, &edit3, &part2, &part3, &edit2]);

            let base_data = write_with_cdc_options(
                &[&base],
                CDC_MIN_CHUNK_SIZE,
                CDC_MAX_CHUNK_SIZE,
                Some(ROW_GROUP_LENGTH),
                false,
            );
            let mod_data = write_with_cdc_options(
                &[&modified],
                CDC_MIN_CHUNK_SIZE,
                CDC_MAX_CHUNK_SIZE,
                Some(ROW_GROUP_LENGTH),
                false,
            );

            for col in 0..s.fields().len() {
                let nullable = s.field(col).is_nullable();
                let base_info = get_column_info(&base_data, col);
                let mod_info = get_column_info(&mod_data, col);

                assert_eq!(base_info.len(), 7);
                assert_eq!(mod_info.len(), 7);

                // Validate CDC chunk sizes on at least the first row group
                assert_cdc_chunk_sizes(
                    &base.column(col).slice(0, ROW_GROUP_LENGTH),
                    &base_info[0],
                    nullable,
                    CDC_MIN_CHUNK_SIZE,
                    CDC_MAX_CHUNK_SIZE,
                    false,
                );

                assert_eq!(base_info[0].page_lengths, mod_info[0].page_lengths);
                assert_eq!(base_info[1].page_lengths, mod_info[1].page_lengths);

                // Row group containing the edit
                assert_page_length_differences_update(&base_info[2], &mod_info[2], 1);

                // Remaining row groups should be identical
                for i in 3..mod_info.len() {
                    assert_eq!(base_info[i].page_lengths, mod_info[i].page_lengths);
                }
            }
        }

        #[test]
        fn append() {
            let s = schema();
            let part1 = generate_table(&s, PART_LENGTH, 0);
            let part2 = generate_table(&s, PART_LENGTH, 2);
            let part3 = generate_table(&s, PART_LENGTH, 4);
            let edit1 = generate_table(&s, EDIT_LENGTH, 1);
            let edit2 = generate_table(&s, EDIT_LENGTH, 3);

            let base = concat_batches([&part1, &edit1, &part2, &part3]);
            let modified = concat_batches([&part1, &edit1, &part2, &part3, &edit2]);

            let base_data = write_with_cdc_options(
                &[&base],
                CDC_MIN_CHUNK_SIZE,
                CDC_MAX_CHUNK_SIZE,
                Some(ROW_GROUP_LENGTH),
                false,
            );
            let mod_data = write_with_cdc_options(
                &[&modified],
                CDC_MIN_CHUNK_SIZE,
                CDC_MAX_CHUNK_SIZE,
                Some(ROW_GROUP_LENGTH),
                false,
            );

            for col in 0..s.fields().len() {
                let nullable = s.field(col).is_nullable();
                let base_info = get_column_info(&base_data, col);
                let mod_info = get_column_info(&mod_data, col);

                assert_eq!(base_info.len(), 7);
                assert_eq!(mod_info.len(), 7);

                // Validate CDC chunk sizes on the first row group
                assert_cdc_chunk_sizes(
                    &base.column(col).slice(0, ROW_GROUP_LENGTH),
                    &base_info[0],
                    nullable,
                    CDC_MIN_CHUNK_SIZE,
                    CDC_MAX_CHUNK_SIZE,
                    false,
                );

                // All row groups except last should be identical
                for i in 0..base_info.len() - 1 {
                    assert_eq!(base_info[i].page_lengths, mod_info[i].page_lengths);
                }

                // Last row group: pages should be identical except last
                let bp = &base_info.last().unwrap().page_lengths;
                let mp = &mod_info.last().unwrap().page_lengths;
                assert!(mp.len() >= bp.len());
                for i in 0..bp.len() - 1 {
                    assert_eq!(bp[i], mp[i]);
                }
            }
        }
    }

    // --- Direct chunker test (kept from original) ---

    #[test]
    fn test_cdc_array_offsets_direct() {
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

        let bpr = bytes_per_record(&DataType::Int32, false);
        let n = CDC_PART_SIZE / bpr;
        let offset = 10usize;

        let array: Int32Array = (0..n).map(|i| test_hash(0, i as u64) as i32).collect();
        let mut chunker = super::ContentDefinedChunker::new(&desc, &options).unwrap();
        let chunks = chunker.get_arrow_chunks(None, None, &array).unwrap();

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
            "offsetted first chunk should be {offset} values shorter"
        );
        assert_eq!(
            &values[1..],
            &values2[1..],
            "all chunks after the first must be identical"
        );
    }

    /// Regression test for <https://github.com/apache/arrow-rs/issues/9637>
    ///
    /// Writing nested list data with CDC enabled panicked with an out-of-bounds
    /// slice access when null list entries had non-zero child ranges.
    #[test]
    fn test_cdc_list_roundtrip() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "_1",
                DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true))),
                true,
            ),
            Field::new(
                "_2",
                DataType::List(Arc::new(Field::new_list_field(DataType::Boolean, true))),
                true,
            ),
            Field::new(
                "_3",
                DataType::LargeList(Arc::new(Field::new_list_field(DataType::Utf8, true))),
                true,
            ),
        ]));
        let batch = create_random_batch(schema, 10_000, 0.25, 0.75).unwrap();
        write_with_cdc_options(
            &[&batch],
            CDC_MIN_CHUNK_SIZE,
            CDC_MAX_CHUNK_SIZE,
            None,
            true,
        );
    }

    /// Test CDC with deeply nested types: List<List<Int32>>, List<Struct<List<Int32>>>
    #[test]
    fn test_cdc_deeply_nested_roundtrip() {
        let inner_field = Field::new_list_field(DataType::Int32, true);
        let inner_type = DataType::List(Arc::new(inner_field));
        let outer_field = Field::new_list_field(inner_type.clone(), true);
        let list_list_type = DataType::List(Arc::new(outer_field));

        let struct_inner_field = Field::new_list_field(DataType::Int32, true);
        let struct_inner_type = DataType::List(Arc::new(struct_inner_field));
        let struct_fields = Fields::from(vec![Field::new("a", struct_inner_type, true)]);
        let struct_type = DataType::Struct(struct_fields);
        let struct_list_field = Field::new_list_field(struct_type, true);
        let list_struct_type = DataType::List(Arc::new(struct_list_field));

        let schema = Arc::new(Schema::new(vec![
            Field::new("list_list", list_list_type, true),
            Field::new("list_struct_list", list_struct_type, true),
        ]));
        let batch = create_random_batch(schema, 10_000, 0.25, 0.75).unwrap();
        write_with_cdc_options(
            &[&batch],
            CDC_MIN_CHUNK_SIZE,
            CDC_MAX_CHUNK_SIZE,
            None,
            true,
        );
    }

    /// Test CDC with list arrays that have non-empty null segments.
    ///
    /// Per the Arrow columnar format spec: "a null value may correspond to a
    /// non-empty segment in the child array". This test constructs such arrays
    /// manually and verifies the CDC writer handles them correctly.
    #[test]
    fn test_cdc_list_non_empty_null_segments() {
        // Build List<Int32> where null entries own non-zero child ranges:
        //   row 0: [1, 2]     offsets[0..2]  valid
        //   row 1: null        offsets[2..5]  null, but owns 3 child values
        //   row 2: [6, 7]     offsets[5..7]  valid
        //   row 3: null        offsets[7..9]  null, but owns 2 child values
        //   row 4: [10]        offsets[9..10] valid
        let values = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let offsets = Buffer::from_iter([0_i32, 2, 5, 7, 9, 10]);
        let null_bitmap = Buffer::from([0b00010101]); // rows 0, 2, 4 valid

        let list_type = DataType::List(Arc::new(Field::new_list_field(DataType::Int32, false)));
        let list_data = unsafe {
            ArrayData::new_unchecked(
                list_type.clone(),
                5,
                None,
                Some(null_bitmap),
                0,
                vec![offsets],
                vec![values.to_data()],
            )
        };
        let list_array = arrow_array::make_array(list_data);

        let schema = Arc::new(Schema::new(vec![Field::new("col", list_type, true)]));
        let batch = RecordBatch::try_new(schema, vec![list_array]).unwrap();

        let buf = write_with_cdc_options(
            &[&batch],
            CDC_MIN_CHUNK_SIZE,
            CDC_MAX_CHUNK_SIZE,
            None,
            true,
        );
        let read = concat_batches(read_batches(&buf));
        let read_list = read.column(0).as_list::<i32>();
        assert_eq!(read_list.len(), 5);
        assert!(read_list.is_valid(0));
        assert!(read_list.is_null(1));
        assert!(read_list.is_valid(2));
        assert!(read_list.is_null(3));
        assert!(read_list.is_valid(4));

        let get_vals = |i: usize| -> Vec<i32> {
            read_list
                .value(i)
                .as_primitive::<arrow_array::types::Int32Type>()
                .values()
                .iter()
                .copied()
                .collect()
        };
        assert_eq!(get_vals(0), vec![1, 2]);
        assert_eq!(get_vals(2), vec![6, 7]);
        assert_eq!(get_vals(4), vec![10]);
    }
}
