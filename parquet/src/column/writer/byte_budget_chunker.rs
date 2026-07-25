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

//! See [`ByteBudgetChunker`] for byte-budget-aware mini-batch sizing.

use crate::basic::Type;
use crate::column::writer::LevelDataRef;
use crate::column::writer::encoder::ColumnValueEncoder;
use crate::file::properties::WriterProperties;
use crate::schema::types::ColumnDescriptor;

/// Picks byte-budget-aware mini-batch sizes for one column.
///
/// The parquet column writer checks the data page byte limit only *after*
/// each mini-batch finishes writing. Mini-batches are sized in rows
/// (`write_batch_size`, default 1024), so for BYTE_ARRAY columns whose
/// values are large (e.g. multi-MiB blobs) a single mini-batch can buffer
/// GiB into one page before the limit is consulted.
///
/// This isolates the per-chunk decision that prevents that: given a chunk's
/// level data and the input values, pick the largest `sub_batch_size` such
/// that one mini-batch fits in one page byte budget. For the overwhelmingly
/// common case (small or fixed-width values) the answer is just `chunk_size`
/// and the decision is O(1) on the column type — only when the input might
/// overflow does the chunker consult the encoder's byte estimate.
pub(crate) struct ByteBudgetChunker {
    /// Configured data page byte limit for the column.
    page_byte_limit: usize,
    /// Max definition level of the column; a level equal to this marks a
    /// present (non-null) leaf value. Used to count values per chunk.
    max_def_level: i16,
    /// `true` when no chunk of `base_batch_size` values can ever overflow
    /// `page_byte_limit` regardless of input. Set once at column open from
    /// the physical type's known per-value byte size; lets the per-chunk
    /// decision short-circuit with no work for every numeric, bool, or
    /// narrow `FIXED_LEN_BYTE_ARRAY` column.
    static_always_fits: bool,
    /// Configured dictionary page byte limit for the column.
    dict_page_byte_limit: usize,
    /// As [`Self::static_always_fits`] but for the dictionary page: `true`
    /// when one `base_batch_size` mini-batch of this fixed-width type cannot
    /// overshoot `dict_page_byte_limit` by more than one mini-batch's worth.
    static_dict_always_fits: bool,
}

impl ByteBudgetChunker {
    #[inline]
    pub(crate) fn new(
        descr: &ColumnDescriptor,
        props: &WriterProperties,
        base_batch_size: usize,
    ) -> Self {
        let page_byte_limit = props.column_data_page_size_limit(descr.path());
        let dict_page_byte_limit = props.column_dictionary_page_size_limit(descr.path());
        let static_bytes_per_value = match descr.physical_type() {
            Type::BOOLEAN => Some(1),
            Type::INT32 | Type::FLOAT => Some(std::mem::size_of::<i32>()),
            Type::INT64 | Type::DOUBLE => Some(std::mem::size_of::<i64>()),
            Type::INT96 => Some(12),
            Type::FIXED_LEN_BYTE_ARRAY => Some(descr.type_length().max(0) as usize),
            Type::BYTE_ARRAY => None,
        };
        let static_fits = |limit: usize| {
            static_bytes_per_value
                .map(|b| b.saturating_mul(base_batch_size) <= limit)
                .unwrap_or(false)
        };
        Self {
            page_byte_limit,
            max_def_level: descr.max_def_level(),
            static_always_fits: static_fits(page_byte_limit),
            dict_page_byte_limit,
            static_dict_always_fits: static_fits(dict_page_byte_limit),
        }
    }

    /// Decide how many levels at the start of a chunk belong in one
    /// mini-batch, so the mini-batch cannot overflow whichever page is
    /// currently accumulating value bytes: the data page when plain-encoding,
    /// or the *dictionary* page while dictionary-encoding. A returned value
    /// smaller than `chunk_size` triggers granular sub-batching in
    /// `write_batch_internal`.
    ///
    /// While dictionary-encoding, the data page holds only small RLE indices,
    /// but the dictionary page accumulates the distinct values themselves —
    /// so it is the dictionary page's remaining budget that must bound the
    /// mini-batch. The per-mini-batch dictionary spill check would otherwise
    /// let one mini-batch of large values balloon the dictionary page.
    ///
    /// Returns `chunk_size` immediately (no value inspection) when the chunk
    /// is empty, or when the column is a fixed-width type whose mini-batches
    /// statically cannot overshoot the relevant page.
    ///
    /// `#[inline]`: this is a tiny per-chunk dispatcher; the actual byte
    /// inspection lives in the out-of-line `byte_budget_sub_batch_size`.
    #[inline]
    pub(crate) fn pick_sub_batch_size<E: ColumnValueEncoder>(
        &self,
        encoder: &E,
        values: &E::Values,
        value_indices: Option<&[usize]>,
        chunk_def: LevelDataRef<'_>,
        values_offset: usize,
        chunk_size: usize,
    ) -> usize {
        if chunk_size == 0 {
            return chunk_size;
        }
        let budget = if encoder.has_dictionary() {
            if self.static_dict_always_fits {
                return chunk_size;
            }
            // Bound the mini-batch by the dictionary page's *remaining*
            // budget (it accumulates across mini-batches until it spills).
            match encoder.estimated_dict_page_size() {
                Some(used) => self.dict_page_byte_limit.saturating_sub(used),
                None => return chunk_size,
            }
        } else {
            if self.static_always_fits {
                return chunk_size;
            }
            self.page_byte_limit
        };
        self.byte_budget_sub_batch_size::<E>(
            values,
            value_indices,
            chunk_def,
            values_offset,
            chunk_size,
            budget,
        )
    }

    /// Inspect value sizes to decide how many of the chunk's values fit in
    /// `budget` bytes (the data page or dictionary page remaining budget).
    ///
    /// `#[inline(never)]` keeps this slow path out of the hot
    /// `write_batch_internal` loop; numeric and bool columns never reach it.
    #[inline(never)]
    fn byte_budget_sub_batch_size<E: ColumnValueEncoder>(
        &self,
        values: &E::Values,
        value_indices: Option<&[usize]>,
        chunk_def: LevelDataRef<'_>,
        values_offset: usize,
        chunk_size: usize,
        budget: usize,
    ) -> usize {
        // How many of this chunk's levels carry an actual value. For a
        // non-nullable, unrepeated column every level is a value, so
        // `value_count` is O(1) (`Absent`/`Uniform` def levels); only
        // nullable or nested columns pay the O(chunk_size) def-level scan.
        let vals_in_chunk = chunk_def.value_count(chunk_size, self.max_def_level);
        if vals_in_chunk == 0 {
            return chunk_size;
        }
        // Ask the encoder how many of the next values fit in one page byte
        // budget. Dispatch on whether the caller supplied gather indices;
        // this mirrors how `write_mini_batch` picks `write_gather` vs
        // `write`.
        let fit = match value_indices {
            Some(idx) => {
                let end = (values_offset + vals_in_chunk).min(idx.len());
                let start = values_offset.min(end);
                E::count_values_within_byte_budget_gather(values, &idx[start..end], budget)
            }
            None => {
                E::count_values_within_byte_budget(values, values_offset, vals_in_chunk, budget)
            }
        };
        match fit {
            None => chunk_size,
            Some(values_per_subbatch) => {
                // Convert the value count back into a level count. For a
                // non-nullable column this is a no-op; for nullable/nested
                // columns scale by the chunk's observed value-to-level
                // ratio.
                let levels_per_subbatch = if vals_in_chunk == chunk_size {
                    values_per_subbatch
                } else {
                    (values_per_subbatch * chunk_size)
                        .div_ceil(vals_in_chunk)
                        .max(1)
                };
                chunk_size.min(levels_per_subbatch.max(1))
            }
        }
    }
}
