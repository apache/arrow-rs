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
use crate::column::writer::encoder::ColumnChunkEncoder;
use crate::column::writer::{ByteBudgetTarget, ColumnWriteSource, LevelWindow};
use crate::file::properties::ResolvedColumnProperties;
use crate::schema::types::ColumnDescriptor;

/// Window in exact values for prefix-compressing data pages, or approximate
/// levels for other encodings and dictionary budgets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubBatchStrategy {
    Values(usize),
    Levels(usize),
}

/// Conservative byte bound for one fixed-width Parquet physical value.
#[derive(Clone, Copy)]
struct PhysicalValueByteUpperBound {
    bytes_per_value: usize,
    include_boundary_value: bool,
}

/// Picks byte-budget-aware mini-batch sizes for one column.
///
/// Given a level window and its selected values, chooses a leading level count
/// using the data or dictionary page value-byte estimate. Fixed-width candidates
/// use a conservative O(1) bound; otherwise definition levels are counted and
/// variable-width values are measured as needed. This is best-effort page sizing,
/// not an exact serialized-page size calculation.
pub(crate) struct ByteBudgetChunker {
    /// Configured data page byte limit for the column.
    page_byte_limit: usize,
    /// Max definition level of the column; a level equal to this marks a
    /// present (non-null) leaf value. Used to count values per chunk.
    max_def_level: i16,
    /// Conservative per-value byte bound for fixed-width physical inputs.
    /// `None` for BYTE_ARRAY, whose sizing must inspect the selected values.
    physical_value_byte_upper_bound: Option<PhysicalValueByteUpperBound>,
    /// Configured dictionary page byte limit for the column.
    dict_page_byte_limit: usize,
}

impl ByteBudgetChunker {
    #[inline]
    pub(crate) fn new(descr: &ColumnDescriptor, column_props: &ResolvedColumnProperties) -> Self {
        let page_byte_limit = column_props.data_page_size_limit;
        let dict_page_byte_limit = column_props.dictionary_page_size_limit;
        let physical_bound = |bytes_per_value: usize, include_boundary_value| {
            Some(PhysicalValueByteUpperBound {
                bytes_per_value,
                include_boundary_value,
            })
        };
        let physical_value_byte_upper_bound = match descr.physical_type() {
            Type::BOOLEAN => physical_bound(1, false),
            Type::INT32 | Type::FLOAT => physical_bound(std::mem::size_of::<i32>(), false),
            Type::INT64 | Type::DOUBLE => physical_bound(std::mem::size_of::<i64>(), false),
            Type::INT96 => physical_bound(12, false),
            Type::FIXED_LEN_BYTE_ARRAY => physical_bound(descr.type_length().max(0) as usize, true),
            Type::BYTE_ARRAY => None,
        };
        Self {
            page_byte_limit,
            max_def_level: descr.max_def_level(),
            physical_value_byte_upper_bound,
            dict_page_byte_limit,
        }
    }

    /// Decide how to divide a chunk using the value-byte estimate for whichever
    /// page is accumulating values. `None` leaves the entire chunk on the fast
    /// path; otherwise the strategy distinguishes exact value boundaries from
    /// approximate level-count windows.
    ///
    /// While dictionary-encoding, the data page holds only small RLE indices,
    /// but the dictionary page accumulates the distinct values themselves —
    /// so it is the dictionary page's remaining budget that must bound the
    /// mini-batch. The per-mini-batch dictionary spill check would otherwise
    /// let one mini-batch of large values balloon the dictionary page.
    ///
    /// The first value that crosses the budget is included so the existing
    /// post-write page check flushes at that mini-batch boundary. This also
    /// guarantees progress for an oversized singleton.
    #[inline]
    pub(crate) fn pick_sub_batch<E, S>(
        &self,
        encoder: &E,
        source: S,
        chunk: LevelWindow<'_>,
        values_offset: usize,
    ) -> Option<SubBatchStrategy>
    where
        E: ColumnChunkEncoder,
        S: ColumnWriteSource<E>,
    {
        if chunk.len == 0 {
            return None;
        }
        let (budget, target) = if encoder.has_dictionary() {
            // Bound the mini-batch by the dictionary page's *remaining*
            // budget (it accumulates across mini-batches until it spills).
            let used = encoder.estimated_dict_page_size()?;
            (
                self.dict_page_byte_limit.saturating_sub(used),
                ByteBudgetTarget::DictionaryPage,
            )
        } else {
            (self.page_byte_limit, ByteBudgetTarget::DataPage)
        };
        // A fixed-width upper bound avoids scanning nullable definition levels
        // whenever the entire candidate is within the relevant estimate.
        if self
            .physical_value_byte_upper_bound
            .is_some_and(|bound| bound.bytes_per_value.saturating_mul(chunk.len) <= budget)
        {
            return None;
        }
        self.byte_budget_sub_batch::<E, S>(encoder, source, chunk, values_offset, (budget, target))
    }

    /// Inspect value sizes or fixed-width bounds to decide how many of the
    /// chunk's values fit in `budget` bytes (the data-page budget or remaining
    /// dictionary-page budget).
    ///
    /// `#[inline(never)]` keeps prefix sizing out of the hot
    /// `write_batch_internal` loop. Fixed-width columns use division after
    /// counting present values; variable-width columns may also inspect values.
    #[inline(never)]
    fn byte_budget_sub_batch<E, S>(
        &self,
        encoder: &E,
        source: S,
        chunk: LevelWindow<'_>,
        values_offset: usize,
        (budget, target): (usize, ByteBudgetTarget),
    ) -> Option<SubBatchStrategy>
    where
        E: ColumnChunkEncoder,
        S: ColumnWriteSource<E>,
    {
        // How many of this chunk's levels carry an actual value. For a
        // non-nullable, unrepeated column every level is a value, so
        // `value_count` is O(1) (`Absent`/`Uniform` def levels); only
        // nullable or nested columns pay the O(chunk_size) def-level scan.
        let vals_in_chunk = chunk.def.value_count(chunk.len, self.max_def_level);
        if vals_in_chunk == 0 {
            return None;
        }
        // Limit measurement to the selected values covered by this level
        // chunk, clamped to the values remaining in the source.
        let remaining_value_count = source.len().saturating_sub(values_offset);
        let values_to_measure = vals_in_chunk.min(remaining_value_count);
        let fit = self
            .physical_value_byte_upper_bound
            .map(|bound| {
                let mut count = budget / bound.bytes_per_value.max(1);
                if bound.include_boundary_value && count < values_to_measure {
                    count += 1;
                }
                count.max(1).min(values_to_measure)
            })
            .or_else(|| {
                source
                    .slice(values_offset, values_to_measure)
                    .count_variable_width_within_byte_budget(encoder, budget, target)
            });
        match fit {
            None => None,
            Some(values_per_subbatch) if values_per_subbatch >= vals_in_chunk => None,
            Some(values_per_subbatch) => {
                let values_per_subbatch = values_per_subbatch.max(1);
                if matches!(target, ByteBudgetTarget::DataPage)
                    && encoder.compresses_against_previous_value()
                {
                    return Some(SubBatchStrategy::Values(values_per_subbatch));
                }
                let levels_per_subbatch = if vals_in_chunk == chunk.len {
                    values_per_subbatch
                } else {
                    (values_per_subbatch * chunk.len)
                        .div_ceil(vals_in_chunk)
                        .max(1)
                };
                Some(SubBatchStrategy::Levels(chunk.len.min(levels_per_subbatch)))
            }
        }
    }
}
