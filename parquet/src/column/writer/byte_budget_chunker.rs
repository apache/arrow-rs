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
use crate::file::properties::ResolvedColumnProperties;
use crate::schema::types::ColumnDescriptor;

/// How [`write_granular_chunk`] should cut mini-batch windows in one chunk.
///
/// Cutting on exact value counts is the precise option and the expensive one:
/// it roughly doubles the number of mini-batches on a nullable column, because
/// the level:value ratio no longer rounds a window up to cover a second value.
/// It is used only where that precision buys something.
///
/// [`write_granular_chunk`]: super::GenericColumnWriter::write_granular_chunk
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubBatchStrategy {
    /// Cut after exactly this many values, walking definition levels to find
    /// the boundary.
    ///
    /// Used against the data page budget for encodings that compress a value
    /// against its predecessor, where the round-up below costs whole values of
    /// output: 16 MiB rather than 2 MiB for 128 values at one null in 16. See
    /// [#10538] for the measurements.
    ///
    /// [#10538]: https://github.com/apache/arrow-rs/issues/10538
    Values(usize),
    /// Cut after this many levels: the value budget scaled by the chunk's
    /// level:value ratio, rounded up.
    ///
    /// Used everywhere else, because everywhere else the round-up changes no
    /// bytes — the *dictionary page* budget shrinks toward zero as the
    /// dictionary fills, so a one-value budget is routine there for ordinary
    /// values, and `PLAIN` and `DELTA_LENGTH_BYTE_ARRAY` values cost the same
    /// wherever they land — while value-exact windows cost throughput
    /// ([#10554]).
    ///
    /// The round-up is bounded, which is what makes it an acceptable price. A
    /// window spans `ceil(values * levels / values_in_chunk)` levels, so where
    /// one value already fills the budget it covers at most two values,
    /// whatever the null density. The page bound [#9972] added therefore still
    /// holds; it is two values per page rather than one.
    ///
    /// [#9972]: https://github.com/apache/arrow-rs/pull/9972
    /// [#10554]: https://github.com/apache/arrow-rs/pull/10554
    Levels(usize),
}

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
        column_props: &ResolvedColumnProperties,
        base_batch_size: usize,
    ) -> Self {
        let page_byte_limit = column_props.data_page_size_limit;
        let dict_page_byte_limit = column_props.dictionary_page_size_limit;
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

    /// Decide how many *values* at the start of a chunk belong in one
    /// mini-batch, so the mini-batch cannot overflow whichever page is
    /// currently accumulating value bytes: the data page when plain-encoding,
    /// or the *dictionary* page while dictionary-encoding.
    ///
    /// `None` means the whole chunk fits in a single mini-batch — the common
    /// case. `Some(_)` triggers granular sub-batching in
    /// `write_batch_internal`; see [`SubBatchStrategy`] for why the data page and
    /// dictionary page budgets get different windowing.
    ///
    /// While dictionary-encoding, the data page holds only small RLE indices,
    /// but the dictionary page accumulates the distinct values themselves —
    /// so it is the dictionary page's remaining budget that must bound the
    /// mini-batch. The per-mini-batch dictionary spill check would otherwise
    /// let one mini-batch of large values balloon the dictionary page.
    ///
    /// Returns `None` immediately (no value inspection) when the chunk is
    /// empty, or when the column is a fixed-width type whose mini-batches
    /// statically cannot overshoot the relevant page.
    ///
    /// `#[inline]`: this is a tiny per-chunk dispatcher; the actual byte
    /// inspection lives in the out-of-line `byte_budget_sub_batch`.
    #[inline]
    pub(crate) fn pick_sub_batch<E: ColumnValueEncoder>(
        &self,
        encoder: &E,
        values: &E::Values,
        value_indices: Option<&[usize]>,
        chunk_def: LevelDataRef<'_>,
        values_offset: usize,
        chunk_size: usize,
    ) -> Option<SubBatchStrategy> {
        if chunk_size == 0 {
            return None;
        }
        // The second element selects the windowing; see [`SubBatchStrategy`]. Only a
        // constant data page budget under an encoding that compresses against
        // the previous value is worth cutting exactly.
        let (budget, value_exact) = if encoder.has_dictionary() {
            if self.static_dict_always_fits {
                return None;
            }
            // Bound the mini-batch by the dictionary page's *remaining*
            // budget (it accumulates across mini-batches until it spills).
            // An encoder that cannot size its dictionary page (`?`) leaves
            // the chunk unsplit.
            let used = encoder.estimated_dict_page_size()?;
            (self.dict_page_byte_limit.saturating_sub(used), false)
        } else {
            if self.static_always_fits {
                return None;
            }
            (
                self.page_byte_limit,
                encoder.compresses_against_previous_value(),
            )
        };
        self.byte_budget_sub_batch::<E>(
            values,
            value_indices,
            chunk_def,
            values_offset,
            chunk_size,
            budget,
            value_exact,
        )
    }

    /// Inspect value sizes to decide how many of the chunk's values fit in
    /// `budget` bytes (the data page or dictionary page remaining budget).
    ///
    /// `#[inline(never)]` keeps this slow path out of the hot
    /// `write_batch_internal` loop; numeric and bool columns never reach it.
    #[inline(never)]
    #[expect(clippy::too_many_arguments)]
    fn byte_budget_sub_batch<E: ColumnValueEncoder>(
        &self,
        values: &E::Values,
        value_indices: Option<&[usize]>,
        chunk_def: LevelDataRef<'_>,
        values_offset: usize,
        chunk_size: usize,
        budget: usize,
        value_exact: bool,
    ) -> Option<SubBatchStrategy> {
        // Ask the encoder how many of the next values fit in one page byte
        // budget, *before* counting the chunk's values: that count is an
        // O(chunk_size) def-level scan on nullable columns, and most chunks
        // fit outright. The chunk holds at most `chunk_size` values, so ask
        // about the next `chunk_size` — a window whose prefix is exactly the
        // chunk's values. The encoder counts the window's leading values up
        // to the first that overflows, so its answer bounds the chunk too: if
        // it covers the whole window, it covers the chunk. Dispatch on whether the caller supplied gather
        // indices; this mirrors how `write_mini_batch` picks `write_gather`
        // vs `write`.
        let (fit, window_len) = match value_indices {
            Some(idx) => {
                let end = values_offset.saturating_add(chunk_size).min(idx.len());
                let start = values_offset.min(end);
                (
                    E::count_values_within_byte_budget_gather(values, &idx[start..end], budget),
                    end - start,
                )
            }
            None => (
                E::count_values_within_byte_budget(values, values_offset, chunk_size, budget),
                // Clamped by the encoder to the values present; at worst
                // this sends a fitting tail chunk down the slower path below.
                chunk_size,
            ),
        };
        let fit = match fit {
            // The encoder cannot size these values; write the chunk whole.
            None => return None,
            // The whole window fits, so the chunk does.
            Some(n) if n >= window_len => return None,
            Some(n) => n,
        };
        // Something in the window overflows; only now count how many of the
        // window's values belong to this chunk. For a non-nullable,
        // unrepeated column `value_count` is O(1) (`Absent`/`Uniform` def
        // levels); nullable or nested columns pay the def-level scan.
        let vals_in_chunk = chunk_def.value_count(chunk_size, self.max_def_level);
        if vals_in_chunk == 0 {
            return None;
        }
        // `fit` counts the window's leading values up to and including the
        // first that overflows. The chunk's values are the window's first
        // `vals_in_chunk`, so the answer for the chunk is the same `fit`, or
        // "everything fits" when `fit` reaches past them.
        if fit >= vals_in_chunk {
            return None;
        }
        // `count_values_within_byte_budget` never reports zero, but a
        // zero-wide window would not advance `write_granular_chunk`.
        let values_per_subbatch = fit.max(1);
        Some(if value_exact {
            SubBatchStrategy::Values(values_per_subbatch)
        } else {
            // Scale to a level count. Inexact on nullable chunks, and
            // deliberately so — see `SubBatchStrategy::Levels`.
            let levels_per_subbatch = if vals_in_chunk == chunk_size {
                values_per_subbatch
            } else {
                (values_per_subbatch * chunk_size)
                    .div_ceil(vals_in_chunk)
                    .max(1)
            };
            SubBatchStrategy::Levels(chunk_size.min(levels_per_subbatch))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::basic::Repetition;
    use crate::column::writer::encoder::ColumnValueEncoderImpl;
    use crate::data_type::{ByteArray, ByteArrayType};
    use crate::file::properties::WriterProperties;
    use crate::schema::types::{ColumnPath, Type as SchemaType};
    use std::sync::Arc;

    const BUDGET: usize = 100;

    /// Runs `pick_sub_batch` for one nullable, plain-encoded BYTE_ARRAY
    /// chunk whose data page budget is [`BUDGET`] bytes, over both the
    /// contiguous and the gather entry points, and checks they agree.
    fn pick(lens: &[usize], def: &[i16]) -> Option<SubBatchStrategy> {
        let descr = Arc::new(ColumnDescriptor::new(
            Arc::new(
                SchemaType::primitive_type_builder("col", Type::BYTE_ARRAY)
                    .with_repetition(Repetition::OPTIONAL)
                    .build()
                    .unwrap(),
            ),
            1,
            0,
            ColumnPath::from("col"),
        ));
        let props = WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_data_page_size_limit(BUDGET)
            .build();
        let column_props = props.resolve_column_properties(descr.path());
        let encoder =
            ColumnValueEncoderImpl::<ByteArrayType>::try_new(&descr, &props, &column_props)
                .unwrap();
        let chunker = ByteBudgetChunker::new(&descr, &column_props, def.len());
        let values: Vec<_> = lens
            .iter()
            .map(|&n| ByteArray::from(vec![b'x'; n]))
            .collect();
        let chunk_def = LevelDataRef::Materialized(def);

        let contiguous =
            chunker.pick_sub_batch(&encoder, &values[..], None, chunk_def, 0, def.len());
        let indices: Vec<usize> = (0..values.len()).collect();
        let gather = chunker.pick_sub_batch(
            &encoder,
            &values[..],
            Some(&indices),
            chunk_def,
            0,
            def.len(),
        );
        assert_eq!(contiguous, gather);
        contiguous
    }

    #[test]
    fn small_chunk_is_not_split() {
        assert_eq!(pick(&[10, 10], &[1, 0, 1, 0]), None);
    }

    #[test]
    fn overflow_past_the_chunk_does_not_split_it() {
        // The chunk holds two small values, but the chunker sizes its window
        // over the next `chunk_size` values, which reach an oversized value
        // belonging to a later chunk. That overflow must not split this one.
        assert_eq!(pick(&[10, 10, 1000, 10], &[1, 0, 1, 0]), None);
    }

    #[test]
    fn overflow_inside_the_chunk_splits_it() {
        // Three values in four levels; the first already overflows the
        // budget, so one value per window, scaled to ceil(1 * 4 / 3) levels.
        assert_eq!(
            pick(&[1000, 10, 10], &[1, 1, 0, 1]),
            Some(SubBatchStrategy::Levels(2))
        );
    }

    #[test]
    fn all_null_chunk_is_not_split() {
        assert_eq!(pick(&[1000], &[0, 0, 0, 0]), None);
    }
}
