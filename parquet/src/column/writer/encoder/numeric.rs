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

//! Numeric sources, batches, and encoding.

use super::*;
#[cfg(feature = "arrow")]
use crate::encodings::encoding::AlpRunEncoder;

/// Maximum values per gathered or expanded numeric batch.
#[cfg(feature = "arrow")]
const NUMERIC_BATCH_VALUES: usize = 64;

/// A native scalar batch handed to the numeric column encoder.
#[derive(Clone, Copy)]
pub(super) enum NumericBatch<'a, T> {
    Flat(&'a [T]),
    #[cfg(feature = "arrow")]
    RunGroups(RunBatch<'a, T>),
}

/// Running numeric extrema and whether they still consist entirely of NaNs.
///
/// Keeping the NaN state explicitly lets callers classify each value once for
/// both NaN counting and extrema, rather than reclassifying the current min and
/// max on every value.
struct NumericExtrema<T> {
    min: Option<T>,
    max: Option<T>,
    are_nan: bool,
}

impl<T> NumericExtrema<T>
where
    T: Copy + 'static + for<'v> MinMaxStrategy<'v, Elem = T, Owned = T>,
{
    fn new() -> Self {
        Self {
            min: None,
            max: None,
            are_nan: false,
        }
    }

    /// Fold a pre-classified value into the running extrema. Floating-point
    /// statistics ignore NaNs once a non-NaN has been observed, but preserve
    /// IEEE-total-ordered extrema when every value is NaN.
    #[inline(always)]
    fn observe(&mut self, ctx: <T as MinMaxStrategy<'static>>::Ctx, value: T, value_is_nan: bool) {
        let Some(current_min) = self.min.as_mut() else {
            debug_assert!(self.max.is_none());
            self.min = Some(value);
            self.max = Some(value);
            self.are_nan = value_is_nan;
            return;
        };
        let current_max = self
            .max
            .as_mut()
            .expect("numeric min and max must be initialized together");

        match (self.are_nan, value_is_nan) {
            // Once a non-NaN is observed, later NaNs do not participate in extrema.
            (false, true) => {}
            // The first non-NaN replaces both all-NaN running extrema.
            (true, false) => {
                *current_min = value;
                *current_max = value;
                self.are_nan = false;
            }
            // Both values are NaN or both are non-NaN. Since min and max are already
            // initialized, one value cannot be both new extrema.
            _ if <T as MinMaxStrategy<'static>>::greater(ctx, *current_min, value) => {
                *current_min = value;
            }
            _ if <T as MinMaxStrategy<'static>>::greater(ctx, value, *current_max) => {
                *current_max = value;
            }
            _ => {}
        }
    }
}

/// Classify a numeric value once and add its logical multiplicity to the NaN
/// count. Non-floating-point specializations compile to `false` without
/// initializing `nan_count`.
#[inline(always)]
fn classify_and_count_nan<T>(
    ctx: <T as MinMaxStrategy<'static>>::Ctx,
    value: T,
    multiplicity: usize,
    nan_count: &mut Option<u64>,
) -> bool
where
    T: Copy + 'static + for<'v> MinMaxStrategy<'v, Elem = T, Owned = T>,
{
    if !<T as MinMaxStrategy<'static>>::TRACKS_NAN {
        return false;
    }

    let value_is_nan = <T as MinMaxStrategy<'static>>::is_nan(ctx, value);
    *nan_count.get_or_insert(0) += (value_is_nan as u64) * multiplicity as u64;
    value_is_nan
}

/// A selected Arrow numeric source, retaining its physical values and mapping.
#[cfg(feature = "arrow")]
#[derive(Clone, Copy)]
pub(crate) struct PhysicalNumericSource<'source, D, T, F> {
    data: &'source [D],
    direct: Option<&'source [T]>,
    selection: PhysicalValueSelection<'source>,
    cast: F,
}

#[cfg(feature = "arrow")]
impl<'source, D, T, F> PhysicalNumericSource<'source, D, T, F>
where
    D: Copy,
    T: Copy,
    F: Fn(D) -> T + Copy,
{
    pub(crate) fn new(
        data: &'source [D],
        direct: Option<&'source [T]>,
        selection: PhysicalValueSelection<'source>,
        cast: F,
    ) -> Self {
        Self {
            data,
            direct,
            selection,
            cast,
        }
    }

    fn write_grouped_to<S>(self, sink: &mut S) -> Result<()>
    where
        T: 'static,
        S: for<'batch> BatchSink<NumericBatch<'batch, T>>,
    {
        debug_assert!(self.selection.is_grouped());
        let data = self.data;
        let cast = self.cast;
        let mapped = map_values(self.selection, move |index| cast(data[index]));
        gather_run_groups_tiled::<NUMERIC_BATCH_VALUES, _, _>(mapped, |values, counts| {
            sink.push_batch(NumericBatch::RunGroups(RunBatch { values, counts }))
        })
    }

    fn write_ungrouped_to<S>(self, sink: &mut S) -> Result<()>
    where
        T: 'static,
        S: for<'batch> BatchSink<NumericBatch<'batch, T>>,
    {
        debug_assert!(!self.selection.is_grouped());
        let data = self.data;
        let cast = self.cast;
        let mapped = map_values(self.selection, move |index| cast(data[index]));
        if let Some(values) = self.direct
            && self.selection.try_for_each_borrowable_range(|range| {
                sink.push_batch(NumericBatch::Flat(&values[range]))
            })?
        {
            return Ok(());
        }

        gather_tiled::<NUMERIC_BATCH_VALUES, _, _, _>(mapped, |values| {
            sink.push_batch(NumericBatch::Flat(values))
        })
    }
}

impl<'batch, T: PlainEncoderType> BatchSink<NumericBatch<'batch, T::T>>
    for TypedColumnChunkEncoder<T>
where
    T::T: ColumnWriterValue<Family<T> = NumericEncodingFamily<T>>,
    T::T: Copy + 'static + for<'v> MinMaxStrategy<'v, Elem = T::T, Owned = T::T>,
{
    #[inline(never)]
    fn push_batch(&mut self, values: NumericBatch<'batch, T::T>) -> Result<()> {
        // INTERVAL has undefined sort order, so it must not emit min/max statistics.
        let should_update_stats = self.statistics_enabled != EnabledStatistics::None
            && self.descr.converted_type() != ConvertedType::INTERVAL;
        let ctx = <T::T as MinMaxStrategy<'static>>::ctx(&self.descr);
        let mut extrema = NumericExtrema::new();
        match values {
            NumericBatch::Flat(values) => {
                self.encode_flat(values, ctx, should_update_stats, &mut extrema)?
            }
            #[cfg(feature = "arrow")]
            NumericBatch::RunGroups(values) => self.encode_run_groups(
                values.values,
                values.counts,
                ctx,
                should_update_stats,
                &mut extrema,
            )?,
        }
        self.merge_batch_stats(extrema.min, extrema.max);
        Ok(())
    }
}

impl<T: DataType> TypedColumnChunkEncoder<T> {
    #[cfg(feature = "arrow")]
    pub(crate) fn write_numeric_source<D, F>(
        &mut self,
        values: PhysicalNumericSource<'_, D, T::T, F>,
    ) -> Result<()>
    where
        T: PlainEncoderType,
        T::T: ColumnWriterValue<Family<T> = NumericEncodingFamily<T>>,
        T::T: Copy + 'static + for<'v> MinMaxStrategy<'v, Elem = T::T, Owned = T::T>,
        D: Copy,
        F: Fn(D) -> T::T + Copy,
    {
        if values.selection.len() == 0 {
            return Ok(());
        }
        let selection = values.selection;
        let grouped = selection.is_grouped();
        if !selection.should_cache_dictionary(values.data.len())
            || !matches!(self.encoding_family, NumericEncodingFamily::Dictionary(_))
        {
            return if grouped {
                values.write_grouped_to(self)
            } else {
                values.write_ungrouped_to(self)
            };
        }
        let should_update_stats = self.statistics_enabled != EnabledStatistics::None
            && self.descr.converted_type() != ConvertedType::INTERVAL;
        let ctx = <T::T as MinMaxStrategy<'static>>::ctx(&self.descr);
        let mut extrema = NumericExtrema::new();
        let NumericEncodingFamily::Dictionary(dict) = &mut self.encoding_family else {
            unreachable!()
        };
        self.num_values += selection.len();
        let nan_count = &mut self.nan_count;
        // Duplicate observations within one write are idempotent for min/max;
        // NaN counts follow logical multiplicity. Bloom values are visited at
        // dictionary flush, without per-row or per-source hashing.
        // The call-local mask cannot suppress later pages.
        let mut observed = (values.data.len() <= u64::BITS as usize).then_some(0_u64);
        let mut observe = |index, multiplicity: usize| {
            let value = (values.cast)(values.data[index]);
            let value_is_nan =
                should_update_stats && classify_and_count_nan(ctx, value, multiplicity, nan_count);
            let first = observed.as_mut().is_none_or(|observed| {
                let bit = 1_u64 << index;
                let first = *observed & bit == 0;
                *observed |= bit;
                first
            });
            if first && should_update_stats {
                extrema.observe(ctx, value, value_is_nan);
            }
            value
        };
        if grouped {
            selection.try_for_each_index_group(|index, count| {
                let value = observe(index, count);
                dict.put_arrow_dictionary(index, Some(count), |dictionary| {
                    dictionary.intern(&value)
                })
            })?;
        } else {
            dict.reserve(selection.len());
            selection.try_for_each_index(|index| {
                let value = observe(index, 1);
                dict.put_arrow_dictionary(index, None, |dictionary| dictionary.intern(&value))
            })?;
        }
        self.merge_batch_stats(extrema.min, extrema.max);
        Ok(())
    }

    #[inline(never)]
    fn merge_batch_stats(&mut self, min: Option<T::T>, max: Option<T::T>) {
        if let Some(min) = min {
            update_min(&self.descr, &min, &mut self.min_value);
        }
        if let Some(max) = max {
            update_max(&self.descr, &max, &mut self.max_value);
        }
    }
}

impl<T: DataType> TypedColumnChunkEncoder<T> {
    #[inline(never)]
    fn encode_flat(
        &mut self,
        values: &[T::T],
        ctx: <T::T as MinMaxStrategy<'static>>::Ctx,
        should_update_stats: bool,
        extrema: &mut NumericExtrema<T::T>,
    ) -> Result<()>
    where
        T: PlainEncoderType,
        T::T: ColumnWriterValue<Family<T> = NumericEncodingFamily<T>>,
        T::T: Copy + 'static + for<'v> MinMaxStrategy<'v, Elem = T::T, Owned = T::T>,
    {
        let Self {
            encoding_family,
            num_values,
            nan_count,
            bloom_filter,
            ..
        } = self;
        if should_update_stats {
            for &value in values {
                let value_is_nan = classify_and_count_nan(ctx, value, 1, nan_count);
                extrema.observe(ctx, value, value_is_nan);
            }
        }
        if !matches!(encoding_family, NumericEncodingFamily::Dictionary(_))
            && let Some(bloom) = bloom_filter.as_mut()
        {
            for &value in values {
                bloom.insert(&value);
            }
        }
        *num_values += values.len();
        match encoding_family {
            NumericEncodingFamily::Dictionary(dict) => {
                // `reserve` flushes any run-buffered indices
                // (`RunIndexBuffer::reserve` -> `materialize`), satisfying
                // `put_one`'s flushed-runs precondition; a separate flush here
                // is a redundant no-op.
                dict.reserve(values.len());
                for &value in values {
                    dict.put_one(&value)?;
                }
                Ok(())
            }
            other => <NumericEncodingFamily<T> as Encoder<T>>::put(other, values),
        }
    }

    /// Encode one run-group batch: `values[i]` spans `counts[i]` logical outputs.
    /// Selection planning already removed nulls, so every group is observed and
    /// every count is non-zero.
    #[cfg(feature = "arrow")]
    #[inline(never)]
    fn encode_run_groups(
        &mut self,
        values: &[T::T],
        counts: &[usize],
        ctx: <T::T as MinMaxStrategy<'static>>::Ctx,
        should_update_stats: bool,
        extrema: &mut NumericExtrema<T::T>,
    ) -> Result<()>
    where
        T: PlainEncoderType,
        T::T: ColumnWriterValue<Family<T> = NumericEncodingFamily<T>>,
        T::T: Copy + 'static + for<'v> MinMaxStrategy<'v, Elem = T::T, Owned = T::T>,
    {
        let Self {
            encoding_family,
            num_values,
            nan_count,
            bloom_filter,
            ..
        } = self;
        match encoding_family {
            NumericEncodingFamily::Dictionary(dict) => {
                for (&value, &run_len) in values.iter().zip(counts) {
                    if should_update_stats {
                        let value_is_nan = classify_and_count_nan(ctx, value, run_len, nan_count);
                        extrema.observe(ctx, value, value_is_nan);
                    }
                    *num_values += run_len;
                    dict.put_value_run(&value, run_len)?;
                }
                Ok(())
            }
            NumericEncodingFamily::Alp(encoder) => {
                for (&value, &run_len) in values.iter().zip(counts) {
                    if should_update_stats {
                        let value_is_nan = classify_and_count_nan(ctx, value, run_len, nan_count);
                        extrema.observe(ctx, value, value_is_nan);
                    }
                    if let Some(bloom) = bloom_filter.as_mut() {
                        bloom.insert(&value);
                    }
                    *num_values += run_len;
                    encoder.put_repeated(value, run_len)?;
                }
                Ok(())
            }
            other => {
                // PLAIN writes every value: expand each group to its logical
                // outputs, buffering into bounded batches for bulk `put`.
                let mut buf = [MaybeUninit::<T::T>::uninit(); NUMERIC_BATCH_VALUES];
                let mut filled = 0usize;
                for (&value, &run_len) in values.iter().zip(counts) {
                    if should_update_stats {
                        let value_is_nan = classify_and_count_nan(ctx, value, run_len, nan_count);
                        extrema.observe(ctx, value, value_is_nan);
                    }
                    if let Some(bloom) = bloom_filter.as_mut() {
                        bloom.insert(&value);
                    }
                    *num_values += run_len;
                    for _ in 0..run_len {
                        buf[filled].write(value);
                        filled += 1;
                        if filled == buf.len() {
                            // SAFETY: every slot has just been initialized.
                            <NumericEncodingFamily<T> as Encoder<T>>::put(other, unsafe {
                                assume_init_prefix(&buf, filled)
                            })?;
                            filled = 0;
                        }
                    }
                }
                if filled > 0 {
                    // SAFETY: values are written sequentially through `filled`.
                    <NumericEncodingFamily<T> as Encoder<T>>::put(other, unsafe {
                        assume_init_prefix(&buf, filled)
                    })?;
                }
                Ok(())
            }
        }
    }
}
