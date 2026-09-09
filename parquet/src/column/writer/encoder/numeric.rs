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

/// A native scalar batch handed to the numeric column encoder.
#[derive(Clone, Copy)]
pub(super) enum NumericBatch<'a, T> {
    Flat(&'a [T]),
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
        }
        self.merge_batch_stats(extrema.min, extrema.max);
        Ok(())
    }
}

impl<T: DataType> TypedColumnChunkEncoder<T> {
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
        if let Some(bloom) = bloom_filter.as_mut() {
            for &value in values {
                bloom.insert(&value);
            }
        }
        *num_values += values.len();
        match encoding_family {
            NumericEncodingFamily::Dictionary(dict) => {
                dict.reserve(values.len());
                for &value in values {
                    dict.put_one(&value)?;
                }
                Ok(())
            }
            other => <NumericEncodingFamily<T> as Encoder<T>>::put(other, values),
        }
    }
}
