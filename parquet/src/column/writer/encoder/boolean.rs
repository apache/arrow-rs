// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Boolean native-batch encoding.

use super::*;

/// The column-chunk encoder consumes borrowed Boolean batches.
/// `push_batch` derives statistics and bloom state from the selection's
/// true count before passing it to the active encoding family.
impl<'source, D: DataType<T = bool>> BatchSink<BoolBatch<'source>> for TypedColumnChunkEncoder<D> {
    #[inline(never)]
    fn push_batch(&mut self, values: BoolBatch<'source>) -> Result<()> {
        // INTERVAL has undefined sort order, so it must not emit min/max statistics.
        let should_update_stats = self.statistics_enabled != EnabledStatistics::None
            && self.descr.converted_type() != ConvertedType::INTERVAL;
        let len = values.len();
        self.num_values += len;
        if len != 0 && (should_update_stats || self.bloom_filter.is_some()) {
            let true_count = values.true_count();
            if should_update_stats {
                let min = true_count == len;
                let max = true_count > 0;
                update_min(&self.descr, &min, &mut self.min_value);
                update_max(&self.descr, &max, &mut self.max_value);
            }
            if let Some(bloom) = self.bloom_filter.as_mut() {
                if true_count < len {
                    bloom.insert(&false);
                }
                if true_count > 0 {
                    bloom.insert(&true);
                }
            }
        }
        self.encoding_family.put_bool_batch(values)
    }
}
