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

//! Leaf batches handed to the column writer.

use arrow_array::Array;

use crate::column::value_selection::ValueSelectionRef;
use crate::column::writer::{LevelDataRef, LevelValueWindow};

pub(super) const LEVEL_RUN_PROBE_SIZE: usize = 128;
pub(super) const MIN_AVERAGE_LEVEL_RUN_LENGTH: usize = 8;

/// One borrowed batch presented to the column writer.
#[derive(Clone, Copy)]
pub(crate) struct LeafBatch<'a> {
    array: &'a (dyn Array + 'static),
    def_levels: LevelDataRef<'a>,
    rep_levels: LevelDataRef<'a>,
    values: ValueSelectionRef<'a>,
}

impl<'a> LeafBatch<'a> {
    pub(crate) fn new(
        array: &'a (dyn Array + 'static),
        def_levels: LevelDataRef<'a>,
        rep_levels: LevelDataRef<'a>,
        values: ValueSelectionRef<'a>,
    ) -> Self {
        Self {
            array,
            def_levels,
            rep_levels,
            values,
        }
    }

    pub(crate) fn array(&self) -> &'a (dyn Array + 'static) {
        self.array
    }

    pub(crate) fn def_level_data(&self) -> LevelDataRef<'a> {
        self.def_levels
    }

    pub(crate) fn rep_level_data(&self) -> LevelDataRef<'a> {
        self.rep_levels
    }

    pub(crate) fn value_selection(&self) -> ValueSelectionRef<'a> {
        self.values
    }
    pub(crate) fn slice(self, window: LevelValueWindow) -> Self {
        Self {
            array: self.array,
            def_levels: self
                .def_levels
                .slice(window.levels.start, window.levels.len()),
            rep_levels: self
                .rep_levels
                .slice(window.levels.start, window.levels.len()),
            values: self.values.slice(window.values.start, window.values.len()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int32Array;
    #[test]
    fn leaf_batch_window_preserves_levels_and_selected_values() {
        let array = Int32Array::from(vec![10, 20, 30, 40]);
        let batch = LeafBatch::new(
            &array,
            LevelDataRef::Materialized(&[1, 0, 1, 1]),
            LevelDataRef::Absent,
            ValueSelectionRef::Sparse(&[3, 0, 2]),
        );
        let sliced = batch.slice(LevelValueWindow {
            levels: 1..4,
            values: 1..3,
        });
        assert_eq!(
            sliced.def_level_data().cursor().collect::<Vec<_>>(),
            [0, 1, 1]
        );
        assert_eq!(
            sliced
                .value_selection()
                .cursor()
                .map(|i| array.value(i))
                .collect::<Vec<_>>(),
            [10, 30]
        );
        assert!(std::ptr::eq(batch.array(), sliced.array()));
    }
}
