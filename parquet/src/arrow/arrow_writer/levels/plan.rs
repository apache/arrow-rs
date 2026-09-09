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
use crate::column::writer::LevelDataRef;

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
}
