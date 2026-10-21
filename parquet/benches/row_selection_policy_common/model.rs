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

pub(crate) const ROWS_PER_GROUP: usize = 65_536;
pub(crate) const BATCH_SIZE: usize = 8_192;
pub(crate) const PAYLOAD_COLUMNS: usize = 8;
pub(crate) const PAYLOAD_VALUE_MODULUS: usize = 1_000_003;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SelectionRun {
    pub(crate) selected: bool,
    pub(crate) len: usize,
}

impl SelectionRun {
    pub(crate) const fn select(len: usize) -> Self {
        Self {
            selected: true,
            len,
        }
    }

    pub(crate) const fn skip(len: usize) -> Self {
        Self {
            selected: false,
            len,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RowGroupPattern {
    Cycle(&'static [SelectionRun]),
    AllSelected,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct CaseSpec {
    pub(crate) name: &'static str,
    pub(crate) row_groups: &'static [RowGroupPattern],
}

impl CaseSpec {
    pub(crate) const fn total_rows(self) -> usize {
        self.row_groups.len() * ROWS_PER_GROUP
    }
}
