// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
