// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::data::ArrayData;

#[inline]
pub(super) fn null_equal(
    _lhs: &ArrayData,
    _rhs: &ArrayData,
    _lhs_start: usize,
    _rhs_start: usize,
    _len: usize,
) -> bool {
    // a null buffer's range is always true, as every element is by definition equal (to null).
    // We only need to compare data_types
    true
}
