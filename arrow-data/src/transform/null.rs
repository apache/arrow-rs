// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use super::{_MutableArrayData, Extend};
use crate::ArrayData;

pub(super) fn build_extend(_: &ArrayData) -> Extend<'_> {
    Box::new(move |_, _, _, _| Ok(()))
}

pub(super) fn extend_nulls(
    _: &mut _MutableArrayData,
    _: usize,
) -> Result<(), arrow_schema::ArrowError> {
    Ok(())
}
