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

use super::{_MutableArrayData, Extend};
use crate::ArrayData;
use arrow_schema::{ArrowError, DataType};

pub(super) fn build_extend(_: &ArrayData) -> Extend<'_> {
    Box::new(
        move |mutable: &mut _MutableArrayData, index: usize, start: usize, len: usize| {
            for (col_idx, child) in mutable.child_data.iter_mut().enumerate() {
                child
                    .try_extend(index, start, start + len)
                    .map_err(|e| wrap_column_error(e, col_idx, &mutable.data_type))?
            }
            Ok(())
        },
    )
}

pub(super) fn extend_nulls(mutable: &mut _MutableArrayData, len: usize) -> Result<(), ArrowError> {
    for (col_idx, child) in mutable.child_data.iter_mut().enumerate() {
        child
            .try_extend_nulls(len)
            .map_err(|e| wrap_column_error(e, col_idx, &mutable.data_type))?;
    }
    Ok(())
}

fn wrap_column_error(e: ArrowError, col_idx: usize, data_type: &DataType) -> ArrowError {
    let name_ctx = if let DataType::Struct(fields) = data_type {
        fields
            .get(col_idx)
            .map(|f| format!(" (\"{}\")", f.name()))
            .unwrap_or_default()
    } else {
        String::new()
    };
    ArrowError::InvalidArgumentError(format!("struct column {col_idx}{name_ctx} failed: {e}"))
}
