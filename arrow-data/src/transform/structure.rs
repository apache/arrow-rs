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
            // Collect field names before the mutable borrow of child_data.
            let field_names = struct_field_names(&mutable.data_type);
            for (col_idx, child) in mutable.child_data.iter_mut().enumerate() {
                child
                    .try_extend(index, start, start + len)
                    .map_err(|e| wrap_column_error(e, col_idx, &field_names))?;
            }
            Ok(())
        },
    )
}

pub(super) fn extend_nulls(mutable: &mut _MutableArrayData, len: usize) -> Result<(), ArrowError> {
    let field_names = struct_field_names(&mutable.data_type);
    for (col_idx, child) in mutable.child_data.iter_mut().enumerate() {
        child
            .try_extend_nulls(len)
            .map_err(|e| wrap_column_error(e, col_idx, &field_names))?;
    }
    Ok(())
}

fn struct_field_names(data_type: &DataType) -> Vec<String> {
    if let DataType::Struct(fields) = data_type {
        fields.iter().map(|f| f.name().to_string()).collect()
    } else {
        vec![]
    }
}

fn wrap_column_error(e: ArrowError, col_idx: usize, field_names: &[String]) -> ArrowError {
    let name_ctx = field_names
        .get(col_idx)
        .map(|n| format!(" (\"{n}\")"))
        .unwrap_or_default();
    ArrowError::InvalidArgumentError(format!("struct column {col_idx}{name_ctx} failed: {e}"))
}
