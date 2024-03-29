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

use crate::ArrayData;
use arrow_schema::DataType;

use super::{Extend, _MutableArrayData};

pub(super) fn build_extend(array: &ArrayData) -> Extend {
    let size = match array.data_type() {
        DataType::FixedSizeList(_, i) => *i as usize,
        _ => unreachable!(),
    };

    Box::new(
        move |mutable: &mut _MutableArrayData, index: usize, start: usize, len: usize| {
            mutable
                .child_data
                .iter_mut()
                .for_each(|child| child.extend(index, start * size, (start + len) * size))
        },
    )
}

pub(super) fn extend_nulls(mutable: &mut _MutableArrayData, len: usize) {
    let size = match mutable.data_type {
        DataType::FixedSizeList(_, i) => i as usize,
        _ => unreachable!(),
    };

    mutable
        .child_data
        .iter_mut()
        .for_each(|child| child.extend_nulls(len * size))
}
