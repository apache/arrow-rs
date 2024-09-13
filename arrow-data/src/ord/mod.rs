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

//! Module containing functionality to compute array order.
//! This module uses [ArrayData] and does not
//! depend on dynamic casting of `Array`.

use crate::ArrayData;
use std::cmp::Ordering;

mod utils;

/// Orders two [ArrayData].
///
/// If the [ArrayData]'s [DataType] fields are both [DataType::Map], orders by the [FieldRef]'s
/// partial_cmp.
///
/// Otherwise, ordering is determined by the partial_cmp result of the [DataType] fields.
pub fn ord(lhs: &ArrayData, rhs: &ArrayData) -> Option<Ordering> {
    utils::partial_ord(lhs, rhs)
}
