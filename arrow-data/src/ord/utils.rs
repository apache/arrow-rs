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

use crate::data::ArrayData;
use arrow_schema::DataType;
use std::cmp::Ordering;

#[inline]
pub(super) fn partial_ord(lhs: &ArrayData, rhs: &ArrayData) -> Option<Ordering> {
    match (lhs.data_type(), rhs.data_type()) {
        (DataType::Map(l_field, _), DataType::Map(r_field, _)) => {
            match (l_field.data_type(), r_field.data_type()) {
                (DataType::Struct(l_fields), DataType::Struct(r_fields))
                    if l_fields.len() == 2 && r_fields.len() == 2 =>
                {
                    l_fields.partial_cmp(r_fields)
                }
                _ => None,
            }
        }
        (l_data_type, r_data_type) => l_data_type.partial_cmp(r_data_type),
    }
}
