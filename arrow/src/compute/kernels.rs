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

//! Computation kernels on Arrow Arrays

pub use arrow_arith::{aggregate, arithmetic, arity, bitwise, boolean, numeric, temporal};
pub use arrow_cast::cast;
pub use arrow_cast::parse as cast_utils;
pub use arrow_ord::{cmp, partition, rank, sort};
pub use arrow_select::{concat, filter, interleave, nullif, take, union_extract, window, zip};
pub use arrow_string::{concat_elements, length, regexp, substring};

/// Comparison kernels for `Array`s.
pub mod comparison {
    pub use arrow_ord::comparison::*;
    pub use arrow_string::like::*;
    // continue to export deprecated methods until they are removed
    pub use arrow_string::regexp::{regexp_is_match, regexp_is_match_scalar};
    #[allow(deprecated)]
    pub use arrow_string::regexp::{regexp_is_match_utf8, regexp_is_match_utf8_scalar};
}
