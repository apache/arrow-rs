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

//! Arrow logical types

mod datatype;
pub use datatype::*;
mod datatype_parse;
mod error;
pub use error::*;
mod field;
pub use field::*;
mod fields;
pub use fields::*;
mod schema;
pub use schema::*;
use std::ops;

#[cfg(feature = "ffi")]
pub mod ffi;

/// Options that define the sort order of a given column
#[derive(Clone, Hash, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct SortOptions {
    /// Whether to sort in descending order
    pub descending: bool,
    /// Whether to sort nulls first
    pub nulls_first: bool,
}

impl Default for SortOptions {
    fn default() -> Self {
        Self {
            descending: false,
            // default to nulls first to match spark's behavior
            nulls_first: true,
        }
    }
}

/// `!` operator is overloaded for `SortOptions` to invert boolean
/// fields of the struct.
impl ops::Not for SortOptions {
    type Output = SortOptions;

    fn not(self) -> SortOptions {
        SortOptions {
            descending: !self.descending,
            nulls_first: !self.nulls_first,
        }
    }
}

#[test]
fn test_overloaded_not_sort_options() {
    let sort_options_array = [
        SortOptions {
            descending: false,
            nulls_first: false,
        },
        SortOptions {
            descending: false,
            nulls_first: true,
        },
        SortOptions {
            descending: true,
            nulls_first: false,
        },
        SortOptions {
            descending: true,
            nulls_first: true,
        },
    ];

    assert!((!sort_options_array[0]).descending);
    assert!((!sort_options_array[0]).nulls_first);

    assert!((!sort_options_array[1]).descending);
    assert!(!(!sort_options_array[1]).nulls_first);

    assert!(!(!sort_options_array[2]).descending);
    assert!((!sort_options_array[2]).nulls_first);

    assert!(!(!sort_options_array[3]).descending);
    assert!(!(!sort_options_array[3]).nulls_first);
}
