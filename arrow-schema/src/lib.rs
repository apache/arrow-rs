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

#![warn(missing_docs)]
//! Arrow logical types

mod datatype;

pub use datatype::*;
use std::fmt::Display;
mod datatype_parse;
mod error;
pub use error::*;
pub mod extension;
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
///
/// The default sorts equivalently to of `ASC NULLS FIRST` in SQL (i.e.
/// ascending order with nulls sorting before any other values).
///
/// # Example creation
/// ```
/// # use arrow_schema::SortOptions;
/// // configure using explicit initialization
/// let options = SortOptions {
///   descending: false,
///   nulls_first: true,
/// };
/// // Default is ASC NULLs First
/// assert_eq!(options, SortOptions::default());
/// assert_eq!(options.to_string(), "ASC NULLS FIRST");
///
/// // Configure using builder APIs
/// let options = SortOptions::default()
///  .desc()
///  .nulls_first();
/// assert_eq!(options.to_string(), "DESC NULLS FIRST");
///
/// // configure using explicit field values
/// let options = SortOptions::default()
///  .with_descending(false)
///  .with_nulls_first(false);
/// assert_eq!(options.to_string(), "ASC NULLS LAST");
/// ```
///
/// # Example operations
/// It is also possible to negate the sort options using the `!` operator.
/// ```
/// use arrow_schema::SortOptions;
/// let options = !SortOptions::default();
/// assert_eq!(options.to_string(), "DESC NULLS LAST");
/// ```
#[derive(Clone, Hash, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct SortOptions {
    /// Whether to sort in descending order
    pub descending: bool,
    /// Whether to sort nulls first
    pub nulls_first: bool,
}

impl Display for SortOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.descending {
            write!(f, "DESC")?;
        } else {
            write!(f, "ASC")?;
        }
        if self.nulls_first {
            write!(f, " NULLS FIRST")?;
        } else {
            write!(f, " NULLS LAST")?;
        }
        Ok(())
    }
}

impl SortOptions {
    /// Create a new `SortOptions` struct
    pub fn new(descending: bool, nulls_first: bool) -> Self {
        Self {
            descending,
            nulls_first,
        }
    }

    /// Set this sort options to sort in descending order
    ///
    /// See [Self::with_descending] to explicitly set the underlying field
    pub fn desc(mut self) -> Self {
        self.descending = true;
        self
    }

    /// Set this sort options to sort in ascending order
    ///
    /// See [Self::with_descending] to explicitly set the underlying field
    pub fn asc(mut self) -> Self {
        self.descending = false;
        self
    }

    /// Set this sort options to sort nulls first
    ///
    /// See [Self::with_nulls_first] to explicitly set the underlying field
    pub fn nulls_first(mut self) -> Self {
        self.nulls_first = true;
        self
    }

    /// Set this sort options to sort nulls last
    ///
    /// See [Self::with_nulls_first] to explicitly set the underlying field
    pub fn nulls_last(mut self) -> Self {
        self.nulls_first = false;
        self
    }

    /// Set this sort options to sort descending if argument is true
    pub fn with_descending(mut self, descending: bool) -> Self {
        self.descending = descending;
        self
    }

    /// Set this sort options to sort nulls first if argument is true
    pub fn with_nulls_first(mut self, nulls_first: bool) -> Self {
        self.nulls_first = nulls_first;
        self
    }
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
