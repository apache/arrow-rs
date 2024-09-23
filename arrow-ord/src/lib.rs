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

//! Arrow ordering kernels
//!
//! # Sort RecordBatch
//!
//! ```
//! # use std::sync::Arc;
//! # use arrow_array::*;
//! # use arrow_array::cast::AsArray;
//! # use arrow_array::types::Int32Type;
//! # use arrow_ord::sort::sort_to_indices;
//! # use arrow_select::take::take;
//! #
//! let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4]));
//! let b: ArrayRef = Arc::new(StringArray::from(vec!["b", "a", "e", "d"]));
//! let batch = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();
//!
//! // Sort by column 1
//! let indices = sort_to_indices(batch.column(1), None, None).unwrap();
//!
//! // Apply indices to batch columns
//! let columns = batch.columns().iter().map(|c| take(&*c, &indices, None).unwrap()).collect();
//! let sorted = RecordBatch::try_new(batch.schema(), columns).unwrap();
//!
//! let col1 = sorted.column(0).as_primitive::<Int32Type>();
//! assert_eq!(col1.values(), &[2, 1, 4, 3]);
//! ```
//!

#![warn(missing_docs)]
pub mod cmp;
#[doc(hidden)]
pub mod comparison;
pub mod ord;
pub mod partition;
pub mod rank;
pub mod sort;
