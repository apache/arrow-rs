// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
//!
//! # Platform Support
//!
//! Only little-endian platforms are officially supported and tested in CI.
//! Big-endian platforms are not tested in CI and may not work correctly.
//! Fixes for big-endian platforms are welcome and handled on a best-effort basis,
//! but compatibility is not guaranteed.

#![doc(
    html_logo_url = "https://arrow.apache.org/img/arrow-logo_chevrons_black-txt_white-bg.svg",
    html_favicon_url = "https://arrow.apache.org/img/arrow-logo_chevrons_black-txt_transparent-bg.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![warn(missing_docs)]
pub mod cmp;
#[doc(hidden)]
pub mod comparison;
pub mod ord;
pub mod partition;
pub mod rank;
pub mod sort;
