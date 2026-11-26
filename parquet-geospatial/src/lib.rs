// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Implementation of [Geometry and Geography Encoding] from [Apache Parquet].
//!
//! [Geometry and Geography Encoding]: https://github.com/apache/parquet-format/blob/master/Geospatial.md
//! [Apache Parquet]: https://parquet.apache.org/
//!
//! # Platform Support
//!
//! Only little-endian platforms are officially supported and tested in CI.
//! Big-endian platforms are not tested in CI and may not work correctly.
//! Fixes for big-endian platforms are welcome and handled on a best-effort basis,
//! but compatibility is not guaranteed.

pub mod bounding;
pub mod interval;
pub mod testing;

mod types;

pub use types::Edges as WkbEdges;
pub use types::Hint as WkbTypeHint;
pub use types::Metadata as WkbMetadata;
pub use types::WkbType;
