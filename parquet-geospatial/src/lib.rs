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

//! Implementation of [Geometry and Geography Encoding] from [Apache Parquet].
//!
//! [Geometry and Geography Encoding]: https://github.com/apache/parquet-format/blob/master/Geospatial.md
//! [Apache Parquet]: https://parquet.apache.org/
//!
//! ## 🚧 Work In Progress
//!
//! This crate is under active development and is not yet ready for production use.
//! If you are interested in helping, you can find more information on the GitHub [Geometry issue]
//!
//! [Geometry issue]: https://github.com/apache/arrow-rs/issues/8373

pub mod bounding;
pub mod interval;
pub mod testing;

mod types;

pub use types::Edges as WkbEdges;
pub use types::Hint as WkbTypeHint;
pub use types::Metadata as WkbMetadata;
pub use types::WkbType;
