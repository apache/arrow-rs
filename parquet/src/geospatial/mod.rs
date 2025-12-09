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

//! This module provides functionality for working with geospatial data in Parquet file as defined in the [spec][parquet-geo-spec].
//!
//! * [`GeospatialStatistics`]: describes the geospatial statistics for a Parquet column.
//! * [`BoundingBox`]: describes the bounding box values for a geospatial column.
//!
//! [`GeospatialStatistics`] describes the geospatial statistics for a Parquet column.
//! * bbox: the [`BoundingBox`] for the geospatial data
//! * geospatial_types: the geospatial types for the geospatial data as specified in [specification][geo-types].
//!
//! Geospatial bounding box describes the spatial extent of the geospatial data within a Parquet row group.
//! * xmin, xmax: the minimum and maximum longitude values
//! * ymin, ymax: the minimum and maximum latitude values
//! * zmin, zmax: (optional) the minimum and maximum elevation values
//! * mmin, mmax: (optional) the minimum and maximum linear reference values
//!
//! In 2D representation, where x are points:
//! ```text
//!  ymax +-----------------------+
//!       |               x       |
//!       |      x                |
//!       |              x        |
//!       |      x                |
//!  ymin +-----------------------+
//!       xmin                    xmax
//! ```
//!
//! [`GeospatialStatistics`]: crate::geospatial::statistics::GeospatialStatistics
//! [`BoundingBox`]: crate::geospatial::bounding_box::BoundingBox
//! [parquet-geo-spec]: https://github.com/apache/parquet-format/blob/master/Geospatial.md
//! [geo-types]: https://github.com/apache/parquet-format/blob/master/Geospatial.md#geospatial-types

pub mod accumulator;
pub mod bounding_box;
pub mod statistics;
