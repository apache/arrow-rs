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
