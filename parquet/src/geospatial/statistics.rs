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

//! Geospatial statistics for Parquet files.
//!
//! This module provides functionality for working with geospatial statistics in Parquet files.
//! It includes support for bounding boxes and geospatial statistics in column chunk metadata.

use crate::format as parquet;
use crate::format::GeospatialStatistics as TGeospatialStatistics;
use crate::errors::{ParquetError, Result};

// ----------------------------------------------------------------------
// Bounding Box

/// A geospatial instance has at least two coordinate dimensions: X and Y for 2D coordinates of each point.
/// X represents longitude/easting and Y represents latitude/northing. A geospatial instance can optionally
/// have Z and/or M values associated with each point.
///
/// The Z values introduce the third dimension coordinate, typically used to indicate height or elevation.
///
/// M values allow tracking a value in a fourth dimension. These can represent:
/// - Linear reference values (e.g., highway milepost)
/// - Timestamps
/// - Other values defined by the CRS
///
/// The bounding box is defined as min/max value pairs of coordinates from each axis. X and Y values are
/// always present, while Z and M are omitted for 2D geospatial instances.
///
/// When calculating a bounding box:
/// - Null or NaN values in a coordinate dimension are skipped
/// - If a dimension has only null/NaN values, that dimension is omitted
/// - If either X or Y dimension is missing, no bounding box is produced
/// - Example: POINT (1 NaN) contributes to X but not to Y, Z, or M dimensions
///
/// Special cases:
/// - For X values only, xmin may exceed xmax. In this case, a point matches if x >= xmin OR x <= xmax
/// - This wraparound occurs when the bounding box crosses the antimeridian line
/// - In geographic terms: xmin=westernmost, xmax=easternmost, ymin=southernmost, ymax=northernmost
///
/// For GEOGRAPHY types:
/// - X values must be within [-180, 180] (longitude)
/// - Y values must be within [-90, 90] (latitude)
/// 
/// Derived from the parquet format spec: https://github.com/apache/parquet-format/blob/ae39061f28d7c508a97af58a3c0a567352c8ea41/Geospatial.md#bounding-box
/// 
/// # Examples
/// 
/// ```
/// use parquet::geospatial::statistics::BoundingBox;
/// 
/// // 2D bounding box
/// let bbox_2d = BoundingBox::new(0.0, 0.0, 100.0, 100.0);
/// 
/// // 3D bounding box with elevation
/// let bbox_3d = BoundingBox::new(0.0, 0.0, 100.0, 100.0).with_zrange(0.0, 1000.0);
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct BoundingBox {
    /// Minimum X coordinate (longitude or easting)
    xmin: f64,
    /// Minimum Y coordinate (latitude or northing)
    ymin: f64,
    /// Maximum X coordinate (longitude or easting)
    xmax: f64,
    /// Maximum Y coordinate (latitude or northing)
    ymax: f64,
    /// Minimum Z coordinate (elevation/height), if present
    zmin: Option<f64>,
    /// Maximum Z coordinate (elevation/height), if present
    zmax: Option<f64>,
    /// Minimum M coordinate (measured value), if present
    mmin: Option<f64>,
    /// Maximum M coordinate (measured value), if present
    mmax: Option<f64>,
}

impl BoundingBox {
    /// Creates a new bounding box with the specified coordinates.
    pub fn new(xmin: f64, ymin: f64, xmax: f64, ymax: f64) -> Self {
        Self { xmin, ymin, xmax, ymax, zmin: None, zmax: None, mmin: None, mmax: None }
    }

    /// Creates a new bounding box with the specified Z-coordinate range.
    pub fn with_zrange(mut self, zmin: f64, zmax: f64) -> Self {
        self.zmin = Some(zmin);
        self.zmax = Some(zmax);
        self
    }

    /// Creates a new bounding box with the specified M-coordinate range.
    pub fn with_mrange(mut self, mmin: f64, mmax: f64) -> Self {
        self.mmin = Some(mmin);
        self.mmax = Some(mmax);
        self
    }

    /// Returns the minimum x-coordinate.
    pub fn get_xmin(self) -> f64 {
        self.xmin
    }

    /// Returns the maximum x-coordinate.
    pub fn get_xmax(self) -> f64 {
        self.xmax
    }

    /// Returns the minimum y-coordinate.
    pub fn get_ymin(self) -> f64 {
        self.ymin
    }

    /// Returns the maximum y-coordinate.
    pub fn get_ymax(self) -> f64 {
        self.ymax
    }

    /// Returns the minimum z-coordinate, if present.
    pub fn get_zmin(self) -> Option<f64> {
        self.zmin
    }

    /// Returns the maximum z-coordinate, if present.
    pub fn get_zmax(self) -> Option<f64> {
        self.zmax
    }

    /// Returns the minimum m-value (measure), if present.
    pub fn get_mmin(self) -> Option<f64> {
        self.mmin
    }

    /// Returns the maximum m-value (measure), if present.
    pub fn get_mmax(self) -> Option<f64> {
        self.mmax
    }

    /// Returns `true` if both zmin and zmax are present.
    pub fn is_z_valid(self) -> bool {
        self.zmin.is_some() && self.zmax.is_some()
    }

    /// Returns `true` if both mmin and mmax are present.
    pub fn is_m_valid(self) -> bool {
        self.mmin.is_some() && self.mmax.is_some()
    }
}

// ----------------------------------------------------------------------
// Geospatial Statistics

/// Represents geospatial statistics for a Parquet column or dataset.
/// 
/// This struct contains metadata about the spatial characteristics of geospatial data,
/// including bounding box information and the types of geospatial geometries present.
/// It's used to optimize spatial queries and provide spatial context for data analysis.
/// 
/// # Examples
/// 
/// ```
/// use parquet::geospatial::statistics::{GeospatialStatistics, BoundingBox};
/// 
/// // Statistics with bounding box
/// let bbox = BoundingBox::new(0.0, 0.0, 100.0, 100.0);
/// let stats = GeospatialStatistics::new(Some(bbox), Some(vec![1, 2, 3]));
/// ```
#[derive(Clone, Debug, PartialEq, Default)]
pub struct GeospatialStatistics {
    /// Optional bounding box encompassing all geospatial data
    bbox: Option<BoundingBox>,
    /// Optional list of geospatial geometry type identifiers
    geospatial_types: Option<Vec<i32>>,
}

impl GeospatialStatistics {
    /// Creates a new geospatial statistics instance with the specified data.
    pub fn new(bbox: Option<BoundingBox>, geospatial_types: Option<Vec<i32>>) -> Self {
        Self { bbox, geospatial_types }
    }
}

/// Converts a Thrift-generated geospatial statistics object to the internal representation.
pub fn from_thrift(geo_statistics: Option<TGeospatialStatistics>) -> Result<Option<GeospatialStatistics>> {
    Ok(match geo_statistics {
        Some(geo_stats) => {
            let bbox = if let Some(bbox) = geo_stats.bbox {
                let mut new_bbox = BoundingBox::new(
                    bbox.xmin.into(),
                    bbox.ymin.into(),
                    bbox.xmax.into(),
                    bbox.ymax.into(),
                );

                new_bbox = match (bbox.zmin, bbox.zmax) {
                    (Some(zmin), Some(zmax)) => new_bbox.with_zrange(zmin.into(), zmax.into()),
                    (None, None) => new_bbox,
                    _ => return Err(ParquetError::General("Z-coordinate values mismatch".to_string())),
                };

                new_bbox = match (bbox.mmin, bbox.mmax) {
                    (Some(mmin), Some(mmax)) => new_bbox.with_mrange(mmin.into(), mmax.into()),
                    (None, None) => new_bbox,
                    _ => return Err(ParquetError::General("M-coordinate values mismatch".to_string())),
                };

                Some(new_bbox)
            } else {
                None    
            };
            let geospatial_types = geo_stats.geospatial_types;
            Some(GeospatialStatistics::new(bbox, geospatial_types))
        }
        None => None,
    })
}

impl From<BoundingBox> for parquet::BoundingBox {
    /// Converts our internal `BoundingBox` to the Thrift-generated format.
    fn from(b: BoundingBox) -> parquet::BoundingBox {
        parquet::BoundingBox {
            xmin: b.xmin.into(),
            ymin: b.ymin.into(),
            xmax: b.xmax.into(),
            ymax: b.ymax.into(),
            zmin: b.zmin.map(|z| z.into()),
            zmax: b.zmax.map(|z| z.into()),
            mmin: b.mmin.map(|m| m.into()),
            mmax: b.mmax.map(|m| m.into()),
        }
    }
}

/// Converts our internal geospatial statistics to the Thrift-generated format.
pub fn to_thrift(geo_statistics: Option<&GeospatialStatistics>) -> Option<TGeospatialStatistics> {
    let geo_stats = geo_statistics?;
    let bbox = geo_stats.bbox.clone().map(|bbox| bbox.into());
    let geospatial_types = geo_stats.geospatial_types.clone();
    Some(TGeospatialStatistics::new(bbox, geospatial_types))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Tests the conversion from Thrift format when no statistics are provided.
    #[test]
    fn test_from_thrift() {
        assert_eq!(from_thrift(None).unwrap(), None);
        assert_eq!(from_thrift(Some(TGeospatialStatistics::new(None, None))).unwrap(), Some(GeospatialStatistics::default()));
    }

    /// Tests the conversion from Thrift format with actual geospatial data.
    #[test]
    fn test_geo_statistics_from_thrift() {
        let stats = GeospatialStatistics::new(Some(BoundingBox::new(0.0, 0.0, 100.0, 100.0)), Some(vec![1, 2, 3]));
        let thrift_stats = to_thrift(Some(&stats));
        assert_eq!(from_thrift(thrift_stats).unwrap(), Some(stats));
    }

    #[test]
    fn test_bounding_box() {
        let bbox = BoundingBox::new(0.0, 0.0, 100.0, 100.0);
        let thrift_bbox: parquet::BoundingBox = bbox.into();
        assert_eq!(thrift_bbox.xmin, 0.0f64);
        assert_eq!(thrift_bbox.ymin, 0.0f64);
        assert_eq!(thrift_bbox.xmax, 100.0f64);
        assert_eq!(thrift_bbox.ymax, 100.0f64);
        assert_eq!(thrift_bbox.zmin, None);
        assert_eq!(thrift_bbox.zmax, None);
        assert_eq!(thrift_bbox.mmin, None);
        assert_eq!(thrift_bbox.mmax, None);
    }
}
