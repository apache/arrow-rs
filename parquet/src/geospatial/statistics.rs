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
use crate::errors::Result;

// ----------------------------------------------------------------------
// Bounding Box

/// Represents a 2D/3D bounding box with optional M-coordinate support.
/// 
/// A bounding box defines the spatial extent of geospatial data by specifying
/// minimum and maximum coordinates along each axis. This struct supports:
/// - 2D coordinates (x, y) which are always required
/// - Optional 3D coordinates (z) for elevation/height data
/// - Optional M-coordinates for measured values (e.g., distance along a route)
/// 
/// # Examples
/// 
/// ```
/// use parquet::geospatial::statistics::BoundingBox;
/// 
/// // 2D bounding box
/// let bbox_2d = BoundingBox::new(0.0, 0.0, 100.0, 100.0, None, None, None, None);
/// 
/// // 3D bounding box with elevation
/// let bbox_3d = BoundingBox::new(0.0, 0.0, 100.0, 100.0, Some(0.0), Some(1000.0), None, None);
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
    /// 
    /// # Arguments
    /// 
    /// * `xmin` - Minimum X coordinate
    /// * `ymin` - Minimum Y coordinate  
    /// * `xmax` - Maximum X coordinate
    /// * `ymax` - Maximum Y coordinate
    /// * `zmin` - Optional minimum Z coordinate
    /// * `zmax` - Optional maximum Z coordinate
    /// * `mmin` - Optional minimum M coordinate
    /// * `mmax` - Optional maximum M coordinate
    /// 
    /// # Returns
    /// 
    /// A new `BoundingBox` instance with the specified coordinates.
    pub fn new(xmin: f64, ymin: f64, xmax: f64, ymax: f64, zmin: Option<f64>, zmax: Option<f64>, mmin: Option<f64>, mmax: Option<f64>) -> Self {
        Self { xmin, ymin, xmax, ymax, zmin, zmax, mmin, mmax }
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
/// // Empty statistics
/// let empty_stats = GeospatialStatistics::new_empty();
/// 
/// // Statistics with bounding box
/// let bbox = BoundingBox::new(0.0, 0.0, 100.0, 100.0, None, None, None, None);
/// let stats = GeospatialStatistics::new(Some(bbox), Some(vec![1, 2, 3]));
/// ```
#[derive(Clone, Debug, PartialEq)]
pub struct GeospatialStatistics {
    /// Optional bounding box encompassing all geospatial data
    bbox: Option<BoundingBox>,
    /// Optional list of geospatial geometry type identifiers
    /// 
    /// Common values include:
    /// - 1: Point
    /// - 2: LineString  
    /// - 3: Polygon
    /// - 4: MultiPoint
    /// - 5: MultiLineString
    /// - 6: MultiPolygon
    /// - 7: GeometryCollection
    geospatial_types: Option<Vec<i32>>,
}

impl GeospatialStatistics {
    /// Creates a new empty geospatial statistics instance.
    /// 
    /// This is useful when no spatial information is available or when
    /// creating a placeholder for statistics that will be populated later.
    /// 
    /// # Returns
    /// 
    /// A `GeospatialStatistics` instance with no bounding box or type information.
    pub fn new_empty() -> Self {
        Self { bbox: None, geospatial_types: None }
    }

    /// Creates a new geospatial statistics instance with the specified data.
    /// 
    /// # Arguments
    /// 
    /// * `bbox` - Optional bounding box defining the spatial extent
    /// * `geospatial_types` - Optional list of geometry type identifiers
    /// 
    /// # Returns
    /// 
    /// A new `GeospatialStatistics` instance with the specified data.
    pub fn new(bbox: Option<BoundingBox>, geospatial_types: Option<Vec<i32>>) -> Self {
        Self { bbox, geospatial_types }
    }
}

/// Converts a Thrift-generated geospatial statistics object to our internal representation.
/// 
/// This function handles the conversion from the auto-generated Thrift types to our
/// more ergonomic Rust structs. It safely handles optional fields and type conversions.
/// 
/// # Arguments
/// 
/// * `geo_statistics` - Optional Thrift-generated geospatial statistics
/// 
/// # Returns
/// 
/// A `Result` containing either:
/// * `Ok(Some(GeospatialStatistics))` - Successfully converted statistics
/// * `Ok(None)` - No statistics provided
/// * `Err(...)` - Error during conversion
/// 
/// # Errors
/// 
/// This function may return an error if there are issues with the input data
/// or type conversions.
pub fn from_thrift(geo_statistics: Option<TGeospatialStatistics>) -> Result<Option<GeospatialStatistics>> {
    Ok(match geo_statistics {
        Some(geo_stats) => {
            let bbox = if let Some(bbox) = geo_stats.bbox {
                Some(BoundingBox::new(
                    bbox.xmin.into(), 
                    bbox.ymin.into(), 
                    bbox.xmax.into(), 
                    bbox.ymax.into(), 
                if let Some(zmin) = bbox.zmin { Some(zmin.into()) } else { None },
                if let Some(zmax) = bbox.zmax { Some(zmax.into()) } else { None },
                if let Some(mmin) = bbox.mmin { Some(mmin.into()) } else { None },
                if let Some(mmax) = bbox.mmax { Some(mmax.into()) } else { None }))
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
    /// 
    /// This implementation allows seamless conversion between our ergonomic
    /// Rust structs and the auto-generated Thrift types used for serialization.
    /// 
    /// # Arguments
    /// 
    /// * `b` - The internal `BoundingBox` to convert
    /// 
    /// # Returns
    /// 
    /// A Thrift-generated `BoundingBox` with the same coordinate values.
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
/// 
/// This function is the inverse of `from_thrift` and is used when serializing
/// geospatial statistics to Parquet format. It handles the conversion from our
/// ergonomic Rust structs back to the auto-generated Thrift types.
/// 
/// # Arguments
/// 
/// * `geo_statistics` - Optional reference to our internal geospatial statistics
/// 
/// # Returns
/// 
/// An `Option<TGeospatialStatistics>` containing:
/// * `Some(...)` - Successfully converted Thrift statistics
/// * `None` - No statistics provided
/// 
/// # Examples
/// 
/// ```
/// use parquet::geospatial::statistics::{GeospatialStatistics, to_thrift};
/// 
/// let stats = GeospatialStatistics::new_empty();
/// let thrift_stats = to_thrift(Some(&stats));
/// ```
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
        assert_eq!(from_thrift(Some(TGeospatialStatistics::new(None, None))).unwrap(), Some(GeospatialStatistics::new_empty()));
    }

    /// Tests the conversion from Thrift format with actual geospatial data.
    #[test]
    fn test_geo_statistics_from_thrift() {
        let stats = GeospatialStatistics::new(Some(BoundingBox::new(0.0, 0.0, 100.0, 100.0, None, None, None, None)), Some(vec![1, 2, 3]));
        let thrift_stats = to_thrift(Some(&stats));
        assert_eq!(from_thrift(thrift_stats).unwrap(), Some(stats));
    }

    #[test]
    fn test_bounding_box() {
        let bbox = BoundingBox::new(0.0, 0.0, 100.0, 100.0, None, None, None, None);
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
