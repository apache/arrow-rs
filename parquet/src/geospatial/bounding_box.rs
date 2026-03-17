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

//! Bounding box for GEOMETRY or GEOGRAPHY type in the representation of min/max
//! value pair of coordinates from each axis.
//!
//! Derived from the parquet format spec: <https://github.com/apache/parquet-format/blob/master/Geospatial.md>
//!
//!

use crate::file::metadata::HeapSize;

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
/// - This wraparound can occur when the bounding box crosses the antimeridian line.
/// - In geographic terms: xmin=westernmost, xmax=easternmost, ymin=southernmost, ymax=northernmost
///
/// For GEOGRAPHY types:
/// - X values must be within [-180, 180] (longitude)
/// - Y values must be within [-90, 90] (latitude)
///
/// Derived from the parquet format [spec][bounding-box-spec]
///
/// # Examples
///
/// ```
/// use parquet::geospatial::bounding_box::BoundingBox;
///
/// // 2D bounding box
/// let bbox_2d = BoundingBox::new(0.0, 0.0, 100.0, 100.0);
///
/// // 3D bounding box with elevation
/// let bbox_3d = BoundingBox::new(0.0, 0.0, 100.0, 100.0)
///     .with_zrange(0.0, 1000.0);
///
/// // 3D bounding box with elevation and measured value
/// let bbox_3d_m = BoundingBox::new(0.0, 0.0, 100.0, 100.0)
///     .with_zrange(0.0, 1000.0)
///     .with_mrange(0.0, 1000.0);
/// ```
///
/// [bounding-box-spec]: https://github.com/apache/parquet-format/blob/master/Geospatial.md#bounding-box
#[derive(Clone, Debug, PartialEq)]
pub struct BoundingBox {
    /// X coordinates (longitude or easting): (min, max)
    x_range: (f64, f64),
    /// Y coordinates (latitude or northing): (min, max)
    y_range: (f64, f64),
    /// Z coordinates (elevation/height): (min, max), if present
    z_range: Option<(f64, f64)>,
    /// M coordinates (measured value): (min, max), if present
    m_range: Option<(f64, f64)>,
}

impl BoundingBox {
    /// Creates a new bounding box with the specified coordinates.
    pub fn new(xmin: f64, xmax: f64, ymin: f64, ymax: f64) -> Self {
        Self {
            x_range: (xmin, xmax),
            y_range: (ymin, ymax),
            z_range: None,
            m_range: None,
        }
    }

    /// Updates the bounding box with specified X-coordinate range.
    pub fn with_xrange(mut self, xmin: f64, xmax: f64) -> Self {
        self.x_range = (xmin, xmax);
        self
    }

    /// Updates the bounding box with specified Y-coordinate range.
    pub fn with_yrange(mut self, ymin: f64, ymax: f64) -> Self {
        self.y_range = (ymin, ymax);
        self
    }

    /// Creates a new bounding box with the specified Z-coordinate range.
    pub fn with_zrange(mut self, zmin: f64, zmax: f64) -> Self {
        self.z_range = Some((zmin, zmax));
        self
    }

    /// Creates a new bounding box with the specified M-coordinate range.
    pub fn with_mrange(mut self, mmin: f64, mmax: f64) -> Self {
        self.m_range = Some((mmin, mmax));
        self
    }

    /// Returns the minimum x-coordinate.
    pub fn get_xmin(&self) -> f64 {
        self.x_range.0
    }

    /// Returns the maximum x-coordinate.
    pub fn get_xmax(&self) -> f64 {
        self.x_range.1
    }

    /// Returns the minimum y-coordinate.
    pub fn get_ymin(&self) -> f64 {
        self.y_range.0
    }

    /// Returns the maximum y-coordinate.
    pub fn get_ymax(&self) -> f64 {
        self.y_range.1
    }

    /// Returns the minimum z-coordinate, if present.
    pub fn get_zmin(&self) -> Option<f64> {
        self.z_range.map(|z| z.0)
    }

    /// Returns the maximum z-coordinate, if present.
    pub fn get_zmax(&self) -> Option<f64> {
        self.z_range.map(|z| z.1)
    }

    /// Returns the minimum m-value (measure), if present.
    pub fn get_mmin(&self) -> Option<f64> {
        self.m_range.map(|m| m.0)
    }

    /// Returns the maximum m-value (measure), if present.
    pub fn get_mmax(&self) -> Option<f64> {
        self.m_range.map(|m| m.1)
    }

    /// Returns `true` if both zmin and zmax are present.
    pub fn is_z_valid(&self) -> bool {
        self.z_range.is_some()
    }

    /// Returns `true` if both mmin and mmax are present.
    pub fn is_m_valid(&self) -> bool {
        self.m_range.is_some()
    }
}

impl HeapSize for BoundingBox {
    fn heap_size(&self) -> usize {
        0 // no heap allocations
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bounding_box() {
        let bbox = BoundingBox::new(0.0, 0.0, 10.0, 10.0);
        assert_eq!(bbox.get_xmin(), 0.0);
        assert_eq!(bbox.get_xmax(), 0.0);
        assert_eq!(bbox.get_ymin(), 10.0);
        assert_eq!(bbox.get_ymax(), 10.0);
        assert_eq!(bbox.get_zmin(), None);
        assert_eq!(bbox.get_zmax(), None);
        assert_eq!(bbox.get_mmin(), None);
        assert_eq!(bbox.get_mmax(), None);
        assert!(!bbox.is_z_valid());
        assert!(!bbox.is_m_valid());

        // test with zrange
        let bbox_z = BoundingBox::new(0.0, 0.0, 10.0, 10.0).with_zrange(5.0, 15.0);
        assert_eq!(bbox_z.get_zmin(), Some(5.0));
        assert_eq!(bbox_z.get_zmax(), Some(15.0));
        assert!(bbox_z.is_z_valid());
        assert!(!bbox_z.is_m_valid());

        // test with mrange
        let bbox_m = BoundingBox::new(0.0, 0.0, 10.0, 10.0).with_mrange(10.0, 20.0);
        assert_eq!(bbox_m.get_mmin(), Some(10.0));
        assert_eq!(bbox_m.get_mmax(), Some(20.0));
        assert!(!bbox_m.is_z_valid());
        assert!(bbox_m.is_m_valid());

        // test with zrange and mrange
        let bbox_zm = BoundingBox::new(0.0, 0.0, 10.0, 10.0)
            .with_zrange(5.0, 15.0)
            .with_mrange(10.0, 20.0);
        assert_eq!(bbox_zm.get_zmin(), Some(5.0));
        assert_eq!(bbox_zm.get_zmax(), Some(15.0));
        assert_eq!(bbox_zm.get_mmin(), Some(10.0));
        assert_eq!(bbox_zm.get_mmax(), Some(20.0));
        assert!(bbox_zm.is_z_valid());
        assert!(bbox_zm.is_m_valid());
    }
}
