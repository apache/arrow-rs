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

use crate::format::GeospatialStatistics as TGeospatialStatistics;
use crate::errors::Result;
use crate::geospatial::bounding_box::BoundingBox;

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
    /// Optional bounding defining the spatial extent, where None represents a lack of information.
    bbox: Option<BoundingBox>,
    /// Optional list of geometry type identifiers, where None represents lack of information
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
                    bbox.xmax.into(),
                    bbox.ymin.into(),
                    bbox.ymax.into(),
                );

                new_bbox = match (bbox.zmin, bbox.zmax) {
                    (Some(zmin), Some(zmax)) => new_bbox.with_zrange(zmin.into(), zmax.into()),
                    // If both None or mismatch, set it to None and don't error
                    _ => new_bbox
                };

                new_bbox = match (bbox.mmin, bbox.mmax) {
                    (Some(mmin), Some(mmax)) => new_bbox.with_mrange(mmin.into(), mmax.into()),
                    // If both None or mismatch, set it to None and don't error
                    _ => new_bbox
                };

                Some(new_bbox)
            } else {
                None    
            };

            // If vector is empty, then set it to None 
            let geospatial_types: Option<Vec<i32>> = geo_stats.geospatial_types.filter(|v| !v.is_empty());
            Some(GeospatialStatistics::new(bbox, geospatial_types))
        }
        None => None,
    })
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
        let bbox = BoundingBox::new(0.0, 0.0, 100.0, 100.0);
        let geospatial_types = vec![1, 2, 3];
        let stats = GeospatialStatistics::new(Some(bbox), Some(geospatial_types));
        let thrift_stats = to_thrift(Some(&stats));
        assert_eq!(from_thrift(thrift_stats).unwrap(), Some(stats));
    }

    #[test]
    fn test_bbox_to_thrift() {
        use crate::format as parquet;
        use thrift::OrderedFloat;

        let bbox = BoundingBox::new(0.0, 0.0, 100.0, 100.0);
        let thrift_bbox: parquet::BoundingBox = bbox.into();
        assert_eq!(thrift_bbox.xmin, 0.0);
        assert_eq!(thrift_bbox.xmax, 0.0);
        assert_eq!(thrift_bbox.ymin, 100.0);
        assert_eq!(thrift_bbox.ymax, 100.0);
        assert_eq!(thrift_bbox.zmin, None);
        assert_eq!(thrift_bbox.zmax, None);
        assert_eq!(thrift_bbox.mmin, None);
        assert_eq!(thrift_bbox.mmax, None);

        let bbox_z = BoundingBox::new(0.0, 0.0, 100.0, 100.0)
            .with_zrange(5.0, 15.0);
        let thrift_bbox_z: parquet::BoundingBox = bbox_z.into();
        assert_eq!(thrift_bbox_z.zmin, Some(OrderedFloat(5.0)));
        assert_eq!(thrift_bbox_z.zmax, Some(OrderedFloat(15.0)));

        let bbox_m = BoundingBox::new(0.0, 0.0, 100.0, 100.0)
            .with_mrange(10.0, 20.0);
        let thrift_bbox_m: parquet::BoundingBox = bbox_m.into();
        assert_eq!(thrift_bbox_m.mmin, Some(OrderedFloat(10.0)));
        assert_eq!(thrift_bbox_m.mmax, Some(OrderedFloat(20.0)));
    }
}
