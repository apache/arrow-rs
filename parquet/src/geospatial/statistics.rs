// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Geospatial statistics for Parquet files.
//!
//! This module provides functionality for working with geospatial statistics in Parquet files.
//! It includes support for bounding boxes and geospatial statistics in column chunk metadata.

use crate::{file::metadata::HeapSize, geospatial::bounding_box::BoundingBox};

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
/// use parquet::geospatial::statistics::GeospatialStatistics;
/// use parquet::geospatial::bounding_box::BoundingBox;
///
/// // Statistics with bounding box
/// let bbox = BoundingBox::new(0.0, 0.0, 100.0, 100.0);
/// let stats = GeospatialStatistics::new(Some(bbox), Some(vec![1, 2, 3]));
/// ```
#[derive(Clone, Debug, PartialEq, Default)]
pub struct GeospatialStatistics {
    bbox: Option<BoundingBox>,
    geospatial_types: Option<Vec<i32>>,
}

impl GeospatialStatistics {
    /// Creates a new geospatial statistics instance with the specified data.
    pub fn new(bbox: Option<BoundingBox>, geospatial_types: Option<Vec<i32>>) -> Self {
        Self {
            bbox,
            geospatial_types,
        }
    }

    /// Optional list of geometry type identifiers, where `None` represents lack of information
    pub fn geospatial_types(&self) -> Option<&Vec<i32>> {
        self.geospatial_types.as_ref()
    }

    /// Optional bounding defining the spatial extent, where `None` represents a lack of information.
    pub fn bounding_box(&self) -> Option<&BoundingBox> {
        self.bbox.as_ref()
    }
}

impl HeapSize for GeospatialStatistics {
    fn heap_size(&self) -> usize {
        self.bbox.heap_size() + self.geospatial_types.heap_size()
    }
}
