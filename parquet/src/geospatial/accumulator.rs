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

//! This module provides implementations and traits for building [GeospatialStatistics]

use crate::{geospatial::statistics::GeospatialStatistics, schema::types::ColumnDescPtr};

/// Factory for [GeospatialStatistics] accumulators
pub trait GeoStatsAccumulatorFactory {
    /// Create a new accumulator
    fn new_accumulator(&self, descr: &ColumnDescPtr) -> Box<dyn GeoStatsAccumulator>;
}

/// Dynamic geospatial accumulator
pub trait GeoStatsAccumulator: Send {
    /// Returns true if this accumulator has any plans to actually return statistics
    fn is_valid(&self) -> bool;

    /// Update with a single slice of possibly wkb-encoded values
    fn update_wkb(&mut self, wkb: &[u8]);

    /// Compute the final statistics from internal state
    fn finish(&mut self) -> Option<Box<GeospatialStatistics>>;
}

/// Default accumulator for [GeospatialStatistics] reflecting the build-time features of this build
#[derive(Debug, Default)]
pub struct DefaultGeoStatsAccumulatorFactory {}

impl GeoStatsAccumulatorFactory for DefaultGeoStatsAccumulatorFactory {
    fn new_accumulator(&self, _descr: &ColumnDescPtr) -> Box<dyn GeoStatsAccumulator> {
        #[cfg(feature = "geospatial")]
        if let Some(crate::basic::LogicalType::Geometry) = _descr.logical_type() {
            Box::new(ParquetGeoStatsAccumulator::default())
        } else {
            Box::new(VoidGeospatialStatisticsAccumulator::default())
        }

        #[cfg(not(feature = "geospatial"))]
        return Box::new(VoidGeospatialStatisticsAccumulator::default());
    }
}

/// A [GeoStatsAccumulator] that never computes any [GeospatialStatistics]
#[derive(Debug, Default)]
pub struct VoidGeospatialStatisticsAccumulator {}

impl GeoStatsAccumulator for VoidGeospatialStatisticsAccumulator {
    fn is_valid(&self) -> bool {
        false
    }

    fn update_wkb(&mut self, _wkb: &[u8]) {}

    fn finish(&mut self) -> Option<Box<GeospatialStatistics>> {
        None
    }
}

/// A [GeoStatsAccumulator] that uses the parquet-geospatial crate to compute statistics
#[cfg(feature = "geospatial")]
#[derive(Debug)]
pub struct ParquetGeoStatsAccumulator {
    bounder: parquet_geospatial::bounding::GeometryBounder,
    invalid: bool,
}

#[cfg(feature = "geospatial")]
impl Default for ParquetGeoStatsAccumulator {
    fn default() -> Self {
        Self {
            bounder: parquet_geospatial::bounding::GeometryBounder::empty(),
            invalid: false,
        }
    }
}

#[cfg(feature = "geospatial")]
impl GeoStatsAccumulator for ParquetGeoStatsAccumulator {
    fn is_valid(&self) -> bool {
        !self.invalid
    }

    fn update_wkb(&mut self, wkb: &[u8]) {
        if self.bounder.update_wkb(wkb).is_err() {
            self.invalid = true;
        }
    }

    fn finish(&mut self) -> Option<Box<GeospatialStatistics>> {
        use parquet_geospatial::interval::IntervalTrait;

        use crate::geospatial::bounding_box::BoundingBox;

        if self.invalid {
            // Reset
            self.invalid = false;
            self.bounder = parquet_geospatial::bounding::GeometryBounder::empty();
            return None;
        }

        let bbox = if self.bounder.x().is_empty() || self.bounder.y().is_empty() {
            None
        } else {
            let mut bbox = BoundingBox::new(
                self.bounder.x().lo(),
                self.bounder.x().hi(),
                self.bounder.y().lo(),
                self.bounder.y().hi(),
            );

            if !self.bounder.z().is_empty() {
                bbox = bbox.with_zrange(self.bounder.z().lo(), self.bounder.z().hi());
            }

            if !self.bounder.m().is_empty() {
                bbox = bbox.with_mrange(self.bounder.m().lo(), self.bounder.m().hi());
            }

            Some(bbox)
        };

        let geometry_types = Some(self.bounder.geometry_types());

        // Reset
        self.bounder = parquet_geospatial::bounding::GeometryBounder::empty();

        Some(Box::new(GeospatialStatistics::new(bbox, geometry_types)))
    }
}
