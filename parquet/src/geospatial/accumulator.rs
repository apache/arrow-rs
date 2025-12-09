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

//! This module provides implementations and traits for building [`GeospatialStatistics`]

use std::sync::{Arc, OnceLock};

use crate::{
    basic::LogicalType, errors::ParquetError, geospatial::statistics::GeospatialStatistics,
    schema::types::ColumnDescPtr,
};

/// Create a new [`GeoStatsAccumulator`] instance if `descr` represents a Geometry or
/// Geography [`LogicalType`]
///
/// Returns a suitable [`GeoStatsAccumulator`] if `descr` represents a non-geospatial type
/// or `None` otherwise.
pub fn try_new_geo_stats_accumulator(
    descr: &ColumnDescPtr,
) -> Option<Box<dyn GeoStatsAccumulator>> {
    if !matches!(
        descr.logical_type_ref(),
        Some(LogicalType::Geometry { .. }) | Some(LogicalType::Geography { .. })
    ) {
        return None;
    }

    Some(
        ACCUMULATOR_FACTORY
            .get_or_init(|| Arc::new(DefaultGeoStatsAccumulatorFactory::default()))
            .new_accumulator(descr),
    )
}

/// Initialize the global [`GeoStatsAccumulatorFactory`]
///
/// This may only be done once before any calls to [`try_new_geo_stats_accumulator`].
/// Clients may use this to implement support for builds of the Parquet crate without
/// geospatial support or to implement support for Geography bounding using external
/// dependencies.
pub fn init_geo_stats_accumulator_factory(
    factory: Arc<dyn GeoStatsAccumulatorFactory>,
) -> Result<(), ParquetError> {
    if ACCUMULATOR_FACTORY.set(factory).is_err() {
        Err(ParquetError::General(
            "Global GeoStatsAccumulatorFactory already set".to_string(),
        ))
    } else {
        Ok(())
    }
}

/// Global accumulator factory instance
static ACCUMULATOR_FACTORY: OnceLock<Arc<dyn GeoStatsAccumulatorFactory>> = OnceLock::new();

/// Factory for [`GeospatialStatistics`] accumulators
///
/// The GeoStatsAccumulatorFactory is a trait implemented by the global factory that
/// generates new instances of a [`GeoStatsAccumulator`] when constructing new
/// encoders for a Geometry or Geography logical type.
pub trait GeoStatsAccumulatorFactory: Send + Sync {
    /// Create a new [`GeoStatsAccumulator`] appropriate for the logical type of a given
    /// [`ColumnDescPtr`]
    fn new_accumulator(&self, descr: &ColumnDescPtr) -> Box<dyn GeoStatsAccumulator>;
}

/// Dynamic [`GeospatialStatistics`] accumulator
///
/// The GeoStatsAccumulator is a trait whose implementors can ingest the (non-null)
/// elements of a column and return compliant [`GeospatialStatistics`] (or `None`).
/// When built with geospatial support this will usually be the
/// [`ParquetGeoStatsAccumulator`]
pub trait GeoStatsAccumulator: Send {
    /// Returns true if this instance can return [`GeospatialStatistics`] from
    /// [`GeoStatsAccumulator::finish`].
    ///
    /// This method returns false when this crate is built without geospatial support
    /// (i.e., from the [`VoidGeoStatsAccumulator`]) or if the accumulator encountered
    /// invalid or unsupported elements for which it cannot compute valid statistics.
    fn is_valid(&self) -> bool;

    /// Update with a single slice of WKB-encoded values
    ///
    /// This method is infallible; however, in the event of improperly encoded values,
    /// implementations must ensure that [`GeoStatsAccumulator::finish`] returns `None`.
    fn update_wkb(&mut self, wkb: &[u8]);

    /// Compute the final statistics and reset internal state
    fn finish(&mut self) -> Option<Box<GeospatialStatistics>>;
}

/// Default accumulator for [`GeospatialStatistics`]
///
/// When this crate is built with geospatial support, this factory constructs a
/// [`ParquetGeoStatsAccumulator`] that ensures Geometry columns are written with
/// statistics when statistics for that column are enabled. Otherwise, this factory
/// returns a [`VoidGeoStatsAccumulator`] that never adds any geospatial statistics.
///
/// Bounding for Geography columns is not currently implemented by parquet-geospatial
/// and this factory will always return a [`VoidGeoStatsAccumulator`].
#[derive(Debug, Default)]
pub struct DefaultGeoStatsAccumulatorFactory {}

impl GeoStatsAccumulatorFactory for DefaultGeoStatsAccumulatorFactory {
    fn new_accumulator(&self, _descr: &ColumnDescPtr) -> Box<dyn GeoStatsAccumulator> {
        #[cfg(feature = "geospatial")]
        if let Some(crate::basic::LogicalType::Geometry { .. }) = _descr.logical_type_ref() {
            Box::new(ParquetGeoStatsAccumulator::default())
        } else {
            Box::new(VoidGeoStatsAccumulator::default())
        }

        #[cfg(not(feature = "geospatial"))]
        return Box::new(VoidGeoStatsAccumulator::default());
    }
}

/// A [`GeoStatsAccumulator`] that never computes any [`GeospatialStatistics`]
#[derive(Debug, Default)]
pub struct VoidGeoStatsAccumulator {}

impl GeoStatsAccumulator for VoidGeoStatsAccumulator {
    fn is_valid(&self) -> bool {
        false
    }

    fn update_wkb(&mut self, _wkb: &[u8]) {}

    fn finish(&mut self) -> Option<Box<GeospatialStatistics>> {
        None
    }
}

/// A [`GeoStatsAccumulator`] that uses the parquet-geospatial crate to compute Geometry statistics
///
/// Note that this accumulator only supports Geometry types and will return invalid statistics for
/// non-point Geography input ([`GeoStatsAccumulatorFactory::new_accumulator`] is responsible
/// for ensuring an appropriate accumulator based on the logical type).
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

        let bounder_geometry_types = self.bounder.geometry_types();
        let geometry_types = if bounder_geometry_types.is_empty() {
            None
        } else {
            Some(bounder_geometry_types)
        };

        // Reset
        self.bounder = parquet_geospatial::bounding::GeometryBounder::empty();

        Some(Box::new(GeospatialStatistics::new(bbox, geometry_types)))
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_void_accumulator() {
        let mut accumulator = VoidGeoStatsAccumulator {};
        assert!(!accumulator.is_valid());
        accumulator.update_wkb(&[0x01, 0x02, 0x03]);
        assert!(accumulator.finish().is_none());
    }

    #[cfg(feature = "geospatial")]
    #[test]
    fn test_default_accumulator_geospatial_factory() {
        use std::sync::Arc;

        use parquet_geospatial::testing::wkb_point_xy;

        use crate::{
            basic::LogicalType,
            geospatial::bounding_box::BoundingBox,
            schema::types::{ColumnDescriptor, ColumnPath, Type},
        };

        // Check that we have a working accumulator for Geometry
        let parquet_type = Type::primitive_type_builder("geom", crate::basic::Type::BYTE_ARRAY)
            .with_logical_type(Some(LogicalType::Geometry { crs: None }))
            .build()
            .unwrap();
        let column_descr =
            ColumnDescriptor::new(Arc::new(parquet_type), 0, 0, ColumnPath::new(vec![]));
        let mut accumulator = try_new_geo_stats_accumulator(&Arc::new(column_descr)).unwrap();

        assert!(accumulator.is_valid());
        accumulator.update_wkb(&wkb_point_xy(1.0, 2.0));
        accumulator.update_wkb(&wkb_point_xy(11.0, 12.0));
        let stats = accumulator.finish().unwrap();
        assert_eq!(
            stats.bounding_box().unwrap(),
            &BoundingBox::new(1.0, 11.0, 2.0, 12.0)
        );

        // Check that we have a void accumulator for Geography
        let parquet_type = Type::primitive_type_builder("geom", crate::basic::Type::BYTE_ARRAY)
            .with_logical_type(Some(LogicalType::Geography {
                crs: None,
                algorithm: None,
            }))
            .build()
            .unwrap();
        let column_descr =
            ColumnDescriptor::new(Arc::new(parquet_type), 0, 0, ColumnPath::new(vec![]));
        let mut accumulator = try_new_geo_stats_accumulator(&Arc::new(column_descr)).unwrap();

        assert!(!accumulator.is_valid());
        assert!(accumulator.finish().is_none());

        // Check that we return None if the type is not geometry or goegraphy
        let parquet_type = Type::primitive_type_builder("geom", crate::basic::Type::BYTE_ARRAY)
            .build()
            .unwrap();
        let column_descr =
            ColumnDescriptor::new(Arc::new(parquet_type), 0, 0, ColumnPath::new(vec![]));
        assert!(try_new_geo_stats_accumulator(&Arc::new(column_descr)).is_none());

        // We should not be able to initialize a global accumulator after we've initialized at least
        // one accumulator
        assert!(
            init_geo_stats_accumulator_factory(Arc::new(
                DefaultGeoStatsAccumulatorFactory::default()
            ))
            .is_err()
        )
    }

    #[cfg(feature = "geospatial")]
    #[test]
    fn test_geometry_accumulator() {
        use parquet_geospatial::testing::{wkb_point_xy, wkb_point_xyzm};

        use crate::geospatial::bounding_box::BoundingBox;

        let mut accumulator = ParquetGeoStatsAccumulator::default();

        // A fresh instance should be able to bound input
        assert!(accumulator.is_valid());
        accumulator.update_wkb(&wkb_point_xy(1.0, 2.0));
        accumulator.update_wkb(&wkb_point_xy(11.0, 12.0));
        let stats = accumulator.finish().unwrap();
        assert_eq!(stats.geospatial_types().unwrap(), &vec![1]);
        assert_eq!(
            stats.bounding_box().unwrap(),
            &BoundingBox::new(1.0, 11.0, 2.0, 12.0)
        );

        // finish() should have reset the bounder such that the first values
        // aren't when computing the next bound of statistics.
        assert!(accumulator.is_valid());
        accumulator.update_wkb(&wkb_point_xy(21.0, 22.0));
        accumulator.update_wkb(&wkb_point_xy(31.0, 32.0));
        let stats = accumulator.finish().unwrap();
        assert_eq!(stats.geospatial_types().unwrap(), &vec![1]);
        assert_eq!(
            stats.bounding_box().unwrap(),
            &BoundingBox::new(21.0, 31.0, 22.0, 32.0)
        );

        // When an accumulator encounters invalid input, it reports is_valid() false
        // and does not compute subsequent statistics
        assert!(accumulator.is_valid());
        accumulator.update_wkb(&wkb_point_xy(41.0, 42.0));
        accumulator.update_wkb("these bytes are not WKB".as_bytes());
        assert!(!accumulator.is_valid());
        assert!(accumulator.finish().is_none());

        // Subsequent rounds of accumulation should work as expected
        assert!(accumulator.is_valid());
        accumulator.update_wkb(&wkb_point_xy(41.0, 42.0));
        accumulator.update_wkb(&wkb_point_xy(51.0, 52.0));
        let stats = accumulator.finish().unwrap();
        assert_eq!(stats.geospatial_types().unwrap(), &vec![1]);
        assert_eq!(
            stats.bounding_box().unwrap(),
            &BoundingBox::new(41.0, 51.0, 42.0, 52.0)
        );

        // When there was no input at all (occurs in the all null case), both geometry
        // types and bounding box will be None. This is because Parquet Thrift statistics
        // have no mechanism to communicate "empty". (The all null situation may be determined
        // from the null count in this case).
        assert!(accumulator.is_valid());
        let stats = accumulator.finish().unwrap();
        assert!(stats.geospatial_types().is_none());
        assert!(stats.bounding_box().is_none());

        // When there was 100% "empty" input (i.e., non-null geometries without
        // coordinates), there should be statistics with geometry types but no
        // bounding box.
        assert!(accumulator.is_valid());
        accumulator.update_wkb(&wkb_point_xy(f64::NAN, f64::NAN));
        let stats = accumulator.finish().unwrap();
        assert_eq!(stats.geospatial_types().unwrap(), &vec![1]);
        assert!(stats.bounding_box().is_none());

        // If Z and/or M are present, they should be reported in the bounding box
        assert!(accumulator.is_valid());
        accumulator.update_wkb(&wkb_point_xyzm(1.0, 2.0, 3.0, 4.0));
        accumulator.update_wkb(&wkb_point_xyzm(5.0, 6.0, 7.0, 8.0));
        let stats = accumulator.finish().unwrap();
        assert_eq!(stats.geospatial_types().unwrap(), &vec![3001]);
        assert_eq!(
            stats.bounding_box().unwrap(),
            &BoundingBox::new(1.0, 5.0, 2.0, 6.0)
                .with_zrange(3.0, 7.0)
                .with_mrange(4.0, 8.0)
        );
    }
}
