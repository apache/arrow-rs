use crate::{geospatial::statistics::GeospatialStatistics, schema::types::ColumnDescriptor};

/// Factory for [GeospatialStatistics] accumulators
pub trait GeoStatsAccumulatorFactory {
    /// Create a new accumulator
    fn new_accumulator(&self) -> Box<dyn GeoStatsAccumulator>;
}

/// Dynamic geospatial accumulator
pub trait GeoStatsAccumulator: Send {
    /// Returns true if this accumulator has any plans to actually return statistics
    fn is_valid(&self) -> bool;

    /// Update with a single slice of possibly wkb-encoded values
    fn update_wkb(&mut self, descr: &ColumnDescriptor, wkb: &[u8]);

    /// Compute the final statistics from internal state
    fn finish(&mut self) -> Option<Box<GeospatialStatistics>>;
}

/// Default accumulator for [GeospatialStatistics] reflecting the build-time features of this build
#[derive(Debug, Default)]
pub struct DefaultGeoStatsAccumulatorFactory {}

impl GeoStatsAccumulatorFactory for DefaultGeoStatsAccumulatorFactory {
    fn new_accumulator(&self) -> Box<dyn GeoStatsAccumulator> {
        #[cfg(feature = "geospatial")]
        return Box::new(ParquetGeoStatsAccumulator::default());

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

    fn update_wkb(&mut self, _descr: &ColumnDescriptor, _wkb: &[u8]) {}

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

    fn update_wkb(&mut self, descr: &ColumnDescriptor, wkb: &[u8]) {
        use crate::basic::LogicalType;

        if let Some(LogicalType::Geometry) = descr.logical_type() {
            if self.bounder.update_wkb(wkb).is_err() {
                self.invalid = true;
            }
        } else {
            self.invalid = true;
        }
    }

    fn finish(&mut self) -> Option<Box<GeospatialStatistics>> {
        todo!()
    }
}
