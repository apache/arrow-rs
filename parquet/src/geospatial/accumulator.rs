use crate::data_type::private::ParquetValueType;
use crate::{geospatial::statistics::GeospatialStatistics, schema::types::ColumnDescriptor};

pub trait GeoStatsAccumulatorFactory {
    fn new_accumulator(&self) -> Box<dyn GeoStatsAccumulator>;
}

pub trait GeoStatsAccumulator {
    fn is_valid(&self) -> bool;
    fn update_wkb(&mut self, descr: &ColumnDescriptor, wkb: &[u8]);
    fn finish(&mut self) -> Option<Box<GeospatialStatistics>>;
}

pub fn update_geospatial_statistics_accumulator<'a, T, I>(
    bounder: &mut dyn GeoStatsAccumulator,
    descr: &ColumnDescriptor,
    iter: I,
) where
    T: ParquetValueType + 'a,
    I: Iterator<Item = &'a T>,
{
    use crate::basic::LogicalType;

    if !bounder.is_valid()
        || !matches!(
            descr.logical_type(),
            Some(LogicalType::Geometry) | Some(LogicalType::Geography)
        )
    {
        return;
    }

    for val in iter {
        bounder.update_wkb(descr, val.as_bytes());
    }
}

#[derive(Debug, Default)]
struct DefaultGeospatialStatisticsAccumulatorFactory {}

impl GeoStatsAccumulatorFactory for DefaultGeospatialStatisticsAccumulatorFactory {
    fn new_accumulator(&self) -> Box<dyn GeoStatsAccumulator> {
        #[cfg(feature = "geospatial")]
        return Box::new(ParquetGeoStatsAccumulator::default());

        #[cfg(not(feature = "geospatial"))]
        return Box::new(VoidGeospatialStatisticsAccumulator::default());
    }
}

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

#[cfg(feature = "geospatial")]
#[derive(Debug)]
struct ParquetGeoStatsAccumulator {
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
            if let Err(_) = self.bounder.update_wkb(wkb) {
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
