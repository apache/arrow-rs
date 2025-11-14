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

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, sync::Arc};

    use arrow::util::test_util::parquet_test_data;
    use arrow_array::{ArrayRef, BinaryArray, RecordBatch};
    use arrow_schema::{Schema, extension::ExtensionType};
    use bytes::Bytes;
    use parquet_geospatial::{WkbArray, WkbMetadata, WkbType};

    use crate::{
        arrow::{ArrowWriter, arrow_reader::ArrowReaderBuilder},
        basic::EdgeInterpolationAlgorithm,
        file::{
            metadata::{ParquetMetaData, ParquetMetaDataReader},
            reader::ChunkReader,
        },
    };

    /// Ensure a file with Geometry LogicalType, written by another writer in
    /// parquet-testing, can be read as a GeometryArray
    #[test]
    fn read_geometry_logical_type() {
        let batch = read_geospatial_test_case("crs-projjson.parquet");

        let wkb_type = assert_geometry_metadata(&batch, "geometry");
        let geom_column = batch
            .column_by_name("geometry")
            .expect("expected geometry column");
        let wkb_array = WkbArray::try_new_geometry(geom_column, wkb_type.metadata().clone())
            .expect("expected geometry column to be a WkbArray");

        // verify the value
        assert_eq!(wkb_array.len(), 1);
        assert!(wkb_array.is_valid(0));
        let wkb_value = wkb_array.value(0);
        assert_eq!(wkb_value.len(), 3549);
    }

    /// Ensure a file with Geography LogicalType, written by another writer in
    /// parquet-testing, can be read as a GeometryArray
    #[test]
    fn read_geography_logical_type() {
        let batch = read_geospatial_test_case("crs-geography.parquet");

        let wkb_type = assert_geometry_metadata(&batch, "geography");
        let geom_column = batch
            .column_by_name("geography")
            .expect("expected geography column");
        let wkb_array = WkbArray::try_new_geography(geom_column, wkb_type.metadata().clone())
            .expect("expected geography column to be a WkbArray");

        // verify the value
        assert_eq!(wkb_array.len(), 1);
        assert!(wkb_array.is_valid(0));
        let wkb_value = wkb_array.value(0);
        assert_eq!(wkb_value.len(), 3549);
    }

    /// Writes a wkb (geometry) array to a parquet file and ensures the parquet logical type
    /// annotation is correct
    #[test]
    fn write_geometry_logical_type() {
        let array = geometry_array();
        let batch = wkb_array_to_batch(array);
        let buffer = write_to_buffer(&batch);

        // read the parquet file's metadata and verify the logical type
        let metadata = read_metadata(&Bytes::from(buffer));
        let schema = metadata.file_metadata().schema_descr();
        let fields = schema.root_schema().get_fields();
        assert_eq!(fields.len(), 1);
        let field = &fields[0];
        assert_eq!(field.name(), "data");
        // data should have been written with the Variant logical type
        assert_eq!(
            field.get_basic_info().logical_type(),
            Some(crate::basic::LogicalType::Geometry {
                crs: Some(String::from("test crs"))
            })
        );
    }

    /// Writes a wkb (geography) array to a parquet file and ensures the parquet logical type
    /// annotation is correct
    #[test]
    fn write_geography_logical_type() {
        let array = geography_array();
        let batch = wkb_array_to_batch(array);
        let buffer = write_to_buffer(&batch);

        // read the parquet file's metadata and verify the logical type
        let metadata = read_metadata(&Bytes::from(buffer));
        let schema = metadata.file_metadata().schema_descr();
        let fields = schema.root_schema().get_fields();
        assert_eq!(fields.len(), 1);
        let field = &fields[0];
        assert_eq!(field.name(), "data");
        // data should have been written with the Variant logical type
        assert_eq!(
            field.get_basic_info().logical_type(),
            Some(crate::basic::LogicalType::Geography {
                crs: Some(String::from("test crs")),
                algorithm: Some(EdgeInterpolationAlgorithm::SPHERICAL),
            })
        );
    }

    /// Return a WkbArray with 3 rows:
    fn geometry_array() -> WkbArray {
        let values: Vec<&[u8]> = vec![b"not", b"actually", b"wkb"];
        let inner = BinaryArray::from_vec(values);
        let md = WkbMetadata::new(Some(String::from("test crs")), None);

        WkbArray::try_new_geometry(&inner, md.clone()).unwrap()
    }

    /// Return a WkbArray with 3 rows:
    fn geography_array() -> WkbArray {
        let values: Vec<&[u8]> = vec![b"not", b"actually", b"wkb"];
        let inner = BinaryArray::from_vec(values);
        let md = WkbMetadata::new(Some(String::from("test crs")), None);

        WkbArray::try_new_geography(&inner, md.clone()).unwrap()
    }

    /// creates a RecordBatch with a single column "data" from a WkbArray,
    fn wkb_array_to_batch(array: WkbArray) -> RecordBatch {
        let field = array.field("data");
        let schema = Schema::new(vec![field]);
        RecordBatch::try_new(Arc::new(schema), vec![ArrayRef::from(array)]).unwrap()
    }

    /// writes a RecordBatch to memory buffer and returns the buffer
    fn write_to_buffer(batch: &RecordBatch) -> Vec<u8> {
        let mut buffer = vec![];
        let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
        buffer
    }

    /// Reads the Parquet metadata
    fn read_metadata<T: ChunkReader + 'static>(input: &T) -> ParquetMetaData {
        let mut reader = ParquetMetaDataReader::new();
        reader.try_parse(input).unwrap();
        reader.finish().unwrap()
    }

    /// Verifies the geospatial metadata is present in the schema for the specified
    /// field name.
    fn assert_geometry_metadata(batch: &RecordBatch, field_name: &str) -> WkbType {
        println!("{batch:?}");
        let schema = batch.schema();
        let field = schema
            .field_with_name(field_name)
            .expect("could not find expected field");

        // explicitly check the metadata so it is clear in the tests what the
        // names are
        let metadata_value = field
            .metadata()
            .get("ARROW:extension:name")
            .expect("metadata does not exist");

        assert_eq!(metadata_value, "geoarrow.wkb");

        // verify that `GeometryType` also correctly finds the metadata
        field
            .try_extension_type::<WkbType>()
            .expect("WkbExtensionType should be readable")
    }

    /// Reads a RecordBatch from a reader (e.g. Vec or File)
    fn read_to_batch<T: ChunkReader + 'static>(reader: T) -> RecordBatch {
        let reader = ArrowReaderBuilder::try_new(reader)
            .unwrap()
            .build()
            .unwrap();
        let mut batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.len(), 1);
        batches.swap_remove(0)
    }

    /// Read the specified test case filename from parquet-testing
    /// See parquet-testing/geospatial/README.md for more details
    fn read_geospatial_test_case(name: &str) -> RecordBatch {
        let case_file = PathBuf::from(parquet_test_data())
            .join("geospatial")
            .join(name);
        let case_file = std::fs::File::open(case_file).unwrap();
        read_to_batch(case_file)
    }
}
