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
    use std::path::PathBuf;

    use arrow::util::test_util::parquet_test_data;
    use arrow_array::RecordBatch;
    use parquet_geospatial::WkbType;

    use crate::{arrow::arrow_reader::ArrowReaderBuilder, file::reader::ChunkReader};

    /// Ensure a file with Geometry LogicalType, written by another writer in
    /// parquet-testing, can be read as a GeometryArray
    #[test]
    fn read_geometry_logical_type() {
        // Note: case-075 2 columns ("id", "var")
        // The variant looks like this:
        // "Variant(metadata=VariantMetadata(dict={}), value=Variant(type=STRING, value=iceberg))"
        let batch = read_geospatial_test_case("crs-projjson.parquet");

        assert_geometry_metadata(&batch, "geometry");
        let _geom_column = batch
            .column_by_name("geometry")
            .expect("expected geometry column");
        // let var_array =
        //     VariantArray::try_new(&var_column).expect("expected var column to be a VariantArray");

        // // verify the value
        // assert_eq!(var_array.len(), 1);
        // assert!(var_array.is_valid(0));
        // let var_value = var_array.value(0);
        // assert_eq!(var_value, Variant::from("iceberg"));
    }

    /// Verifies the geospatial metadata is present in the schema for the specified
    /// field name.
    fn assert_geometry_metadata(batch: &RecordBatch, field_name: &str) {
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
            .expect("GeometryExtensionType should be readable");
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
