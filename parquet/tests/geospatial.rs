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

//! Tests for Geometry and Geography logical types

use parquet::{
    basic::{EdgeInterpolationAlgorithm, LogicalType},
    file::{
        metadata::ParquetMetaData,
        reader::{FileReader, SerializedFileReader},
    },
    geospatial::bounding_box::BoundingBox,
};
use serde_json::Value;
use std::fs::File;

fn read_metadata(geospatial_test_file: &str) -> ParquetMetaData {
    let path = format!(
        "{}/geospatial/{geospatial_test_file}",
        arrow::util::test_util::parquet_test_data(),
    );
    let file = File::open(path).unwrap();
    let reader = SerializedFileReader::try_from(file).unwrap();
    reader.metadata().clone()
}

#[test]
fn test_read_logical_type() {
    // Some crs values are short strings
    let expected_logical_type = [
        ("crs-default.parquet", LogicalType::Geometry { crs: None }),
        (
            "crs-srid.parquet",
            LogicalType::Geometry {
                crs: Some("srid:5070".to_string()),
            },
        ),
        (
            "crs-projjson.parquet",
            LogicalType::Geometry {
                crs: Some("projjson:projjson_epsg_5070".to_string()),
            },
        ),
        (
            "crs-geography.parquet",
            LogicalType::Geography {
                crs: None,
                algorithm: Some(EdgeInterpolationAlgorithm::SPHERICAL),
            },
        ),
    ];

    for (geospatial_file, expected_type) in expected_logical_type {
        let metadata = read_metadata(geospatial_file);
        let column_descr = metadata.file_metadata().schema_descr().column(1);
        let logical_type = column_descr.logical_type_ref().unwrap();

        assert_eq!(logical_type, &expected_type);
    }

    // The crs value may also contain arbitrary values (in this case some JSON
    // a bit too lengthy to type out)
    let metadata = read_metadata("crs-arbitrary-value.parquet");
    let column_descr = metadata.file_metadata().schema_descr().column(1);
    let logical_type = column_descr.logical_type_ref().unwrap();

    if let LogicalType::Geometry { crs } = logical_type {
        let crs = crs.as_ref();
        let crs_parsed: Value = serde_json::from_str(crs.unwrap()).unwrap();
        assert_eq!(crs_parsed.get("id").unwrap().get("code").unwrap(), 5070);
    } else {
        panic!("Expected geometry type but got {logical_type:?}");
    }
}

#[test]
fn test_read_geospatial_statistics() {
    let metadata = read_metadata("geospatial.parquet");

    // geospatial.parquet schema:
    //    optional binary field_id=-1 group (String);
    //    optional binary field_id=-1 wkt (String);
    //    optional binary field_id=-1 geometry (Geometry(crs=));
    let fields = metadata.file_metadata().schema().get_fields();
    let logical_type = fields[2].get_basic_info().logical_type_ref().unwrap();
    assert_eq!(logical_type, &LogicalType::Geometry { crs: None });

    let geo_statistics = metadata.row_group(0).column(2).geo_statistics();
    assert!(geo_statistics.is_some());

    let expected_bbox = BoundingBox::new(10.0, 40.0, 10.0, 40.0)
        .with_zrange(30.0, 80.0)
        .with_mrange(200.0, 1600.0);
    let expected_geospatial_types = vec![
        1, 2, 3, 4, 5, 6, 7, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 2001, 2002, 2003, 2004,
        2005, 2006, 2007, 3001, 3002, 3003, 3004, 3005, 3006, 3007,
    ];
    assert_eq!(
        geo_statistics.unwrap().geospatial_types(),
        Some(&expected_geospatial_types)
    );
    assert_eq!(geo_statistics.unwrap().bounding_box(), Some(&expected_bbox));
}

#[cfg(all(feature = "arrow", feature = "geospatial"))]
mod test {
    //! Tests for Geometry and Geography logical types that require the arrow
    //! and/or geospatial features enabled

    use super::*;

    use std::{iter::zip, sync::Arc};

    use arrow_array::{ArrayRef, BinaryArray, RecordBatch, create_array};
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::{
        arrow::{ArrowWriter, arrow_writer::ArrowWriterOptions},
        column::reader::ColumnReader,
        data_type::{ByteArray, ByteArrayType},
        file::{
            metadata::RowGroupMetaData,
            properties::{EnabledStatistics, WriterProperties},
            reader::FileReader,
            writer::SerializedFileWriter,
        },
        geospatial::statistics::GeospatialStatistics,
        schema::types::{SchemaDescriptor, Type},
    };
    use parquet_geospatial::testing::wkb_point_xy;

    fn read_row_group_metadata(b: Bytes) -> Vec<RowGroupMetaData> {
        let reader = SerializedFileReader::new(b).unwrap();
        reader.metadata().row_groups().to_vec()
    }

    fn read_geo_statistics(b: Bytes, column: usize) -> Vec<Option<GeospatialStatistics>> {
        read_row_group_metadata(b)
            .iter()
            .map(|row_group| row_group.column(column).geo_statistics().cloned())
            .collect()
    }

    #[test]
    fn test_write_statistics_not_arrow() {
        // Four row groups: one all non-null, one with a null, one with all nulls,
        // one with invalid WKB
        let column_values = vec![
            [wkb_point_xy(1.0, 2.0), wkb_point_xy(11.0, 12.0)].map(ByteArray::from),
            ["this is not valid wkb".into(), wkb_point_xy(31.0, 32.0)].map(ByteArray::from),
            [wkb_point_xy(21.0, 22.0), vec![]].map(ByteArray::from),
            [ByteArray::new(), ByteArray::new()],
        ];
        let def_levels = [[1, 1], [1, 1], [1, 0], [0, 0]];

        // Ensure that nulls are omitted, that completely empty stats are omitted,
        // and that invalid WKB results in empty stats
        let expected_geometry_types = [Some(vec![1]), None, Some(vec![1]), None];
        let expected_bounding_box = [
            Some(BoundingBox::new(1.0, 11.0, 2.0, 12.0)),
            None,
            Some(BoundingBox::new(21.0, 21.0, 22.0, 22.0)),
            None,
        ];

        let root = parquet_schema_geometry();
        let schema = SchemaDescriptor::new(root.into());
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();

        let mut buf = Vec::with_capacity(1024);
        let mut writer =
            SerializedFileWriter::new(&mut buf, schema.root_schema_ptr(), Arc::new(props)).unwrap();

        for (def_levels, values) in zip(&def_levels, &column_values) {
            let mut rg = writer.next_row_group().unwrap();
            let mut col = rg.next_column().unwrap().unwrap();
            col.typed::<ByteArrayType>()
                .write_batch(values, Some(def_levels), None)
                .unwrap();
            col.close().unwrap();
            rg.close().unwrap();
        }

        writer.close().unwrap();

        // Check geospatial statistics on file read
        let buf_bytes = Bytes::from(buf);
        let all_geo_stats = read_geo_statistics(buf_bytes.clone(), 0);
        assert_eq!(all_geo_stats.len(), column_values.len());
        assert_eq!(expected_geometry_types.len(), column_values.len());
        assert_eq!(expected_bounding_box.len(), column_values.len());

        for i in 0..column_values.len() {
            if let Some(geo_stats) = all_geo_stats[i].as_ref() {
                assert_eq!(
                    geo_stats.geospatial_types(),
                    expected_geometry_types[i].as_ref()
                );
                assert_eq!(geo_stats.bounding_box(), expected_bounding_box[i].as_ref());
            } else {
                assert!(expected_geometry_types[i].is_none());
                assert!(expected_bounding_box[i].is_none());
            }
        }

        for (i, rg) in read_row_group_metadata(buf_bytes).iter().enumerate() {
            // We should have written Statistics with a null_count
            let stats = rg.column(0).statistics().unwrap();
            let expected_null_count: u64 = def_levels[i].iter().map(|l| (*l == 0) as u64).sum();
            assert_eq!(stats.null_count_opt(), Some(expected_null_count));

            // ...but there should be no min or max value
            assert!(stats.min_bytes_opt().is_none());
            assert!(stats.max_bytes_opt().is_none());

            // There should be no index for this column
            assert!(rg.column(0).column_index_length().is_none());
            assert!(rg.column(0).column_index_offset().is_none());
        }
    }

    #[test]
    fn test_write_statistics_arrow() {
        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "geom",
            DataType::Binary,
            true,
        )]));

        // Check the same cases as for the non-arrow writer. These need checking again because
        // the arrow writer uses a different encoder where the code path for skipping nulls
        // is independent.
        let column_values = [
            wkb_array_xy([Some((1.0, 2.0)), Some((11.0, 12.0))]),
            create_array!(
                Binary,
                [
                    "this is not valid wkb".as_bytes(),
                    &wkb_point_xy(31.0, 32.0)
                ]
            ),
            wkb_array_xy([Some((21.0, 22.0)), None]),
            wkb_array_xy([None, None]),
        ];

        let expected_geometry_types = [Some(vec![1]), None, Some(vec![1]), None];
        let expected_bounding_box = [
            Some(BoundingBox::new(1.0, 11.0, 2.0, 12.0)),
            None,
            Some(BoundingBox::new(21.0, 21.0, 22.0, 22.0)),
            None,
        ];

        let root = parquet_schema_geometry();
        let schema = SchemaDescriptor::new(root.into());

        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();
        let options = ArrowWriterOptions::new()
            .with_parquet_schema(schema)
            .with_properties(props);

        let mut buf = Vec::with_capacity(1024);
        let mut file_writer =
            ArrowWriter::try_new_with_options(&mut buf, arrow_schema.clone(), options).unwrap();

        for values in &column_values {
            let batch = RecordBatch::try_new(arrow_schema.clone(), vec![values.clone()]).unwrap();
            file_writer.write(&batch).unwrap();
            file_writer.flush().unwrap();
        }

        file_writer.close().unwrap();

        // Check statistics on file read
        let buf_bytes = Bytes::from(buf);
        let all_geo_stats = read_geo_statistics(buf_bytes.clone(), 0);
        assert_eq!(all_geo_stats.len(), column_values.len());

        for i in 0..column_values.len() {
            if let Some(geo_stats) = all_geo_stats[i].as_ref() {
                assert_eq!(
                    geo_stats.geospatial_types(),
                    expected_geometry_types[i].as_ref()
                );
                assert_eq!(geo_stats.bounding_box(), expected_bounding_box[i].as_ref());
            } else {
                assert!(expected_geometry_types[i].is_none());
                assert!(expected_bounding_box[i].is_none());
            }
        }

        for (i, rg) in read_row_group_metadata(buf_bytes).iter().enumerate() {
            // We should have written Statistics with a null_count
            let stats = rg.column(0).statistics().unwrap();
            let expected_null_count = column_values[i].null_count();
            assert_eq!(stats.null_count_opt(), Some(expected_null_count as u64));

            // ...but there should be no min or max value
            assert!(stats.min_bytes_opt().is_none());
            assert!(stats.max_bytes_opt().is_none());

            // There should be no index for this column
            assert!(rg.column(0).column_index_length().is_none());
            assert!(rg.column(0).column_index_offset().is_none());
        }
    }

    #[test]
    fn test_roundtrip_statistics_geospatial() {
        let path = format!(
            "{}/geospatial/geospatial.parquet",
            arrow::util::test_util::parquet_test_data(),
        );

        test_roundtrip_statistics(&path, 2);
    }

    #[test]
    fn test_roundtrip_geospatial_with_nan() {
        let path = format!(
            "{}/geospatial/geospatial-with-nan.parquet",
            arrow::util::test_util::parquet_test_data(),
        );

        test_roundtrip_statistics(&path, 0);
    }

    #[test]
    fn test_roundtrip_statistics_crs() {
        let path = format!(
            "{}/geospatial/crs-default.parquet",
            arrow::util::test_util::parquet_test_data(),
        );

        test_roundtrip_statistics(&path, 0);
    }

    fn test_roundtrip_statistics(path: &str, column: usize) {
        let file_bytes = Bytes::from(std::fs::read(path).unwrap());

        let reader = SerializedFileReader::new(file_bytes.clone()).unwrap();
        let mut values = Vec::new();
        let mut def_levels = Vec::new();

        let root = parquet_schema_geometry();
        let schema = SchemaDescriptor::new(root.into());
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();

        let mut buf = Vec::with_capacity(1024);
        let mut writer =
            SerializedFileWriter::new(&mut buf, schema.root_schema_ptr(), Arc::new(props)).unwrap();

        for i in 0..reader.num_row_groups() {
            let row_group = reader.get_row_group(i).unwrap();
            values.truncate(0);
            def_levels.truncate(0);

            let mut row_group_out = writer.next_row_group().unwrap();

            if let ColumnReader::ByteArrayColumnReader(mut reader) =
                row_group.get_column_reader(column).unwrap()
            {
                reader
                    .read_records(1000000, Some(&mut def_levels), None, &mut values)
                    .unwrap();

                let mut col = row_group_out.next_column().unwrap().unwrap();
                col.typed::<ByteArrayType>()
                    .write_batch(&values, Some(&def_levels), None)
                    .unwrap();
                col.close().unwrap();
                row_group_out.close().unwrap();
            } else {
                panic!("Unexpected geometry column type");
            }
        }

        writer.close().unwrap();

        let actual_stats = read_geo_statistics(buf.into(), 0);
        let expected_stats = read_geo_statistics(file_bytes.clone(), column);

        assert_eq!(actual_stats.len(), expected_stats.len());
        for i in 0..expected_stats.len() {
            assert_eq!(actual_stats[i], expected_stats[i], "Row group {i}");
        }
    }

    fn parquet_schema_geometry() -> Type {
        Type::group_type_builder("root")
            .with_fields(vec![
                Type::primitive_type_builder("geo", parquet::basic::Type::BYTE_ARRAY)
                    .with_logical_type(Some(LogicalType::Geometry { crs: None }))
                    .build()
                    .unwrap()
                    .into(),
            ])
            .build()
            .unwrap()
    }

    fn wkb_array_xy(coords: impl IntoIterator<Item = Option<(f64, f64)>>) -> ArrayRef {
        let array = BinaryArray::from_iter(
            coords
                .into_iter()
                .map(|maybe_xy| maybe_xy.map(|(x, y)| wkb_point_xy(x, y))),
        );
        Arc::new(array)
    }
}
