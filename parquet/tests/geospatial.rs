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

#[cfg(all(feature = "arrow", feature = "geospatial"))]
mod test {
    use std::{iter::zip, sync::Arc};

    use arrow_array::{create_array, ArrayRef, BinaryArray, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::{
        arrow::{arrow_writer::ArrowWriterOptions, ArrowWriter},
        basic::LogicalType,
        data_type::{ByteArray, ByteArrayType},
        file::{
            properties::{EnabledStatistics, WriterProperties},
            reader::{FileReader, SerializedFileReader},
            writer::SerializedFileWriter,
        },
        geospatial::{bounding_box::BoundingBox, statistics::GeospatialStatistics},
        schema::types::{SchemaDescriptor, Type},
    };

    fn read_geo_statistics(buf: Vec<u8>) -> Vec<Option<GeospatialStatistics>> {
        let b = Bytes::from(buf);
        let reader = SerializedFileReader::new(b).unwrap();
        reader
            .metadata()
            .row_groups()
            .iter()
            .map(|row_group| row_group.column(0).geo_statistics().cloned())
            .collect()
    }

    #[test]
    fn test_write_statistics_not_arrow() {
        // Four row groups: one all non-null, one with a null, one with all nulls,
        // one with invalid WKB
        let column_values = vec![
            [wkb_item_xy(1.0, 2.0), wkb_item_xy(11.0, 12.0)].map(ByteArray::from),
            ["this is not valid wkb".into(), wkb_item_xy(31.0, 32.0)].map(ByteArray::from),
            [wkb_item_xy(21.0, 22.0), vec![]].map(ByteArray::from),
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

        let root = Type::group_type_builder("root")
            .with_fields(vec![Type::primitive_type_builder(
                "geo",
                parquet::basic::Type::BYTE_ARRAY,
            )
            .with_logical_type(Some(LogicalType::Geometry))
            .build()
            .unwrap()
            .into()])
            .build()
            .unwrap();
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

        // Check statistics on file read
        let all_geo_stats = read_geo_statistics(buf);
        assert_eq!(all_geo_stats.len(), column_values.len());
        assert_eq!(expected_geometry_types.len(), column_values.len());
        assert_eq!(expected_bounding_box.len(), column_values.len());

        for i in 0..column_values.len() {
            if let Some(geo_stats) = all_geo_stats[i].as_ref() {
                assert_eq!(geo_stats.geospatial_types, expected_geometry_types[i]);
                assert_eq!(geo_stats.bbox, expected_bounding_box[i]);
            } else {
                assert!(expected_geometry_types[i].is_none());
                assert!(expected_bounding_box[i].is_none());
            }
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
                ["this is not valid wkb".as_bytes(), &wkb_item_xy(31.0, 32.0)]
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

        let root = Type::group_type_builder("root")
            .with_fields(vec![Type::primitive_type_builder(
                "geo",
                parquet::basic::Type::BYTE_ARRAY,
            )
            .with_logical_type(Some(LogicalType::Geometry))
            .build()
            .unwrap()
            .into()])
            .build()
            .unwrap();
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

        file_writer.finish().unwrap();
        drop(file_writer);

        // Check statistics on file read
        let all_geo_stats = read_geo_statistics(buf);
        assert_eq!(all_geo_stats.len(), column_values.len());

        for i in 0..column_values.len() {
            if let Some(geo_stats) = all_geo_stats[i].as_ref() {
                assert_eq!(geo_stats.geospatial_types, expected_geometry_types[i]);
                assert_eq!(geo_stats.bbox, expected_bounding_box[i]);
            } else {
                assert!(expected_geometry_types[i].is_none());
                assert!(expected_bounding_box[i].is_none());
            }
        }
    }

    fn wkb_array_xy(coords: impl IntoIterator<Item = Option<(f64, f64)>>) -> ArrayRef {
        let array = BinaryArray::from_iter(
            coords
                .into_iter()
                .map(|maybe_xy| maybe_xy.map(|(x, y)| wkb_item_xy(x, y))),
        );
        Arc::new(array)
    }

    fn wkb_item_xy(x: f64, y: f64) -> Vec<u8> {
        let mut item: [u8; 21] = [0; 21];
        item[0] = 0x01;
        item[1] = 0x01;
        item[5..13].copy_from_slice(x.to_le_bytes().as_slice());
        item[13..21].copy_from_slice(y.to_le_bytes().as_slice());
        item.to_vec()
    }
}
