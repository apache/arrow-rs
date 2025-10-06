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
    use std::sync::Arc;

    use arrow_array::{ArrayRef, BinaryArray, RecordBatch};
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
    fn test_write_statistics_arrow() {
        let arrow_schema = Arc::new(Schema::new(vec![Field::new(
            "geom",
            DataType::Binary,
            true,
        )]));
        let batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![wkb_array_xy([(1.0, 2.0), (11.0, 12.0)])],
        )
        .unwrap();
        let expected_geometry_types = vec![1];
        let expected_bounding_box = BoundingBox::new(1.0, 11.0, 2.0, 12.0);

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
        file_writer.write(&batch).unwrap();

        file_writer.finish().unwrap();
        drop(file_writer);

        // Check statistics on file read
        let all_geo_stats = read_geo_statistics(buf);
        assert_eq!(all_geo_stats.len(), 1);
        let geo_stats = all_geo_stats[0].as_ref().unwrap();

        assert_eq!(
            geo_stats.geospatial_types.as_ref().unwrap(),
            &expected_geometry_types
        );
        assert_eq!(geo_stats.bbox.as_ref().unwrap(), &expected_bounding_box);
    }

    #[test]
    fn test_write_statistics_not_arrow() {
        let column_values = [wkb_item_xy(1.0, 2.0), wkb_item_xy(11.0, 12.0)].map(ByteArray::from);
        let expected_geometry_types = vec![1];
        let expected_bounding_box = BoundingBox::new(1.0, 11.0, 2.0, 12.0);

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
        let mut rg = writer.next_row_group().unwrap();
        let mut col = rg.next_column().unwrap().unwrap();
        col.typed::<ByteArrayType>()
            .write_batch(&column_values, Some(&[1, 1]), None)
            .unwrap();
        col.close().unwrap();
        rg.close().unwrap();

        writer.close().unwrap();

        // Check statistics on file read
        let all_geo_stats = read_geo_statistics(buf);
        assert_eq!(all_geo_stats.len(), 1);
        let geo_stats = all_geo_stats[0].as_ref().unwrap();

        assert_eq!(
            geo_stats.geospatial_types.as_ref().unwrap(),
            &expected_geometry_types
        );
        assert_eq!(geo_stats.bbox.as_ref().unwrap(), &expected_bounding_box);
    }

    fn wkb_array_xy(coords: impl IntoIterator<Item = (f64, f64)>) -> ArrayRef {
        let array =
            BinaryArray::from_iter_values(coords.into_iter().map(|(x, y)| wkb_item_xy(x, y)));
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
