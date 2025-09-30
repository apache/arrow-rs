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
    basic::LogicalType,
    file::reader::{FileReader, SerializedFileReader},
    geospatial::bounding_box::BoundingBox,
};
use std::fs::File;

#[test]
fn test_read_geospatial_statistics_from_file() {
    let path = format!(
        "{}/geospatial/geospatial.parquet",
        arrow::util::test_util::parquet_test_data(),
    );
    let file = File::open(path).unwrap();
    let reader = SerializedFileReader::try_from(file).unwrap();
    let metadata = reader.metadata();

    // geospatial.parquet schema:
    //    optional binary field_id=-1 group (String);
    //    optional binary field_id=-1 wkt (String);
    //    optional binary field_id=-1 geometry (Geometry(crs=));
    let fields = metadata.file_metadata().schema().get_fields();
    let logical_type = fields[2].get_basic_info().logical_type().unwrap();
    assert_eq!(logical_type, LogicalType::Geometry { crs: None });

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
