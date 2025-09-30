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
                algorithm: EdgeInterpolationAlgorithm::SPHERICAL,
            },
        ),
    ];

    for (geospatial_file, expected_type) in expected_logical_type {
        let metadata = read_metadata(geospatial_file);
        let logical_type = metadata
            .file_metadata()
            .schema_descr()
            .column(1)
            .logical_type()
            .unwrap();

        assert_eq!(logical_type, expected_type);
    }

    // The crs value may also contain arbitrary values (in this case some JSON
    // a bit too lengthy to type out)
    let metadata = read_metadata("crs-arbitrary-value.parquet");
    let logical_type = metadata
        .file_metadata()
        .schema_descr()
        .column(1)
        .logical_type()
        .unwrap();

    if let LogicalType::Geometry { crs } = logical_type {
        let crs_parsed: Value = serde_json::from_str(&crs.unwrap()).unwrap();
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
