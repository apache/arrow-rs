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

use std::fs::File;
use std::io::{Cursor, Read};
use std::sync::Arc;

use arrow_array::*;
use arrow_csv::{Reader, ReaderBuilder};
use arrow_schema::*;

#[test]
#[cfg(feature = "chrono-tz")]
fn test_export_csv_timestamps() {
    let schema = Schema::new(vec![
        Field::new(
            "c1",
            DataType::Timestamp(
                TimeUnit::Millisecond,
                Some("Australia/Sydney".to_string()),
            ),
            true,
        ),
        Field::new("c2", DataType::Timestamp(TimeUnit::Millisecond, None), true),
    ]);

    let c1 = TimestampMillisecondArray::from(
        // 1555584887 converts to 2019-04-18, 20:54:47 in time zone Australia/Sydney (AEST).
        // The offset (difference to UTC) is +10:00.
        // 1635577147 converts to 2021-10-30 17:59:07 in time zone Australia/Sydney (AEDT)
        // The offset (difference to UTC) is +11:00. Note that daylight savings is in effect on 2021-10-30.
        //
        vec![Some(1555584887378), Some(1635577147000)],
    )
    .with_timezone("Australia/Sydney".to_string());
    let c2 =
        TimestampMillisecondArray::from(vec![Some(1555584887378), Some(1635577147000)]);
    let batch =
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(c1), Arc::new(c2)]).unwrap();

    let mut sw = Vec::new();
    let mut writer = arrow_csv::Writer::new(&mut sw);
    let batches = vec![&batch];
    for batch in batches {
        writer.write(batch).unwrap();
    }
    drop(writer);

    let left = "c1,c2
2019-04-18T20:54:47.378000000+10:00,2019-04-18T10:54:47.378000000
2021-10-30T17:59:07.000000000+11:00,2021-10-30T06:59:07.000000000\n";
    let right = String::from_utf8(sw).unwrap();
    assert_eq!(left, right);
}

#[test]
fn test_csv() {
    let _: Vec<()> = vec![None, Some("%Y-%m-%dT%H:%M:%S%.f%:z".to_string())]
        .into_iter()
        .map(|format| {
            let schema = Schema::new(vec![
                Field::new("city", DataType::Utf8, false),
                Field::new("lat", DataType::Float64, false),
                Field::new("lng", DataType::Float64, false),
            ]);

            let file = File::open("test/data/uk_cities.csv").unwrap();
            let mut csv = Reader::new(
                file,
                Arc::new(schema.clone()),
                false,
                None,
                1024,
                None,
                None,
                format,
            );
            assert_eq!(Arc::new(schema), csv.schema());
            let batch = csv.next().unwrap().unwrap();
            assert_eq!(37, batch.num_rows());
            assert_eq!(3, batch.num_columns());

            // access data from a primitive array
            let lat = batch
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            assert_eq!(57.653484, lat.value(0));

            // access data from a string array (ListArray<u8>)
            let city = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            assert_eq!("Aberdeen, Aberdeen City, UK", city.value(13));
        })
        .collect();
}

#[test]
fn test_csv_schema_metadata() {
    let mut metadata = std::collections::HashMap::new();
    metadata.insert("foo".to_owned(), "bar".to_owned());
    let schema = Schema::new_with_metadata(
        vec![
            Field::new("city", DataType::Utf8, false),
            Field::new("lat", DataType::Float64, false),
            Field::new("lng", DataType::Float64, false),
        ],
        metadata.clone(),
    );

    let file = File::open("test/data/uk_cities.csv").unwrap();

    let mut csv = Reader::new(
        file,
        Arc::new(schema.clone()),
        false,
        None,
        1024,
        None,
        None,
        None,
    );
    assert_eq!(Arc::new(schema), csv.schema());
    let batch = csv.next().unwrap().unwrap();
    assert_eq!(37, batch.num_rows());
    assert_eq!(3, batch.num_columns());

    assert_eq!(&metadata, batch.schema().metadata());
}

#[test]
fn test_csv_reader_with_decimal() {
    let schema = Schema::new(vec![
        Field::new("city", DataType::Utf8, false),
        Field::new("lat", DataType::Decimal128(38, 6), false),
        Field::new("lng", DataType::Decimal128(38, 6), false),
    ]);

    let file = File::open("test/data/decimal_test.csv").unwrap();

    let mut csv =
        Reader::new(file, Arc::new(schema), false, None, 1024, None, None, None);
    let batch = csv.next().unwrap().unwrap();
    // access data from a primitive array
    let lat = batch
        .column(1)
        .as_any()
        .downcast_ref::<Decimal128Array>()
        .unwrap();

    assert_eq!("57.653484", lat.value_as_string(0));
    assert_eq!("53.002666", lat.value_as_string(1));
    assert_eq!("52.412811", lat.value_as_string(2));
    assert_eq!("51.481583", lat.value_as_string(3));
    assert_eq!("12.123456", lat.value_as_string(4));
    assert_eq!("50.760000", lat.value_as_string(5));
    assert_eq!("0.123000", lat.value_as_string(6));
    assert_eq!("123.000000", lat.value_as_string(7));
    assert_eq!("123.000000", lat.value_as_string(8));
    assert_eq!("-50.760000", lat.value_as_string(9));
}

#[test]
fn test_csv_from_buf_reader() {
    let schema = Schema::new(vec![
        Field::new("city", DataType::Utf8, false),
        Field::new("lat", DataType::Float64, false),
        Field::new("lng", DataType::Float64, false),
    ]);

    let file_with_headers = File::open("test/data/uk_cities_with_headers.csv").unwrap();
    let file_without_headers = File::open("test/data/uk_cities.csv").unwrap();
    let both_files = file_with_headers
        .chain(Cursor::new("\n".to_string()))
        .chain(file_without_headers);
    let mut csv = Reader::from_reader(
        both_files,
        Arc::new(schema),
        true,
        None,
        1024,
        None,
        None,
        None,
    );
    let batch = csv.next().unwrap().unwrap();
    assert_eq!(74, batch.num_rows());
    assert_eq!(3, batch.num_columns());
}

#[test]
fn test_csv_with_schema_inference() {
    let file = File::open("test/data/uk_cities_with_headers.csv").unwrap();

    let builder = ReaderBuilder::new().has_header(true).infer_schema(None);

    let mut csv = builder.build(file).unwrap();
    let expected_schema = Schema::new(vec![
        Field::new("city", DataType::Utf8, true),
        Field::new("lat", DataType::Float64, true),
        Field::new("lng", DataType::Float64, true),
    ]);
    assert_eq!(Arc::new(expected_schema), csv.schema());
    let batch = csv.next().unwrap().unwrap();
    assert_eq!(37, batch.num_rows());
    assert_eq!(3, batch.num_columns());

    // access data from a primitive array
    let lat = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(57.653484, lat.value(0));

    // access data from a string array (ListArray<u8>)
    let city = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!("Aberdeen, Aberdeen City, UK", city.value(13));
}

#[test]
fn test_csv_with_schema_inference_no_headers() {
    let file = File::open("test/data/uk_cities.csv").unwrap();

    let builder = ReaderBuilder::new().infer_schema(None);

    let mut csv = builder.build(file).unwrap();

    // csv field names should be 'column_{number}'
    let schema = csv.schema();
    assert_eq!("column_1", schema.field(0).name());
    assert_eq!("column_2", schema.field(1).name());
    assert_eq!("column_3", schema.field(2).name());
    let batch = csv.next().unwrap().unwrap();
    let batch_schema = batch.schema();

    assert_eq!(schema, batch_schema);
    assert_eq!(37, batch.num_rows());
    assert_eq!(3, batch.num_columns());

    // access data from a primitive array
    let lat = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(57.653484, lat.value(0));

    // access data from a string array (ListArray<u8>)
    let city = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!("Aberdeen, Aberdeen City, UK", city.value(13));
}

#[test]
fn test_csv_builder_with_bounds() {
    let file = File::open("test/data/uk_cities.csv").unwrap();

    // Set the bounds to the lines 0, 1 and 2.
    let mut csv = ReaderBuilder::new().with_bounds(0, 2).build(file).unwrap();
    let batch = csv.next().unwrap().unwrap();

    // access data from a string array (ListArray<u8>)
    let city = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    // The value on line 0 is within the bounds
    assert_eq!("Elgin, Scotland, the UK", city.value(0));

    // The value on line 13 is outside of the bounds. Therefore
    // the call to .value() will panic.
    let result = std::panic::catch_unwind(|| city.value(13));
    assert!(result.is_err());
}

#[test]
fn test_csv_with_projection() {
    let schema = Schema::new(vec![
        Field::new("city", DataType::Utf8, false),
        Field::new("lat", DataType::Float64, false),
        Field::new("lng", DataType::Float64, false),
    ]);

    let file = File::open("test/data/uk_cities.csv").unwrap();

    let mut csv = Reader::new(
        file,
        Arc::new(schema),
        false,
        None,
        1024,
        None,
        Some(vec![0, 1]),
        None,
    );
    let projected_schema = Arc::new(Schema::new(vec![
        Field::new("city", DataType::Utf8, false),
        Field::new("lat", DataType::Float64, false),
    ]));
    assert_eq!(projected_schema, csv.schema());
    let batch = csv.next().unwrap().unwrap();
    assert_eq!(projected_schema, batch.schema());
    assert_eq!(37, batch.num_rows());
    assert_eq!(2, batch.num_columns());
}

#[test]
fn test_csv_with_dictionary() {
    let schema = Schema::new(vec![
        Field::new(
            "city",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("lat", DataType::Float64, false),
        Field::new("lng", DataType::Float64, false),
    ]);

    let file = File::open("test/data/uk_cities.csv").unwrap();

    let mut csv = Reader::new(
        file,
        Arc::new(schema),
        false,
        None,
        1024,
        None,
        Some(vec![0, 1]),
        None,
    );
    let projected_schema = Arc::new(Schema::new(vec![
        Field::new(
            "city",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("lat", DataType::Float64, false),
    ]));
    assert_eq!(projected_schema, csv.schema());
    let batch = csv.next().unwrap().unwrap();
    assert_eq!(projected_schema, batch.schema());
    assert_eq!(37, batch.num_rows());
    assert_eq!(2, batch.num_columns());

    let strings = arrow_cast::cast(batch.column(0), &DataType::Utf8).unwrap();
    let strings = strings.as_any().downcast_ref::<StringArray>().unwrap();

    assert_eq!(strings.value(0), "Elgin, Scotland, the UK");
    assert_eq!(strings.value(4), "Eastbourne, East Sussex, UK");
    assert_eq!(strings.value(29), "Uckfield, East Sussex, UK");
}

#[test]
fn test_nulls() {
    let schema = Schema::new(vec![
        Field::new("c_int", DataType::UInt64, false),
        Field::new("c_float", DataType::Float32, true),
        Field::new("c_string", DataType::Utf8, false),
    ]);

    let file = File::open("test/data/null_test.csv").unwrap();

    let mut csv = Reader::new(file, Arc::new(schema), true, None, 1024, None, None, None);
    let batch = csv.next().unwrap().unwrap();

    assert!(!batch.column(1).is_null(0));
    assert!(!batch.column(1).is_null(1));
    assert!(batch.column(1).is_null(2));
    assert!(!batch.column(1).is_null(3));
    assert!(!batch.column(1).is_null(4));
}

#[test]
fn test_nulls_with_inference() {
    let file = File::open("test/data/various_types.csv").unwrap();

    let builder = ReaderBuilder::new()
        .infer_schema(None)
        .has_header(true)
        .with_delimiter(b'|')
        .with_batch_size(512)
        .with_projection(vec![0, 1, 2, 3, 4, 5]);

    let mut csv = builder.build(file).unwrap();
    let batch = csv.next().unwrap().unwrap();

    assert_eq!(7, batch.num_rows());
    assert_eq!(6, batch.num_columns());

    let schema = batch.schema();

    assert_eq!(&DataType::Int64, schema.field(0).data_type());
    assert_eq!(&DataType::Float64, schema.field(1).data_type());
    assert_eq!(&DataType::Float64, schema.field(2).data_type());
    assert_eq!(&DataType::Boolean, schema.field(3).data_type());
    assert_eq!(&DataType::Date32, schema.field(4).data_type());
    assert_eq!(&DataType::Date64, schema.field(5).data_type());

    let names: Vec<&str> = schema.fields().iter().map(|x| x.name().as_str()).collect();
    assert_eq!(
        names,
        vec![
            "c_int",
            "c_float",
            "c_string",
            "c_bool",
            "c_date",
            "c_datetime"
        ]
    );

    assert!(schema.field(0).is_nullable());
    assert!(schema.field(1).is_nullable());
    assert!(schema.field(2).is_nullable());
    assert!(schema.field(3).is_nullable());
    assert!(schema.field(4).is_nullable());
    assert!(schema.field(5).is_nullable());

    assert!(!batch.column(1).is_null(0));
    assert!(!batch.column(1).is_null(1));
    assert!(batch.column(1).is_null(2));
    assert!(!batch.column(1).is_null(3));
    assert!(!batch.column(1).is_null(4));
}

#[test]
fn test_parse_invalid_csv() {
    let file = File::open("test/data/various_types_invalid.csv").unwrap();

    let schema = Schema::new(vec![
        Field::new("c_int", DataType::UInt64, false),
        Field::new("c_float", DataType::Float32, false),
        Field::new("c_string", DataType::Utf8, false),
        Field::new("c_bool", DataType::Boolean, false),
    ]);

    let builder = ReaderBuilder::new()
        .with_schema(Arc::new(schema))
        .has_header(true)
        .with_delimiter(b'|')
        .with_batch_size(512)
        .with_projection(vec![0, 1, 2, 3]);

    let mut csv = builder.build(file).unwrap();
    match csv.next() {
        Some(e) => match e {
            Err(e) => assert_eq!(
                "ParseError(\"Error while parsing value 4.x4 for column 1 at line 4\")",
                format!("{:?}", e)
            ),
            Ok(_) => panic!("should have failed"),
        },
        None => panic!("should have failed"),
    }
}
