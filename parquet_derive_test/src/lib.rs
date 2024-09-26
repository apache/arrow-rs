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

#![allow(clippy::approx_constant)]

use parquet_derive::{ParquetRecordReader, ParquetRecordWriter};

#[derive(ParquetRecordWriter)]
struct ACompleteRecord<'a> {
    pub a_bool: bool,
    pub a_str: &'a str,
    pub a_string: String,
    pub a_borrowed_string: &'a String,
    pub maybe_a_str: Option<&'a str>,
    pub maybe_a_string: Option<String>,
    pub i16: i16,
    pub i32: i32,
    pub u64: u64,
    pub maybe_u8: Option<u8>,
    pub maybe_i16: Option<i16>,
    pub maybe_u32: Option<u32>,
    pub maybe_usize: Option<usize>,
    pub isize: isize,
    pub float: f32,
    pub double: f64,
    pub maybe_float: Option<f32>,
    pub maybe_double: Option<f64>,
    pub borrowed_maybe_a_string: &'a Option<String>,
    pub borrowed_maybe_a_str: &'a Option<&'a str>,
    pub now: chrono::NaiveDateTime,
    pub uuid: uuid::Uuid,
    pub byte_vec: Vec<u8>,
    pub maybe_byte_vec: Option<Vec<u8>>,
    pub borrowed_byte_vec: &'a [u8],
    pub borrowed_maybe_byte_vec: &'a Option<Vec<u8>>,
    pub borrowed_maybe_borrowed_byte_vec: &'a Option<&'a [u8]>,
}

#[derive(PartialEq, ParquetRecordWriter, ParquetRecordReader, Debug)]
struct APartiallyCompleteRecord {
    pub bool: bool,
    pub string: String,
    pub i16: i16,
    pub i32: i32,
    pub u64: u64,
    pub isize: isize,
    pub float: f32,
    pub double: f64,
    pub now: chrono::NaiveDateTime,
    pub date: chrono::NaiveDate,
    pub uuid: uuid::Uuid,
    pub byte_vec: Vec<u8>,
}

// This struct has OPTIONAL columns
// If these fields are guaranteed to be valid
// we can load this struct into APartiallyCompleteRecord
#[derive(PartialEq, ParquetRecordWriter, Debug)]
struct APartiallyOptionalRecord {
    pub bool: bool,
    pub string: String,
    pub i16: Option<i16>,
    pub i32: Option<i32>,
    pub u64: Option<u64>,
    pub isize: isize,
    pub float: f32,
    pub double: f64,
    pub now: chrono::NaiveDateTime,
    pub date: chrono::NaiveDate,
    pub uuid: uuid::Uuid,
    pub byte_vec: Vec<u8>,
}

// This struct removes several fields from the "APartiallyCompleteRecord",
// and it shuffles the fields.
// we should still be able to load it from APartiallyCompleteRecord parquet file
#[derive(PartialEq, ParquetRecordReader, Debug)]
struct APrunedRecord {
    pub bool: bool,
    pub string: String,
    pub byte_vec: Vec<u8>,
    pub float: f32,
    pub double: f64,
    pub i16: i16,
    pub i32: i32,
    pub u64: u64,
    pub isize: isize,
}

#[cfg(test)]
mod tests {
    use super::*;

    use chrono::SubsecRound;
    use std::{env, fs, io::Write, sync::Arc};

    use parquet::{
        file::writer::SerializedFileWriter,
        record::{RecordReader, RecordWriter},
        schema::parser::parse_message_type,
    };

    #[test]
    fn test_parquet_derive_hello() {
        let file = get_temp_file("test_parquet_derive_hello", &[]);

        // The schema is not required, but this tests that the generated
        // schema agrees with what one would write by hand.
        let schema_str = "message rust_schema {
            REQUIRED boolean         a_bool;
            REQUIRED BINARY          a_str (STRING);
            REQUIRED BINARY          a_string (STRING);
            REQUIRED BINARY          a_borrowed_string (STRING);
            OPTIONAL BINARY          maybe_a_str (STRING);
            OPTIONAL BINARY          maybe_a_string (STRING);
            REQUIRED INT32           i16 (INTEGER(16,true));
            REQUIRED INT32           i32;
            REQUIRED INT64           u64 (INTEGER(64,false));
            OPTIONAL INT32           maybe_u8 (INTEGER(8,false));
            OPTIONAL INT32           maybe_i16 (INTEGER(16,true));
            OPTIONAL INT32           maybe_u32 (INTEGER(32,false));
            OPTIONAL INT64           maybe_usize (INTEGER(64,false));
            REQUIRED INT64           isize (INTEGER(64,true));
            REQUIRED FLOAT           float;
            REQUIRED DOUBLE          double;
            OPTIONAL FLOAT           maybe_float;
            OPTIONAL DOUBLE          maybe_double;
            OPTIONAL BINARY          borrowed_maybe_a_string (STRING);
            OPTIONAL BINARY          borrowed_maybe_a_str (STRING);
            REQUIRED INT64           now (TIMESTAMP_MILLIS);
            REQUIRED FIXED_LEN_BYTE_ARRAY (16) uuid (UUID);
            REQUIRED BINARY          byte_vec;
            OPTIONAL BINARY          maybe_byte_vec;
            REQUIRED BINARY          borrowed_byte_vec;
            OPTIONAL BINARY          borrowed_maybe_byte_vec;
            OPTIONAL BINARY          borrowed_maybe_borrowed_byte_vec;
        }";

        let schema = Arc::new(parse_message_type(schema_str).unwrap());

        let a_str = "hello mother".to_owned();
        let a_borrowed_string = "cool news".to_owned();
        let maybe_a_string = Some("it's true, I'm a string".to_owned());
        let maybe_a_str = Some(&a_str[..]);
        let borrowed_byte_vec = vec![0x68, 0x69, 0x70];
        let borrowed_maybe_byte_vec = Some(vec![0x71, 0x72]);
        let borrowed_maybe_borrowed_byte_vec = Some(&borrowed_byte_vec[..]);

        let drs: Vec<ACompleteRecord> = vec![ACompleteRecord {
            a_bool: true,
            a_str: &a_str[..],
            a_string: "hello father".into(),
            a_borrowed_string: &a_borrowed_string,
            maybe_a_str: Some(&a_str[..]),
            maybe_a_string: Some(a_str.clone()),
            i16: -45,
            i32: 456,
            u64: 4563424,
            maybe_u8: None,
            maybe_i16: Some(3),
            maybe_u32: None,
            maybe_usize: Some(4456),
            isize: -365,
            float: 3.5,
            double: f64::NAN,
            maybe_float: None,
            maybe_double: Some(f64::MAX),
            borrowed_maybe_a_string: &maybe_a_string,
            borrowed_maybe_a_str: &maybe_a_str,
            now: chrono::Utc::now().naive_local(),
            uuid: uuid::Uuid::new_v4(),
            byte_vec: vec![0x65, 0x66, 0x67],
            maybe_byte_vec: Some(vec![0x88, 0x89, 0x90]),
            borrowed_byte_vec: &borrowed_byte_vec,
            borrowed_maybe_byte_vec: &borrowed_maybe_byte_vec,
            borrowed_maybe_borrowed_byte_vec: &borrowed_maybe_borrowed_byte_vec,
        }];

        let generated_schema = drs.as_slice().schema().unwrap();

        assert_eq!(&schema, &generated_schema);

        let props = Default::default();
        let mut writer = SerializedFileWriter::new(file, generated_schema, props).unwrap();

        let mut row_group = writer.next_row_group().unwrap();
        drs.as_slice().write_to_row_group(&mut row_group).unwrap();
        row_group.close().unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn test_parquet_derive_read_write_combined() {
        let file = get_temp_file("test_parquet_derive_combined", &[]);

        let mut drs: Vec<APartiallyCompleteRecord> = vec![APartiallyCompleteRecord {
            bool: true,
            string: "a string".into(),
            i16: -45,
            i32: 456,
            u64: 4563424,
            isize: -365,
            float: 3.5,
            double: f64::NAN,
            now: chrono::Utc::now().naive_local(),
            date: chrono::naive::NaiveDate::from_ymd_opt(2015, 3, 14).unwrap(),
            uuid: uuid::Uuid::new_v4(),
            byte_vec: vec![0x65, 0x66, 0x67],
        }];

        let mut out: Vec<APartiallyCompleteRecord> = Vec::new();

        use parquet::file::{reader::FileReader, serialized_reader::SerializedFileReader};

        let generated_schema = drs.as_slice().schema().unwrap();

        let props = Default::default();
        let mut writer =
            SerializedFileWriter::new(file.try_clone().unwrap(), generated_schema, props).unwrap();

        let mut row_group = writer.next_row_group().unwrap();
        drs.as_slice().write_to_row_group(&mut row_group).unwrap();
        row_group.close().unwrap();
        writer.close().unwrap();

        let reader = SerializedFileReader::new(file).unwrap();

        let mut row_group = reader.get_row_group(0).unwrap();
        out.read_from_row_group(&mut *row_group, 1).unwrap();

        // correct for rounding error when writing milliseconds

        drs[0].now = drs[0].now.trunc_subsecs(3);

        assert!(out[0].double.is_nan()); // these three lines are necessary because NAN != NAN
        out[0].double = 0.;
        drs[0].double = 0.;

        assert_eq!(drs[0], out[0]);
    }

    #[test]
    fn test_parquet_derive_read_optional_but_valid_column() {
        let file = get_temp_file("test_parquet_derive_read_optional", &[]);
        let drs = vec![APartiallyOptionalRecord {
            bool: true,
            string: "a string".into(),
            i16: Some(-45),
            i32: Some(456),
            u64: Some(4563424),
            isize: -365,
            float: 3.5,
            double: f64::NAN,
            now: chrono::Utc::now().naive_local(),
            date: chrono::naive::NaiveDate::from_ymd_opt(2015, 3, 14).unwrap(),
            uuid: uuid::Uuid::new_v4(),
            byte_vec: vec![0x65, 0x66, 0x67],
        }];

        let generated_schema = drs.as_slice().schema().unwrap();

        let props = Default::default();
        let mut writer =
            SerializedFileWriter::new(file.try_clone().unwrap(), generated_schema, props).unwrap();

        let mut row_group = writer.next_row_group().unwrap();
        drs.as_slice().write_to_row_group(&mut row_group).unwrap();
        row_group.close().unwrap();
        writer.close().unwrap();

        use parquet::file::{reader::FileReader, serialized_reader::SerializedFileReader};
        let reader = SerializedFileReader::new(file).unwrap();
        let mut out: Vec<APartiallyCompleteRecord> = Vec::new();

        let mut row_group = reader.get_row_group(0).unwrap();
        out.read_from_row_group(&mut *row_group, 1).unwrap();

        assert_eq!(drs[0].i16.unwrap(), out[0].i16);
        assert_eq!(drs[0].i32.unwrap(), out[0].i32);
        assert_eq!(drs[0].u64.unwrap(), out[0].u64);
    }

    #[test]
    fn test_parquet_derive_read_pruned_and_shuffled_columns() {
        let file = get_temp_file("test_parquet_derive_read_pruned", &[]);
        let drs = vec![APartiallyCompleteRecord {
            bool: true,
            string: "a string".into(),
            i16: -45,
            i32: 456,
            u64: 4563424,
            isize: -365,
            float: 3.5,
            double: f64::NAN,
            now: chrono::Utc::now().naive_local(),
            date: chrono::naive::NaiveDate::from_ymd_opt(2015, 3, 14).unwrap(),
            uuid: uuid::Uuid::new_v4(),
            byte_vec: vec![0x65, 0x66, 0x67],
        }];

        let generated_schema = drs.as_slice().schema().unwrap();

        let props = Default::default();
        let mut writer =
            SerializedFileWriter::new(file.try_clone().unwrap(), generated_schema, props).unwrap();

        let mut row_group = writer.next_row_group().unwrap();
        drs.as_slice().write_to_row_group(&mut row_group).unwrap();
        row_group.close().unwrap();
        writer.close().unwrap();

        use parquet::file::{reader::FileReader, serialized_reader::SerializedFileReader};
        let reader = SerializedFileReader::new(file).unwrap();
        let mut out: Vec<APrunedRecord> = Vec::new();

        let mut row_group = reader.get_row_group(0).unwrap();
        out.read_from_row_group(&mut *row_group, 1).unwrap();

        assert_eq!(drs[0].bool, out[0].bool);
        assert_eq!(drs[0].string, out[0].string);
        assert_eq!(drs[0].byte_vec, out[0].byte_vec);
        assert_eq!(drs[0].float, out[0].float);
        assert!(drs[0].double.is_nan());
        assert!(out[0].double.is_nan());
        assert_eq!(drs[0].i16, out[0].i16);
        assert_eq!(drs[0].i32, out[0].i32);
        assert_eq!(drs[0].u64, out[0].u64);
        assert_eq!(drs[0].isize, out[0].isize);
    }

    /// Returns file handle for a temp file in 'target' directory with a provided content
    pub fn get_temp_file(file_name: &str, content: &[u8]) -> fs::File {
        // build tmp path to a file in "target/debug/testdata"
        let mut path_buf = env::current_dir().unwrap();
        path_buf.push("target");
        path_buf.push("debug");
        path_buf.push("testdata");
        fs::create_dir_all(&path_buf).unwrap();
        path_buf.push(file_name);

        // write file content
        let mut tmp_file = fs::File::create(path_buf.as_path()).unwrap();
        tmp_file.write_all(content).unwrap();
        tmp_file.sync_all().unwrap();

        // return file handle for both read and write
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path_buf.as_path());
        assert!(file.is_ok());
        file.unwrap()
    }
}
