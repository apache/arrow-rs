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

extern crate parquet;

#[macro_use]
extern crate parquet_derive;

use parquet::record::RecordWriter;

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
}

#[cfg(test)]
mod tests {
    use super::*;

    use parquet::{
        file::{
            properties::WriterProperties,
            writer::{FileWriter, SerializedFileWriter},
        },
        schema::parser::parse_message_type,
    };
    use std::{env, fs, io::Write, sync::Arc};

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
        }";

        let schema = Arc::new(parse_message_type(schema_str).unwrap());

        let a_str = "hello mother".to_owned();
        let a_borrowed_string = "cool news".to_owned();
        let maybe_a_string = Some("it's true, I'm a string".to_owned());
        let maybe_a_str = Some(&a_str[..]);

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
            double: std::f64::NAN,
            maybe_float: None,
            maybe_double: Some(std::f64::MAX),
            borrowed_maybe_a_string: &maybe_a_string,
            borrowed_maybe_a_str: &maybe_a_str,
            now: chrono::Utc::now().naive_local(),
        }];

        let generated_schema = drs.as_slice().schema().unwrap();

        assert_eq!(&schema, &generated_schema);

        let props = Arc::new(WriterProperties::builder().build());
        let mut writer =
            SerializedFileWriter::new(file, generated_schema, props).unwrap();

        let mut row_group = writer.next_row_group().unwrap();
        drs.as_slice().write_to_row_group(&mut row_group).unwrap();
        writer.close_row_group(row_group).unwrap();
        writer.close().unwrap();
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
