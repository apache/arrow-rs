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

//! Common code used in the integration test binaries

#![warn(missing_docs)]
use serde_json::Value;

use arrow::array::{Array, StructArray};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::error::{ArrowError, Result};
use arrow::ffi::{from_ffi_and_data_type, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;
use arrow::util::test_util::arrow_test_data;
use arrow_integration_test::*;
use std::collections::HashMap;
use std::ffi::{c_char, c_int, CStr, CString};
use std::fs::File;
use std::io::BufReader;
use std::iter::zip;
use std::ptr;
use std::sync::Arc;

/// The expected username for the basic auth integration test.
pub const AUTH_USERNAME: &str = "arrow";
/// The expected password for the basic auth integration test.
pub const AUTH_PASSWORD: &str = "flight";

pub mod flight_client_scenarios;
pub mod flight_server_scenarios;

/// An Arrow file in JSON format
pub struct ArrowFile {
    /// The schema of the file
    pub schema: Schema,
    // we can evolve this into a concrete Arrow type
    // this is temporarily not being read from
    dictionaries: HashMap<i64, ArrowJsonDictionaryBatch>,
    arrow_json: Value,
}

impl ArrowFile {
    /// Read a single [RecordBatch] from the file
    pub fn read_batch(&self, batch_num: usize) -> Result<RecordBatch> {
        let b = self.arrow_json["batches"].get(batch_num).unwrap();
        let json_batch: ArrowJsonBatch = serde_json::from_value(b.clone()).unwrap();
        record_batch_from_json(&self.schema, json_batch, Some(&self.dictionaries))
    }

    /// Read all [RecordBatch]es from the file
    pub fn read_batches(&self) -> Result<Vec<RecordBatch>> {
        self.arrow_json["batches"]
            .as_array()
            .unwrap()
            .iter()
            .map(|b| {
                let json_batch: ArrowJsonBatch = serde_json::from_value(b.clone()).unwrap();
                record_batch_from_json(&self.schema, json_batch, Some(&self.dictionaries))
            })
            .collect()
    }
}

/// Canonicalize the names of map fields in a schema
pub fn canonicalize_schema(schema: &Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Map(child_field, sorted) => match child_field.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => {
                    let first_field = &fields[0];
                    let key_field =
                        Arc::new(Field::new("key", first_field.data_type().clone(), false));
                    let second_field = &fields[1];
                    let value_field = Arc::new(Field::new(
                        "value",
                        second_field.data_type().clone(),
                        second_field.is_nullable(),
                    ));

                    let fields = Fields::from([key_field, value_field]);
                    let struct_type = DataType::Struct(fields);
                    let child_field = Field::new("entries", struct_type, false);

                    Arc::new(Field::new(
                        field.name().as_str(),
                        DataType::Map(Arc::new(child_field), *sorted),
                        field.is_nullable(),
                    ))
                }
                _ => panic!("The child field of Map type should be Struct type with 2 fields."),
            },
            _ => field.clone(),
        })
        .collect::<Fields>();

    Schema::new(fields).with_metadata(schema.metadata().clone())
}

/// Read an Arrow file in JSON format
pub fn open_json_file(json_name: &str) -> Result<ArrowFile> {
    let json_file = File::open(json_name)?;
    let reader = BufReader::new(json_file);
    let arrow_json: Value = serde_json::from_reader(reader).unwrap();
    let schema = schema_from_json(&arrow_json["schema"])?;
    // read dictionaries
    let mut dictionaries = HashMap::new();
    if let Some(dicts) = arrow_json.get("dictionaries") {
        for d in dicts
            .as_array()
            .expect("Unable to get dictionaries as array")
        {
            let json_dict: ArrowJsonDictionaryBatch =
                serde_json::from_value(d.clone()).expect("Unable to get dictionary from JSON");
            // TODO: convert to a concrete Arrow type
            dictionaries.insert(json_dict.id, json_dict);
        }
    }
    Ok(ArrowFile {
        schema,
        dictionaries,
        arrow_json,
    })
}

/// Read gzipped JSON test file
///
/// For example given the input:
/// version = `0.17.1`
/// path = `generated_union`
///
/// Returns the contents of
/// `arrow-ipc-stream/integration/0.17.1/generated_union.json.gz`
pub fn read_gzip_json(version: &str, path: &str) -> ArrowJson {
    use flate2::read::GzDecoder;
    use std::io::Read;

    let testdata = arrow_test_data();
    let file = File::open(format!(
        "{testdata}/arrow-ipc-stream/integration/{version}/{path}.json.gz"
    ))
    .unwrap();
    let mut gz = GzDecoder::new(&file);
    let mut s = String::new();
    gz.read_to_string(&mut s).unwrap();
    // convert to Arrow JSON
    let arrow_json: ArrowJson = serde_json::from_str(&s).unwrap();
    arrow_json
}

/// C Data Integration entrypoint to export the schema from a JSON file
fn cdata_integration_export_schema_from_json(
    c_json_name: *const c_char,
    out: *mut FFI_ArrowSchema,
) -> Result<()> {
    let json_name = unsafe { CStr::from_ptr(c_json_name) };
    let f = open_json_file(json_name.to_str()?)?;
    let c_schema = FFI_ArrowSchema::try_from(&f.schema)?;
    // Move exported schema into output struct
    unsafe { ptr::write(out, c_schema) };
    Ok(())
}

/// C Data Integration entrypoint to export a batch from a JSON file
fn cdata_integration_export_batch_from_json(
    c_json_name: *const c_char,
    batch_num: c_int,
    out: *mut FFI_ArrowArray,
) -> Result<()> {
    let json_name = unsafe { CStr::from_ptr(c_json_name) };
    let b = open_json_file(json_name.to_str()?)?.read_batch(batch_num.try_into().unwrap())?;
    let a = StructArray::from(b).into_data();
    let c_array = FFI_ArrowArray::new(&a);
    // Move exported array into output struct
    unsafe { ptr::write(out, c_array) };
    Ok(())
}

fn cdata_integration_import_schema_and_compare_to_json(
    c_json_name: *const c_char,
    c_schema: *mut FFI_ArrowSchema,
) -> Result<()> {
    let json_name = unsafe { CStr::from_ptr(c_json_name) };
    let json_schema = open_json_file(json_name.to_str()?)?.schema;

    // The source ArrowSchema will be released when this is dropped
    let imported_schema = unsafe { FFI_ArrowSchema::from_raw(c_schema) };
    let imported_schema = Schema::try_from(&imported_schema)?;

    // compare schemas
    if canonicalize_schema(&json_schema) != canonicalize_schema(&imported_schema) {
        return Err(ArrowError::ComputeError(format!(
            "Schemas do not match.\n- JSON: {:?}\n- Imported: {:?}",
            json_schema, imported_schema
        )));
    }
    Ok(())
}

fn compare_batches(a: &RecordBatch, b: &RecordBatch) -> Result<()> {
    if a.num_columns() != b.num_columns() {
        return Err(ArrowError::InvalidArgumentError(
            "batches do not have the same number of columns".to_string(),
        ));
    }
    for (a_column, b_column) in zip(a.columns(), b.columns()) {
        if a_column != b_column {
            return Err(ArrowError::InvalidArgumentError(
                "batch columns are not the same".to_string(),
            ));
        }
    }
    Ok(())
}

fn cdata_integration_import_batch_and_compare_to_json(
    c_json_name: *const c_char,
    batch_num: c_int,
    c_array: *mut FFI_ArrowArray,
) -> Result<()> {
    let json_name = unsafe { CStr::from_ptr(c_json_name) };
    let json_batch =
        open_json_file(json_name.to_str()?)?.read_batch(batch_num.try_into().unwrap())?;
    let schema = json_batch.schema();

    let data_type_for_import = DataType::Struct(schema.fields.clone());
    let imported_array = unsafe { FFI_ArrowArray::from_raw(c_array) };
    let imported_array = unsafe { from_ffi_and_data_type(imported_array, data_type_for_import) }?;
    imported_array.validate_full()?;
    let imported_batch = RecordBatch::from(StructArray::from(imported_array));

    compare_batches(&json_batch, &imported_batch)
}

// If Result is an error, then export a const char* to its string display, otherwise NULL
fn result_to_c_error<T, E: std::fmt::Display>(result: &std::result::Result<T, E>) -> *mut c_char {
    match result {
        Ok(_) => ptr::null_mut(),
        Err(e) => CString::new(format!("{}", e)).unwrap().into_raw(),
    }
}

/// Release a const char* exported by result_to_c_error()
///
/// # Safety
///
/// The pointer is assumed to have been obtained using CString::into_raw.
#[no_mangle]
pub unsafe extern "C" fn arrow_rs_free_error(c_error: *mut c_char) {
    if !c_error.is_null() {
        drop(unsafe { CString::from_raw(c_error) });
    }
}

/// A C-ABI for exporting an Arrow schema from a JSON file
#[no_mangle]
pub extern "C" fn arrow_rs_cdata_integration_export_schema_from_json(
    c_json_name: *const c_char,
    out: *mut FFI_ArrowSchema,
) -> *mut c_char {
    let r = cdata_integration_export_schema_from_json(c_json_name, out);
    result_to_c_error(&r)
}

/// A C-ABI to compare an Arrow schema against a JSON file
#[no_mangle]
pub extern "C" fn arrow_rs_cdata_integration_import_schema_and_compare_to_json(
    c_json_name: *const c_char,
    c_schema: *mut FFI_ArrowSchema,
) -> *mut c_char {
    let r = cdata_integration_import_schema_and_compare_to_json(c_json_name, c_schema);
    result_to_c_error(&r)
}

/// A C-ABI for exporting a RecordBatch from a JSON file
#[no_mangle]
pub extern "C" fn arrow_rs_cdata_integration_export_batch_from_json(
    c_json_name: *const c_char,
    batch_num: c_int,
    out: *mut FFI_ArrowArray,
) -> *mut c_char {
    let r = cdata_integration_export_batch_from_json(c_json_name, batch_num, out);
    result_to_c_error(&r)
}

/// A C-ABI to compare a RecordBatch against a JSON file
#[no_mangle]
pub extern "C" fn arrow_rs_cdata_integration_import_batch_and_compare_to_json(
    c_json_name: *const c_char,
    batch_num: c_int,
    c_array: *mut FFI_ArrowArray,
) -> *mut c_char {
    let r = cdata_integration_import_batch_and_compare_to_json(c_json_name, batch_num, c_array);
    result_to_c_error(&r)
}
