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

use arrow::datatypes::Schema;
use arrow::datatypes::{DataType, Field};
use arrow::error::{ArrowError, Result};
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::FileWriter;
use arrow::util::integration_util::*;
use arrow_integration_testing::read_json_file;
use clap::Parser;
use std::fs::File;

#[derive(clap::ArgEnum, Debug, Clone)]
#[clap(rename_all = "SCREAMING_SNAKE_CASE")]
enum Mode {
    ArrowToJson,
    JsonToArrow,
    Validate,
}

#[derive(Debug, Parser)]
#[clap(author, version, about("rust arrow-json-integration-test"), long_about = None)]
struct Args {
    #[clap(short, long)]
    integration: bool,
    #[clap(short, long, help("Path to ARROW file"))]
    arrow: String,
    #[clap(short, long, help("Path to JSON file"))]
    json: String,
    #[clap(arg_enum, short, long, default_value_t = Mode::Validate, help="Mode of integration testing tool")]
    mode: Mode,
    #[clap(short, long)]
    verbose: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let arrow_file = args.arrow;
    let json_file = args.json;
    let verbose = args.verbose;
    match args.mode {
        Mode::JsonToArrow => json_to_arrow(&json_file, &arrow_file, verbose),
        Mode::ArrowToJson => arrow_to_json(&arrow_file, &json_file, verbose),
        Mode::Validate => validate(&arrow_file, &json_file, verbose),
    }
}

fn json_to_arrow(json_name: &str, arrow_name: &str, verbose: bool) -> Result<()> {
    if verbose {
        eprintln!("Converting {} to {}", json_name, arrow_name);
    }

    let json_file = read_json_file(json_name)?;

    let arrow_file = File::create(arrow_name)?;
    let mut writer = FileWriter::try_new(arrow_file, &json_file.schema)?;

    for b in json_file.batches {
        writer.write(&b)?;
    }

    writer.finish()?;

    Ok(())
}

fn arrow_to_json(arrow_name: &str, json_name: &str, verbose: bool) -> Result<()> {
    if verbose {
        eprintln!("Converting {} to {}", arrow_name, json_name);
    }

    let arrow_file = File::open(arrow_name)?;
    let reader = FileReader::try_new(arrow_file, None)?;

    let mut fields: Vec<ArrowJsonField> = vec![];
    for f in reader.schema().fields() {
        fields.push(ArrowJsonField::from(f));
    }
    let schema = ArrowJsonSchema { fields };

    let batches = reader
        .map(|batch| Ok(ArrowJsonBatch::from_batch(&batch?)))
        .collect::<Result<Vec<_>>>()?;

    let arrow_json = ArrowJson {
        schema,
        batches,
        dictionaries: None,
    };

    let json_file = File::create(json_name)?;
    serde_json::to_writer(&json_file, &arrow_json).unwrap();

    Ok(())
}

fn canonicalize_schema(schema: &Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Map(child_field, sorted) => match child_field.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => {
                    let first_field = fields.get(0).unwrap();
                    let key_field = Field::new(
                        "key",
                        first_field.data_type().clone(),
                        first_field.is_nullable(),
                    );
                    let second_field = fields.get(1).unwrap();
                    let value_field = Field::new(
                        "value",
                        second_field.data_type().clone(),
                        second_field.is_nullable(),
                    );

                    let struct_type = DataType::Struct(vec![key_field, value_field]);
                    let child_field =
                        Field::new("entries", struct_type, child_field.is_nullable());

                    Field::new(
                        field.name().as_str(),
                        DataType::Map(Box::new(child_field), *sorted),
                        field.is_nullable(),
                    )
                }
                _ => panic!(
                    "The child field of Map type should be Struct type with 2 fields."
                ),
            },
            _ => field.clone(),
        })
        .collect::<Vec<_>>();

    Schema::new(fields).with_metadata(schema.metadata().clone())
}

fn validate(arrow_name: &str, json_name: &str, verbose: bool) -> Result<()> {
    if verbose {
        eprintln!("Validating {} and {}", arrow_name, json_name);
    }

    // open JSON file
    let json_file = read_json_file(json_name)?;

    // open Arrow file
    let arrow_file = File::open(arrow_name)?;
    let mut arrow_reader = FileReader::try_new(arrow_file, None)?;
    let arrow_schema = arrow_reader.schema().as_ref().to_owned();

    // compare schemas
    if canonicalize_schema(&json_file.schema) != canonicalize_schema(&arrow_schema) {
        return Err(ArrowError::ComputeError(format!(
            "Schemas do not match. JSON: {:?}. Arrow: {:?}",
            json_file.schema, arrow_schema
        )));
    }

    let json_batches = &json_file.batches;

    // compare number of batches
    assert!(
        json_batches.len() == arrow_reader.num_batches(),
        "JSON batches and Arrow batches are unequal"
    );

    if verbose {
        eprintln!(
            "Schemas match. JSON file has {} batches.",
            json_batches.len()
        );
    }

    for json_batch in json_batches {
        if let Some(Ok(arrow_batch)) = arrow_reader.next() {
            // compare batches
            let num_columns = arrow_batch.num_columns();
            assert!(num_columns == json_batch.num_columns());
            assert!(arrow_batch.num_rows() == json_batch.num_rows());

            for i in 0..num_columns {
                assert_eq!(
                    arrow_batch.column(i).data(),
                    json_batch.column(i).data(),
                    "Arrow and JSON batch columns not the same"
                );
            }
        } else {
            return Err(ArrowError::ComputeError(
                "no more arrow batches left".to_owned(),
            ));
        }
    }

    if arrow_reader.next().is_some() {
        return Err(ArrowError::ComputeError(
            "no more json batches left".to_owned(),
        ));
    }

    Ok(())
}
