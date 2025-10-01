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

use arrow_schema::Schema;
use parquet::{
    arrow::{arrow_writer::ArrowWriterOptions, ArrowWriter},
    basic::LogicalType,
    schema::types::{SchemaDescriptor, Type},
};

#[test]
fn test_write_statistics() {
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

    let arrow_schema = Schema::empty();
    let options = ArrowWriterOptions::new().with_parquet_schema(schema);

    let buf = Vec::with_capacity(1024);
    let mut file_writer =
        ArrowWriter::try_new_with_options(buf, arrow_schema.into(), options).unwrap();
    file_writer.finish().unwrap();
}
