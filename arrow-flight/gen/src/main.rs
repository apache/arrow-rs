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

use std::{
    fs::OpenOptions,
    io::{Read, Write},
    path::Path,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = Path::new("../format");
    let proto_path = Path::new("../format/Flight.proto");

    tonic_build::configure()
        // protoc in Ubuntu builder needs this option
        .protoc_arg("--experimental_allow_proto3_optional")
        .out_dir("src")
        .compile_protos_with_config(prost_config(), &[proto_path], &[proto_dir])?;

    // read file contents to string
    let mut file = OpenOptions::new()
        .read(true)
        .open("src/arrow.flight.protocol.rs")?;
    let mut buffer = String::new();
    file.read_to_string(&mut buffer)?;
    // append warning that file was auto-generated
    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open("src/arrow.flight.protocol.rs")?;
    file.write_all("// This file was automatically generated through the build.rs script, and should not be edited.\n\n".as_bytes())?;
    file.write_all(buffer.as_bytes())?;

    let proto_dir = Path::new("../format");
    let proto_path = Path::new("../format/FlightSql.proto");

    tonic_build::configure()
        // protoc in Ubuntu builder needs this option
        .protoc_arg("--experimental_allow_proto3_optional")
        .out_dir("src/sql")
        .compile_protos_with_config(prost_config(), &[proto_path], &[proto_dir])?;

    // read file contents to string
    let mut file = OpenOptions::new()
        .read(true)
        .open("src/sql/arrow.flight.protocol.sql.rs")?;
    let mut buffer = String::new();
    file.read_to_string(&mut buffer)?;
    // append warning that file was auto-generate
    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open("src/sql/arrow.flight.protocol.sql.rs")?;
    file.write_all("// This file was automatically generated through the build.rs script, and should not be edited.\n\n".as_bytes())?;
    file.write_all(buffer.as_bytes())?;

    // Prost currently generates an empty file, this was fixed but then reverted
    // https://github.com/tokio-rs/prost/pull/639
    let google_protobuf_rs = Path::new("src/sql/google.protobuf.rs");
    if google_protobuf_rs.exists() && google_protobuf_rs.metadata().unwrap().len() == 0 {
        std::fs::remove_file(google_protobuf_rs).unwrap();
    }

    // As the proto file is checked in, the build should not fail if the file is not found
    Ok(())
}

fn prost_config() -> prost_build::Config {
    let mut config = prost_build::Config::new();
    config.bytes([".arrow"]);
    config
}
