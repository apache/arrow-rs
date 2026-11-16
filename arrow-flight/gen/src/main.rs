// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Generates the Rust bindings for the Arrow Flight protobuf definitions.

use std::{fs::OpenOptions, io::Write, path::Path};

use prost::Message;
use tonic_prost_build::FileDescriptorSet;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = Path::new("../format");
    let proto_path = Path::new("../format/Flight.proto");
    let descriptor_path = Path::new("src/flight_descriptor.bin");

    tonic_prost_build::configure()
        // protoc in Ubuntu builder needs this option
        .protoc_arg("--experimental_allow_proto3_optional")
        .out_dir("src")
        .file_descriptor_set_path(descriptor_path)
        .compile_with_config(prost_config(), &[proto_path], &[proto_dir])?;

    let mut descriptors = FileDescriptorSet::decode(std::fs::read(descriptor_path)?.as_slice())?;
    // Comments and source locations can differ between protobuf installations,
    // causing the vendored-code CI check to fail. Strip them after code generation
    // to preserve the generated Rust documentation.
    for file in &mut descriptors.file {
        file.source_code_info = None;
    }
    std::fs::write(descriptor_path, descriptors.encode_to_vec())?;

    let buffer = std::fs::read_to_string("src/arrow.flight.protocol.rs")?;
    // append warning that file was auto-generated
    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .open("src/arrow.flight.protocol.rs")?;
    file.write_all("// This file was automatically generated through the build.rs script, and should not be edited.\n\n".as_bytes())?;
    file.write_all(buffer.as_bytes())?;

    let proto_dir = Path::new("../format");
    let proto_path = Path::new("../format/FlightSql.proto");

    tonic_prost_build::configure()
        // protoc in Ubuntu builder needs this option
        .protoc_arg("--experimental_allow_proto3_optional")
        .out_dir("src/sql")
        .compile_with_config(prost_config(), &[proto_path], &[proto_dir])?;

    let buffer = std::fs::read_to_string("src/sql/arrow.flight.protocol.sql.rs")?;
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
