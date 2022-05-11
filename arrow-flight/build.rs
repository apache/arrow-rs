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
    env,
    fs::OpenOptions,
    io::{Read, Write},
    path::{Path, PathBuf},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let descriptor_path = PathBuf::from(env::var("OUT_DIR").unwrap()).join("proto_descriptor.bin");

    // override the build location, in order to check in the changes to proto files
    env::set_var("OUT_DIR", "src");

    // The current working directory can vary depending on how the project is being
    // built or released so we build an absolute path to the proto file
    let path = Path::new("../format/Flight.proto");
    if path.exists() {
        // avoid rerunning build if the file has not changed
        println!("cargo:rerun-if-changed=../format/Flight.proto");

        let proto_dir = Path::new("../format");
        let proto_path = Path::new("../format/Flight.proto");

        tonic_build::configure()
            .file_descriptor_set_path(&descriptor_path)
            // protoc in unbuntu builder needs this option
            .protoc_arg("--experimental_allow_proto3_optional")
            .compile(&[proto_path], &[proto_dir])?;

        // read file contents to string
        let mut file = OpenOptions::new()
            .read(true)
            .open("src/arrow.flight.protocol.rs")?;
        let mut buffer = String::new();
        file.read_to_string(&mut buffer)?;
        // append warning that file was auto-generate
        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open("src/arrow.flight.protocol.rs")?;
        file.write_all("// This file was automatically generated through the build.rs script, and should not be edited.\n\n".as_bytes())?;
        file.write_all(buffer.as_bytes())?;
    }

    // override the build location, in order to check in the changes to proto files
    env::set_var("OUT_DIR", "src/sql");
    // The current working directory can vary depending on how the project is being
    // built or released so we build an absolute path to the proto file
    let path = Path::new("../format/FlightSql.proto");
    if path.exists() {
        // avoid rerunning build if the file has not changed
        println!("cargo:rerun-if-changed=../format/FlightSql.proto");

        let proto_dir = Path::new("../format");
        let proto_path = Path::new("../format/FlightSql.proto");

        tonic_build::configure()
            .file_descriptor_set_path(&descriptor_path)
            // protoc in unbuntu builder needs this option
            .protoc_arg("--experimental_allow_proto3_optional")
            .compile(&[proto_path], &[proto_dir])?;

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
    }

    // As the proto file is checked in, the build should not fail if the file is not found
    Ok(())
}
