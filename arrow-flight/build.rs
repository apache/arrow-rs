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
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::Path,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // override the build location, in order to check in the changes to proto files
    env::set_var("OUT_DIR", "src");

    // The current working directory can vary depending on how the project is being
    // built or released so we build an absolute path to the proto file
    let path = Path::new("../format/Flight.proto");
    if path.exists() {
        // avoid rerunning build if the file has not changed
        println!("cargo:rerun-if-changed=../format/Flight.proto");

        tonic_build::compile_protos("../format/Flight.proto")?;
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

    // The current working directory can vary depending on how the project is being
    // built or released so we build an absolute path to the proto file
    let path = Path::new("../format/FlightSql.proto");
    if path.exists() {
        // avoid rerunning build if the file has not changed
        println!("cargo:rerun-if-changed=../format/FlightSql.proto");

        // https://docs.rs/protoc-rust/2.27.1/protoc_rust/
        protoc_rust::Codegen::new()
            .out_dir("src/sql")
            .input(path)
            .include("../format")
            .run()
            .expect("Running protoc failed.");
        // Work around
        // https://github.com/stepancheg/rust-protobuf/issues/117
        // https://github.com/rust-lang/rust/issues/18810.
        // We open the file, add 'mod proto { }' around the contents and write it back. This allows us
        // to include! the file in lib.rs and have a proper proto module.
        // https://github.com/cartographer-project/point_cloud_viewer/blob/bb73289523/build.rs
        let proto_path = Path::new("src/sql/FlightSql.rs");
        let mut contents = String::new();
        File::open(&proto_path)
            .unwrap()
            .read_to_string(&mut contents)
            .unwrap();
        let new_contents = format!("pub mod proto {{\n{}\n}}", contents);

        File::create(&proto_path)
            .unwrap()
            .write_all(new_contents.as_bytes())
            .unwrap();
    }

    // As the proto file is checked in, the build should not fail if the file is not found
    Ok(())
}
