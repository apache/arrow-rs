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
    fs::{remove_file, OpenOptions},
    io::{Read, Write},
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    prost_build::Config::new()
        .out_dir("src/")
        .compile_well_known_types()
        .extern_path(".google.protobuf", "::pbjson_types")
        .compile_protos(&["../format/orc_proto.proto"], &["../format"])?;

    // read file contents to string
    let mut file = OpenOptions::new().read(true).open("src/orc.proto.rs")?;
    let mut buffer = String::new();
    file.read_to_string(&mut buffer)?;
    // append warning that file was auto-generate
    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open("src/proto.rs")?;
    file.write_all("// This file was automatically generated through the regen.sh script, and should not be edited.\n\n".as_bytes())?;
    file.write_all(buffer.as_bytes())?;

    // since we renamed file to proto.rs to avoid period in the name
    remove_file("src/orc.proto.rs")?;

    // As the proto file is checked in, the build should not fail if the file is not found
    Ok(())
}
