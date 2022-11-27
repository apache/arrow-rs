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

use arrow::error::Result;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::StreamWriter;
use clap::Parser;
use std::fs::File;
use std::io::{self, BufReader};

#[derive(Debug, Parser)]
#[clap(author, version, about("Read an arrow file and stream to stdout"), long_about = None)]
struct Args {
    file_name: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let f = File::open(args.file_name)?;
    let reader = BufReader::new(f);
    let mut reader = FileReader::try_new(reader, None)?;
    let schema = reader.schema();

    let mut writer = StreamWriter::try_new(io::stdout(), &schema)?;

    reader.try_for_each(|batch| {
        let batch = batch?;
        writer.write(&batch)
    })?;
    writer.finish()?;

    Ok(())
}
