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

use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{StructArray, UInt64Builder};
use arrow::datatypes::DataType::UInt64;
use arrow::datatypes::{Field, Schema};
use clap::{Parser, ValueEnum};
use parquet::arrow::ArrowWriter as ParquetWriter;
use parquet::basic::Encoding;
use parquet::errors::Result;
use parquet::file::properties::{BloomFilterPosition, WriterProperties};
use sysinfo::{MemoryRefreshKind, ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System};

#[derive(ValueEnum, Clone)]
enum BloomFilterPositionArg {
    End,
    AfterRowGroup,
}

#[derive(Parser)]
#[command(version)]
/// Writes sequences of integers, with a Bloom Filter, while logging timing and memory usage.
struct Args {
    #[arg(long, default_value_t = 1000)]
    /// Number of batches to write
    iterations: u64,

    #[arg(long, default_value_t = 1000000)]
    /// Number of rows in each batch
    batch: u64,

    #[arg(long, value_enum, default_value_t=BloomFilterPositionArg::AfterRowGroup)]
    /// Where to write Bloom Filters
    bloom_filter_position: BloomFilterPositionArg,

    /// Path to the file to write
    path: PathBuf,
}

fn now() -> String {
    chrono::Local::now().format("%Y-%m-%d %H:%M:%S").to_string()
}

fn mem(system: &mut System) -> String {
    let pid = match sysinfo::get_current_pid() {
        Ok(pid) => pid,
        Err(e) => return format!("Can't get process PID: {e}"),
    };

    let remove_dead_processes = true;
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        remove_dead_processes,
        ProcessRefreshKind::everything(),
    );

    system
        .process(pid)
        .map(|proc| format!("{}MB", proc.memory() / 1_000_000))
        .unwrap_or("N/A".to_string())
}

fn main() -> Result<()> {
    let args = Args::parse();

    let bloom_filter_position = match args.bloom_filter_position {
        BloomFilterPositionArg::End => BloomFilterPosition::End,
        BloomFilterPositionArg::AfterRowGroup => BloomFilterPosition::AfterRowGroup,
    };

    let properties = WriterProperties::builder()
        .set_column_bloom_filter_enabled("id".into(), true)
        .set_column_encoding("id".into(), Encoding::DELTA_BINARY_PACKED)
        .set_bloom_filter_position(bloom_filter_position)
        .build();
    let schema = Arc::new(Schema::new(vec![Field::new("id", UInt64, false)]));
    // Create parquet file that will be read.
    let file = File::create(args.path).unwrap();
    let mut writer = ParquetWriter::try_new(file, schema.clone(), Some(properties))?;

    let mut system =
        System::new_with_specifics(RefreshKind::new().with_memory(MemoryRefreshKind::everything()));
    eprintln!(
        "{} Writing {} batches of {} rows. RSS = {}",
        now(),
        args.iterations,
        args.batch,
        mem(&mut system)
    );

    let mut array_builder = UInt64Builder::new();
    let mut last_log = Instant::now();
    for i in 0..args.iterations {
        if Instant::now() - last_log > Duration::new(10, 0) {
            last_log = Instant::now();
            eprintln!(
                "{} Iteration {}/{}. RSS = {}",
                now(),
                i + 1,
                args.iterations,
                mem(&mut system)
            );
        }
        for j in 0..args.batch {
            array_builder.append_value(i + j);
        }
        writer.write(
            &StructArray::new(
                schema.fields().clone(),
                vec![Arc::new(array_builder.finish())],
                None,
            )
            .into(),
        )?;
    }
    writer.flush()?;
    writer.close()?;

    eprintln!("{} Done. RSS = {}", now(), mem(&mut system));

    Ok(())
}
