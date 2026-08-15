// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0

//! Convert raw little-endian f64 files to one-column Parquet files.
//!
//! For every `.bin` file below the input path, writes PLAIN + ZSTD and ALP
//! Parquet files. The input is streamed in bounded batches.

use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::errors::{ParquetError, Result};
use parquet::file::properties::WriterProperties;

const VALUES_PER_BATCH: usize = 128 * 1024;

fn main() -> Result<()> {
    let mut args = std::env::args_os().skip(1);
    let input = args.next().map(PathBuf::from).unwrap_or_else(|| usage(2));
    let output = args.next().map(PathBuf::from).unwrap_or_else(|| usage(2));
    if args.next().is_some() {
        usage(2);
    }

    if !input.exists() {
        return Err(ParquetError::General(format!(
            "input path does not exist: {}",
            input.display()
        )));
    }
    fs::create_dir_all(&output)?;

    let mut inputs = Vec::new();
    collect_bin_files(&input, &mut inputs)?;
    inputs.sort();
    if inputs.is_empty() {
        return Err(ParquetError::General(format!(
            "no .bin files found below {}",
            input.display()
        )));
    }

    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float64,
        false,
    )]));
    for (index, input) in inputs.iter().enumerate() {
        eprintln!(
            "[{}/{}] converting {}",
            index + 1,
            inputs.len(),
            input.display()
        );
        convert(input, &output, schema.clone())?;
    }
    Ok(())
}

fn usage(code: i32) -> ! {
    eprintln!(
        "usage: alp_to_parquet <input-dir-or-file> <output-dir>\n\n\
         Input .bin files contain raw little-endian IEEE-754 f64 values."
    );
    std::process::exit(code);
}

fn collect_bin_files(path: &Path, output: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if path.is_file() {
        if path
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("bin"))
        {
            output.push(path.to_owned());
        }
        return Ok(());
    }
    for entry in fs::read_dir(path)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_bin_files(&path, output)?;
        } else if path
            .extension()
            .is_some_and(|ext| ext.eq_ignore_ascii_case("bin"))
        {
            output.push(path);
        }
    }
    Ok(())
}

fn convert(input: &Path, output_dir: &Path, schema: Arc<Schema>) -> Result<()> {
    let stem = input.file_stem().ok_or_else(|| {
        ParquetError::General(format!("input has no file stem: {}", input.display()))
    })?;
    let stem = stem.to_string_lossy();
    let plain_zstd_path = output_dir.join(format!("{stem}.plain.zstd.parquet"));
    let alp_path = output_dir.join(format!("{stem}.alp.parquet"));

    let plain_zstd_props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let alp_props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::ALP)
        .set_compression(Compression::UNCOMPRESSED)
        .build();

    let mut plain_zstd_writer = ArrowWriter::try_new(
        File::create(plain_zstd_path)?,
        schema.clone(),
        Some(plain_zstd_props),
    )?;
    let mut alp_writer =
        ArrowWriter::try_new(File::create(alp_path)?, schema.clone(), Some(alp_props))?;

    let mut reader = BufReader::new(File::open(input)?);
    let mut bytes = vec![0_u8; VALUES_PER_BATCH * size_of::<f64>()];
    loop {
        let mut bytes_read = 0;
        while bytes_read < bytes.len() {
            let read = reader.read(&mut bytes[bytes_read..])?;
            if read == 0 {
                break;
            }
            bytes_read += read;
        }
        if bytes_read == 0 {
            break;
        }
        if bytes_read % size_of::<f64>() != 0 {
            return Err(ParquetError::General(format!(
                "input size is not a multiple of 8 bytes: {}",
                input.display()
            )));
        }
        let values = bytes[..bytes_read]
            .chunks_exact(size_of::<f64>())
            .map(|chunk| f64::from_le_bytes(chunk.try_into().unwrap()))
            .collect::<Vec<_>>();
        let values: ArrayRef = Arc::new(Float64Array::from(values));
        let batch = RecordBatch::try_new(schema.clone(), vec![values])?;
        plain_zstd_writer.write(&batch)?;
        alp_writer.write(&batch)?;
    }
    plain_zstd_writer.close()?;
    alp_writer.close()?;
    Ok(())
}
