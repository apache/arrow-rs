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

//! Measures what the ALP encoding costs per value, against `PLAIN` and
//! `BYTE_STREAM_SPLIT`, on columns of doubles.
//!
//! # Reproducing the numbers
//!
//! The datasets are the ones the ALP paper is evaluated on, published by the
//! authors as one double per line:
//!
//! ```shell
//! git clone https://github.com/cwida/ALP /tmp/ALP
//! cargo run --release --example alp_compression_stats --features arrow,zstd -- /tmp/ALP/data/samples
//! ```
//!
//! Any directory of `.csv` files holding one double per line will do.
//!
//! # What is measured
//!
//! For each column, the values are written to an in-memory Parquet file under
//! each encoding and the *encoded page* is measured, excluding the file's
//! footer and page headers, so the number reflects the encoding alone.
//!
//! ALP's headline property is that it compresses *without* a block compressor
//! standing between the reader and the data: an ALP page can be decoded a
//! 1024-value vector at a time, so a reader can seek to one vector and decode
//! only it. Running a compressor over the page takes that away - the whole page
//! must be inflated before any value can be touched. The uncompressed columns
//! are therefore the interesting comparison, and `--zstd` is offered only to
//! show what the heavyweight alternative buys and costs.
//!
//! Note that a compressed measurement of a small column flatters ALP: zstd has
//! little to model in a few kilobytes. Prefer full-size columns before drawing
//! conclusions from the `--zstd` output.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::column::page::Page;
use parquet::errors::Result;
use parquet::file::properties::WriterProperties;
use parquet::file::reader::{FileReader, SerializedFileReader};

/// The spec's default ALP vector size, and the unit a reader can seek to.
const VECTOR_SIZE: usize = 1024;

/// One column's measurements, in bits per value.
struct Row {
    name: String,
    num_values: usize,
    alp: f64,
    plain: f64,
    byte_stream_split: f64,
    /// Share of values ALP could not encode as integers, in percent.
    exception_rate: f64,
    /// Bit width of the first vector's packed deltas.
    bit_width: u8,
    /// The first vector's decimal parameters.
    exponent: u8,
    factor: u8,
    /// Only populated with `--zstd`.
    compressed: Option<Compressed>,
}

struct Compressed {
    alp: f64,
    plain: f64,
    byte_stream_split: f64,
}

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let mut dir = None;
    let mut zstd = false;
    for arg in args.by_ref() {
        match arg.as_str() {
            "--zstd" => zstd = true,
            other => dir = Some(PathBuf::from(other)),
        }
    }
    let Some(dir) = dir else {
        eprintln!(
            "usage: alp_compression_stats <dir of csvs, one double per line> [--zstd]\n\n\
             The ALP paper's datasets:\n  \
             git clone https://github.com/cwida/ALP /tmp/ALP\n  \
             cargo run --release --example alp_compression_stats --features arrow,zstd -- /tmp/ALP/data/samples"
        );
        std::process::exit(2);
    };

    let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("cannot read {}: {e}", dir.display()))
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "csv"))
        .collect();
    paths.sort();
    assert!(!paths.is_empty(), "no .csv files in {}", dir.display());

    let mut rows = Vec::with_capacity(paths.len());
    for path in &paths {
        rows.push(measure(path, zstd)?);
    }
    // Best compression first.
    rows.sort_by(|a, b| a.alp.total_cmp(&b.alp));

    print_uncompressed(&rows);
    if zstd {
        print_compressed(&rows);
    }
    print_summary(&rows);
    Ok(())
}

fn measure(path: &Path, zstd: bool) -> Result<Row> {
    let text = std::fs::read_to_string(path).unwrap();
    let values: Vec<f64> = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            line.parse::<f64>()
                .unwrap_or_else(|e| panic!("{}: cannot parse {line:?}: {e}", path.display()))
        })
        .collect();
    assert!(!values.is_empty(), "{} is empty", path.display());

    let none = Compression::UNCOMPRESSED;
    let alp_file = write(&values, Encoding::ALP, none)?;
    let (exception_rate, bit_width, exponent, factor) = alp_vector_stats(&alp_file)?;

    let bits = |file: &Bytes| -> Result<f64> {
        Ok(encoded_bytes(file)? as f64 * 8.0 / values.len() as f64)
    };

    let compressed = if zstd {
        let zstd = Compression::ZSTD(Default::default());
        Some(Compressed {
            // The encoded page is no longer what lands on disk once a compressor
            // runs over it, so measure the column chunk as written.
            alp: chunk_bits(&write(&values, Encoding::ALP, zstd)?, values.len())?,
            plain: chunk_bits(&write(&values, Encoding::PLAIN, zstd)?, values.len())?,
            byte_stream_split: chunk_bits(
                &write(&values, Encoding::BYTE_STREAM_SPLIT, zstd)?,
                values.len(),
            )?,
        })
    } else {
        None
    };

    Ok(Row {
        name: path.file_stem().unwrap().to_string_lossy().into_owned(),
        num_values: values.len(),
        alp: bits(&alp_file)?,
        plain: bits(&write(&values, Encoding::PLAIN, none)?)?,
        byte_stream_split: bits(&write(&values, Encoding::BYTE_STREAM_SPLIT, none)?)?,
        exception_rate,
        bit_width,
        exponent,
        factor,
        compressed,
    })
}

/// Write one column of doubles to an in-memory Parquet file.
///
/// The dictionary is disabled so the requested encoding is the one actually used.
fn write(values: &[f64], encoding: Encoding, compression: Compression) -> Result<Bytes> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float64,
        false,
    )]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Float64Array::from(values.to_vec()))])?;
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_encoding(encoding)
        .set_compression(compression)
        .build();

    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(buf.into())
}

/// Size of the encoded data pages, excluding footer and page headers.
///
/// Page buffers are already decompressed when read back, so this is the size of
/// the encoding itself and is only meaningful for an uncompressed file.
fn encoded_bytes(file: &Bytes) -> Result<usize> {
    let reader = SerializedFileReader::new(file.clone())?;
    let mut total = 0;
    for row_group in 0..reader.metadata().num_row_groups() {
        let mut pages = reader.get_row_group(row_group)?.get_column_page_reader(0)?;
        while let Some(page) = pages.get_next_page()? {
            if let Page::DataPage { buf, .. } = &page {
                total += buf.len();
            }
        }
    }
    Ok(total)
}

/// Bits per value actually written to disk, taken from the column chunk metadata.
fn chunk_bits(file: &Bytes, num_values: usize) -> Result<f64> {
    let reader = SerializedFileReader::new(file.clone())?;
    let bytes: i64 = (0..reader.metadata().num_row_groups())
        .map(|rg| reader.metadata().row_group(rg).column(0).compressed_size())
        .sum();
    Ok(bytes as f64 * 8.0 / num_values as f64)
}

/// Exception rate over every vector, plus the first vector's bit width and
/// decimal parameters, read straight out of the ALP page.
///
/// The page is `[header][vector offsets][vectors...]`, and each vector starts
/// with `[exponent, factor, num_exceptions(u16)][frame_of_reference(u64), bit_width]`.
fn alp_vector_stats(file: &Bytes) -> Result<(f64, u8, u8, u8)> {
    const ALP_HEADER_SIZE: usize = 7;
    const ALP_INFO_SIZE: usize = 4;
    const FRAME_OF_REFERENCE_SIZE: usize = 8; // f64

    let reader = SerializedFileReader::new(file.clone())?;
    let mut pages = reader.get_row_group(0)?.get_column_page_reader(0)?;

    let mut exceptions = 0u64;
    let mut total_values = 0usize;
    let mut first = None;

    while let Some(page) = pages.get_next_page()? {
        let Page::DataPage {
            buf,
            num_values,
            encoding: Encoding::ALP,
            ..
        } = &page
        else {
            continue;
        };
        // The column is required, so the page holds no levels and the ALP page
        // starts at byte 0.
        let page = buf.as_ref();
        let num_values = *num_values as usize;
        total_values += num_values;

        let vector_size = 1usize << page[2];
        let body = &page[ALP_HEADER_SIZE..];
        for vector in 0..num_values.div_ceil(vector_size) {
            let at = vector * std::mem::size_of::<u32>();
            let offset = u32::from_le_bytes(body[at..at + 4].try_into().unwrap()) as usize;
            let vector = &body[offset..];
            exceptions += u64::from(u16::from_le_bytes([vector[2], vector[3]]));
            first.get_or_insert((
                vector[0],
                vector[1],
                vector[ALP_INFO_SIZE + FRAME_OF_REFERENCE_SIZE],
            ));
        }
    }

    let (exponent, factor, bit_width) = first.expect("no ALP page found");
    let rate = 100.0 * exceptions as f64 / total_values as f64;
    Ok((rate, bit_width, exponent, factor))
}

fn print_uncompressed(rows: &[Row]) {
    println!("\n## Bits per value, no compressor\n");
    println!(
        "| dataset | values | ALP | vs PLAIN | PLAIN | BYTE_STREAM_SPLIT | exceptions | bit width | (exponent, factor) |"
    );
    println!("|---|---|---|---|---|---|---|---|---|");
    for row in rows {
        println!(
            "| {} | {} | **{:.2}** | {:.2}x | {:.2} | {:.2} | {:.2}% | {} | ({}, {}) |",
            row.name,
            row.num_values,
            row.alp,
            row.plain / row.alp,
            row.plain,
            row.byte_stream_split,
            row.exception_rate,
            row.bit_width,
            row.exponent,
            row.factor,
        );
    }
}

fn print_compressed(rows: &[Row]) {
    println!("\n## Bits per value, zstd\n");
    println!("| dataset | ALP+zstd | PLAIN+zstd | BYTE_STREAM_SPLIT+zstd | ALP alone |");
    println!("|---|---|---|---|---|");
    for row in rows {
        let Some(c) = &row.compressed else { continue };
        println!(
            "| {} | {:.2} | {:.2} | {:.2} | **{:.2}** |",
            row.name, c.alp, c.plain, c.byte_stream_split, row.alp,
        );
    }
    println!(
        "\nThe last column repeats ALP without a compressor: the operating point that keeps \
         random access to a {VECTOR_SIZE}-value vector."
    );
}

fn print_summary(rows: &[Row]) {
    let mut sorted: Vec<f64> = rows.iter().map(|r| r.alp).collect();
    sorted.sort_by(f64::total_cmp);
    let median = sorted[sorted.len() / 2];

    // Ratios are a spread of scales, so the geometric mean is the honest average.
    let geomean =
        (rows.iter().map(|r| (r.plain / r.alp).ln()).sum::<f64>() / rows.len() as f64).exp();

    let losses: Vec<&Row> = rows.iter().filter(|r| r.alp >= r.plain).collect();
    println!(
        "\n{} columns. Median ALP {median:.2} bits/value, geometric mean {geomean:.2}x smaller than PLAIN.",
        rows.len()
    );
    if losses.is_empty() {
        return;
    }
    println!(
        "\nALP is larger than PLAIN on {}: {}. These columns hold real doubles rather than \
         decimals, so nearly every value becomes an exception, costing its 8 bytes plus a 2-byte \
         position. The ALP paper handles them with a second scheme, ALP-RD; Parquet deliberately \
         left it out in favour of BYTE_STREAM_SPLIT + ZSTD as the fallback, a substitution the \
         ALP author endorsed on dev@parquet.apache.org (2025-11-24), with the caveat that only \
         the first two byte streams are worth compressing - the low mantissa bytes are noise.",
        losses.len(),
        losses
            .iter()
            .map(|r| format!("{} ({:.0}% exceptions)", r.name, r.exception_rate))
            .collect::<Vec<_>>()
            .join(", "),
    );
}
