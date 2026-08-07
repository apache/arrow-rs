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

//! Compares the on-disk size of three Parquet choices for columns of doubles:
//! `PLAIN`, `PLAIN + ZSTD`, and `ALP` without a block compressor.
//!
//! # Reproducing the numbers
//!
//! The companion script downloads and verifies the complete CWI ALP corpus,
//! extracts the 30 `f64` datasets used by the paper into a durable, gitignored
//! directory, and runs this example:
//!
//! ```shell
//! ./parquet/examples/alp_compression_stats.sh
//! ```
//!
//! Set `ALP_DATASET_DIR` to use a different dataset directory, or
//! `ALP_KEEP_ARCHIVE=1` to retain the downloaded 6.7 GiB archive after a
//! successful extraction. Downloads resume if the script is interrupted. The
//! extracted inputs occupy roughly 15 GiB, with roughly 22 GiB needed while
//! both the archive and extracted inputs are present.
//!
//! These CWI files are raw little-endian IEEE-754 `f64` values. The remaining
//! archive entries are `f32` datasets or a dummy fixture and are outside this
//! double-only benchmark. A directory of one-double-per-line CSV files, such as
//! `CWI/ALP/data/samples`, also works. Directories are searched recursively for
//! `.bin` and `.csv` files.
//!
//! # What is measured
//!
//! Each input is streamed through a Parquet writer whose output is discarded.
//! The returned Parquet metadata supplies the compressed column
//! chunk size, including data-page headers but excluding the file footer. Using
//! that same boundary for all three choices makes the bits/value figures
//! directly comparable without retaining a potentially multi-gigabyte Parquet
//! file in memory.

use std::fs::File;
use std::io::{BufRead, BufReader, Read, sink};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::errors::{ParquetError, Result};
use parquet::file::properties::WriterProperties;

/// Keeps input and Arrow writer memory bounded for the full CWI corpus.
const INPUT_BATCH_VALUES: usize = 128 * 1024;

struct Row {
    name: String,
    num_values: usize,
    plain: u64,
    plain_zstd: u64,
    alp: u64,
}

struct Measurement {
    num_values: usize,
    compressed_bytes: u64,
}

fn main() -> Result<()> {
    let input = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            eprintln!(
                "usage: alp_compression_stats <CWI complete_binaries directory or dataset file>\n\n\
             Download complete_binaries.zip as described in:\n  \
             https://github.com/cwida/ALP/blob/main/BENCHMARKING.md"
            );
            std::process::exit(2);
        });

    let mut paths = Vec::new();
    collect_datasets(&input, &mut paths)
        .unwrap_or_else(|e| panic!("cannot discover datasets in {}: {e}", input.display()));
    paths.sort();
    assert!(
        !paths.is_empty(),
        "no .bin or .csv files in {}",
        input.display()
    );

    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Float64,
        false,
    )]));
    let mut rows = Vec::with_capacity(paths.len());
    for (idx, path) in paths.iter().enumerate() {
        eprintln!("[{}/{}] measuring {}", idx + 1, paths.len(), path.display());
        rows.push(measure(path, &schema)?);
    }

    print_table(&rows);
    print_summary(&rows);
    Ok(())
}

fn collect_datasets(path: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    if path.is_file() {
        if is_dataset(path) {
            out.push(path.to_owned());
        }
        return Ok(());
    }

    for entry in std::fs::read_dir(path)? {
        let path = entry?.path();
        if path.is_dir() {
            collect_datasets(&path, out)?;
        } else if is_dataset(&path) {
            out.push(path);
        }
    }
    Ok(())
}

fn is_dataset(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("bin") || ext.eq_ignore_ascii_case("csv"))
}

fn measure(path: &Path, schema: &SchemaRef) -> Result<Row> {
    let plain = write(path, schema, Encoding::PLAIN, Compression::UNCOMPRESSED)?;
    let plain_zstd = write(
        path,
        schema,
        Encoding::PLAIN,
        Compression::ZSTD(ZstdLevel::default()),
    )?;
    let alp = write(path, schema, Encoding::ALP, Compression::UNCOMPRESSED)?;

    if plain.num_values != plain_zstd.num_values || plain.num_values != alp.num_values {
        return Err(ParquetError::General(format!(
            "{} changed length between encodings",
            path.display()
        )));
    }

    Ok(Row {
        name: path.file_stem().unwrap().to_string_lossy().into_owned(),
        num_values: plain.num_values,
        plain: plain.compressed_bytes,
        plain_zstd: plain_zstd.compressed_bytes,
        alp: alp.compressed_bytes,
    })
}

fn write(
    path: &Path,
    schema: &SchemaRef,
    encoding: Encoding,
    compression: Compression,
) -> Result<Measurement> {
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_encoding(encoding)
        .set_compression(compression)
        .build();
    let mut writer = ArrowWriter::try_new(sink(), schema.clone(), Some(props))?;

    let num_values = for_each_batch(path, |values| {
        let batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Float64Array::from(values))])?;
        writer.write(&batch)
    })?;
    let metadata = writer.close()?;
    if num_values == 0 {
        return Err(ParquetError::General(format!(
            "{} contains no values",
            path.display()
        )));
    }
    let compressed_bytes = metadata
        .row_groups()
        .iter()
        .map(|row_group| row_group.column(0).compressed_size())
        .try_fold(0u64, |total, bytes| {
            let bytes = u64::try_from(bytes).map_err(|_| {
                ParquetError::General(format!("negative column size for {}", path.display()))
            })?;
            total.checked_add(bytes).ok_or_else(|| {
                ParquetError::General(format!("column size overflow for {}", path.display()))
            })
        })?;

    Ok(Measurement {
        num_values,
        compressed_bytes,
    })
}

fn for_each_batch(path: &Path, consume: impl FnMut(Vec<f64>) -> Result<()>) -> Result<usize> {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some(ext) if ext.eq_ignore_ascii_case("bin") => read_binary(path, consume),
        Some(ext) if ext.eq_ignore_ascii_case("csv") => read_csv(path, consume),
        _ => Err(ParquetError::General(format!(
            "unsupported dataset file {}",
            path.display()
        ))),
    }
}

fn read_binary(path: &Path, mut consume: impl FnMut(Vec<f64>) -> Result<()>) -> Result<usize> {
    let mut reader = BufReader::new(File::open(path)?);
    let mut bytes = vec![0u8; INPUT_BATCH_VALUES * std::mem::size_of::<f64>()];
    let mut total = 0usize;

    loop {
        let mut filled = 0;
        while filled < bytes.len() {
            let read = reader.read(&mut bytes[filled..])?;
            if read == 0 {
                break;
            }
            filled += read;
        }
        if filled == 0 {
            break;
        }
        if filled % std::mem::size_of::<f64>() != 0 {
            return Err(ParquetError::General(format!(
                "{} has {} trailing bytes; expected raw little-endian f64 values",
                path.display(),
                filled % std::mem::size_of::<f64>()
            )));
        }

        let values: Vec<f64> = bytes[..filled]
            .chunks_exact(std::mem::size_of::<f64>())
            .map(|chunk| f64::from_le_bytes(chunk.try_into().unwrap()))
            .collect();
        total += values.len();
        consume(values)?;

        if filled < bytes.len() {
            break;
        }
    }
    Ok(total)
}

fn read_csv(path: &Path, mut consume: impl FnMut(Vec<f64>) -> Result<()>) -> Result<usize> {
    let reader = BufReader::new(File::open(path)?);
    let mut values = Vec::with_capacity(INPUT_BATCH_VALUES);
    let mut total = 0usize;

    for (idx, line) in reader.lines().enumerate() {
        let line = line?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let value = line.parse::<f64>().map_err(|e| {
            ParquetError::General(format!(
                "{}:{}: cannot parse {line:?} as f64: {e}",
                path.display(),
                idx + 1
            ))
        })?;
        values.push(value);
        if values.len() == INPUT_BATCH_VALUES {
            total += values.len();
            consume(std::mem::take(&mut values))?;
            values = Vec::with_capacity(INPUT_BATCH_VALUES);
        }
    }

    if !values.is_empty() {
        total += values.len();
        consume(values)?;
    }
    Ok(total)
}

fn bits_per_value(bytes: u64, num_values: usize) -> f64 {
    bytes as f64 * 8.0 / num_values as f64
}

fn print_table(rows: &[Row]) {
    println!("\n## Parquet bits per value\n");
    println!("| dataset | values | input MiB | PLAIN | PLAIN + ZSTD | ALP | ALP / PLAIN+ZSTD |");
    println!("|---|---:|---:|---:|---:|---:|---:|");
    for row in rows {
        let plain = bits_per_value(row.plain, row.num_values);
        let plain_zstd = bits_per_value(row.plain_zstd, row.num_values);
        let alp = bits_per_value(row.alp, row.num_values);
        println!(
            "| {} | {} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2}x |",
            row.name,
            row.num_values,
            row.num_values as f64 * 8.0 / (1024.0 * 1024.0),
            plain,
            plain_zstd,
            alp,
            alp / plain_zstd,
        );
    }

    let (plain, plain_zstd, alp) = arithmetic_means(rows);
    println!(
        "| **ALL AVG.** | — | — | **{plain:.2}** | **{plain_zstd:.2}** | **{alp:.2}** | **{:.2}x** |",
        alp / plain_zstd,
    );
}

fn print_summary(rows: &[Row]) {
    let (plain_mean, plain_zstd_mean, alp_mean) = arithmetic_means(rows);
    let mut alp_bits: Vec<f64> = rows
        .iter()
        .map(|row| bits_per_value(row.alp, row.num_values))
        .collect();
    alp_bits.sort_by(f64::total_cmp);
    let median_alp = alp_bits[alp_bits.len() / 2];

    let alp_vs_plain_geomean = (rows
        .iter()
        .map(|row| (row.alp as f64 / row.plain as f64).ln())
        .sum::<f64>()
        / rows.len() as f64)
        .exp();
    let alp_vs_zstd_geomean = (rows
        .iter()
        .map(|row| (row.alp as f64 / row.plain_zstd as f64).ln())
        .sum::<f64>()
        / rows.len() as f64)
        .exp();
    let beats_zstd = rows.iter().filter(|row| row.alp < row.plain_zstd).count();

    println!(
        "\n{} datasets. Arithmetic mean: PLAIN {plain_mean:.2}, PLAIN + ZSTD {plain_zstd_mean:.2}, ALP {alp_mean:.2} bits/value.",
        rows.len(),
    );
    println!(
        "Median ALP: {median_alp:.2} bits/value. ALP is {:.2}x the size of PLAIN and {:.2}x the size of PLAIN + ZSTD by geometric mean.",
        alp_vs_plain_geomean, alp_vs_zstd_geomean,
    );
    println!(
        "ALP is smaller than PLAIN + ZSTD on {beats_zstd}/{} datasets.",
        rows.len()
    );
}

fn arithmetic_means(rows: &[Row]) -> (f64, f64, f64) {
    let count = rows.len() as f64;
    let plain = rows
        .iter()
        .map(|row| bits_per_value(row.plain, row.num_values))
        .sum::<f64>()
        / count;
    let plain_zstd = rows
        .iter()
        .map(|row| bits_per_value(row.plain_zstd, row.num_values))
        .sum::<f64>()
        / count;
    let alp = rows
        .iter()
        .map(|row| bits_per_value(row.alp, row.num_values))
        .sum::<f64>()
        / count;
    (plain, plain_zstd, alp)
}
