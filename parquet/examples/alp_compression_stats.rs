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
//!
//! Speed processes every value in every dataset in 131,072-value (1 MiB) pages.
//! Short pages are repeated to stabilize the elapsed-time measurement and
//! normalized back to one page before being added to the dataset total. The
//! reported GB/s uses the uncompressed input size (eight bytes per value). The
//! companion script builds with `-C target-cpu=native` unless `RUSTFLAGS` is
//! already set.

use std::fs::File;
use std::hint::black_box;
use std::io::{BufRead, BufReader, Read, sink};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use arrow_array::{Float64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::basic::Type as PhysicalType;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::compression::create_codec;
use parquet::data_type::DoubleType;
use parquet::decoding::{Decoder, get_decoder};
use parquet::encoding::get_encoder;
use parquet::errors::{ParquetError, Result};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::{ColumnDescPtr, ColumnDescriptor, ColumnPath, Type};

/// Keeps input and Arrow writer memory bounded for the full CWI corpus.
const INPUT_BATCH_VALUES: usize = 128 * 1024;
/// The page size used by all three speed comparisons.
const SPEED_PAGE_VALUES: usize = 128 * 1024;

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

#[derive(Clone, Copy)]
struct Speed {
    compression: f64,
    decompression: f64,
}

struct SpeedRow {
    name: String,
    plain: Speed,
    plain_zstd: Speed,
    alp: Speed,
}

#[derive(Default)]
struct TimingTotals {
    values: usize,
    compression: f64,
    decompression: f64,
}

impl TimingTotals {
    fn add(&mut self, values: usize, compression: f64, decompression: f64) {
        self.values += values;
        self.compression += compression;
        self.decompression += decompression;
    }

    fn speed(&self) -> Speed {
        let input_gb = self.values as f64 * std::mem::size_of::<f64>() as f64 / 1_000_000_000.0;
        Speed {
            compression: input_gb / self.compression,
            decompression: input_gb / self.decompression,
        }
    }
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

    let speed_rows = measure_speed(&paths)?;
    print_table(&rows, &speed_rows);
    print_summary(&rows, &speed_rows);
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

fn print_table(rows: &[Row], speed_rows: &[SpeedRow]) {
    assert_eq!(rows.len(), speed_rows.len());
    println!("\n## Parquet compression results\n");
    println!(
        "| Dataset | Parquet choice | Compression (GB/s) | Decompression (GB/s) | Compressed size (bits/value) |"
    );
    println!("|---|---|---:|---:|---:|");
    for (row, speed) in rows.iter().zip(speed_rows) {
        assert_eq!(row.name, speed.name);
        print_result_row(
            &row.name,
            "PLAIN",
            speed.plain,
            bits_per_value(row.plain, row.num_values),
        );
        print_result_row(
            &row.name,
            "PLAIN + ZSTD",
            speed.plain_zstd,
            bits_per_value(row.plain_zstd, row.num_values),
        );
        print_result_row(
            &row.name,
            "ALP",
            speed.alp,
            bits_per_value(row.alp, row.num_values),
        );
    }

    let (plain_bits, plain_zstd_bits, alp_bits) = arithmetic_means(rows);
    let (plain_speed, plain_zstd_speed, alp_speed) = speed_arithmetic_means(speed_rows);
    print_average_row("PLAIN", plain_speed, plain_bits);
    print_average_row("PLAIN + ZSTD", plain_zstd_speed, plain_zstd_bits);
    print_average_row("ALP", alp_speed, alp_bits);

    println!(
        "\nGB/s is decimal billions of uncompressed input bytes processed per second; higher is better. Compressed size includes Parquet data-page headers but excludes the file footer. Speed processes every value in pages of up to {SPEED_PAGE_VALUES} values and excludes file I/O. PLAIN + ZSTD includes both stages: PLAIN encoding plus ZSTD compression, and ZSTD decompression plus PLAIN decoding. Short pages are repeated for timing stability and normalized to one page."
    );
}

fn print_result_row(dataset: &str, choice: &str, speed: Speed, bits: f64) {
    println!(
        "| {dataset} | {choice} | {:.3} | {:.3} | {bits:.2} |",
        speed.compression, speed.decompression
    );
}

fn print_average_row(choice: &str, speed: Speed, bits: f64) {
    println!(
        "| **ALL AVG.** | **{choice}** | **{:.3}** | **{:.3}** | **{bits:.2}** |",
        speed.compression, speed.decompression
    );
}

fn print_summary(rows: &[Row], speed_rows: &[SpeedRow]) {
    let (plain_mean, plain_zstd_mean, alp_mean) = arithmetic_means(rows);
    let (plain_speed, plain_zstd_speed, alp_speed) = speed_arithmetic_means(speed_rows);
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
    println!(
        "Arithmetic mean compression/decompression speed in GB/s: PLAIN {:.3}/{:.3}, PLAIN + ZSTD {:.3}/{:.3}, ALP {:.3}/{:.3}.",
        plain_speed.compression,
        plain_speed.decompression,
        plain_zstd_speed.compression,
        plain_zstd_speed.decompression,
        alp_speed.compression,
        alp_speed.decompression,
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

fn speed_arithmetic_means(rows: &[SpeedRow]) -> (Speed, Speed, Speed) {
    let average = |select: fn(&SpeedRow) -> Speed| Speed {
        compression: rows.iter().map(|row| select(row).compression).sum::<f64>()
            / rows.len() as f64,
        decompression: rows
            .iter()
            .map(|row| select(row).decompression)
            .sum::<f64>()
            / rows.len() as f64,
    };
    (
        average(|row| row.plain),
        average(|row| row.plain_zstd),
        average(|row| row.alp),
    )
}

fn measure_speed(paths: &[PathBuf]) -> Result<Vec<SpeedRow>> {
    let descriptor = double_column_descriptor()?;
    let mut rows = Vec::with_capacity(paths.len());

    eprintln!("Measuring full-dataset page speed");
    for (idx, path) in paths.iter().enumerate() {
        eprintln!("[{}/{}] timing {}", idx + 1, paths.len(), path.display());
        rows.push(benchmark_dataset(path, &descriptor)?);
    }

    Ok(rows)
}

fn double_column_descriptor() -> Result<ColumnDescPtr> {
    let primitive = Type::primitive_type_builder("value", PhysicalType::DOUBLE).build()?;
    Ok(Arc::new(ColumnDescriptor::new(
        Arc::new(primitive),
        0,
        0,
        ColumnPath::new(vec!["value".into()]),
    )))
}

fn benchmark_dataset(path: &Path, descriptor: &ColumnDescPtr) -> Result<SpeedRow> {
    let mut plain_encoder = get_encoder::<DoubleType>(Encoding::PLAIN, descriptor)?;
    let mut plain_decoder: Box<dyn Decoder<DoubleType>> =
        get_decoder(descriptor.clone(), Encoding::PLAIN)?;
    let mut alp_encoder = get_encoder::<DoubleType>(Encoding::ALP, descriptor)?;
    let mut alp_decoder: Box<dyn Decoder<DoubleType>> =
        get_decoder(descriptor.clone(), Encoding::ALP)?;
    let mut codec = create_codec(Compression::ZSTD(ZstdLevel::default()), &Default::default())?
        .expect("ZSTD is a compressed codec");
    let mut plain_totals = TimingTotals::default();
    let mut zstd_totals = TimingTotals::default();
    let mut alp_totals = TimingTotals::default();
    let mut alp_preset_ready = false;

    let num_values = for_each_batch(path, |values| {
        if !alp_preset_ready {
            // Build the row-group preset outside the timed region, matching the
            // paper's exclusion of first-level sampling from compression speed.
            alp_encoder.put(&values)?;
            black_box(alp_encoder.flush_buffer()?);
            alp_preset_ready = true;
        }

        let repetitions = SPEED_PAGE_VALUES.div_ceil(values.len());
        let (plain_page, compression, decompression) = benchmark_encoded_page(
            &values,
            &mut plain_encoder,
            &mut plain_decoder,
            Encoding::PLAIN,
            repetitions,
        )?;
        plain_totals.add(values.len(), compression, decompression);

        let (zstd_compression, zstd_decompression) =
            benchmark_zstd_page(&plain_page, &mut codec, repetitions)?;
        zstd_totals.add(
            values.len(),
            compression + zstd_compression,
            zstd_decompression + decompression,
        );

        let (_, compression, decompression) = benchmark_encoded_page(
            &values,
            &mut alp_encoder,
            &mut alp_decoder,
            Encoding::ALP,
            repetitions,
        )?;
        alp_totals.add(values.len(), compression, decompression);
        Ok(())
    })?;

    if num_values == 0 {
        return Err(ParquetError::General(format!(
            "{} contains no values",
            path.display()
        )));
    }

    Ok(SpeedRow {
        name: path.file_stem().unwrap().to_string_lossy().into_owned(),
        plain: plain_totals.speed(),
        plain_zstd: zstd_totals.speed(),
        alp: alp_totals.speed(),
    })
}

fn benchmark_encoded_page(
    values: &[f64],
    encoder: &mut Box<dyn parquet::encoding::Encoder<DoubleType>>,
    decoder: &mut Box<dyn Decoder<DoubleType>>,
    encoding: Encoding,
    repetitions: usize,
) -> Result<(bytes::Bytes, f64, f64)> {
    let start = Instant::now();
    let mut page = bytes::Bytes::new();
    for _ in 0..repetitions {
        encoder.put(black_box(values))?;
        page = encoder.flush_buffer()?;
        black_box(page.len());
    }
    let compression = elapsed_seconds(start, repetitions)?;

    let mut decoded = vec![0.0; values.len()];
    let start = Instant::now();
    for _ in 0..repetitions {
        decoder.set_data(page.clone(), values.len())?;
        let read = decoder.get(&mut decoded)?;
        if read != values.len() {
            return Err(ParquetError::General(format!(
                "{encoding} decoded {read} of {} values",
                values.len()
            )));
        }
        black_box(decoded[0]);
    }
    let decompression = elapsed_seconds(start, repetitions)?;
    assert_f64_bits_eq(values, &decoded, encoding)?;

    Ok((page, compression, decompression))
}

fn benchmark_zstd_page(
    plain: &bytes::Bytes,
    codec: &mut Box<dyn parquet::compression::Codec>,
    repetitions: usize,
) -> Result<(f64, f64)> {
    let mut compressed = Vec::new();
    let start = Instant::now();
    for _ in 0..repetitions {
        compressed.clear();
        codec.compress(black_box(plain.as_ref()), &mut compressed)?;
        black_box(compressed.len());
    }
    let compression = elapsed_seconds(start, repetitions)?;

    let mut decompressed = Vec::with_capacity(plain.len());
    let start = Instant::now();
    for _ in 0..repetitions {
        decompressed.clear();
        let read = codec.decompress(
            black_box(compressed.as_slice()),
            &mut decompressed,
            Some(plain.len()),
        )?;
        if read != plain.len() {
            return Err(ParquetError::General(format!(
                "ZSTD decompressed {read} of {} bytes",
                plain.len()
            )));
        }
        black_box(decompressed[0]);
    }
    let decompression = elapsed_seconds(start, repetitions)?;
    if decompressed != plain.as_ref() {
        return Err(ParquetError::General(
            "ZSTD did not reproduce the PLAIN page".into(),
        ));
    }

    Ok((compression, decompression))
}

fn elapsed_seconds(start: Instant, repetitions: usize) -> Result<f64> {
    let seconds = start.elapsed().as_secs_f64() / repetitions as f64;
    if seconds == 0.0 {
        return Err(ParquetError::General(
            "elapsed-time clock did not advance".into(),
        ));
    }
    Ok(seconds)
}

fn assert_f64_bits_eq(expected: &[f64], actual: &[f64], encoding: Encoding) -> Result<()> {
    if expected
        .iter()
        .zip(actual)
        .all(|(left, right)| left.to_bits() == right.to_bits())
    {
        return Ok(());
    }
    Err(ParquetError::General(format!(
        "{encoding} speed fixture failed to round-trip"
    )))
}
