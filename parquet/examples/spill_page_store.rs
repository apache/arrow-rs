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

//! Demonstrates the pluggable [`PageStore`] API by implementing a **spilling**
//! page store that keeps completed Parquet pages in temp files instead of on the
//! heap.
//!
//! # Background
//!
//! Parquet requires every column chunk to be contiguous in the file, but Arrow
//! record batches arrive with all columns interleaved. So while a row group is
//! being written, [`ArrowWriter`] must buffer every column's completed,
//! compressed pages until the row group is flushed. Peak write memory therefore
//! grows with the row group size — painful for wide schemas with large, skewed
//! columns (e.g. a few `id` columns next to a pile of fat string columns).
//!
//! A [`PageStore`] lets that page buffer live somewhere other than the heap. This
//! example plugs in a [`TempFilePageStore`] (one temp file per column chunk) and
//! compares peak writer memory against the default in-memory buffering.
//!
//! # Running
//!
//! ```sh
//! # Default in-memory page buffering (baseline): peak writer memory grows with
//! # the row group.
//! cargo run --release --features cli --example spill_page_store
//!
//! # Spill completed pages to temp files: peak writer memory stays bounded by
//! # the in-flight encoder buffers, independent of the row group size.
//! cargo run --release --features cli --example spill_page_store -- --spill
//!
//! # Make the schema wider / the skew worse:
//! cargo run --release --features cli --example spill_page_store -- --spill --large-string-columns 40
//! ```
//!
//! [`ArrowWriter`]: parquet::arrow::ArrowWriter
//! [`PageStore`]: parquet::arrow::arrow_writer::PageStore

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use clap::Parser;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_writer::{
    ArrowWriterOptions, PageKey, PageStore, PageStoreFactory,
};
use parquet::basic::Compression;
use parquet::errors::Result;
use parquet::file::properties::WriterProperties;
use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System};

/// Write a skewed, wide Parquet file and compare peak writer memory with and
/// without a spilling `PageStore`.
#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    /// Number of large (~8 KiB average) string columns to write — the fat
    /// columns that make the in-memory page buffer blow up.
    #[arg(long, default_value_t = 10)]
    large_string_columns: usize,

    /// Number of small (~20 byte average) string columns to write.
    #[arg(long, default_value_t = 5)]
    small_string_columns: usize,

    /// Number of integer (`Int64`) columns to write.
    #[arg(long, default_value_t = 3)]
    int_columns: usize,

    /// Total number of rows, all funnelled into a single row group.
    #[arg(long, default_value_t = 2048)]
    rows: usize,

    /// Rows per input batch fed to the writer. Each batch is dropped right after
    /// it is written, so only the writer's internal buffering accumulates.
    #[arg(long, default_value_t = 8192)]
    batch_size: usize,

    /// Use the spilling [`TempFilePageStore`] instead of the default in-memory
    /// page buffering.
    #[arg(long)]
    spill: bool,

    /// Optional path to write the Parquet file to. Defaults to `io::sink()` so
    /// the produced file bytes never live on the heap and the reported memory
    /// reflects only the writer's page buffering.
    #[arg(long)]
    output: Option<std::path::PathBuf>,
}

/// Average length, in bytes, of values in a "large" string column.
const LARGE_AVG_LEN: usize = 8 * 1024;
/// Average length, in bytes, of values in a "small" string column.
const SMALL_AVG_LEN: usize = 20;

// ---------------------------------------------------------------------------
// The spilling page store.
//
// A `PageStore` is intentionally "dumb": it maps an opaque, store-allocated
// `PageKey` to a blob of bytes and knows nothing about pages, dictionaries, or
// ordering. The caller (`ArrowWriter`) keeps the handles and decides what they
// mean. That is all a backend has to implement to move the page buffer off the
// heap.
// ---------------------------------------------------------------------------

/// Shared counters describing how much spilling actually happened, aggregated
/// across every per-column [`TempFilePageStore`]. Each store holds an `Arc` to
/// the same instance and bumps the atomics, so the totals survive the stores
/// being dropped at row group flush.
#[derive(Debug, Default)]
struct SpillStats {
    /// Number of temp files created — one per column chunk that was opened.
    files_created: AtomicU64,
    /// Total bytes handed to `put` and written to a temp file.
    bytes_written: AtomicU64,
    /// Total bytes read back out of a temp file by `take` (at row group flush).
    bytes_read: AtomicU64,
}

/// A spilling [`PageStore`]: one temp file per column chunk.
///
/// `put` appends the blob to the file and records its `(offset, len)`; `take`
/// seeks and reads it back. The file is unlinked on creation (via
/// [`tempfile::tempfile`]) so the OS reclaims it when the store is dropped.
struct TempFilePageStore {
    file: File,
    /// Logical end of the file — where the next `put` appends.
    end: u64,
    /// `(offset, len)` for each stored blob, indexed by the `PageKey` we minted.
    locs: Vec<(u64, usize)>,
    /// Shared, cross-store spill counters (see [`SpillStats`]).
    stats: Arc<SpillStats>,
}

impl TempFilePageStore {
    fn new(stats: Arc<SpillStats>) -> Result<Self> {
        stats.files_created.fetch_add(1, Ordering::Relaxed);
        Ok(Self {
            file: tempfile::tempfile()?,
            end: 0,
            locs: Vec::new(),
            stats,
        })
    }
}

impl PageStore for TempFilePageStore {
    fn put(&mut self, value: Bytes) -> Result<PageKey> {
        // Always append at the logical end (a prior `take` may have moved the
        // OS file cursor).
        self.file.seek(SeekFrom::Start(self.end))?;
        self.file.write_all(&value)?;
        self.stats
            .bytes_written
            .fetch_add(value.len() as u64, Ordering::Relaxed);
        let key = PageKey::new(self.locs.len() as u64);
        self.locs.push((self.end, value.len()));
        self.end += value.len() as u64;
        Ok(key)
    }

    fn take(&mut self, key: PageKey) -> Result<Bytes> {
        let (offset, len) = self.locs[key.get() as usize];
        let mut buf = vec![0u8; len];
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(&mut buf)?;
        self.stats
            .bytes_read
            .fetch_add(len as u64, Ordering::Relaxed);
        Ok(Bytes::from(buf))
    }

    // `memory_size` keeps its default of 0: once a blob is handed to `put` it
    // lives in the temp file, not on the heap. That zero is what makes
    // `ArrowWriter::memory_size()` drop to just the in-flight encoder buffers.
}

/// Creates a fresh [`TempFilePageStore`] for every column chunk the writer opens,
/// handing each one an `Arc` to the shared [`SpillStats`].
#[derive(Debug)]
struct TempFilePageStoreFactory {
    stats: Arc<SpillStats>,
}

impl PageStoreFactory for TempFilePageStoreFactory {
    fn create(&self, _column_index: usize) -> Result<Box<dyn PageStore>> {
        Ok(Box::new(TempFilePageStore::new(self.stats.clone())?))
    }
}

// ---------------------------------------------------------------------------
// Schema + data generation.
// ---------------------------------------------------------------------------

/// Build the wide, skewed schema: a few integer columns, then a bunch of small
/// string columns, then the fat large string columns.
fn build_schema(args: &Args) -> SchemaRef {
    let mut fields = Vec::new();
    for i in 0..args.int_columns {
        fields.push(Field::new(format!("int_{i}"), DataType::Int64, false));
    }
    for i in 0..args.small_string_columns {
        fields.push(Field::new(format!("small_str_{i}"), DataType::Utf8, false));
    }
    for i in 0..args.large_string_columns {
        fields.push(Field::new(format!("large_str_{i}"), DataType::Utf8, false));
    }
    Arc::new(Schema::new(fields))
}

/// A tiny deterministic xorshift RNG so runs are reproducible without pulling in
/// a `rand` dependency.
struct XorShift(u64);

impl XorShift {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

/// Build a string column of `rows` rows whose values average `avg_len` bytes.
///
/// Lengths vary uniformly in `[1, 2 * avg_len)` (so the mean is ≈ `avg_len`) and
/// the content is high-entropy printable ASCII, so the values neither
/// dictionary-encode nor compress away — the page buffer holds real bytes.
fn make_string_array(rows: usize, avg_len: usize, rng: &mut XorShift) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(rows, rows * avg_len);
    let mut value = String::new();
    for _ in 0..rows {
        let len = 1 + (rng.next() as usize % (2 * avg_len - 1));
        value.clear();
        for _ in 0..len {
            // Map to printable ASCII (33..=126).
            value.push((33 + (rng.next() % 94) as u8) as char);
        }
        builder.append_value(&value);
    }
    Arc::new(builder.finish())
}

/// Build one record batch of `rows` rows for `schema`.
fn make_batch(schema: &SchemaRef, args: &Args, rows: usize, rng: &mut XorShift) -> RecordBatch {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    for _ in 0..args.int_columns {
        let vals: Vec<i64> = (0..rows).map(|_| rng.next() as i64).collect();
        columns.push(Arc::new(Int64Array::from(vals)));
    }
    for _ in 0..args.small_string_columns {
        columns.push(make_string_array(rows, SMALL_AVG_LEN, rng));
    }
    for _ in 0..args.large_string_columns {
        columns.push(make_string_array(rows, LARGE_AVG_LEN, rng));
    }
    RecordBatch::try_new(schema.clone(), columns).unwrap()
}

// ---------------------------------------------------------------------------
// Memory reporting.
// ---------------------------------------------------------------------------

/// Current process resident set size (RSS), in bytes.
fn rss_bytes(system: &mut System) -> u64 {
    let Ok(pid) = sysinfo::get_current_pid() else {
        return 0;
    };
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::everything(),
    );
    system.process(pid).map(|p| p.memory()).unwrap_or(0)
}

fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

fn main() -> Result<()> {
    let args = Args::parse();
    let schema = build_schema(&args);

    // One uncompressed row group for the whole dataset, so the page buffer (the
    // thing a PageStore governs) is the only thing that grows. Uncompressed keeps
    // the reported numbers easy to reason about — the buffer holds the raw page
    // bytes.
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_row_count(Some(args.rows * 2))
        .build();

    let mut options = ArrowWriterOptions::new().with_properties(props);
    let spill_stats = Arc::new(SpillStats::default());
    if args.spill {
        options = options.with_page_store_factory(Arc::new(TempFilePageStoreFactory {
            stats: spill_stats.clone(),
        }));
    }

    // Total logical payload across the large columns — the part that dominates.
    let large_payload = args.large_string_columns * LARGE_AVG_LEN * args.rows;
    println!(
        "Writing {} rows × {} columns ({} int, {} small-string ~{}B, {} large-string ~{}B)",
        args.rows,
        args.int_columns + args.small_string_columns + args.large_string_columns,
        args.int_columns,
        args.small_string_columns,
        SMALL_AVG_LEN,
        args.large_string_columns,
        LARGE_AVG_LEN,
    );
    println!(
        "Page buffering: {}  (large-column payload ≈ {:.1} MiB)",
        if args.spill {
            "TempFilePageStore (spilling to temp files)"
        } else {
            "InMemoryPageStore (default, on the heap)"
        },
        mib(large_payload),
    );

    let mut system = System::new_with_specifics(RefreshKind::everything());
    let rss_start = rss_bytes(&mut system);

    // The output sink. `io::sink()` discards the file bytes so they never inflate
    // the heap — the measured peak then reflects only the writer's buffering.
    let writer_sink: Box<dyn Write + Send> = match &args.output {
        Some(path) => Box::new(File::create(path)?),
        None => Box::new(std::io::sink()),
    };
    let mut writer = ArrowWriter::try_new_with_options(writer_sink, schema.clone(), options)?;

    let mut rng = XorShift(0x9E37_79B9_7F4A_7C15);
    let mut peak_writer_memory = 0usize;
    let mut peak_rss = rss_start;

    let mut written = 0;
    while written < args.rows {
        let rows = args.batch_size.min(args.rows - written);
        let batch = make_batch(&schema, &args, rows, &mut rng);
        writer.write(&batch)?;
        written += rows;
        // `batch` is dropped here — only the writer's internal page buffering
        // persists. `memory_size()` reports the bytes the writer holds *resident*
        // on the heap: with the in-memory store this climbs toward the whole row
        // group; with the spilling store it stays flat.
        peak_writer_memory = peak_writer_memory.max(writer.memory_size());
        peak_rss = peak_rss.max(rss_bytes(&mut system));
    }

    peak_writer_memory = peak_writer_memory.max(writer.memory_size());
    writer.close()?;
    peak_rss = peak_rss.max(rss_bytes(&mut system));

    println!();
    println!("Done. Wrote {written} rows.");
    println!(
        "Peak ArrowWriter::memory_size() : {:>8.1} MiB   <- bytes the writer held on the heap",
        mib(peak_writer_memory),
    );
    println!(
        "Peak process RSS delta          : {:>8.1} MiB",
        (peak_rss.saturating_sub(rss_start)) as f64 / (1024.0 * 1024.0),
    );
    println!();
    if args.spill {
        let files = spill_stats.files_created.load(Ordering::Relaxed);
        let written_bytes = spill_stats.bytes_written.load(Ordering::Relaxed);
        let read_bytes = spill_stats.bytes_read.load(Ordering::Relaxed);
        println!("Spill temp files created        : {files:>8}   <- one per column chunk");
        println!(
            "Total bytes spilled (written)   : {:>8.1} MiB",
            mib(written_bytes as usize),
        );
        println!(
            "Total bytes read back (take)    : {:>8.1} MiB",
            mib(read_bytes as usize),
        );
        println!();
        println!(
            "With spilling, peak writer memory is bounded by the in-flight encoder \n\
             buffers (a page or two per column), not the {:.1} MiB row group.",
            mib(large_payload),
        );
    } else {
        println!(
            "Re-run with --spill to keep those pages off the heap and watch peak \n\
             writer memory drop well below the {:.1} MiB row group payload.",
            mib(large_payload),
        );
    }

    Ok(())
}
