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
//! being written, the writer must buffer every column's completed, compressed
//! pages until the row group is flushed. Peak write memory therefore grows with
//! the row group size — painful for wide schemas with large, skewed columns
//! (e.g. a few `id` columns next to a pile of fat string columns).
//!
//! A [`PageStore`] lets that page buffer live somewhere other than the heap. This
//! example plugs in a [`TempFilePageStore`] (one temp file per column chunk) and
//! compares peak writer memory against the default in-memory buffering.
//!
//! # Concurrency
//!
//! To exercise the store from multiple threads, this example uses the low-level
//! [`ArrowColumnWriter`] API (rather than the single-threaded [`ArrowWriter`]):
//! it spawns one worker thread per column, hands each its own
//! [`ArrowColumnWriter`], and fans each record batch's columns out to the
//! workers via [`compute_leaves`]. Every column encodes on its own thread into
//! its own [`PageStore`], and the finished [`ArrowColumnChunk`]s are spliced into
//! the row group — streaming back out of the store one page at a time — on the
//! main thread. (One thread per column is for clarity; production code would use
//! a bounded pool such as rayon or tokio.)
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
//! [`ArrowColumnWriter`]: parquet::arrow::arrow_writer::ArrowColumnWriter
//! [`ArrowColumnChunk`]: parquet::arrow::arrow_writer::ArrowColumnChunk
//! [`compute_leaves`]: parquet::arrow::arrow_writer::compute_leaves
//! [`PageStore`]: parquet::arrow::arrow_writer::PageStore

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::sync_channel;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

use tempfile::NamedTempFile;

use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use clap::Parser;
use parquet::arrow::ArrowSchemaConverter;
use parquet::arrow::arrow_writer::{
    ArrowColumnChunk, ArrowLeafColumn, ArrowRowGroupWriterFactory, PageKey, PageStore,
    PageStoreFactory, compute_leaves,
};
use parquet::basic::Compression;
use parquet::errors::Result;
use parquet::file::properties::WriterProperties;
use parquet::file::writer::SerializedFileWriter;
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

/// Shared spill bookkeeping, aggregated across every per-column
/// [`TempFilePageStore`]. Each store holds an `Arc` to the same instance, so the
/// totals and per-file records survive the stores being dropped at row group
/// flush.
#[derive(Debug, Default)]
struct SpillStats {
    /// Total bytes handed to `put` and written to a temp file.
    bytes_written: AtomicU64,
    /// Total bytes read back out of a temp file by `take` (at row group flush).
    bytes_read: AtomicU64,
    /// One record per temp file, pushed when each store is dropped.
    files: Mutex<Vec<FileRecord>>,
}

/// What a single spill temp file ended up holding.
#[derive(Debug)]
struct FileRecord {
    /// Leaf column index the store was created for.
    column_index: usize,
    /// Filesystem path of the temp file (valid while the store was alive).
    path: PathBuf,
    /// Total bytes written into the file.
    bytes: u64,
}

/// A spilling [`PageStore`]: one temp file per column chunk.
///
/// `put` appends the blob to the file and records its `(offset, len)`; `take`
/// seeks and reads it back. A [`NamedTempFile`] is used (rather than an
/// anonymous one) so the file has a reportable path; the OS reclaims it when the
/// store is dropped at row group flush.
struct TempFilePageStore {
    file: NamedTempFile,
    /// Leaf column index this store backs (used only for reporting).
    column_index: usize,
    /// Logical end of the file — where the next `put` appends.
    end: u64,
    /// `(offset, len)` for each stored blob, indexed by the `PageKey` we minted.
    locs: Vec<(u64, usize)>,
    /// Shared, cross-store spill bookkeeping (see [`SpillStats`]).
    stats: Arc<SpillStats>,
}

impl TempFilePageStore {
    fn new(stats: Arc<SpillStats>, column_index: usize) -> Result<Self> {
        Ok(Self {
            file: NamedTempFile::new()?,
            column_index,
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

impl Drop for TempFilePageStore {
    fn drop(&mut self) {
        // Record this file's path and final byte count before the NamedTempFile
        // is unlinked, so `main` can list it after the writer is closed.
        self.stats.files.lock().unwrap().push(FileRecord {
            column_index: self.column_index,
            path: self.file.path().to_path_buf(),
            bytes: self.end,
        });
    }
}

/// Creates a fresh [`TempFilePageStore`] for every column chunk the writer opens,
/// handing each one an `Arc` to the shared [`SpillStats`].
#[derive(Debug)]
struct TempFilePageStoreFactory {
    stats: Arc<SpillStats>,
}

impl PageStoreFactory for TempFilePageStoreFactory {
    fn create(&self, column_index: usize) -> Result<Box<dyn PageStore>> {
        Ok(Box::new(TempFilePageStore::new(
            self.stats.clone(),
            column_index,
        )?))
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
    let start = Instant::now();
    let args = Args::parse();
    let schema = build_schema(&args);

    // One uncompressed row group for the whole dataset, so the page buffer (the
    // thing a PageStore governs) is the only thing that grows. Uncompressed keeps
    // the reported numbers easy to reason about — the buffer holds the raw page
    // bytes.
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_max_row_group_row_count(Some(args.rows * 2))
            .build(),
    );

    let spill_stats = Arc::new(SpillStats::default());

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
    println!("Encoding columns concurrently: one worker thread per column.");

    let mut system = System::new_with_specifics(RefreshKind::everything());
    let rss_start = rss_bytes(&mut system);

    // Build the lower-level file writer so columns can be encoded in parallel.
    // `io::sink()` discards the produced file bytes so they never inflate the
    // heap — the measured peak then reflects only the writer's page buffering.
    let parquet_schema = ArrowSchemaConverter::new()
        .with_coerce_types(props.coerce_types())
        .convert(&schema)?;
    let writer_sink: Box<dyn Write + Send> = match &args.output {
        Some(path) => Box::new(File::create(path)?),
        None => Box::new(std::io::sink()),
    };
    let mut file_writer =
        SerializedFileWriter::new(writer_sink, parquet_schema.root_schema_ptr(), props.clone())?;

    // One `ArrowColumnWriter` per leaf column, each backed by its own PageStore.
    // With `--spill`, that store is a TempFilePageStore, so each worker thread's
    // completed pages land in a temp file instead of the heap.
    let mut factory = ArrowRowGroupWriterFactory::new(&file_writer, schema.clone());
    if args.spill {
        factory = factory.with_page_store_factory(Arc::new(TempFilePageStoreFactory {
            stats: spill_stats.clone(),
        }));
    }
    let col_writers = factory.create_column_writers(0)?;

    // Spawn a worker per column. Each owns its `ArrowColumnWriter`, receives
    // `ArrowLeafColumn`s over a small bounded channel (back-pressure keeps in-
    // flight input from piling up), tracks the peak bytes its writer held
    // resident, and returns the finished chunk plus that peak. The bounded
    // channel makes the workers run concurrently with batch generation.
    let workers: Vec<_> = col_writers
        .into_iter()
        .map(|mut col_writer| {
            let (send, recv) = sync_channel::<ArrowLeafColumn>(2);
            let handle = thread::spawn(move || -> Result<(ArrowColumnChunk, usize)> {
                let mut peak_memory = 0usize;
                for leaf in recv {
                    col_writer.write(&leaf)?;
                    // `memory_size()` is the bytes this column's writer holds
                    // resident — pages in its PageStore plus in-flight encoder
                    // buffers. With the in-memory store it climbs toward the whole
                    // column chunk; with spilling it stays flat.
                    peak_memory = peak_memory.max(col_writer.memory_size());
                }
                peak_memory = peak_memory.max(col_writer.memory_size());
                Ok((col_writer.close()?, peak_memory))
            });
            (handle, send)
        })
        .collect();

    // Generate batches and fan each batch's columns out to the workers. `schema`
    // is flat, so leaf order matches field order and each field maps to exactly
    // one worker.
    let mut rng = XorShift(0x9E37_79B9_7F4A_7C15);
    let mut peak_rss = rss_start;
    let mut written = 0;
    let rows = args.batch_size.min(args.rows - written);
    let batch = make_batch(&schema, &args, rows, &mut rng);
    while written < args.rows {
        //let batch = make_batch(&schema, &args, rows, &mut rng);
        for (col_idx, (field, array)) in schema.fields().iter().zip(batch.columns()).enumerate() {
            for leaf in compute_leaves(field, array)? {
                // Blocks if this worker is busy — bounding in-flight input.
                workers[col_idx].1.send(leaf).unwrap();
            }
        }
        written += rows;
        // `batch` is dropped here; its data lives on in whatever leaves are still
        // in flight to the workers.
        peak_rss = peak_rss.max(rss_bytes(&mut system));
    }

    // Signal end-of-input to every worker, then join in column order and splice
    // each finished chunk into the row group (streaming pages back out of the
    // store). Columns must be appended in schema order.
    let mut row_group_writer = file_writer.next_row_group()?;
    let mut peak_writer_memory = 0usize;
    for (handle, send) in workers {
        drop(send); // closes the channel so the worker's `for leaf in recv` ends
        let (chunk, col_peak) = handle.join().expect("worker thread panicked")?;
        peak_writer_memory += col_peak;
        chunk.append_to_row_group(&mut row_group_writer)?;
        peak_rss = peak_rss.max(rss_bytes(&mut system));
    }
    row_group_writer.close()?;
    file_writer.close()?;
    peak_rss = peak_rss.max(rss_bytes(&mut system));
    let elapsed = start.elapsed();

    println!();
    println!("Done. Wrote {written} rows.");
    println!(
        "Peak writer memory (Σ per-column): {:>8.1} MiB   <- bytes the column writers held on the heap",
        mib(peak_writer_memory),
    );
    println!(
        "Peak process RSS delta          : {:>8.1} MiB",
        (peak_rss.saturating_sub(rss_start)) as f64 / (1024.0 * 1024.0),
    );
    println!(
        "Total elapsed time              : {:>8.3} s",
        elapsed.as_secs_f64(),
    );
    println!();
    if args.spill {
        let mut files = spill_stats.files.lock().unwrap();
        files.sort_by_key(|f| f.column_index);
        let written_bytes = spill_stats.bytes_written.load(Ordering::Relaxed);
        let read_bytes = spill_stats.bytes_read.load(Ordering::Relaxed);
        println!(
            "Spill temp files created        : {:>8}   <- one per column chunk",
            files.len(),
        );
        println!(
            "Total bytes spilled (written)   : {:>8.1} MiB",
            mib(written_bytes as usize),
        );
        println!(
            "Total bytes read back (take)    : {:>8.1} MiB",
            mib(read_bytes as usize),
        );
        println!();
        println!("Per spill file (column index, type, path, bytes stored):");
        for f in files.iter() {
            // `schema` is flat, so the leaf column index is also the field index.
            let field = schema.field(f.column_index);
            println!(
                "  col {:>3}  {:<8}  {}  {} bytes ({:.1} MiB)",
                f.column_index,
                format!("{}", field.data_type()),
                f.path.display(),
                f.bytes,
                mib(f.bytes as usize),
            );
        }
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
