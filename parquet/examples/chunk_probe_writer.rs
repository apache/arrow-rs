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

//! An adaptive Parquet writer that measures encodings instead of guessing them.
//!
//! The writer's default encoding choices are made ahead of time from the schema
//! and a handful of size limits. They cannot know whether a particular column's
//! actual values compress better as a dictionary, as deltas, or as plain
//! values. This example shows how a caller can find out by measurement, using
//! [`ArrowRowGroupWriterFactory::create_column_writer`], which builds a writer
//! for one leaf column of one row group at properties of the caller's choosing.
//!
//! For each row group, and for each column that is due to decide, the writer
//! encodes a probe of that column's leading rows once per candidate set of
//! writer properties, through throwaway single-column writers. Closing
//! one yields a `ColumnCloseResult` whose metadata carries the compressed size
//! that candidate actually achieved. The smallest wins, the probe chunks are
//! discarded, and the row group is then written for real through one ordinary
//! column writer per column at that column's current choice.
//!
//! The cost model is deliberately simple. A column that is deciding encodes its
//! probe K + 1 times: K throwaway passes plus the real one. The probe is one
//! data page worth of rows rather than a whole row group, so the extra work is
//! bounded by the page size and not by the data. A column that has settled costs
//! nothing extra at all: no probe writer is built for it, so no page store is
//! allocated for it either. That is what addressing one column at a time buys
//! over building every column writer at once.
//!
//! Growing the probe instead, a page at a time until the leader is decisive, is
//! a natural next step and works. It is left out here because it did not pay:
//! measured over TPC-H and ClickBench it bought under a point of compression for
//! 10 to 30 percent more write CPU, since a chunk's compressed size can only be
//! read once the chunk is closed, so each extra page re-encodes the ones before
//! it. A caller whose data rewards a longer look can grow the probe the same
//! way; this example keeps one page.
//!
//! # Deciding, and how often to decide again
//!
//! A race that comes back close decides nothing. A leader that has not beaten
//! the runner-up by [`DECISIVE_MARGIN`] has won by an amount as likely to be an
//! accident of this row group as a property of the column, so it is used for
//! this row group but held provisionally, and the column races again on the very
//! next one. A column whose leader is decisive doubles the interval before its
//! next race, up to [`MAX_RERACE_INTERVAL`] row groups, so a column with an
//! obvious answer stops paying to be asked it, while a column whose data keeps
//! drifting is asked often.
//!
//! Closer still, inside [`NEAR_TIE_BAND`], the sizes are not telling the
//! candidates apart at all. There the tie is broken toward the page a reader
//! decodes most cheaply rather than toward a nominally smaller chunk, and it is
//! broken deterministically, never by a coin flip.
//!
//! # Dictionary candidates
//!
//! A dictionary is not a special case here, because a probe measures it rather
//! than estimating it. Closing a probe writer produces a complete column chunk,
//! and the `compressed_size` on its `ColumnCloseResult` is the size of the whole
//! chunk: the dictionary page, then every data page that indexes into it. A
//! dictionary candidate is therefore charged the full cost of the dictionary it
//! built, and cheap looking `RLE_DICTIONARY` data pages cannot flatter it. So
//! "is a dictionary worthwhile for this column" needs no ratio or cardinality
//! heuristic; it is decided the same way as every other candidate, by the
//! smallest measured chunk winning.
//!
//! What a probe cannot see is the rest of the row group. A prefix's distinct
//! value count is only a lower bound on the row group's, so a prefix can flatter
//! a dictionary that the full data would not sustain. Two things keep that from
//! being a problem. The first is the writer's own fallback: a column chunk may
//! carry at most one dictionary page, so when the dictionary being built passes
//! `dictionary_page_size_limit`, the writer emits that page for the values
//! indexed so far, flushes the data pages that reference it, and encodes the
//! remainder of the chunk with the fallback encoding. The chunk stays valid and
//! the choice degrades instead of failing. The second is the re-race above,
//! which is what moves a column off a dictionary in later row groups once the
//! data has drifted away from what the probe suggested.
//!
//! ```text
//! cargo run --example chunk_probe_writer --features arrow
//! ```

use std::alloc::{GlobalAlloc, Layout, System};
use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use arrow_array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::{
    ArrowColumnWriter, ArrowLeafColumn, ArrowWriter, compute_leaves,
};
use parquet::basic::{Compression, Encoding, Type as PhysicalType};
use parquet::errors::Result;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder, WriterPropertiesPtr};
use parquet::schema::types::{ColumnDescPtr, ColumnPath};

/// Total rows in the generated dataset.
const ROWS: usize = 400_000;
/// Rows per row group. Eight row groups, so re-racing has something to do.
const ROW_GROUP_ROWS: usize = 50_000;
/// Rows per record batch handed to the writers.
const BATCH_ROWS: usize = 10_000;
/// Rows per data page, shared by both writers so their output is comparable.
/// This is also the length of a probe, so what a probe measures is one whole
/// data page: enough for a dictionary candidate to have built a real dictionary
/// and for the compressor to have something to chew on.
const DATA_PAGE_ROWS: usize = 10_000;

/// How much smaller than the runner-up the leading candidate's chunk must be
/// for the race to count as decided, and so for the column to be allowed to
/// skip row groups before racing again.
const DECISIVE_MARGIN: f64 = 0.10;

/// How close two candidates must be before the difference between them stops
/// counting as a size difference at all and the tie is broken on decoding cost.
///
/// Deliberately much narrower than [`DECISIVE_MARGIN`], and measured rather than
/// guessed. A band as wide as the decisive margin gives away real wins: on
/// TPC-H, delta beats plain or dictionary by 5 to 10 percent on many columns, so
/// a 10 percent band hands those columns to the cheaper-to-decode candidate and
/// costs several percent of the whole file. At 2 percent the tie-break is free
/// on that data, and still catches the case it is for, where the sizes are close
/// enough that picking the smaller one is reading noise.
const NEAR_TIE_BAND: f64 = 0.02;

/// The most row groups a column may go without racing again.
///
/// A column that keeps winning decisively doubles its way up to this cadence.
/// Racing again is also what corrects a dictionary that a short probe made look
/// better than the whole column can sustain, so the cadence is capped rather
/// than allowed to grow without limit.
const MAX_RERACE_INTERVAL: usize = 8;

// ---------------------------------------------------------------------------
// Measurement: what each arm of the comparison costs
// ---------------------------------------------------------------------------

/// A pass-through allocator that counts live bytes and keeps a high water mark,
/// so each arm below can report the heap it actually needed.
///
/// Counting in the allocator, rather than reading RSS, keeps the number about
/// what this program asked for instead of what the OS chose to reclaim.
struct CountingAllocator;

/// Bytes handed out and not yet freed.
static LIVE: AtomicUsize = AtomicUsize::new(0);
/// The largest `LIVE` has reached since the last [`reset_peak`].
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) };
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// Drops the high water mark to the bytes live right now, and returns that
/// baseline. Without this an arm's peak would be swamped by the input batches,
/// which are much larger than anything the writers themselves allocate.
fn reset_peak() -> usize {
    let live = LIVE.load(Ordering::Relaxed);
    PEAK.store(live, Ordering::Relaxed);
    live
}

/// How far above `baseline` the live bytes rose since [`reset_peak`].
fn peak_over(baseline: usize) -> usize {
    PEAK.load(Ordering::Relaxed).saturating_sub(baseline)
}

/// Bytes as mebibytes, for the report.
fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

// ---------------------------------------------------------------------------
// Candidates: the property sets a column races against each other
// ---------------------------------------------------------------------------

/// One candidate way of encoding a column.
///
/// Each variant is only a recipe for a few per-column settings on a
/// [`WriterPropertiesBuilder`]. Nothing here reaches inside the writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Candidate {
    /// Dictionary encoding, which the writer would pick by default.
    Dictionary,
    /// No dictionary, values written as they are.
    Plain,
    /// No dictionary, delta encoded. Only for physical types that have a delta
    /// encoding.
    Delta,
}

impl Candidate {
    fn name(self) -> &'static str {
        match self {
            Candidate::Dictionary => "dictionary",
            Candidate::Plain => "plain",
            Candidate::Delta => "delta",
        }
    }

    /// What a reader pays to decode this candidate's data pages, cheapest
    /// first. `PLAIN` is a memcpy, and `RLE_DICTIONARY` is a bit-unpacked index
    /// followed by a lookup in a dictionary page the reader decodes once per
    /// chunk. The `DELTA_*` encodings are dearer: every value comes out of a
    /// bit-packed miniblock through a running sum, and `DELTA_BYTE_ARRAY` also
    /// reassembles each value from a prefix of the one before it.
    fn decode_rank(self) -> u8 {
        match self {
            Candidate::Dictionary | Candidate::Plain => 0,
            Candidate::Delta => 1,
        }
    }

    /// The delta encoding for a physical type, if it has one. DOUBLE does not,
    /// which is why a float column races only two candidates.
    fn delta_encoding(physical: PhysicalType) -> Option<Encoding> {
        match physical {
            PhysicalType::INT32 | PhysicalType::INT64 => Some(Encoding::DELTA_BINARY_PACKED),
            PhysicalType::BYTE_ARRAY => Some(Encoding::DELTA_BYTE_ARRAY),
            _ => None,
        }
    }

    /// The candidates worth racing for one column.
    fn for_column(descr: &ColumnDescPtr) -> Vec<Candidate> {
        let mut out = vec![Candidate::Dictionary, Candidate::Plain];
        if Self::delta_encoding(descr.physical_type()).is_some() {
            out.push(Candidate::Delta);
        }
        out
    }

    /// Applies this candidate's settings for `column` to `builder`.
    fn apply(
        self,
        builder: WriterPropertiesBuilder,
        column: ColumnPath,
        physical: PhysicalType,
    ) -> WriterPropertiesBuilder {
        match self {
            Candidate::Dictionary => builder.set_column_dictionary_enabled(column, true),
            Candidate::Plain => builder
                .set_column_dictionary_enabled(column.clone(), false)
                .set_column_encoding(column, Encoding::PLAIN),
            Candidate::Delta => {
                let encoding = Self::delta_encoding(physical)
                    .expect("a Delta candidate is only built for a type that has one");
                builder
                    .set_column_dictionary_enabled(column.clone(), false)
                    .set_column_encoding(column, encoding)
            }
        }
    }
}

/// What the writer knows about one column as it works through the row groups.
struct ColumnState {
    path: ColumnPath,
    physical: PhysicalType,
    candidates: Vec<Candidate>,
    /// The candidate in force: the last race's winner, or the writer's own
    /// default until a race has run.
    current: Candidate,
    /// The next row group this column races on.
    next_race: usize,
    /// Row groups to wait before racing again. Doubles after every decisive
    /// race, up to [`MAX_RERACE_INTERVAL`], and drops back to one row group as
    /// soon as a race comes back short of [`DECISIVE_MARGIN`].
    race_interval: usize,
}

impl ColumnState {
    fn new(descr: &ColumnDescPtr) -> Self {
        let candidates = Candidate::for_column(descr);
        Self {
            path: descr.path().clone(),
            physical: descr.physical_type(),
            current: candidates[0],
            candidates,
            next_race: 0,
            race_interval: 1,
        }
    }

    /// Whether this column races on `row_group`. Every column races on the
    /// first one; after that the cadence below decides.
    fn is_racing(&self, row_group: usize) -> bool {
        row_group >= self.next_race
    }

    /// Records the outcome of one race, and scales the cadence to it.
    ///
    /// A decisive winner is worth asking about less often, so the interval
    /// doubles: a column whose answer is obvious pays for a probe on a
    /// vanishing fraction of its row groups. A race the leader did not win
    /// decisively is not an answer to keep, so the interval drops back to the
    /// minimum and the column races on the next row group.
    fn record(&mut self, row_group: usize, winner: Candidate, margin: f64) {
        self.current = winner;
        self.race_interval = if margin >= DECISIVE_MARGIN {
            (self.race_interval * 2).min(MAX_RERACE_INTERVAL)
        } else {
            1
        };
        self.next_race = row_group + self.race_interval;
    }
}

// ---------------------------------------------------------------------------
// The adaptive writer
// ---------------------------------------------------------------------------

/// Writes `groups` to `path`, choosing each column's encoding by measurement.
///
/// Returns the candidate each column ended on, in column order.
fn write_with_probes(
    schema: &SchemaRef,
    groups: &[Vec<RecordBatch>],
    props: &WriterProperties,
    path: &Path,
) -> Result<Vec<Candidate>> {
    let arrow_writer =
        ArrowWriter::try_new(File::create(path)?, schema.clone(), Some(props.clone()))?;
    // Taking the file writer and the row group writer factory apart from the
    // ArrowWriter is what gives access to individual column writers.
    let (mut file_writer, factory) = arrow_writer.into_serialized_writer()?;

    let mut columns: Vec<ColumnState> = file_writer
        .schema_descr()
        .columns()
        .iter()
        .map(ColumnState::new)
        .collect();

    for (row_group, group) in groups.iter().enumerate() {
        // Step 1: cut the probe, the leading data page of this row group, and
        // flatten it once into leaf columns. Every probe writer below is fed
        // from it. A row group in which nothing is racing cuts no probe at all.
        let probe: Vec<Vec<ArrowLeafColumn>> = if columns.iter().any(|c| c.is_racing(row_group)) {
            probe_page(group)
                .iter()
                .map(|batch| columns_of(schema, batch))
                .collect::<Result<_>>()?
        } else {
            Vec::new()
        };

        // Step 2: race the candidates, one column at a time. A column that is
        // not due is skipped entirely, so nothing at all is built for it.
        for (idx, column) in columns.iter_mut().enumerate() {
            if !column.is_racing(row_group) {
                continue;
            }

            // One throwaway writer per candidate, all of them for this one leaf
            // column, each at that candidate's properties.
            let mut probes: Vec<ArrowColumnWriter> = column
                .candidates
                .iter()
                .map(|candidate| {
                    let props = candidate_props(props, column, *candidate);
                    factory.create_column_writer(row_group, idx, &props)
                })
                .collect::<Result<_>>()?;

            for leaves in &probe {
                for writer in &mut probes {
                    writer.write(&leaves[idx])?;
                }
            }

            // Closing a probe yields a `ColumnCloseResult` carrying the
            // compressed size that candidate actually achieved, counting every
            // page of the chunk: for a dictionary candidate that is the
            // dictionary page as well as the data pages indexing into it, so
            // each candidate is charged its full cost. The column chunks
            // themselves are thrown away; only the sizes matter.
            let mut sizes: Vec<u64> = Vec::with_capacity(probes.len());
            for writer in probes {
                let chunk = writer.close()?;
                sizes.push(chunk.close().metadata.compressed_size() as u64);
            }

            let (winner, margin) = decide(&column.candidates, &sizes);
            column.record(row_group, winner, margin);
        }

        // Step 3: write the whole row group for real, through one ordinary
        // column writer per column at that column's current candidate. The
        // probe rows are encoded a second time here, which is the price of the
        // measurement.
        let mut writers: Vec<ArrowColumnWriter> = Vec::with_capacity(columns.len());
        for (idx, column) in columns.iter().enumerate() {
            let props = candidate_props(props, column, column.current);
            writers.push(factory.create_column_writer(row_group, idx, &props)?);
        }

        for batch in group {
            for (idx, column) in columns_of(schema, batch)?.into_iter().enumerate() {
                writers[idx].write(&column)?;
            }
        }

        // Step 4: hand the finished column chunks to the file writer. They are
        // ordinary chunks: the same page store and, with the encryption feature
        // on, the same encryptor as the default write path would have used.
        let mut rg = file_writer.next_row_group()?;
        for writer in writers {
            writer.close()?.append_to_row_group(&mut rg)?;
        }
        rg.close()?;
    }

    file_writer.close()?;
    Ok(columns.iter().map(|c| c.current).collect())
}

/// How much smaller `best` is than `other`, as a fraction of `other`.
fn margin_over(best: u64, other: u64) -> f64 {
    if other == 0 {
        0.0
    } else {
        (other - best) as f64 / other as f64
    }
}

/// The winning candidate and the margin the leader won by, from one round of
/// measured sizes.
fn decide(candidates: &[Candidate], sizes: &[u64]) -> (Candidate, f64) {
    let mut order: Vec<usize> = (0..sizes.len()).collect();
    order.sort_by_key(|&k| (sizes[k], k));
    let best = sizes[order[0]];
    // Every column races at least a dictionary and a plain candidate, so there
    // is always a runner-up for the leader to be measured against.
    let margin = margin_over(best, sizes[order[1]]);

    // Candidates within NEAR_TIE_BAND of the leader are a near-tie: the sizes
    // are not telling them apart, and picking the nominally smallest would be
    // reading noise. The tie goes to the cheapest page to decode instead, which
    // spends it where it is nearly free, on bytes the reader barely pays for.
    //
    // Deliberately not a coin flip. This example promises that the same input
    // produces the same file, and a random tie-break would make a column's
    // encoding depend on the run rather than on the data, so two writes of one
    // dataset could differ and neither would be reproducible.
    let winner = order
        .iter()
        .copied()
        .take_while(|&k| margin_over(best, sizes[k]) < NEAR_TIE_BAND)
        .min_by_key(|&k| (candidates[k].decode_rank(), sizes[k], k))
        .expect("the leader is always within the margin of itself");
    (candidates[winner], margin)
}

/// The properties for one column encoded with `candidate`: the file's own
/// properties, with that one column's dictionary and encoding settings
/// overridden. Every other column's settings are irrelevant here, because a
/// writer built for a single column only ever consults its own.
fn candidate_props(
    base: &WriterProperties,
    column: &ColumnState,
    candidate: Candidate,
) -> WriterPropertiesPtr {
    let builder = candidate.apply(
        base.clone().into_builder(),
        column.path.clone(),
        column.physical,
    );
    Arc::new(builder.build())
}

/// The columns of one record batch, flattened into the order the column
/// writers expect. A nested field would expand into several entries here, one
/// per column of the Parquet schema.
fn columns_of(schema: &SchemaRef, batch: &RecordBatch) -> Result<Vec<ArrowLeafColumn>> {
    let mut out = Vec::new();
    for (field, array) in schema.fields().iter().zip(batch.columns()) {
        out.extend(compute_leaves(field.as_ref(), array)?);
    }
    Ok(out)
}

/// The leading [`DATA_PAGE_ROWS`] rows of a row group, cut at a record batch
/// boundary with a slice of the batch that straddles the cut. Slicing an Arrow
/// array copies nothing, so cutting the probe costs no data movement, and a row
/// group shorter than a page simply yields all of it.
fn probe_page(group: &[RecordBatch]) -> Vec<RecordBatch> {
    let mut out = Vec::new();
    let mut taken = 0usize;
    for batch in group {
        if taken >= DATA_PAGE_ROWS {
            break;
        }
        let take = (DATA_PAGE_ROWS - taken).min(batch.num_rows());
        out.push(batch.slice(0, take));
        taken += take;
    }
    out
}

// ---------------------------------------------------------------------------
// The dataset, and the stock writer to compare against
// ---------------------------------------------------------------------------

/// splitmix64, so every run generates byte-identical data.
fn mix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Three columns whose right answers differ: a low-cardinality string column
/// that wants a dictionary, a monotonic timestamp column that wants deltas, and
/// a column of unique floats that wants neither.
fn dataset() -> (SchemaRef, Vec<RecordBatch>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("category", DataType::Utf8, false),
        Field::new(
            "event_time",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("measurement", DataType::Float64, false),
    ]));

    let mut seed = 0x5eedu64;
    let mut batches = Vec::new();
    let mut row = 0usize;
    while row < ROWS {
        let n = BATCH_ROWS.min(ROWS - row);
        let category: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..n).map(|i| format!("region-{:02}", (row + i) % 24)),
        ));
        // Strictly increasing microseconds, which is what delta encoding is
        // built for and what a dictionary is useless against.
        let event_time: ArrayRef = Arc::new(
            Int64Array::from_iter_values(
                (0..n).map(|i| 1_700_000_000_000_000i64 + (row + i) as i64 * 1_000),
            )
            .reinterpret_cast::<arrow_array::types::TimestampMicrosecondType>(),
        );
        let measurement: ArrayRef = Arc::new(Float64Array::from_iter_values(
            (0..n).map(|_| mix(&mut seed) as f64 / 1e6),
        ));
        batches.push(
            RecordBatch::try_new(schema.clone(), vec![category, event_time, measurement]).unwrap(),
        );
        row += n;
    }
    (schema, batches)
}

/// Groups the batches into fixed-size row groups.
fn row_groups(batches: &[RecordBatch]) -> Vec<Vec<RecordBatch>> {
    batches
        .chunks(ROW_GROUP_ROWS / BATCH_ROWS)
        .map(|c| c.to_vec())
        .collect()
}

/// A stock [`ArrowWriter`] at the same properties, cutting row groups in the
/// same places, so the two files differ only in encoding choices.
fn write_stock(
    schema: &SchemaRef,
    groups: &[Vec<RecordBatch>],
    props: &WriterProperties,
    path: &Path,
) -> Result<()> {
    let mut writer =
        ArrowWriter::try_new(File::create(path)?, schema.clone(), Some(props.clone()))?;
    for group in groups {
        for batch in group {
            writer.write(batch)?;
        }
        writer.flush()?;
    }
    writer.close()?;
    Ok(())
}

/// Reads `path` back and checks it row for row against the source batches.
fn verify(path: &Path, source: &[RecordBatch], schema: &SchemaRef) -> Result<()> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?
        .with_batch_size(BATCH_ROWS)
        .build()?;
    let read: Vec<RecordBatch> = reader.collect::<std::result::Result<_, _>>()?;
    let read = arrow_select::concat::concat_batches(schema, &read).unwrap();
    let want = arrow_select::concat::concat_batches(schema, source).unwrap();
    assert_eq!(read, want, "{} did not read back exactly", path.display());
    Ok(())
}

/// The encodings each column chunk of the first row group actually used.
fn encodings(path: &Path) -> Result<Vec<String>> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let rg = reader.metadata().row_group(0);
    Ok((0..rg.num_columns())
        .map(|i| {
            let c = rg.column(i);
            let mut names: Vec<String> = c.encodings().map(|e| format!("{e}")).collect();
            names.sort();
            names.join("+")
        })
        .collect())
}

fn main() -> Result<()> {
    let (schema, batches) = dataset();
    let groups = row_groups(&batches);

    // Identical properties for both writers. Only the per-column encoding
    // settings the probe writer adds on top may differ.
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_data_page_row_count_limit(DATA_PAGE_ROWS)
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .build();

    let dir = std::env::temp_dir();
    let probed_path = dir.join("chunk_probe_writer_probed.parquet");
    let stock_path = dir.join("chunk_probe_writer_stock.parquet");

    // Both arms are measured from here, after the input batches have been
    // materialized, so what is reported is the cost of writing and not of
    // generating the data. Each arm runs once. The byte totals are
    // deterministic; the time and memory figures are indicative only, and the
    // arm that runs second can look slightly cheaper for reusing heap that the
    // first one freed. Wall clock is a stand-in for CPU time, which the standard
    // library cannot read; both arms are single threaded, so on an unloaded
    // machine the two track each other closely.
    //
    // The probe arm can come out ahead on time as well as bytes, which is not a
    // contradiction: a probe pass costs one page worth of rows per candidate,
    // while a column the stock writer starts encoding as a dictionary and then
    // falls back on has cost a dictionary build over the whole row group. The
    // encodings printed below show where that happened.
    let baseline = reset_peak();
    let started = Instant::now();
    write_stock(&schema, &groups, &props, &stock_path)?;
    let stock_wall = started.elapsed();
    let stock_peak = peak_over(baseline);

    let baseline = reset_peak();
    let started = Instant::now();
    let chosen = write_with_probes(&schema, &groups, &props, &probed_path)?;
    let probed_wall = started.elapsed();
    let probed_peak = peak_over(baseline);

    verify(&probed_path, &batches, &schema)?;
    verify(&stock_path, &batches, &schema)?;

    let probed_bytes = std::fs::metadata(&probed_path)?.len();
    let stock_bytes = std::fs::metadata(&stock_path)?.len();
    let delta = 100.0 * (probed_bytes as f64 - stock_bytes as f64) / stock_bytes as f64;

    let probed_encodings = encodings(&probed_path)?;
    let stock_encodings = encodings(&stock_path)?;
    println!(
        "{ROWS} rows in {} row groups, both read back exactly\n",
        groups.len()
    );
    println!(
        "  {:<18} {:>10}  {:>6}  {:>10}",
        "arm", "bytes", "wall s", "peak alloc"
    );
    println!(
        "  {:<18} {stock_bytes:>10}  {:>6.2}  {:>7.1} MB",
        "stock ArrowWriter",
        stock_wall.as_secs_f64(),
        mib(stock_peak)
    );
    println!(
        "  {:<18} {probed_bytes:>10}  {:>6.2}  {:>7.1} MB  ({delta:+.1}% bytes)\n",
        "probe writer",
        probed_wall.as_secs_f64(),
        mib(probed_peak)
    );
    println!(
        "  {:<14} {:<12} {:<24} encodings (stock)",
        "column", "chosen", "encodings (probed)"
    );
    for (i, field) in schema.fields().iter().enumerate() {
        let (probed, stock) = (&probed_encodings[i], &stock_encodings[i]);
        println!(
            "  {:<14} {:<12} {probed:<24} {stock}",
            field.name(),
            chosen[i].name()
        );
    }

    std::fs::remove_file(probed_path)?;
    std::fs::remove_file(stock_path)?;
    Ok(())
}
