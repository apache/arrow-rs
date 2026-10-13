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

//! [`DecompressionMetrics`], for measuring what a reader spends on decompression.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Whether a decompressed page was used to return values or only walked past.
///
/// Both paths decompress: a page has to be turned back into plaintext before
/// it can be walked, whether its values are then decoded or thrown away. Only
/// a page skipped whole is free. Splitting the two says how much of the cost
/// was paid for rows that were never returned.
///
/// The phase is decided by what happens to the page, not by which call loaded
/// it. A page first loaded while skipping counts as [`Self::Read`] once any of
/// its values is read, and as [`Self::Skip`] only if the reader moves past it
/// without reading. A dictionary page counts as [`Self::Read`] once any value of
/// its column chunk is read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DecompressionPhase {
    /// At least one value of the page (or of the dictionary's column chunk) was
    /// read.
    #[default]
    Read,
    /// The page was decompressed and then walked past without reading any of
    /// its values, so the work was thrown away.
    Skip,
}

/// Whether a page held the column chunk's dictionary or its values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageKind {
    /// The column chunk's dictionary page.
    Dictionary,
    /// A data page.
    Data,
}

/// Records how long a reader spends decompressing, split by page kind and by
/// whether the page was being read or skipped.
///
/// Decompression sits inside decoding and inside skipping, so these numbers
/// are a share of those, not a separate stage to add alongside them.
///
/// The four cells answer different questions. A dictionary page decompressed
/// while skipping is pure waste, paid for a column chunk whose values are
/// never decoded. A data page decompressed while skipping is the unavoidable
/// cost of walking to a selected row inside it, and points at page sizing
/// rather than at the dictionary.
///
/// Collection is off by default. When [`DecompressionMetrics::Disabled`], no
/// clock is read and no counter is touched, so a reader that does not ask for
/// these numbers pays nothing for them.
///
/// The counters live behind an `Arc`, so cloning shares them rather than
/// copying, and one handle can be given to every column's page reader.
///
/// # Example
/// ```
/// # use parquet::file::metrics::{DecompressionMetrics, DecompressionPhase, PageKind};
/// let metrics = DecompressionMetrics::enabled();
/// // ... hand a clone to a reader, read some data ...
/// if let Some(wasted) =
///     metrics.decompress_time(PageKind::Dictionary, DecompressionPhase::Skip)
/// {
///     println!("dictionary decompression paid for skipped rows: {wasted:?}");
/// }
/// ```
#[derive(Debug, Clone, Default)]
pub enum DecompressionMetrics {
    /// Nothing is measured. This is the default.
    #[default]
    Disabled,
    /// Measurements accumulate in the shared inner counters.
    ///
    /// Create this via [`DecompressionMetrics::enabled`].
    Enabled(Arc<DecompressionMetricsInner>),
}

impl DecompressionMetrics {
    /// Creates a handle that measures nothing.
    pub fn disabled() -> Self {
        Self::Disabled
    }

    /// Creates a handle that accumulates measurements.
    pub fn enabled() -> Self {
        Self::Enabled(Arc::new(DecompressionMetricsInner::default()))
    }

    /// Whether this handle is collecting.
    ///
    /// Callers check this before reading a clock, so that the disabled path
    /// costs one enum discriminant test and nothing else.
    #[inline]
    pub fn is_enabled(&self) -> bool {
        matches!(self, Self::Enabled(_))
    }

    /// Time spent decompressing pages of `kind` during `phase`.
    ///
    /// `None` when disabled.
    pub fn decompress_time(&self, kind: PageKind, phase: DecompressionPhase) -> Option<Duration> {
        self.cell(kind, phase)
            .map(|c| Duration::from_nanos(c.time_ns.load(Ordering::Relaxed)))
    }

    /// Uncompressed bytes produced from pages of `kind` during `phase`.
    ///
    /// Worth reading next to the time: a large dictionary that decompresses
    /// quickly and a small one that does not are different problems.
    ///
    /// `None` when disabled.
    pub fn decompress_bytes(&self, kind: PageKind, phase: DecompressionPhase) -> Option<u64> {
        self.cell(kind, phase)
            .map(|c| c.bytes.load(Ordering::Relaxed))
    }

    /// Number of pages of `kind` decompressed during `phase`.
    ///
    /// `None` when disabled.
    pub fn pages_decompressed(&self, kind: PageKind, phase: DecompressionPhase) -> Option<u64> {
        self.cell(kind, phase)
            .map(|c| c.pages.load(Ordering::Relaxed))
    }

    /// Time spent decompressing dictionary pages, both phases together.
    ///
    /// `None` when disabled.
    pub fn dictionary_decompress_time(&self) -> Option<Duration> {
        self.total_time(PageKind::Dictionary)
    }

    /// Time spent decompressing data pages, both phases together.
    ///
    /// `None` when disabled.
    pub fn data_decompress_time(&self) -> Option<Duration> {
        self.total_time(PageKind::Data)
    }

    /// Uncompressed bytes produced from dictionary pages, both phases together.
    ///
    /// `None` when disabled.
    pub fn dictionary_decompressed_bytes(&self) -> Option<u64> {
        self.total_bytes(PageKind::Dictionary)
    }

    /// Uncompressed bytes produced from data pages, both phases together.
    ///
    /// `None` when disabled.
    pub fn data_decompressed_bytes(&self) -> Option<u64> {
        self.total_bytes(PageKind::Data)
    }

    fn total_time(&self, kind: PageKind) -> Option<Duration> {
        let read = self.decompress_time(kind, DecompressionPhase::Read)?;
        let skip = self.decompress_time(kind, DecompressionPhase::Skip)?;
        Some(read + skip)
    }

    fn total_bytes(&self, kind: PageKind) -> Option<u64> {
        let read = self.decompress_bytes(kind, DecompressionPhase::Read)?;
        let skip = self.decompress_bytes(kind, DecompressionPhase::Skip)?;
        Some(read + skip)
    }

    fn cell(&self, kind: PageKind, phase: DecompressionPhase) -> Option<&PageCounters> {
        match self {
            Self::Disabled => None,
            Self::Enabled(inner) => Some(match (kind, phase) {
                (PageKind::Dictionary, DecompressionPhase::Read) => &inner.dictionary_read,
                (PageKind::Dictionary, DecompressionPhase::Skip) => &inner.dictionary_skip,
                (PageKind::Data, DecompressionPhase::Read) => &inner.data_read,
                (PageKind::Data, DecompressionPhase::Skip) => &inner.data_skip,
            }),
        }
    }

    /// Records one decompressed page.
    ///
    /// `elapsed` covers only the codec call, not the surrounding copies or the
    /// size checks, so it stays comparable across codecs.
    pub(crate) fn record(
        &self,
        kind: PageKind,
        phase: DecompressionPhase,
        elapsed: Duration,
        bytes: usize,
    ) {
        let Some(cell) = self.cell(kind, phase) else {
            return;
        };
        cell.time_ns
            .fetch_add(elapsed.as_nanos() as u64, Ordering::Relaxed);
        cell.bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        cell.pages.fetch_add(1, Ordering::Relaxed);
    }
}

/// One page's decompression cost, held until it is known whether the page was
/// read or only walked past.
#[derive(Debug, Clone, Copy)]
struct Sample {
    elapsed: Duration,
    bytes: usize,
}

/// Attributes one page reader's decompression to [`DecompressionPhase::Read`]
/// or [`DecompressionPhase::Skip`] by what happens to each page afterwards.
///
/// A page decompressed while reading is recorded as read at once. A page
/// decompressed while skipping is held back: it becomes read if values are then
/// read from it ([`Self::current_page_read`]) and skip if the reader moves on to
/// the next page, or is dropped, first. The column chunk's dictionary is held
/// the same way and becomes read as soon as any value of the chunk is read.
#[derive(Debug, Default)]
pub(crate) struct DecompressionRecorder {
    metrics: DecompressionMetrics,
    phase: DecompressionPhase,
    pending_dictionary: Option<Sample>,
    pending_data: Option<Sample>,
}

impl DecompressionRecorder {
    /// A recorder that writes into `metrics`.
    pub(crate) fn new(metrics: DecompressionMetrics) -> Self {
        Self {
            metrics,
            ..Self::default()
        }
    }

    /// Whether decompression should be timed at all.
    #[inline]
    pub(crate) fn is_enabled(&self) -> bool {
        self.metrics.is_enabled()
    }

    /// Whether the pages loaded from now on are loaded to read or to skip.
    pub(crate) fn set_phase(&mut self, phase: DecompressionPhase) {
        self.phase = phase;
    }

    /// Records one decompressed page of `kind`.
    pub(crate) fn record(&mut self, kind: PageKind, elapsed: Duration, bytes: usize) {
        let sample = Sample { elapsed, bytes };
        match (kind, self.phase) {
            (PageKind::Data, DecompressionPhase::Read) => {
                // The previous page was left without being read, and a value of
                // this chunk is about to be read, so its dictionary is used.
                self.flush_data(DecompressionPhase::Skip);
                self.flush_dictionary(DecompressionPhase::Read);
                self.commit(PageKind::Data, DecompressionPhase::Read, sample);
            }
            (PageKind::Data, DecompressionPhase::Skip) => {
                self.flush_data(DecompressionPhase::Skip);
                self.pending_data = Some(sample);
            }
            (PageKind::Dictionary, DecompressionPhase::Read) => {
                self.flush_dictionary(DecompressionPhase::Skip);
                self.commit(PageKind::Dictionary, DecompressionPhase::Read, sample);
            }
            (PageKind::Dictionary, DecompressionPhase::Skip) => {
                self.flush_dictionary(DecompressionPhase::Skip);
                self.pending_dictionary = Some(sample);
            }
        }
    }

    /// Values are about to be read from the page currently loaded, so neither
    /// that page nor the chunk's dictionary was decompressed for nothing.
    pub(crate) fn current_page_read(&mut self) {
        self.flush_data(DecompressionPhase::Read);
        self.flush_dictionary(DecompressionPhase::Read);
    }

    fn flush_data(&mut self, phase: DecompressionPhase) {
        if let Some(sample) = self.pending_data.take() {
            self.commit(PageKind::Data, phase, sample);
        }
    }

    fn flush_dictionary(&mut self, phase: DecompressionPhase) {
        if let Some(sample) = self.pending_dictionary.take() {
            self.commit(PageKind::Dictionary, phase, sample);
        }
    }

    fn commit(&self, kind: PageKind, phase: DecompressionPhase, sample: Sample) {
        self.metrics
            .record(kind, phase, sample.elapsed, sample.bytes);
    }
}

impl Drop for DecompressionRecorder {
    /// Whatever is still held was walked past without being read.
    fn drop(&mut self) {
        self.flush_data(DecompressionPhase::Skip);
        self.flush_dictionary(DecompressionPhase::Skip);
    }
}

/// One cell of the page kind by phase matrix.
#[derive(Debug, Default)]
struct PageCounters {
    time_ns: AtomicU64,
    bytes: AtomicU64,
    pages: AtomicU64,
}

/// The counters behind [`DecompressionMetrics`].
///
/// See [`DecompressionMetrics`] for the public interface.
#[derive(Debug, Default)]
pub struct DecompressionMetricsInner {
    dictionary_read: PageCounters,
    dictionary_skip: PageCounters,
    data_read: PageCounters,
    data_skip: PageCounters,
}

#[cfg(test)]
mod tests {
    use super::*;

    const DICT_READ: (PageKind, DecompressionPhase) =
        (PageKind::Dictionary, DecompressionPhase::Read);
    const DICT_SKIP: (PageKind, DecompressionPhase) =
        (PageKind::Dictionary, DecompressionPhase::Skip);
    const DATA_READ: (PageKind, DecompressionPhase) = (PageKind::Data, DecompressionPhase::Read);

    #[test]
    fn disabled_reports_nothing_and_ignores_records() {
        let metrics = DecompressionMetrics::disabled();
        assert!(!metrics.is_enabled());
        metrics.record(
            PageKind::Dictionary,
            DecompressionPhase::Read,
            Duration::from_millis(5),
            1024,
        );
        assert_eq!(metrics.decompress_time(DICT_READ.0, DICT_READ.1), None);
        assert_eq!(metrics.decompress_bytes(DICT_READ.0, DICT_READ.1), None);
        assert_eq!(metrics.pages_decompressed(DICT_READ.0, DICT_READ.1), None);
        assert_eq!(metrics.dictionary_decompress_time(), None);
        assert_eq!(metrics.data_decompressed_bytes(), None);
    }

    #[test]
    fn default_is_disabled() {
        assert!(!DecompressionMetrics::default().is_enabled());
        assert_eq!(DecompressionPhase::default(), DecompressionPhase::Read);
    }

    #[test]
    fn the_four_cells_are_counted_apart() {
        let metrics = DecompressionMetrics::enabled();
        metrics.record(
            PageKind::Dictionary,
            DecompressionPhase::Read,
            Duration::from_micros(300),
            4096,
        );
        metrics.record(
            PageKind::Dictionary,
            DecompressionPhase::Skip,
            Duration::from_micros(700),
            8192,
        );
        metrics.record(
            PageKind::Data,
            DecompressionPhase::Read,
            Duration::from_micros(100),
            512,
        );
        metrics.record(
            PageKind::Data,
            DecompressionPhase::Read,
            Duration::from_micros(150),
            256,
        );

        assert_eq!(
            metrics.decompress_time(DICT_READ.0, DICT_READ.1),
            Some(Duration::from_micros(300))
        );
        assert_eq!(
            metrics.decompress_time(DICT_SKIP.0, DICT_SKIP.1),
            Some(Duration::from_micros(700)),
            "a dictionary decompressed while skipping is the wasted cell"
        );
        assert_eq!(
            metrics.pages_decompressed(DATA_READ.0, DATA_READ.1),
            Some(2)
        );
        assert_eq!(
            metrics.decompress_bytes(DATA_READ.0, DATA_READ.1),
            Some(768)
        );
    }

    #[test]
    fn totals_add_the_two_phases() {
        let metrics = DecompressionMetrics::enabled();
        metrics.record(
            PageKind::Dictionary,
            DecompressionPhase::Read,
            Duration::from_micros(300),
            4096,
        );
        metrics.record(
            PageKind::Dictionary,
            DecompressionPhase::Skip,
            Duration::from_micros(700),
            8192,
        );
        assert_eq!(
            metrics.dictionary_decompress_time(),
            Some(Duration::from_micros(1000))
        );
        assert_eq!(metrics.dictionary_decompressed_bytes(), Some(12288));
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    fn pages(m: &DecompressionMetrics, kind: PageKind, phase: DecompressionPhase) -> u64 {
        m.pages_decompressed(kind, phase).unwrap()
    }

    #[test]
    fn a_page_loaded_by_a_skip_and_then_read_counts_as_read() {
        let metrics = DecompressionMetrics::enabled();
        let mut r = DecompressionRecorder::new(metrics.clone());
        r.set_phase(DecompressionPhase::Skip);
        r.record(PageKind::Dictionary, ms(1), 10);
        r.record(PageKind::Data, ms(1), 100);
        r.set_phase(DecompressionPhase::Read);
        r.current_page_read();
        drop(r);
        assert_eq!(pages(&metrics, PageKind::Data, DecompressionPhase::Read), 1);
        assert_eq!(pages(&metrics, PageKind::Data, DecompressionPhase::Skip), 0);
        assert_eq!(
            pages(&metrics, PageKind::Dictionary, DecompressionPhase::Read),
            1
        );
        assert_eq!(
            pages(&metrics, PageKind::Dictionary, DecompressionPhase::Skip),
            0
        );
    }

    #[test]
    fn a_page_walked_past_counts_as_skip_and_a_later_read_saves_the_dictionary() {
        let metrics = DecompressionMetrics::enabled();
        let mut r = DecompressionRecorder::new(metrics.clone());
        r.set_phase(DecompressionPhase::Skip);
        r.record(PageKind::Dictionary, ms(1), 10);
        r.record(PageKind::Data, ms(1), 100); // walked past
        r.set_phase(DecompressionPhase::Read);
        r.record(PageKind::Data, ms(1), 100); // loaded and read
        drop(r);
        assert_eq!(pages(&metrics, PageKind::Data, DecompressionPhase::Skip), 1);
        assert_eq!(pages(&metrics, PageKind::Data, DecompressionPhase::Read), 1);
        assert_eq!(
            pages(&metrics, PageKind::Dictionary, DecompressionPhase::Read),
            1
        );
    }

    #[test]
    fn a_chunk_that_is_only_skipped_counts_everything_as_skip() {
        let metrics = DecompressionMetrics::enabled();
        let mut r = DecompressionRecorder::new(metrics.clone());
        r.set_phase(DecompressionPhase::Skip);
        r.record(PageKind::Dictionary, ms(1), 10);
        r.record(PageKind::Data, ms(1), 100);
        r.record(PageKind::Data, ms(1), 100);
        drop(r);
        assert_eq!(pages(&metrics, PageKind::Data, DecompressionPhase::Skip), 2);
        assert_eq!(
            pages(&metrics, PageKind::Dictionary, DecompressionPhase::Skip),
            1
        );
        assert_eq!(pages(&metrics, PageKind::Data, DecompressionPhase::Read), 0);
        assert_eq!(
            pages(&metrics, PageKind::Dictionary, DecompressionPhase::Read),
            0
        );
    }

    #[test]
    fn a_clone_shares_the_counters() {
        let metrics = DecompressionMetrics::enabled();
        let handed_to_a_page_reader = metrics.clone();
        handed_to_a_page_reader.record(
            PageKind::Data,
            DecompressionPhase::Skip,
            Duration::from_micros(10),
            64,
        );
        assert_eq!(metrics.data_decompressed_bytes(), Some(64));
    }

    #[test]
    fn counters_start_at_zero_rather_than_none_when_enabled() {
        let metrics = DecompressionMetrics::enabled();
        assert_eq!(
            metrics.dictionary_decompress_time(),
            Some(Duration::ZERO),
            "an enabled handle that saw no pages reports zero, not None"
        );
        assert_eq!(
            metrics.pages_decompressed(DATA_READ.0, DATA_READ.1),
            Some(0)
        );
    }
}

#[cfg(all(test, feature = "arrow", feature = "zstd"))]
mod end_to_end_tests {
    use std::sync::Arc;

    use arrow_array::types::Int32Type;
    use arrow_array::{ArrayRef, Int64Array, ListArray, RecordBatch, StringArray};
    use bytes::Bytes;

    use super::*;
    use crate::DecodeResult;
    use crate::arrow::ArrowWriter;
    use crate::arrow::arrow_reader::metrics::ArrowReaderMetrics;
    use crate::arrow::arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelection, RowSelector};
    use crate::arrow::push_decoder::ParquetPushDecoderBuilder;
    use crate::basic::{Compression, ZstdLevel};
    use crate::file::metadata::ParquetMetaDataPushDecoder;
    use crate::file::properties::{EnabledStatistics, WriterProperties};

    const ROWS: usize = 40_000;
    const DISTINCT: usize = 4_000;

    /// A zstd file with a dictionary big enough that decompressing it is
    /// measurable: many distinct, long strings.
    fn file() -> Bytes {
        let strings: Vec<String> = (0..ROWS)
            .map(|i| format!("TICKER{:06}-{}", i % DISTINCT, "x".repeat(32)))
            .collect();
        let ticker: ArrayRef = Arc::new(StringArray::from(
            strings.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
        ));
        let value: ArrayRef = Arc::new(Int64Array::from(
            (0..ROWS).map(|i| i as i64).collect::<Vec<_>>(),
        ));
        let batch = RecordBatch::try_from_iter(vec![("ticker", ticker), ("value", value)]).unwrap();

        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
            .set_dictionary_enabled(true)
            .set_data_page_row_count_limit(2_000)
            .set_write_batch_size(2_000)
            .build();
        let mut out = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut out, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(out)
    }

    fn read_all(data: Bytes, metrics: &ArrowReaderMetrics) -> usize {
        let reader = ParquetRecordBatchReaderBuilder::try_new(data)
            .unwrap()
            .with_metrics(metrics.clone())
            .build()
            .unwrap();
        reader.map(|b| b.unwrap().num_rows()).sum()
    }

    #[test]
    fn a_full_read_attributes_everything_to_the_read_phase() {
        let metrics = ArrowReaderMetrics::enabled();
        let rows = read_all(file(), &metrics);
        assert_eq!(rows, ROWS);

        let dict_read = metrics
            .decompression()
            .pages_decompressed(PageKind::Dictionary, DecompressionPhase::Read)
            .unwrap();
        let data_read = metrics
            .decompression()
            .pages_decompressed(PageKind::Data, DecompressionPhase::Read)
            .unwrap();
        let dict_skip = metrics
            .decompression()
            .pages_decompressed(PageKind::Dictionary, DecompressionPhase::Skip)
            .unwrap();
        let data_skip = metrics
            .decompression()
            .pages_decompressed(PageKind::Data, DecompressionPhase::Skip)
            .unwrap();

        assert!(dict_read > 0, "the string column has a dictionary page");
        assert!(data_read > 0, "and several data pages");
        assert_eq!(
            (dict_skip, data_skip),
            (0, 0),
            "nothing was skipped, so the skip cells stay empty"
        );
        assert!(
            metrics.dictionary_decompressed_bytes().unwrap() > 0,
            "bytes are recorded alongside the time"
        );
    }

    #[test]
    fn skipping_one_row_then_reading_the_rest_counts_as_read() {
        // The first page is loaded by the skip, but all its other values are
        // returned, so none of its decompression was thrown away.
        let metrics = ArrowReaderMetrics::enabled();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file())
            .unwrap()
            .with_metrics(metrics.clone())
            .with_row_selection(RowSelection::from(vec![
                RowSelector::skip(1),
                RowSelector::select(ROWS - 1),
            ]))
            .build()
            .unwrap();
        let rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
        assert_eq!(rows, ROWS - 1);

        let d = metrics.decompression();
        for kind in [PageKind::Dictionary, PageKind::Data] {
            assert_eq!(
                d.pages_decompressed(kind, DecompressionPhase::Skip),
                Some(0),
                "{kind:?}"
            );
            assert!(
                d.pages_decompressed(kind, DecompressionPhase::Read)
                    .unwrap()
                    > 0
            );
        }
    }

    /// A zstd file with one list column and no offset index, so skipping rows
    /// cannot jump over whole pages: every page on the way has to be
    /// decompressed to walk its repetition levels.
    fn list_file() -> Bytes {
        let lists: Vec<Option<Vec<Option<i32>>>> = (0..ROWS)
            .map(|i| Some((0..(i % 4) as i32).map(Some).collect()))
            .collect();
        let tags: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(lists));
        let batch = RecordBatch::try_from_iter(vec![("tags", tags)]).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
            .set_offset_index_disabled(true)
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_data_page_row_count_limit(2_000)
            .set_write_batch_size(2_000)
            .build();
        let mut out = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut out, batch.schema(), Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        Bytes::from(out)
    }

    #[test]
    fn a_selective_read_splits_pages_by_whether_any_value_was_read() {
        // Keep one row in every 5000. A page holding a kept row is decompressed
        // and read from; a page between kept rows is decompressed only to walk
        // past it.
        let mut selectors = Vec::new();
        let mut left = ROWS;
        while left > 0 {
            let skip = 4_999.min(left);
            selectors.push(RowSelector::skip(skip));
            left -= skip;
            if left == 0 {
                break;
            }
            selectors.push(RowSelector::select(1));
            left -= 1;
        }

        let metrics = ArrowReaderMetrics::enabled();
        let reader = ParquetRecordBatchReaderBuilder::try_new(list_file())
            .unwrap()
            .with_metrics(metrics.clone())
            .with_row_selection(RowSelection::from(selectors))
            .build()
            .unwrap();
        let rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
        assert!(rows > 0 && rows < ROWS / 100, "a few rows out of many");

        let d = metrics.decompression();
        let read = d
            .pages_decompressed(PageKind::Data, DecompressionPhase::Read)
            .unwrap();
        let skip = d
            .pages_decompressed(PageKind::Data, DecompressionPhase::Skip)
            .unwrap();
        assert_eq!(read, rows as u64, "one page per kept row, each read from");
        assert!(skip > 0, "pages between kept rows were walked past");
        assert!(
            d.decompress_bytes(PageKind::Data, DecompressionPhase::Skip)
                .unwrap()
                > 0
        );
    }

    /// Reads `data` through the push decoder, the path the async reader and
    /// query engines built on it take.
    fn read_all_push(data: Bytes, metrics: &ArrowReaderMetrics) -> usize {
        let len = data.len() as u64;
        let mut metadata_decoder = ParquetMetaDataPushDecoder::try_new(len).unwrap();
        metadata_decoder.push_range(0..len, data.clone()).unwrap();
        let DecodeResult::Data(metadata) = metadata_decoder.try_decode().unwrap() else {
            panic!("metadata did not decode");
        };
        let mut decoder = ParquetPushDecoderBuilder::try_new_decoder(Arc::new(metadata))
            .unwrap()
            .with_metrics(metrics.clone())
            .build()
            .unwrap();
        let mut rows = 0;
        loop {
            match decoder.try_decode().unwrap() {
                DecodeResult::NeedsData(ranges) => {
                    let bytes = ranges
                        .iter()
                        .map(|r| data.slice(r.start as usize..r.end as usize))
                        .collect();
                    decoder.push_ranges(ranges, bytes).unwrap();
                }
                DecodeResult::Data(batch) => rows += batch.num_rows(),
                DecodeResult::Finished => return rows,
            }
        }
    }

    #[test]
    fn the_push_decoder_records_what_the_sync_reader_records() {
        let data = file();
        let sync = ArrowReaderMetrics::enabled();
        let push = ArrowReaderMetrics::enabled();
        assert_eq!(read_all(data.clone(), &sync), ROWS);
        assert_eq!(read_all_push(data, &push), ROWS);

        // Bytes and page counts are deterministic; times are not compared.
        for kind in [PageKind::Dictionary, PageKind::Data] {
            let sync_bytes = sync
                .decompression()
                .decompress_bytes(kind, DecompressionPhase::Read)
                .unwrap();
            assert!(sync_bytes > 0, "{kind:?}");
            assert_eq!(
                push.decompression()
                    .decompress_bytes(kind, DecompressionPhase::Read),
                Some(sync_bytes),
                "{kind:?}"
            );
            assert_eq!(
                push.decompression()
                    .pages_decompressed(kind, DecompressionPhase::Read),
                sync.decompression()
                    .pages_decompressed(kind, DecompressionPhase::Read),
                "{kind:?}"
            );
        }
        assert!(push.dictionary_decompress_time().is_some());
    }

    #[test]
    fn disabled_metrics_report_nothing_after_a_real_read() {
        let metrics = ArrowReaderMetrics::disabled();
        let rows = read_all(file(), &metrics);
        assert_eq!(rows, ROWS, "the read still works");
        assert_eq!(metrics.dictionary_decompress_time(), None);
        assert_eq!(metrics.data_decompress_time(), None);
        assert_eq!(metrics.dictionary_decompressed_bytes(), None);
        assert_eq!(
            metrics
                .decompression()
                .pages_decompressed(PageKind::Data, DecompressionPhase::Read),
            None
        );
    }

    #[test]
    fn totals_are_the_two_phases_added() {
        let metrics = ArrowReaderMetrics::enabled();
        read_all(file(), &metrics);
        let d = metrics.decompression();
        assert_eq!(
            metrics.data_decompressed_bytes().unwrap(),
            d.decompress_bytes(PageKind::Data, DecompressionPhase::Read)
                .unwrap()
                + d.decompress_bytes(PageKind::Data, DecompressionPhase::Skip)
                    .unwrap()
        );
    }
}
