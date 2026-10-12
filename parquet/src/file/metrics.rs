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

/// Whether a page was decompressed to decode its values or to skip past them.
///
/// Both paths decompress: a page has to be turned back into plaintext before
/// it can be walked, whether its values are then decoded or thrown away. Only
/// a page skipped whole is free. Splitting the two says how much of the cost
/// was paid for rows that were never returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DecompressionPhase {
    /// Decompressed while decoding values.
    #[default]
    Read,
    /// Decompressed while skipping past values, so the work was thrown away.
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

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use bytes::Bytes;

    use super::*;
    use crate::DecodeResult;
    use crate::arrow::ArrowWriter;
    use crate::arrow::arrow_reader::metrics::ArrowReaderMetrics;
    use crate::arrow::arrow_reader::{ParquetRecordBatchReaderBuilder, RowSelection, RowSelector};
    use crate::arrow::push_decoder::ParquetPushDecoderBuilder;
    use crate::basic::{Compression, ZstdLevel};
    use crate::file::metadata::ParquetMetaDataPushDecoder;
    use crate::file::properties::WriterProperties;

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
    fn a_selective_read_attributes_walked_pages_to_the_skip_phase() {
        // Keep one row in every 5000, so every page has to be walked through
        // and none can be skipped whole.
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
        let reader = ParquetRecordBatchReaderBuilder::try_new(file())
            .unwrap()
            .with_metrics(metrics.clone())
            .with_row_selection(RowSelection::from(selectors))
            .build()
            .unwrap();
        let rows: usize = reader.map(|b| b.unwrap().num_rows()).sum();
        assert!(rows > 0 && rows < ROWS / 100, "a few rows out of many");

        let data_skip = metrics
            .decompression()
            .pages_decompressed(PageKind::Data, DecompressionPhase::Skip)
            .unwrap();
        assert!(
            data_skip > 0,
            "walking to a selected row inside a page decompresses that page, \
             and that cost belongs to skipping"
        );
        assert!(
            metrics
                .decompression()
                .decompress_bytes(PageKind::Data, DecompressionPhase::Skip)
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
