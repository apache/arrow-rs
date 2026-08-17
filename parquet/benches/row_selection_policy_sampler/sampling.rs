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

use std::hint::black_box;
use std::ops::Range;
use std::time::Instant;

use arrow_array::RecordBatch;
use parquet::DecodeResult;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ParquetRecordBatchReaderBuilder, RowSelection, RowSelectionPolicy,
};
use parquet::arrow::push_decoder::{ParquetPushDecoderBuilder, PushBuffers};
use rand::{RngExt, SeedableRng, rngs::StdRng};
use serde_json::{Value, json};

use super::cli::Cli;
use super::fixture::{Fixture, logical_checksum};
use super::model::{ExecutionMode, Experiment, stable_hash};

#[derive(Debug)]
struct PairSample {
    pair_index: usize,
    mask_first: bool,
    mask_ns: u64,
    selectors_ns: u64,
    log_ratio: f64,
}

impl PairSample {
    fn to_json(&self) -> Value {
        json!({
            "pair_index": self.pair_index,
            "order": if self.mask_first { "mask-first" } else { "selectors-first" },
            "mask_ns": self.mask_ns,
            "selectors_ns": self.selectors_ns,
            "log_mask_over_selectors": self.log_ratio,
        })
    }
}

#[derive(Debug)]
struct SampleSummary {
    median_mask_ns: f64,
    median_selectors_ns: f64,
    median_log_ratio: f64,
    ci_low: f64,
    ci_high: f64,
    stop_reason: &'static str,
}

impl SampleSummary {
    fn to_json(&self, pairs: usize) -> Value {
        json!({
            "pairs": pairs,
            "median_mask_ns": self.median_mask_ns,
            "median_selectors_ns": self.median_selectors_ns,
            "median_log_mask_over_selectors": self.median_log_ratio,
            "bootstrap_ci95": [self.ci_low, self.ci_high],
            "stop_reason": self.stop_reason,
        })
    }

    pub(crate) fn baseline_ns(&self) -> f64 {
        f64::midpoint(self.median_mask_ns, self.median_selectors_ns)
    }
}

pub(crate) struct SampledPoint {
    samples: Vec<PairSample>,
    summary: SampleSummary,
    rows: usize,
    checksum: u64,
    selection_stats: Value,
    metadata: Value,
    diagnostic: Value,
    sampling: Value,
    elapsed_ms: u64,
    complete: bool,
}

impl SampledPoint {
    pub(crate) fn baseline_ns(&self) -> f64 {
        self.summary.baseline_ns()
    }

    pub(crate) fn to_json(&self, experiment: &Experiment, record_type: &str) -> Value {
        json!({
            "record_type": record_type,
            "status": if self.complete { "complete" } else { "incomplete" },
            "experiment": experiment.to_json(),
            "selection_stats": self.selection_stats,
            "column_metadata": self.metadata,
            "preflight": {
                "rows": self.rows,
                "checksum": format!("{:016x}", self.checksum),
            },
            "diagnostic": self.diagnostic,
            "sampling": self.sampling,
            "samples": self.samples.iter().map(PairSample::to_json).collect::<Vec<_>>(),
            "summary": self.summary.to_json(self.samples.len()),
            "elapsed_ms": self.elapsed_ms,
        })
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.complete
    }
}

pub(crate) fn sample_point(
    cli: &Cli,
    experiment: &Experiment,
    fixture: &Fixture,
    control: bool,
) -> Result<SampledPoint, String> {
    let started = Instant::now();
    let materialized = experiment
        .selection
        .materialize(experiment.fixture.rows, experiment.batch_size);
    if materialized.stats.selected_rows() == 0 {
        return Err(format!(
            "experiment {} generated an empty selection",
            experiment.id
        ));
    }
    let projection = ProjectionMask::roots(fixture.metadata.parquet_schema(), [0]);
    let prepared = PreparedRun::new(
        experiment.mode,
        fixture,
        projection,
        materialized.selection,
        experiment.batch_size,
    )?;
    let diagnostic = prepared.diagnostic()?;

    let mask_batches = prepared.capture(RowSelectionPolicy::Mask)?;
    let selector_batches = prepared.capture(RowSelectionPolicy::Selectors)?;
    if mask_batches != selector_batches {
        return Err(format!(
            "correctness failure for {}: Mask and Selectors returned different RecordBatches",
            experiment.id
        ));
    }
    let rows = mask_batches
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    if rows != materialized.stats.selected_rows() {
        return Err(format!(
            "correctness failure for {}: expected {} selected rows, got {rows}",
            experiment.id,
            materialized.stats.selected_rows()
        ));
    }
    let checksum = logical_checksum(&mask_batches)?;

    let seed =
        cli.seed ^ stable_hash(experiment.id.as_bytes()) ^ if control { 0xc017_7010 } else { 0 };
    let mut rng = StdRng::seed_from_u64(seed);
    for _ in 0..cli.warmup_pairs {
        if rng.random_bool(0.5) {
            prepared.run_rows(RowSelectionPolicy::Mask)?;
            prepared.run_rows(RowSelectionPolicy::Selectors)?;
        } else {
            prepared.run_rows(RowSelectionPolicy::Selectors)?;
            prepared.run_rows(RowSelectionPolicy::Mask)?;
        }
    }

    let mut samples = Vec::with_capacity(cli.max_pairs);
    let mut stop_reason = "max_pairs";
    while samples.len() < cli.max_pairs {
        let mask_first = rng.random_bool(0.5);
        let (mask_ns, selectors_ns) = if mask_first {
            (
                measure(&prepared, RowSelectionPolicy::Mask, cli.inner_iterations)?,
                measure(
                    &prepared,
                    RowSelectionPolicy::Selectors,
                    cli.inner_iterations,
                )?,
            )
        } else {
            let selectors = measure(
                &prepared,
                RowSelectionPolicy::Selectors,
                cli.inner_iterations,
            )?;
            let mask = measure(&prepared, RowSelectionPolicy::Mask, cli.inner_iterations)?;
            (mask, selectors)
        };
        samples.push(PairSample {
            pair_index: samples.len(),
            mask_first,
            mask_ns,
            selectors_ns,
            log_ratio: (mask_ns as f64 / selectors_ns as f64).ln(),
        });

        if samples.len() >= cli.min_pairs {
            let (_, ci_low, ci_high) =
                bootstrap_log_ratio(&samples, cli.bootstrap_samples, seed ^ samples.len() as u64);
            if ci_low > cli.decision_band || ci_high < -cli.decision_band {
                stop_reason = "direction_confident";
                break;
            }
            if ci_high - ci_low <= cli.target_ci_width {
                stop_reason = "target_precision";
                break;
            }
        }
        if started.elapsed() >= cli.point_timeout {
            stop_reason = "point_timeout";
            break;
        }
    }

    let (median_log_ratio, ci_low, ci_high) =
        bootstrap_log_ratio(&samples, cli.bootstrap_samples, seed ^ 0xb007_57a9);
    let summary = SampleSummary {
        median_mask_ns: median(samples.iter().map(|sample| sample.mask_ns as f64).collect()),
        median_selectors_ns: median(
            samples
                .iter()
                .map(|sample| sample.selectors_ns as f64)
                .collect(),
        ),
        median_log_ratio,
        ci_low,
        ci_high,
        stop_reason,
    };
    let complete = stop_reason != "point_timeout" || samples.len() == cli.max_pairs;
    Ok(SampledPoint {
        samples,
        summary,
        rows,
        checksum,
        selection_stats: materialized.stats.to_json(),
        metadata: fixture.metadata_json(),
        diagnostic,
        sampling: cli.sampling_json(),
        elapsed_ms: millis(started.elapsed()),
        complete,
    })
}

struct PreparedRun<'a> {
    mode: ExecutionMode,
    fixture: &'a Fixture,
    projection: ProjectionMask,
    selection: RowSelection,
    batch_size: usize,
    full_buffers: Option<PushBuffers>,
}

impl<'a> PreparedRun<'a> {
    fn new(
        mode: ExecutionMode,
        fixture: &'a Fixture,
        projection: ProjectionMask,
        selection: RowSelection,
        batch_size: usize,
    ) -> Result<Self, String> {
        let full_buffers = if mode == ExecutionMode::PageValidation {
            let len = fixture.bytes.len() as u64;
            let mut buffers = PushBuffers::new(len);
            buffers
                .push_range(0..len, fixture.bytes.clone())
                .map_err(|error| error.to_string())?;
            Some(buffers)
        } else {
            None
        };
        Ok(Self {
            mode,
            fixture,
            projection,
            selection,
            batch_size,
            full_buffers,
        })
    }

    fn capture(&self, policy: RowSelectionPolicy) -> Result<Vec<RecordBatch>, String> {
        match self.mode {
            ExecutionMode::SyncOracle => self.capture_sync(policy),
            ExecutionMode::PageValidation => self.capture_push(policy),
        }
    }

    fn run_rows(&self, policy: RowSelectionPolicy) -> Result<usize, String> {
        match self.mode {
            ExecutionMode::SyncOracle => self.run_sync_rows(policy),
            ExecutionMode::PageValidation => self.run_push_rows(policy),
        }
    }

    fn capture_sync(&self, policy: RowSelectionPolicy) -> Result<Vec<RecordBatch>, String> {
        ParquetRecordBatchReaderBuilder::new_with_metadata(
            self.fixture.bytes.clone(),
            self.fixture.metadata.clone(),
        )
        .with_projection(self.projection.clone())
        .with_batch_size(self.batch_size)
        .with_row_selection(self.selection.clone())
        .with_row_selection_policy(policy)
        .build()
        .map_err(|error| error.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())
    }

    fn run_sync_rows(&self, policy: RowSelectionPolicy) -> Result<usize, String> {
        let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
            self.fixture.bytes.clone(),
            self.fixture.metadata.clone(),
        )
        .with_projection(self.projection.clone())
        .with_batch_size(self.batch_size)
        .with_row_selection(self.selection.clone())
        .with_row_selection_policy(policy)
        .build()
        .map_err(|error| error.to_string())?;
        let mut rows = 0usize;
        for batch in reader {
            let batch = batch.map_err(|error| error.to_string())?;
            rows += black_box(batch.num_rows());
            black_box(batch);
        }
        Ok(rows)
    }

    fn capture_push(&self, policy: RowSelectionPolicy) -> Result<Vec<RecordBatch>, String> {
        let mut decoder = self
            .push_builder(policy, true)?
            .build()
            .map_err(to_string)?;
        let mut batches = Vec::new();
        loop {
            match decoder.try_decode().map_err(to_string)? {
                DecodeResult::Data(batch) => batches.push(batch),
                DecodeResult::NeedsData(ranges) => {
                    return Err(format!(
                        "prefetched push decoder unexpectedly requested {ranges:?}"
                    ));
                }
                DecodeResult::Finished => return Ok(batches),
            }
        }
    }

    fn run_push_rows(&self, policy: RowSelectionPolicy) -> Result<usize, String> {
        let mut decoder = self
            .push_builder(policy, true)?
            .build()
            .map_err(to_string)?;
        let mut rows = 0usize;
        loop {
            match decoder.try_decode().map_err(to_string)? {
                DecodeResult::Data(batch) => {
                    rows += black_box(batch.num_rows());
                    black_box(batch);
                }
                DecodeResult::NeedsData(ranges) => {
                    return Err(format!(
                        "prefetched push decoder unexpectedly requested {ranges:?}"
                    ));
                }
                DecodeResult::Finished => return Ok(rows),
            }
        }
    }

    fn diagnostic(&self) -> Result<Value, String> {
        if self.mode != ExecutionMode::PageValidation {
            return Ok(json!({"requested_ranges": []}));
        }

        let selectors = self.requested_ranges(RowSelectionPolicy::Selectors)?;
        let mask = self.requested_ranges(RowSelectionPolicy::Mask)?;
        let column_compressed_bytes = u64::try_from(
            self.fixture
                .metadata
                .metadata()
                .row_group(0)
                .column(0)
                .compressed_size(),
        )
        .map_err(|_| "generated column has a negative compressed size".to_string())?;
        for (policy, ranges) in [("Selectors", &selectors), ("Mask", &mask)] {
            let bytes = requested_bytes(ranges);
            if ranges.is_empty() || bytes >= column_compressed_bytes {
                return Err(format!(
                    "page-pruning validation failed for {policy}: requested {} ranges / {bytes} bytes from a {column_compressed_bytes}-byte column",
                    ranges.len()
                ));
            }
        }

        Ok(json!({
            "page_pruning_validated": true,
            "column_compressed_bytes": column_compressed_bytes,
            "policies_requested_identical_ranges": selectors == mask,
            "by_policy": {
                "selectors": ranges_json(&selectors),
                "mask": ranges_json(&mask),
            },
        }))
    }

    fn requested_ranges(&self, policy: RowSelectionPolicy) -> Result<Vec<Range<u64>>, String> {
        let mut decoder = self
            .push_builder(policy, false)?
            .build()
            .map_err(to_string)?;
        match decoder.try_decode().map_err(to_string)? {
            DecodeResult::NeedsData(ranges) => Ok(ranges),
            DecodeResult::Data(batch) => Err(format!(
                "empty-buffer diagnostic unexpectedly decoded {} rows",
                batch.num_rows()
            )),
            DecodeResult::Finished => Err("empty-buffer diagnostic unexpectedly finished".into()),
        }
    }

    fn push_builder(
        &self,
        policy: RowSelectionPolicy,
        prefetched: bool,
    ) -> Result<ParquetPushDecoderBuilder, String> {
        let builder = ParquetPushDecoderBuilder::new_with_metadata(self.fixture.metadata.clone())
            .with_projection(self.projection.clone())
            .with_batch_size(self.batch_size)
            .with_row_selection(self.selection.clone())
            .with_row_selection_policy(policy);
        if prefetched {
            Ok(builder.with_buffers(
                self.full_buffers
                    .as_ref()
                    .ok_or_else(|| "page validation is missing prefetched buffers".to_string())?
                    .clone(),
            ))
        } else {
            Ok(builder)
        }
    }
}

fn measure(
    prepared: &PreparedRun<'_>,
    policy: RowSelectionPolicy,
    inner_iterations: usize,
) -> Result<u64, String> {
    let started = Instant::now();
    let mut rows = 0usize;
    for _ in 0..inner_iterations {
        rows = rows.saturating_add(prepared.run_rows(policy)?);
    }
    black_box(rows);
    let nanos = started.elapsed().as_nanos() / inner_iterations as u128;
    Ok(u64::try_from(nanos.max(1)).unwrap_or(u64::MAX))
}

fn bootstrap_log_ratio(
    samples: &[PairSample],
    bootstrap_samples: usize,
    seed: u64,
) -> (f64, f64, f64) {
    let observed = samples
        .iter()
        .map(|sample| sample.log_ratio)
        .collect::<Vec<_>>();
    let estimate = median(observed.clone());
    let mut rng = StdRng::seed_from_u64(seed);
    let mut bootstrapped = Vec::with_capacity(bootstrap_samples);
    for _ in 0..bootstrap_samples {
        let values = (0..observed.len())
            .map(|_| observed[rng.random_range(0..observed.len())])
            .collect();
        bootstrapped.push(median(values));
    }
    bootstrapped.sort_by(f64::total_cmp);
    let low_idx = ((bootstrap_samples as f64 * 0.025).floor() as usize)
        .min(bootstrap_samples.saturating_sub(1));
    let high_idx = ((bootstrap_samples as f64 * 0.975).ceil() as usize)
        .saturating_sub(1)
        .min(bootstrap_samples.saturating_sub(1));
    (estimate, bootstrapped[low_idx], bootstrapped[high_idx])
}

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    if values.len().is_multiple_of(2) {
        f64::midpoint(values[middle - 1], values[middle])
    } else {
        values[middle]
    }
}

fn ranges_json(ranges: &[Range<u64>]) -> Value {
    json!({
        "requested_range_count": ranges.len(),
        "requested_bytes": requested_bytes(ranges),
        "requested_ranges": ranges.iter().map(|range| json!({
            "start": range.start,
            "end": range.end,
            "bytes": range.end - range.start,
        })).collect::<Vec<_>>(),
    })
}

fn requested_bytes(ranges: &[Range<u64>]) -> u64 {
    ranges.iter().map(|range| range.end - range.start).sum()
}

fn millis(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn to_string(error: impl std::fmt::Display) -> String {
    error.to_string()
}
