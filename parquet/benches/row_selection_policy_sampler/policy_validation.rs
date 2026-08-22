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

//! Paired end-to-end validation of `Auto` against `AutoPerColumn`.

use std::hint::black_box;
use std::time::Instant;

use arrow::array::{Int32Array, RecordBatch};
use arrow::compute::kernels::cmp::eq;
use futures::StreamExt;
use parquet::arrow::arrow_reader::metrics::ArrowReaderMetrics;
use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter, RowSelectionPolicy};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use rand::{RngExt, SeedableRng, rngs::StdRng};
use serde_json::{Value, json};
use tokio::runtime::Runtime;

use super::cli::Cli;
use super::fixture::logical_checksum;
use super::model::{OUTPUT_SCHEMA_VERSION, Stage, stable_hash};
use super::output::JsonlOutput;
use super::sampling::{bootstrap_median_ci, median};
use crate::row_selection_policy_common::cases::HETEROGENEOUS_CASES;
use crate::row_selection_policy_common::fixture::{
    CaseFixture, InMemoryAsyncReader, build_heterogeneous_fixture,
};
use crate::row_selection_policy_common::model::{
    BATCH_SIZE, CaseSpec, PAYLOAD_COLUMNS, ROWS_PER_GROUP, RowGroupPattern,
};

const CONTROL_DRIFT_LIMIT: f64 = 0.10;
const ORDER_BIAS_LIMIT: f64 = 0.05;

#[derive(Clone)]
struct PolicyExperiment {
    id: String,
    case: &'static CaseSpec,
}

impl PolicyExperiment {
    fn to_json(&self) -> Value {
        json!({
            "experiment_id": self.id,
            "mandatory": true,
            "execution_mode": "async-policy-pair",
            "case": self.case.name,
            "batch_size": BATCH_SIZE,
            "row_group_count": self.case.row_groups.len(),
            "rows_per_group": ROWS_PER_GROUP,
            "total_rows": self.case.total_rows(),
            "payload_columns": PAYLOAD_COLUMNS,
        })
    }
}

struct PolicyManifest {
    id: String,
    experiments: Vec<PolicyExperiment>,
}

impl PolicyManifest {
    fn generate(case_filters: &[String], seed: u64) -> Result<Self, String> {
        for case_filter in case_filters {
            if !HETEROGENEOUS_CASES
                .iter()
                .any(|case| case.name == case_filter)
            {
                let known = HETEROGENEOUS_CASES
                    .iter()
                    .map(|case| case.name)
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(format!(
                    "unknown policy-validation case '{case_filter}', expected one of: {known}"
                ));
            }
        }

        let experiments = HETEROGENEOUS_CASES
            .iter()
            .filter(|case| case_filters.is_empty() || case_filters.iter().any(|v| v == case.name))
            .map(|case| {
                let canonical = format!(
                    "policy-validation-v1;case={};patterns={};rows_per_group={ROWS_PER_GROUP};batch_size={BATCH_SIZE};payload_columns={PAYLOAD_COLUMNS}",
                    case.name,
                    case_patterns_canonical(case)
                );
                let id = format!("{:016x}", stable_hash(canonical.as_bytes()));
                PolicyExperiment { id, case }
            })
            .collect::<Vec<_>>();

        let mut manifest_key = format!(
            "schema={OUTPUT_SCHEMA_VERSION};stage={};seed={seed}",
            Stage::PolicyValidation.as_str()
        );
        for experiment in &experiments {
            manifest_key.push(';');
            manifest_key.push_str(&experiment.id);
        }
        Ok(Self {
            id: format!("{:016x}", stable_hash(manifest_key.as_bytes())),
            experiments,
        })
    }
}

fn case_patterns_canonical(case: &CaseSpec) -> String {
    case.row_groups
        .iter()
        .map(|pattern| match pattern {
            RowGroupPattern::AllSelected => "all".to_string(),
            RowGroupPattern::Cycle(runs) => runs
                .iter()
                .map(|run| format!("{}{}", if run.selected { 's' } else { 'k' }, run.len))
                .collect::<Vec<_>>()
                .join("-"),
        })
        .collect::<Vec<_>>()
        .join("|")
}

#[derive(Debug)]
struct PolicyPairSample {
    pair_index: usize,
    auto_first: bool,
    auto_ns: u64,
    auto_per_column_ns: u64,
    log_ratio: f64,
}

impl PolicyPairSample {
    fn to_json(&self) -> Value {
        json!({
            "pair_index": self.pair_index,
            "order": if self.auto_first { "auto-first" } else { "auto-per-column-first" },
            "auto_ns": self.auto_ns,
            "auto_per_column_ns": self.auto_per_column_ns,
            "log_auto_per_column_over_auto": self.log_ratio,
        })
    }
}

#[derive(Debug)]
struct ControlSample {
    position: &'static str,
    after_pairs: usize,
    auto_ns: u64,
    drift_from_start: f64,
}

impl ControlSample {
    fn to_json(&self) -> Value {
        json!({
            "position": self.position,
            "after_pairs": self.after_pairs,
            "auto_ns": self.auto_ns,
            "drift_from_start": self.drift_from_start,
        })
    }
}

#[derive(Debug)]
struct DecisionCounts {
    mask: usize,
    selectors: usize,
    fallback: usize,
}

impl DecisionCounts {
    fn from_metrics(metrics: &ArrowReaderMetrics) -> Self {
        Self {
            mask: metrics.row_selection_mask_decisions().unwrap_or_default(),
            selectors: metrics
                .row_selection_selector_decisions()
                .unwrap_or_default(),
            fallback: metrics
                .row_selection_fallback_decisions()
                .unwrap_or_default(),
        }
    }

    fn to_json(&self) -> Value {
        json!({
            "mask": self.mask,
            "selectors": self.selectors,
            "fallback": self.fallback,
        })
    }
}

struct PolicyPoint {
    samples: Vec<PolicyPairSample>,
    controls: Vec<ControlSample>,
    rows: usize,
    checksum: u64,
    decisions: DecisionCounts,
    median_auto_ns: f64,
    median_auto_per_column_ns: f64,
    median_log_ratio: f64,
    ci_low: f64,
    ci_high: f64,
    order_effect: Option<f64>,
    max_abs_control_drift: f64,
    end_control_drift: f64,
    stability_warning: bool,
    decision: &'static str,
    stop_reason: &'static str,
    elapsed_ms: u64,
    complete: bool,
}

impl PolicyPoint {
    fn to_json(&self, experiment: &PolicyExperiment, cli: &Cli) -> Value {
        json!({
            "record_type": "experiment",
            "status": if self.complete { "complete" } else { "incomplete" },
            "experiment": experiment.to_json(),
            "preflight": {
                "rows": self.rows,
                "checksum": format!("{:016x}", self.checksum),
            },
            "policy_decisions": {
                "auto_per_column": self.decisions.to_json(),
            },
            "sampling": cli.sampling_json(),
            "samples": self.samples.iter().map(PolicyPairSample::to_json).collect::<Vec<_>>(),
            "controls": self.controls.iter().map(ControlSample::to_json).collect::<Vec<_>>(),
            "summary": {
                "pairs": self.samples.len(),
                "median_auto_ns": self.median_auto_ns,
                "median_auto_per_column_ns": self.median_auto_per_column_ns,
                "median_log_auto_per_column_over_auto": self.median_log_ratio,
                "bootstrap_ci95": [self.ci_low, self.ci_high],
                "practical_decision": self.decision,
                "decision_band": cli.decision_band,
                "order_effect_log_ratio": self.order_effect,
                "order_bias_limit": ORDER_BIAS_LIMIT,
                "max_abs_control_drift": self.max_abs_control_drift,
                "end_control_drift": self.end_control_drift,
                "control_drift_limit": CONTROL_DRIFT_LIMIT,
                "stability_warning": self.stability_warning,
                "stop_reason": self.stop_reason,
            },
            "elapsed_ms": self.elapsed_ms,
        })
    }
}

pub(crate) fn run(cli: Cli) -> Result<(), String> {
    let manifest = PolicyManifest::generate(&cli.cases, cli.seed)?;
    let mut output = JsonlOutput::open(
        &cli,
        &manifest.id,
        manifest.experiments.len(),
        manifest.experiments.len(),
    )?;
    println!(
        "row-selection sampler: stage={}, manifest={}, experiments={}, output={}",
        cli.stage,
        manifest.id,
        manifest.experiments.len(),
        cli.output.display()
    );
    if output.resumed_records != 0 {
        println!(
            "resuming after {} completed policy-validation records",
            output.resumed_records
        );
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    let started = Instant::now();
    let mut completed_this_run = 0usize;
    let mut incomplete_this_run = 0usize;
    let mut skipped_resume = 0usize;

    for (idx, experiment) in manifest.experiments.iter().enumerate() {
        if output.is_completed(&experiment.id) {
            skipped_resume += 1;
            continue;
        }
        println!(
            "[{}/{}] {} {}",
            idx + 1,
            manifest.experiments.len(),
            experiment.id,
            experiment.case.name
        );
        let fixture = build_heterogeneous_fixture(experiment.case).map_err(|error| {
            format!(
                "failed to build mandatory policy-validation fixture {}: {error}",
                experiment.case.name
            )
        })?;
        let point = sample_policy_point(&cli, experiment, &fixture, &runtime)?;
        output.write(&point.to_json(experiment, &cli))?;
        if point.complete {
            output.mark_completed(&experiment.id);
            if point.stability_warning {
                output.mark_validation_warning(&experiment.id);
            }
            if point.decision == "inconclusive" {
                output.mark_validation_inconclusive(&experiment.id);
            }
            completed_this_run += 1;
        } else {
            incomplete_this_run += 1;
        }
    }

    let remaining = manifest
        .experiments
        .iter()
        .filter(|experiment| !output.is_completed(&experiment.id))
        .count();
    let validation_warnings = output.validation_warning_count();
    let inconclusive_points = output.validation_inconclusive_count();
    let validation_passed = remaining == 0 && validation_warnings == 0;
    let promotion_eligible = validation_passed && inconclusive_points == 0;
    output.write(&json!({
        "record_type": "run_end",
        "stage": cli.stage.as_str(),
        "manifest_id": manifest.id,
        "elapsed_seconds": started.elapsed().as_secs_f64(),
        "completed_this_run": completed_this_run,
        "unsupported_this_run": 0,
        "incomplete_this_run": incomplete_this_run,
        "skipped_from_resume": skipped_resume,
        "remaining_experiments": remaining,
        "budget_exhausted": false,
        "validation_warning_points": validation_warnings,
        "inconclusive_points": inconclusive_points,
        "validation_passed": validation_passed,
        "promotion_eligible": promotion_eligible,
        "sampling": cli.sampling_json(),
    }))?;
    println!(
        "policy validation finished in {:.3}s: completed={}, incomplete={}, remaining={}, stability warnings={}, inconclusive={}, validation passed={}, promotion eligible={}",
        started.elapsed().as_secs_f64(),
        completed_this_run,
        incomplete_this_run,
        remaining,
        validation_warnings,
        inconclusive_points,
        validation_passed,
        promotion_eligible
    );

    let ephemeral_output = cli.ephemeral_output;
    let output_path = cli.output.clone();
    drop(output);
    if ephemeral_output {
        std::fs::remove_file(&output_path).map_err(|error| {
            format!(
                "failed to remove ephemeral output {}: {error}",
                output_path.display()
            )
        })?;
    }
    Ok(())
}

fn sample_policy_point(
    cli: &Cli,
    experiment: &PolicyExperiment,
    fixture: &CaseFixture,
    runtime: &Runtime,
) -> Result<PolicyPoint, String> {
    let started = Instant::now();
    let auto_batches = runtime.block_on(capture_batches(
        fixture,
        RowSelectionPolicy::default(),
        None,
    ))?;
    let metrics = ArrowReaderMetrics::enabled();
    let auto_per_column_batches = runtime.block_on(capture_batches(
        fixture,
        RowSelectionPolicy::AutoPerColumn,
        Some(metrics.clone()),
    ))?;
    if auto_batches != auto_per_column_batches {
        return Err(format!(
            "correctness failure for {}: Auto and AutoPerColumn returned different RecordBatches",
            experiment.case.name
        ));
    }
    let rows = auto_batches
        .iter()
        .map(RecordBatch::num_rows)
        .sum::<usize>();
    if rows != fixture.expected_rows {
        return Err(format!(
            "correctness failure for {}: expected {} rows, got {rows}",
            experiment.case.name, fixture.expected_rows
        ));
    }
    let checksum = logical_checksum(&auto_batches)?;
    let decisions = DecisionCounts::from_metrics(&metrics);
    if decisions.mask + decisions.selectors == 0 {
        return Err(format!(
            "policy-validation fixture {} recorded no AutoPerColumn decisions",
            experiment.case.name
        ));
    }
    drop(auto_batches);
    drop(auto_per_column_batches);

    let seed = cli.seed ^ stable_hash(experiment.id.as_bytes()) ^ 0xa070_c011_2026_0817;
    let mut rng = StdRng::seed_from_u64(seed);
    for _ in 0..cli.warmup_pairs {
        run_pair(fixture, runtime, rng.random_bool(0.5), cli.inner_iterations)?;
    }

    let start_control_ns = measure_policy(
        fixture,
        runtime,
        RowSelectionPolicy::default(),
        cli.inner_iterations,
    )?;
    let mut controls = vec![ControlSample {
        position: "start",
        after_pairs: 0,
        auto_ns: start_control_ns,
        drift_from_start: 0.0,
    }];
    let mut samples = Vec::with_capacity(cli.max_pairs);
    let mut stop_reason = "max_pairs";
    let mut block_auto_first = false;

    while samples.len() < cli.max_pairs {
        if samples.len().is_multiple_of(2) {
            block_auto_first = rng.random_bool(0.5);
        }
        let auto_first = if samples.len().is_multiple_of(2) {
            block_auto_first
        } else {
            !block_auto_first
        };
        let (auto_ns, auto_per_column_ns) =
            run_pair(fixture, runtime, auto_first, cli.inner_iterations)?;
        samples.push(PolicyPairSample {
            pair_index: samples.len(),
            auto_first,
            auto_ns,
            auto_per_column_ns,
            log_ratio: (auto_per_column_ns as f64 / auto_ns as f64).ln(),
        });

        if samples.len().is_multiple_of(cli.control_interval_pairs) {
            let auto_ns = measure_policy(
                fixture,
                runtime,
                RowSelectionPolicy::default(),
                cli.inner_iterations,
            )?;
            controls.push(ControlSample {
                position: "periodic",
                after_pairs: samples.len(),
                auto_ns,
                drift_from_start: auto_ns as f64 / start_control_ns as f64 - 1.0,
            });
        }

        // Only stop after a complete two-pair block so both execution orders
        // always contribute equally to a completed point.
        if samples.len() >= cli.min_pairs && samples.len().is_multiple_of(2) {
            let log_ratios = samples
                .iter()
                .map(|sample| sample.log_ratio)
                .collect::<Vec<_>>();
            let (_, ci_low, ci_high) = bootstrap_median_ci(
                &log_ratios,
                cli.bootstrap_samples,
                seed ^ samples.len() as u64,
            );
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

    let end_control_ns = measure_policy(
        fixture,
        runtime,
        RowSelectionPolicy::default(),
        cli.inner_iterations,
    )?;
    let end_control_drift = end_control_ns as f64 / start_control_ns as f64 - 1.0;
    controls.push(ControlSample {
        position: "end",
        after_pairs: samples.len(),
        auto_ns: end_control_ns,
        drift_from_start: end_control_drift,
    });

    let log_ratios = samples
        .iter()
        .map(|sample| sample.log_ratio)
        .collect::<Vec<_>>();
    let (median_log_ratio, ci_low, ci_high) =
        bootstrap_median_ci(&log_ratios, cli.bootstrap_samples, seed ^ 0xb007_57a9);
    let order_effect = order_effect(&samples);
    let max_abs_control_drift = controls
        .iter()
        .map(|control| control.drift_from_start.abs())
        .fold(0.0, f64::max);
    let stability_warning = max_abs_control_drift > CONTROL_DRIFT_LIMIT
        || order_effect.is_some_and(|effect| effect.abs() > ORDER_BIAS_LIMIT);
    let decision = practical_decision(stability_warning, ci_low, ci_high, cli.decision_band);
    let complete = stop_reason != "point_timeout" || samples.len() == cli.max_pairs;

    Ok(PolicyPoint {
        median_auto_ns: median(samples.iter().map(|sample| sample.auto_ns as f64).collect()),
        median_auto_per_column_ns: median(
            samples
                .iter()
                .map(|sample| sample.auto_per_column_ns as f64)
                .collect(),
        ),
        samples,
        controls,
        rows,
        checksum,
        decisions,
        median_log_ratio,
        ci_low,
        ci_high,
        order_effect,
        max_abs_control_drift,
        end_control_drift,
        stability_warning,
        decision,
        stop_reason,
        elapsed_ms: millis(started.elapsed()),
        complete,
    })
}

fn run_pair(
    fixture: &CaseFixture,
    runtime: &Runtime,
    auto_first: bool,
    inner_iterations: usize,
) -> Result<(u64, u64), String> {
    if auto_first {
        Ok((
            measure_policy(
                fixture,
                runtime,
                RowSelectionPolicy::default(),
                inner_iterations,
            )?,
            measure_policy(
                fixture,
                runtime,
                RowSelectionPolicy::AutoPerColumn,
                inner_iterations,
            )?,
        ))
    } else {
        let auto_per_column = measure_policy(
            fixture,
            runtime,
            RowSelectionPolicy::AutoPerColumn,
            inner_iterations,
        )?;
        let auto = measure_policy(
            fixture,
            runtime,
            RowSelectionPolicy::default(),
            inner_iterations,
        )?;
        Ok((auto, auto_per_column))
    }
}

fn measure_policy(
    fixture: &CaseFixture,
    runtime: &Runtime,
    policy: RowSelectionPolicy,
    inner_iterations: usize,
) -> Result<u64, String> {
    let started = Instant::now();
    let mut rows = 0usize;
    for _ in 0..inner_iterations {
        let observed = runtime.block_on(run_rows(fixture, policy))?;
        if observed != fixture.expected_rows {
            return Err(format!(
                "policy-validation timed run expected {} rows, got {observed}",
                fixture.expected_rows
            ));
        }
        rows = rows.saturating_add(observed);
    }
    black_box(rows);
    let nanos = started.elapsed().as_nanos() / inner_iterations as u128;
    Ok(u64::try_from(nanos.max(1)).unwrap_or(u64::MAX))
}

async fn capture_batches(
    fixture: &CaseFixture,
    policy: RowSelectionPolicy,
    metrics: Option<ArrowReaderMetrics>,
) -> Result<Vec<RecordBatch>, String> {
    let mut stream = stream_builder(fixture, policy, metrics)
        .await?
        .build()
        .map_err(|error| error.to_string())?;
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch.map_err(|error| error.to_string())?);
    }
    Ok(batches)
}

async fn run_rows(fixture: &CaseFixture, policy: RowSelectionPolicy) -> Result<usize, String> {
    let mut stream = stream_builder(fixture, policy, None)
        .await?
        .build()
        .map_err(|error| error.to_string())?;
    let mut rows = 0usize;
    while let Some(batch) = stream.next().await {
        let batch = batch.map_err(|error| error.to_string())?;
        rows += black_box(batch.num_rows());
        black_box(batch);
    }
    Ok(rows)
}

async fn stream_builder(
    fixture: &CaseFixture,
    policy: RowSelectionPolicy,
    metrics: Option<ArrowReaderMetrics>,
) -> Result<ParquetRecordBatchStreamBuilder<InMemoryAsyncReader>, String> {
    let predicate_projection = ProjectionMask::roots(fixture.schema_descr(), [0]);
    let output_projection = ProjectionMask::roots(fixture.schema_descr(), 1..=PAYLOAD_COLUMNS);
    let predicate = ArrowPredicateFn::new(predicate_projection, |batch: RecordBatch| {
        eq(batch.column(0), &Int32Array::new_scalar(1))
    });
    let row_filter = RowFilter::new(vec![Box::new(predicate)]);
    let mut builder = ParquetRecordBatchStreamBuilder::new(fixture.reader())
        .await
        .map_err(|error| error.to_string())?
        .with_batch_size(BATCH_SIZE)
        .with_projection(output_projection)
        .with_row_filter(row_filter)
        .with_row_selection_policy(policy);
    if let Some(metrics) = metrics {
        builder = builder.with_metrics(metrics);
    }
    Ok(builder)
}

fn order_effect(samples: &[PolicyPairSample]) -> Option<f64> {
    let auto_first = samples
        .iter()
        .filter(|sample| sample.auto_first)
        .map(|sample| sample.log_ratio)
        .collect::<Vec<_>>();
    let auto_per_column_first = samples
        .iter()
        .filter(|sample| !sample.auto_first)
        .map(|sample| sample.log_ratio)
        .collect::<Vec<_>>();
    (!auto_first.is_empty() && !auto_per_column_first.is_empty())
        .then(|| median(auto_first) - median(auto_per_column_first))
}

fn practical_decision(
    stability_warning: bool,
    ci_low: f64,
    ci_high: f64,
    decision_band: f64,
) -> &'static str {
    if stability_warning {
        "unstable"
    } else if ci_high < -decision_band {
        "auto-per-column-faster"
    } else if ci_low > decision_band {
        "auto-faster"
    } else if ci_low >= -decision_band && ci_high <= decision_band {
        "practical-tie"
    } else {
        "inconclusive"
    }
}

fn millis(duration: std::time::Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}
