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

mod cli;
mod fixture;
mod model;
mod output;
mod sampling;

use std::time::Instant;

use serde_json::{Value, json};

use self::cli::{Cli, ParseOutcome};
use self::fixture::FixtureCache;
use self::model::ExperimentManifest;
use self::output::JsonlOutput;
use self::sampling::{SampledPoint, sample_point};

pub(crate) fn run() -> Result<(), String> {
    let ParseOutcome::Run(cli) = Cli::parse()? else {
        return Ok(());
    };
    let manifest = ExperimentManifest::generate(cli.stage, &cli.kinds, cli.seed)?;
    let mut output = JsonlOutput::open(&cli, &manifest)?;
    println!(
        "row-selection sampler: stage={}, manifest={}, experiments={}, output={}",
        cli.stage,
        manifest.id,
        manifest.experiments.len(),
        cli.output.display()
    );
    if output.resumed_records != 0 {
        println!(
            "resuming after {} completed/unsupported experiment records",
            output.resumed_records
        );
    }

    let started = Instant::now();
    let mut fixtures = FixtureCache::new();
    let control_experiment = manifest
        .experiments
        .first()
        .expect("a non-empty manifest was validated");
    let control_fixture = fixtures.get(&control_experiment.fixture).map_err(|error| {
        format!(
            "failed to build mandatory control fixture {}: {error}",
            control_experiment.id
        )
    })?;
    let start_control = sample_point(&cli, control_experiment, &control_fixture, true)?;
    output.write(&control_record(&start_control, control_experiment, "start"))?;

    let mut completed_this_run = 0usize;
    let mut unsupported_this_run = 0usize;
    let mut incomplete_this_run = 0usize;
    let mut skipped_resume = 0usize;
    let mut budget_exhausted = false;

    for (idx, experiment) in manifest.experiments.iter().enumerate() {
        if output.is_completed(&experiment.id) {
            skipped_resume += 1;
            continue;
        }
        if started.elapsed() >= cli.budget && !experiment.mandatory {
            budget_exhausted = true;
            break;
        }
        println!(
            "[{}/{}] {} {} / {} / {}",
            idx + 1,
            manifest.experiments.len(),
            experiment.id,
            experiment.fixture.kind,
            experiment.selection.name,
            experiment.batch_size
        );

        let fixture = match fixtures.get(&experiment.fixture) {
            Ok(fixture) => fixture,
            Err(error) if !experiment.mandatory => {
                output.write(&json!({
                    "record_type": "experiment",
                    "status": "unsupported",
                    "experiment": experiment.to_json(),
                    "sampling": cli.sampling_json(),
                    "reason": error,
                }))?;
                output.mark_completed(&experiment.id);
                unsupported_this_run += 1;
                continue;
            }
            Err(error) => {
                return Err(format!(
                    "mandatory experiment {} is unsupported: {error}",
                    experiment.id
                ));
            }
        };
        let point = sample_point(&cli, experiment, &fixture, false)?;
        output.write(&point.to_json(experiment, "experiment"))?;
        if point.is_complete() {
            output.mark_completed(&experiment.id);
            completed_this_run += 1;
        } else {
            incomplete_this_run += 1;
        }
    }

    let end_control = sample_point(&cli, control_experiment, &control_fixture, true)?;
    output.write(&control_record(&end_control, control_experiment, "end"))?;
    let control_drift = end_control.baseline_ns() / start_control.baseline_ns() - 1.0;
    let remaining = manifest
        .experiments
        .iter()
        .filter(|experiment| !output.is_completed(&experiment.id))
        .count();
    output.write(&json!({
        "record_type": "run_end",
        "stage": cli.stage.as_str(),
        "manifest_id": manifest.id,
        "elapsed_seconds": started.elapsed().as_secs_f64(),
        "completed_this_run": completed_this_run,
        "unsupported_this_run": unsupported_this_run,
        "incomplete_this_run": incomplete_this_run,
        "skipped_from_resume": skipped_resume,
        "remaining_experiments": remaining,
        "budget_exhausted": budget_exhausted,
        "control_baseline_drift": control_drift,
        "control_drift_warning": control_drift.abs() > 0.10,
        "sampling": cli.sampling_json(),
    }))?;

    println!(
        "sampler finished in {:.3}s: completed={}, unsupported={}, incomplete={}, remaining={}, control drift={:+.2}%",
        started.elapsed().as_secs_f64(),
        completed_this_run,
        unsupported_this_run,
        incomplete_this_run,
        remaining,
        control_drift * 100.0
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

fn control_record(point: &SampledPoint, experiment: &model::Experiment, position: &str) -> Value {
    let mut value = point.to_json(experiment, "control");
    value
        .as_object_mut()
        .expect("sample records are JSON objects")
        .insert("position".into(), json!(position));
    value
}
