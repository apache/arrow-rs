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

use std::collections::HashSet;
use std::ffi::OsString;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

use super::model::{FixtureKind, Stage};

pub(crate) enum ParseOutcome {
    Help,
    Run(Cli),
}

pub(crate) struct Cli {
    pub(crate) stage: Stage,
    pub(crate) budget: Duration,
    pub(crate) seed: u64,
    pub(crate) output: PathBuf,
    pub(crate) resume: bool,
    pub(crate) kinds: Vec<FixtureKind>,
    pub(crate) min_pairs: usize,
    pub(crate) max_pairs: usize,
    pub(crate) warmup_pairs: usize,
    pub(crate) bootstrap_samples: usize,
    pub(crate) decision_band: f64,
    pub(crate) target_ci_width: f64,
    pub(crate) point_timeout: Duration,
    pub(crate) inner_iterations: usize,
    pub(crate) ephemeral_output: bool,
}

impl Cli {
    pub(crate) fn parse() -> Result<ParseOutcome, String> {
        let mut args = std::env::args_os().skip(1);
        let mut stage = None;
        let mut budget_seconds = None;
        let mut seed = 0x5eed_c057_2026_0817u64;
        let mut output = None;
        let mut resume = false;
        let mut kinds = Vec::new();
        let mut min_pairs = None;
        let mut max_pairs = None;
        let mut warmup_pairs = None;
        let mut bootstrap_samples = None;
        let mut decision_band = None;
        let mut target_ci_width = None;
        let mut point_timeout_seconds = 60.0;
        let mut inner_iterations = 1usize;
        let mut ephemeral_output = false;

        while let Some(arg) = args.next() {
            let arg = arg
                .into_string()
                .map_err(|_| "sampler arguments must be valid UTF-8".to_string())?;
            match arg.as_str() {
                "-h" | "--help" => {
                    print_help();
                    return Ok(ParseOutcome::Help);
                }
                "--test" => ephemeral_output = true,
                "--smoke" => stage = Some(Stage::Smoke),
                // Cargo appends this libtest-compatible flag to bench
                // binaries even when `harness = false`.
                "--bench" => {}
                "--stage" => {
                    stage = Some(Stage::parse(&next_string(&mut args, "--stage")?)?);
                }
                "--budget-seconds" => {
                    budget_seconds = Some(parse_f64(
                        &next_string(&mut args, "--budget-seconds")?,
                        "--budget-seconds",
                    )?);
                }
                "--seed" => {
                    seed = parse_u64(&next_string(&mut args, "--seed")?, "--seed")?;
                }
                "--output" => {
                    output = Some(PathBuf::from(next_os(&mut args, "--output")?));
                }
                "--resume" => resume = true,
                "--kind" => {
                    kinds.push(FixtureKind::parse(&next_string(&mut args, "--kind")?)?);
                }
                "--min-pairs" => {
                    min_pairs = Some(parse_usize(
                        &next_string(&mut args, "--min-pairs")?,
                        "--min-pairs",
                    )?);
                }
                "--max-pairs" => {
                    max_pairs = Some(parse_usize(
                        &next_string(&mut args, "--max-pairs")?,
                        "--max-pairs",
                    )?);
                }
                "--warmup-pairs" => {
                    warmup_pairs = Some(parse_usize(
                        &next_string(&mut args, "--warmup-pairs")?,
                        "--warmup-pairs",
                    )?);
                }
                "--bootstrap-samples" => {
                    bootstrap_samples = Some(parse_usize(
                        &next_string(&mut args, "--bootstrap-samples")?,
                        "--bootstrap-samples",
                    )?);
                }
                "--decision-band" => {
                    decision_band = Some(parse_f64(
                        &next_string(&mut args, "--decision-band")?,
                        "--decision-band",
                    )?);
                }
                "--target-ci-width" => {
                    target_ci_width = Some(parse_f64(
                        &next_string(&mut args, "--target-ci-width")?,
                        "--target-ci-width",
                    )?);
                }
                "--point-timeout-seconds" => {
                    point_timeout_seconds = parse_f64(
                        &next_string(&mut args, "--point-timeout-seconds")?,
                        "--point-timeout-seconds",
                    )?;
                }
                "--inner-iterations" => {
                    inner_iterations = parse_usize(
                        &next_string(&mut args, "--inner-iterations")?,
                        "--inner-iterations",
                    )?;
                }
                _ => return Err(format!("unknown argument '{arg}', use --help")),
            }
        }

        if ephemeral_output && stage.is_some_and(|stage| stage != Stage::Smoke) {
            return Err("--test cannot be combined with a non-smoke --stage".into());
        }
        let stage = stage.unwrap_or(Stage::Smoke);
        let defaults = SamplingDefaults::for_stage(stage);
        let min_pairs = min_pairs.unwrap_or(defaults.min_pairs);
        let max_pairs = max_pairs.unwrap_or(defaults.max_pairs);
        let warmup_pairs = warmup_pairs.unwrap_or(defaults.warmup_pairs);
        let bootstrap_samples = bootstrap_samples.unwrap_or(defaults.bootstrap_samples);
        let budget_seconds = budget_seconds.unwrap_or(defaults.budget_seconds);
        let decision_band = decision_band.unwrap_or(defaults.decision_band);
        let target_ci_width = target_ci_width.unwrap_or(defaults.target_ci_width);

        if !budget_seconds.is_finite() || budget_seconds <= 0.0 {
            return Err("--budget-seconds must be finite and greater than zero".into());
        }
        if min_pairs == 0 || max_pairs < min_pairs {
            return Err("pair counts must satisfy 0 < min-pairs <= max-pairs".into());
        }
        if bootstrap_samples == 0 {
            return Err("--bootstrap-samples must be greater than zero".into());
        }
        if !decision_band.is_finite() || decision_band < 0.0 {
            return Err("--decision-band must be finite and non-negative".into());
        }
        if !target_ci_width.is_finite() || target_ci_width <= 0.0 {
            return Err("--target-ci-width must be finite and greater than zero".into());
        }
        if !point_timeout_seconds.is_finite() || point_timeout_seconds <= 0.0 {
            return Err("--point-timeout-seconds must be finite and greater than zero".into());
        }
        if inner_iterations == 0 {
            return Err("--inner-iterations must be greater than zero".into());
        }
        let mut seen = HashSet::new();
        kinds.retain(|kind| seen.insert(*kind));

        if ephemeral_output && output.is_some() {
            return Err("--test cannot be combined with --output".into());
        }
        if ephemeral_output && resume {
            return Err("--test cannot be combined with --resume".into());
        }
        let output = output.unwrap_or_else(|| default_output(stage, ephemeral_output));

        Ok(ParseOutcome::Run(Self {
            stage,
            budget: Duration::from_secs_f64(budget_seconds),
            seed,
            output,
            resume,
            kinds,
            min_pairs,
            max_pairs,
            warmup_pairs,
            bootstrap_samples,
            decision_band,
            target_ci_width,
            point_timeout: Duration::from_secs_f64(point_timeout_seconds),
            inner_iterations,
            ephemeral_output,
        }))
    }

    pub(crate) fn sampling_json(&self) -> Value {
        json!({
            "budget_seconds": self.budget.as_secs_f64(),
            "min_pairs": self.min_pairs,
            "max_pairs": self.max_pairs,
            "warmup_pairs": self.warmup_pairs,
            "bootstrap_samples": self.bootstrap_samples,
            "decision_band": self.decision_band,
            "target_ci_width": self.target_ci_width,
            "point_timeout_seconds": self.point_timeout.as_secs_f64(),
            "inner_iterations": self.inner_iterations,
            "kind_filters": self.kinds.iter().map(|kind| kind.as_str()).collect::<Vec<_>>(),
        })
    }
}

struct SamplingDefaults {
    budget_seconds: f64,
    min_pairs: usize,
    max_pairs: usize,
    warmup_pairs: usize,
    bootstrap_samples: usize,
    decision_band: f64,
    target_ci_width: f64,
}

impl SamplingDefaults {
    fn for_stage(stage: Stage) -> Self {
        match stage {
            Stage::Smoke => Self {
                budget_seconds: 30.0,
                min_pairs: 2,
                max_pairs: 2,
                warmup_pairs: 1,
                bootstrap_samples: 100,
                decision_band: 0.0,
                target_ci_width: 0.05,
            },
            Stage::Pilot => Self {
                budget_seconds: 300.0,
                min_pairs: 6,
                max_pairs: 30,
                warmup_pairs: 2,
                bootstrap_samples: 400,
                decision_band: 0.0,
                target_ci_width: 0.05,
            },
            Stage::Refinement => Self {
                budget_seconds: 300.0,
                min_pairs: 8,
                max_pairs: 40,
                warmup_pairs: 2,
                bootstrap_samples: 600,
                decision_band: 0.03,
                target_ci_width: 0.04,
            },
            Stage::PageValidation => Self {
                budget_seconds: 60.0,
                min_pairs: 4,
                max_pairs: 12,
                warmup_pairs: 1,
                bootstrap_samples: 200,
                decision_band: 0.0,
                target_ci_width: 0.05,
            },
        }
    }
}

fn default_output(stage: Stage, ephemeral: bool) -> PathBuf {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let name = format!(
        "row-selection-{}-{now}-{}.jsonl",
        stage.as_str(),
        std::process::id()
    );
    if ephemeral {
        return std::env::temp_dir().join(name);
    }
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .expect("parquet crate must be in a workspace")
                .join("target")
        });
    target.join("row-selection-cost-samples").join(name)
}

fn next_os(args: &mut impl Iterator<Item = OsString>, flag: &str) -> Result<OsString, String> {
    args.next()
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn next_string(args: &mut impl Iterator<Item = OsString>, flag: &str) -> Result<String, String> {
    next_os(args, flag)?
        .into_string()
        .map_err(|_| format!("{flag} requires a UTF-8 value"))
}

fn parse_usize(value: &str, flag: &str) -> Result<usize, String> {
    value
        .parse()
        .map_err(|_| format!("{flag} requires a positive integer, got '{value}'"))
}

fn parse_u64(value: &str, flag: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|_| format!("{flag} requires an unsigned integer, got '{value}'"))
}

fn parse_f64(value: &str, flag: &str) -> Result<f64, String> {
    value
        .parse()
        .map_err(|_| format!("{flag} requires a number, got '{value}'"))
}

fn print_help() {
    println!(
        r"Offline paired row-selection cost sampler

Usage:
  cargo bench -p parquet --features arrow,async \
    --bench arrow_reader_row_selection_policy_sampler -- [OPTIONS]

Options:
  --stage <smoke|pilot|refinement|page-validation>
                                           Sampling stage (default: smoke)
  --smoke                                 Alias for --stage smoke
  --test                                  Ephemeral smoke run used by validation
  --budget-seconds <SECONDS>              Expansion sampling wall-clock budget
  --seed <U64>                            Deterministic manifest/order seed
  --output <PATH>                         JSONL output path
  --resume                                Resume an existing matching JSONL file
  --kind <KIND>                           Repeatable type filter: int32,
                                           string-view, dictionary, fixed-binary
  --min-pairs <N>                         Minimum measured pairs per point
  --max-pairs <N>                         Maximum measured pairs per point
  --warmup-pairs <N>                      Untimed warmup pairs per point
  --bootstrap-samples <N>                 Bootstrap resamples for the median CI
  --decision-band <LOG_RATIO>             Early-stop band around zero
  --target-ci-width <LOG_RATIO>           Precision early-stop threshold
  --point-timeout-seconds <SECONDS>       Mark a slow point incomplete
  --inner-iterations <N>                  Full scans per timed observation
  -h, --help                              Print this help
"
    );
}
