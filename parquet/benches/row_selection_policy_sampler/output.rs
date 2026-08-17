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
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};
use sysinfo::System;

use super::cli::Cli;
use super::model::{ExperimentManifest, OUTPUT_SCHEMA_VERSION, stable_hash};

pub(crate) struct JsonlOutput {
    writer: BufWriter<File>,
    completed: HashSet<String>,
    pub(crate) resumed_records: usize,
}

impl JsonlOutput {
    pub(crate) fn open(cli: &Cli, manifest: &ExperimentManifest) -> Result<Self, String> {
        let machine = machine_info();
        if let Some(parent) = cli.output.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent).map_err(|error| {
                format!(
                    "failed to create output directory {}: {error}",
                    parent.display()
                )
            })?;
        }

        if cli.resume {
            let completed = read_resume_state(cli, manifest, &machine.signature)?;
            let resumed_records = completed.len();
            let file = OpenOptions::new()
                .append(true)
                .open(&cli.output)
                .map_err(|error| format!("failed to append {}: {error}", cli.output.display()))?;
            return Ok(Self {
                writer: BufWriter::new(file),
                completed,
                resumed_records,
            });
        }

        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&cli.output)
            .map_err(|error| {
                format!(
                    "failed to create {}: {error}; use --resume for an existing file",
                    cli.output.display()
                )
            })?;
        let mut output = Self {
            writer: BufWriter::new(file),
            completed: HashSet::new(),
            resumed_records: 0,
        };
        output.write(&json!({
            "record_type": "manifest",
            "schema_version": OUTPUT_SCHEMA_VERSION,
            "manifest_id": manifest.id,
            "stage": cli.stage.as_str(),
            "seed": cli.seed,
            "experiment_count": manifest.experiments.len(),
            "mandatory_count": manifest.mandatory_count,
            "sampling": cli.sampling_json(),
            "machine_signature": machine.signature,
            "machine": machine.details,
            "crate_version": env!("CARGO_PKG_VERSION"),
            "created_unix_seconds": unix_seconds(),
            "git": git_info(),
        }))?;
        Ok(output)
    }

    pub(crate) fn is_completed(&self, experiment_id: &str) -> bool {
        self.completed.contains(experiment_id)
    }

    pub(crate) fn mark_completed(&mut self, experiment_id: &str) {
        self.completed.insert(experiment_id.to_string());
    }

    pub(crate) fn write(&mut self, value: &Value) -> Result<(), String> {
        serde_json::to_writer(&mut self.writer, value).map_err(|error| error.to_string())?;
        self.writer.write_all(b"\n").map_err(to_string)?;
        self.writer.flush().map_err(to_string)
    }
}

struct MachineInfo {
    signature: String,
    details: Value,
}

fn machine_info() -> MachineInfo {
    let mut system = System::new();
    system.refresh_cpu_all();
    let cpu = system.cpus().first();
    let os = System::long_os_version().or_else(System::name);
    let kernel = System::kernel_version();
    let cpu_brand = cpu.map(|cpu| cpu.brand().to_string());
    let logical_cpus = system.cpus().len();
    let signature_source = format!(
        "arch={};os={os:?};kernel={kernel:?};cpu={cpu_brand:?};logical_cpus={logical_cpus}",
        std::env::consts::ARCH
    );
    MachineInfo {
        signature: format!("{:016x}", stable_hash(signature_source.as_bytes())),
        details: json!({
            "arch": std::env::consts::ARCH,
            "os_family": std::env::consts::OS,
            "os_version": os,
            "kernel_version": kernel,
            "cpu_brand": cpu_brand,
            "cpu_frequency_mhz_at_start": cpu.map(|cpu| cpu.frequency()),
            "logical_cpus": logical_cpus,
            "available_parallelism": std::thread::available_parallelism().ok().map(|value| value.get()),
            "rustc": command_output("rustc", &["--version"]),
            "power_state": null,
        }),
    }
}

fn git_info() -> Value {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let commit = command_output("git", &["-C", manifest_dir, "rev-parse", "HEAD"]);
    let dirty = Command::new("git")
        .args(["-C", manifest_dir, "status", "--porcelain"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .is_some_and(|output| !output.stdout.is_empty());
    json!({"commit": commit, "dirty": dirty})
}

fn command_output(command: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(command).args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn read_resume_state(
    cli: &Cli,
    manifest: &ExperimentManifest,
    machine_signature: &str,
) -> Result<HashSet<String>, String> {
    let file = File::open(&cli.output)
        .map_err(|error| format!("failed to read {}: {error}", cli.output.display()))?;
    let mut completed = HashSet::new();
    let mut header_seen = false;
    for (line_idx, line) in BufReader::new(file).lines().enumerate() {
        let line = line.map_err(to_string)?;
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(&line)
            .map_err(|error| format!("invalid JSONL at line {}: {error}", line_idx + 1))?;
        match value.get("record_type").and_then(Value::as_str) {
            Some("manifest") if !header_seen => {
                validate_header(cli, manifest, machine_signature, &value)?;
                header_seen = true;
            }
            Some("experiment") => {
                let status = value.get("status").and_then(Value::as_str);
                if matches!(status, Some("complete" | "unsupported"))
                    && let Some(id) = value
                        .pointer("/experiment/experiment_id")
                        .and_then(Value::as_str)
                {
                    completed.insert(id.to_string());
                }
            }
            _ => {}
        }
    }
    if !header_seen {
        return Err(format!(
            "{} does not contain a sampler manifest header",
            cli.output.display()
        ));
    }
    Ok(completed)
}

fn validate_header(
    cli: &Cli,
    manifest: &ExperimentManifest,
    machine_signature: &str,
    header: &Value,
) -> Result<(), String> {
    let checks = [
        (
            "schema_version",
            header.get("schema_version").cloned(),
            json!(OUTPUT_SCHEMA_VERSION),
        ),
        (
            "manifest_id",
            header.get("manifest_id").cloned(),
            json!(manifest.id),
        ),
        (
            "stage",
            header.get("stage").cloned(),
            json!(cli.stage.as_str()),
        ),
        ("seed", header.get("seed").cloned(), json!(cli.seed)),
        (
            "machine_signature",
            header.get("machine_signature").cloned(),
            json!(machine_signature),
        ),
    ];
    for (name, actual, expected) in checks {
        if actual.as_ref() != Some(&expected) {
            return Err(format!(
                "cannot resume: manifest field {name} is {actual:?}, expected {expected}"
            ));
        }
    }
    Ok(())
}

fn unix_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn to_string(error: impl std::fmt::Display) -> String {
    error.to_string()
}
