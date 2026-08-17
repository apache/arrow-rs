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
use std::fmt::{Display, Formatter};

use arrow_buffer::BooleanBuffer;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use rand::{RngExt, SeedableRng, rngs::StdRng};
use serde_json::{Value, json};

pub(crate) const OUTPUT_SCHEMA_VERSION: u64 = 1;
const MAX_PILOT_EXPANSION_POINTS: usize = 192;
const MAX_REFINEMENT_EXPANSION_POINTS: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Stage {
    Smoke,
    Pilot,
    Refinement,
    PageValidation,
}

impl Stage {
    pub(crate) fn parse(value: &str) -> Result<Self, String> {
        match value {
            "smoke" => Ok(Self::Smoke),
            "pilot" => Ok(Self::Pilot),
            "refinement" => Ok(Self::Refinement),
            "page-validation" => Ok(Self::PageValidation),
            _ => Err(format!(
                "unknown stage '{value}', expected smoke, pilot, refinement, or page-validation"
            )),
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Pilot => "pilot",
            Self::Refinement => "refinement",
            Self::PageValidation => "page-validation",
        }
    }
}

impl Display for Stage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum FixtureKind {
    Int32,
    StringView,
    Dictionary,
    FixedBinary,
}

impl FixtureKind {
    pub(crate) const ALL: [Self; 4] = [
        Self::Int32,
        Self::StringView,
        Self::Dictionary,
        Self::FixedBinary,
    ];

    pub(crate) fn parse(value: &str) -> Result<Self, String> {
        match value {
            "int32" => Ok(Self::Int32),
            "string-view" => Ok(Self::StringView),
            "dictionary" => Ok(Self::Dictionary),
            "fixed-binary" => Ok(Self::FixedBinary),
            _ => Err(format!(
                "unknown kind '{value}', expected int32, string-view, dictionary, or fixed-binary"
            )),
        }
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Int32 => "int32",
            Self::StringView => "string-view",
            Self::Dictionary => "dictionary",
            Self::FixedBinary => "fixed-binary",
        }
    }
}

impl Display for FixtureKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum SelectionBacking {
    Selectors,
    Mask,
}

impl SelectionBacking {
    fn as_str(self) -> &'static str {
        match self {
            Self::Selectors => "selectors",
            Self::Mask => "mask",
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct FixtureSpec {
    pub(crate) kind: FixtureKind,
    pub(crate) rows: usize,
    pub(crate) nullable: bool,
    pub(crate) null_every: Option<usize>,
    pub(crate) value_width: usize,
    pub(crate) dictionary_cardinality: usize,
    pub(crate) page_rows: usize,
}

impl FixtureSpec {
    fn default_for(kind: FixtureKind, nullable: bool) -> Self {
        let (value_width, dictionary_cardinality) = match kind {
            FixtureKind::Int32 => (4, 0),
            FixtureKind::StringView => (64, 0),
            FixtureKind::Dictionary => (32, 256),
            FixtureKind::FixedBinary => (32, 0),
        };
        Self {
            kind,
            rows: 16_384,
            nullable,
            null_every: nullable.then_some(4),
            value_width,
            dictionary_cardinality,
            page_rows: 512,
        }
    }

    pub(crate) fn canonical(&self) -> String {
        format!(
            "kind={};rows={};nullable={};null_every={};width={};cardinality={};page_rows={}",
            self.kind,
            self.rows,
            self.nullable,
            self.null_every.unwrap_or(0),
            self.value_width,
            self.dictionary_cardinality,
            self.page_rows
        )
    }

    pub(crate) fn to_json(&self) -> Value {
        json!({
            "kind": self.kind.as_str(),
            "rows": self.rows,
            "nullable": self.nullable,
            "null_every": self.null_every,
            "value_width": self.value_width,
            "dictionary_cardinality": self.dictionary_cardinality,
            "page_rows": self.page_rows,
            "compression": "UNCOMPRESSED",
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct SelectionSpec {
    pub(crate) name: &'static str,
    pub(crate) skip_run: usize,
    pub(crate) select_run: usize,
    pub(crate) offset: usize,
    pub(crate) backing: SelectionBacking,
}

impl SelectionSpec {
    fn canonical(&self) -> String {
        format!(
            "selection={};skip={};select={};offset={};backing={}",
            self.name,
            self.skip_run,
            self.select_run,
            self.offset,
            self.backing.as_str()
        )
    }

    pub(crate) fn to_json(&self) -> Value {
        json!({
            "name": self.name,
            "skip_run": self.skip_run,
            "select_run": self.select_run,
            "offset": self.offset,
            "source_backing": self.backing.as_str(),
        })
    }

    pub(crate) fn materialize(&self, rows: usize, batch_size: usize) -> SelectionMaterialization {
        let mut bits = vec![false; rows];
        let mut cursor = self.offset.min(rows);
        while cursor < rows {
            cursor = cursor.saturating_add(self.skip_run).min(rows);
            let end = cursor.saturating_add(self.select_run).min(rows);
            bits[cursor..end].fill(true);
            cursor = end;
        }

        let selectors = selectors_from_bits(&bits);
        let selection = match self.backing {
            SelectionBacking::Selectors => RowSelection::from(selectors),
            SelectionBacking::Mask => {
                RowSelection::from_boolean_buffer(BooleanBuffer::from(bits.clone()))
            }
        };
        let stats = SelectionStats::new(&bits, batch_size);
        SelectionMaterialization { selection, stats }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExecutionMode {
    SyncOracle,
    PageValidation,
}

impl ExecutionMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::SyncOracle => "sync-oracle",
            Self::PageValidation => "page-validation",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Experiment {
    pub(crate) id: String,
    pub(crate) fixture: FixtureSpec,
    pub(crate) selection: SelectionSpec,
    pub(crate) batch_size: usize,
    pub(crate) mode: ExecutionMode,
    pub(crate) mandatory: bool,
}

impl Experiment {
    fn new(
        fixture: FixtureSpec,
        selection: SelectionSpec,
        batch_size: usize,
        mode: ExecutionMode,
        mandatory: bool,
    ) -> Self {
        let canonical = format!(
            "{};{};batch_size={};mode={}",
            fixture.canonical(),
            selection.canonical(),
            batch_size,
            mode.as_str()
        );
        Self {
            id: format!("{:016x}", stable_hash(canonical.as_bytes())),
            fixture,
            selection,
            batch_size,
            mode,
            mandatory,
        }
    }

    pub(crate) fn to_json(&self) -> Value {
        json!({
            "experiment_id": self.id,
            "mandatory": self.mandatory,
            "execution_mode": self.mode.as_str(),
            "batch_size": self.batch_size,
            "fixture": self.fixture.to_json(),
            "selection": self.selection.to_json(),
        })
    }
}

pub(crate) struct ExperimentManifest {
    pub(crate) id: String,
    pub(crate) experiments: Vec<Experiment>,
    pub(crate) mandatory_count: usize,
}

impl ExperimentManifest {
    pub(crate) fn generate(stage: Stage, kinds: &[FixtureKind], seed: u64) -> Result<Self, String> {
        let kinds = if kinds.is_empty() {
            FixtureKind::ALL.to_vec()
        } else {
            kinds.to_vec()
        };
        let mut experiments = match stage {
            Stage::Smoke => smoke_experiments(&kinds),
            Stage::Pilot => pilot_experiments(&kinds, seed),
            Stage::Refinement => refinement_experiments(&kinds, seed),
            Stage::PageValidation => page_validation_experiments(&kinds),
        };
        if experiments.is_empty() {
            return Err("the selected stage and kind filters produced no experiments".into());
        }

        let mut seen = HashSet::new();
        experiments.retain(|experiment| seen.insert(experiment.id.clone()));
        let mandatory_count = experiments
            .iter()
            .filter(|experiment| experiment.mandatory)
            .count();
        let mut manifest_key = format!(
            "schema={OUTPUT_SCHEMA_VERSION};stage={};seed={seed}",
            stage.as_str()
        );
        for experiment in &experiments {
            manifest_key.push(';');
            manifest_key.push_str(&experiment.id);
        }
        Ok(Self {
            id: format!("{:016x}", stable_hash(manifest_key.as_bytes())),
            experiments,
            mandatory_count,
        })
    }
}

pub(crate) struct SelectionMaterialization {
    pub(crate) selection: RowSelection,
    pub(crate) stats: SelectionStats,
}

#[derive(Debug)]
pub(crate) struct SelectionStats {
    total_rows: usize,
    selected_rows: usize,
    selected_runs: usize,
    skipped_runs: usize,
    transitions: usize,
    first_selected: Option<usize>,
    last_selected: Option<usize>,
    selected_span_rows: usize,
    mask_decode_rows_without_page_pruning: usize,
    output_batches: usize,
    mean_selected_run: f64,
    mean_skipped_run: f64,
    max_selected_run: usize,
    max_skipped_run: usize,
}

impl SelectionStats {
    fn new(bits: &[bool], batch_size: usize) -> Self {
        let selected_positions = bits
            .iter()
            .enumerate()
            .filter_map(|(idx, selected)| selected.then_some(idx))
            .collect::<Vec<_>>();
        let selected_rows = selected_positions.len();
        let first_selected = selected_positions.first().copied();
        let last_selected = selected_positions.last().copied();
        let selected_span_rows = first_selected
            .zip(last_selected)
            .map(|(first, last)| last - first + 1)
            .unwrap_or(0);
        let mask_decode_rows_without_page_pruning = selected_positions
            .chunks(batch_size)
            .map(|chunk| chunk[chunk.len() - 1] - chunk[0] + 1)
            .sum();

        let execution_bits = last_selected.map_or(&bits[..0], |last| &bits[..=last]);
        let mut selected_lengths = Vec::new();
        let mut skipped_lengths = Vec::new();
        for selector in selectors_from_bits(execution_bits) {
            if selector.skip {
                skipped_lengths.push(selector.row_count);
            } else {
                selected_lengths.push(selector.row_count);
            }
        }
        let selected_runs = selected_lengths.len();
        let skipped_runs = skipped_lengths.len();
        let transitions = selected_runs.saturating_add(skipped_runs).saturating_sub(1);

        Self {
            total_rows: bits.len(),
            selected_rows,
            selected_runs,
            skipped_runs,
            transitions,
            first_selected,
            last_selected,
            selected_span_rows,
            mask_decode_rows_without_page_pruning,
            output_batches: selected_rows.div_ceil(batch_size),
            mean_selected_run: mean(&selected_lengths),
            mean_skipped_run: mean(&skipped_lengths),
            max_selected_run: selected_lengths.into_iter().max().unwrap_or(0),
            max_skipped_run: skipped_lengths.into_iter().max().unwrap_or(0),
        }
    }

    pub(crate) fn selected_rows(&self) -> usize {
        self.selected_rows
    }

    pub(crate) fn to_json(&self) -> Value {
        json!({
            "total_rows": self.total_rows,
            "selected_rows": self.selected_rows,
            "selectivity": self.selected_rows as f64 / self.total_rows as f64,
            "selected_runs": self.selected_runs,
            "skipped_runs": self.skipped_runs,
            "transitions": self.transitions,
            "first_selected": self.first_selected,
            "last_selected": self.last_selected,
            "selected_span_rows": self.selected_span_rows,
            "mask_decode_rows_without_page_pruning": self.mask_decode_rows_without_page_pruning,
            "output_batches": self.output_batches,
            "mean_selected_run": self.mean_selected_run,
            "mean_skipped_run": self.mean_skipped_run,
            "max_selected_run": self.max_selected_run,
            "max_skipped_run": self.max_skipped_run,
        })
    }
}

fn selectors_from_bits(bits: &[bool]) -> Vec<RowSelector> {
    let mut selectors = Vec::new();
    let Some((&first, tail)) = bits.split_first() else {
        return selectors;
    };
    let mut selected = first;
    let mut rows = 1usize;
    for &next in tail {
        if next == selected {
            rows += 1;
        } else {
            selectors.push(if selected {
                RowSelector::select(rows)
            } else {
                RowSelector::skip(rows)
            });
            selected = next;
            rows = 1;
        }
    }
    selectors.push(if selected {
        RowSelector::select(rows)
    } else {
        RowSelector::skip(rows)
    });
    selectors
}

fn mean(values: &[usize]) -> f64 {
    if values.is_empty() {
        0.0
    } else {
        values.iter().sum::<usize>() as f64 / values.len() as f64
    }
}

fn selection_anchors() -> Vec<SelectionSpec> {
    [
        ("sparse", 63, 1, 0),
        ("fragmented", 1, 1, 0),
        ("clustered", 128, 128, 17),
        ("dense", 1, 63, 0),
    ]
    .into_iter()
    .flat_map(|(name, skip_run, select_run, offset)| {
        [SelectionBacking::Selectors, SelectionBacking::Mask].map(|backing| SelectionSpec {
            name,
            skip_run,
            select_run,
            offset,
            backing,
        })
    })
    .collect()
}

fn smoke_experiments(kinds: &[FixtureKind]) -> Vec<Experiment> {
    let selections = selection_anchors();
    kinds
        .iter()
        .copied()
        .enumerate()
        .map(|(idx, kind)| {
            let nullable = idx % 2 == 1;
            let mut fixture = FixtureSpec::default_for(kind, nullable);
            fixture.rows = 8_192;
            fixture.page_rows = 256;
            Experiment::new(
                fixture,
                selections[idx % selections.len()].clone(),
                1_024,
                ExecutionMode::SyncOracle,
                true,
            )
        })
        .collect()
}

fn pilot_experiments(kinds: &[FixtureKind], seed: u64) -> Vec<Experiment> {
    let selections = selection_anchors();
    let mut mandatory = Vec::new();
    for &kind in kinds {
        for (idx, selection) in [
            SelectionSpec {
                name: "sparse",
                skip_run: 63,
                select_run: 1,
                offset: 0,
                backing: SelectionBacking::Selectors,
            },
            SelectionSpec {
                name: "fragmented",
                skip_run: 1,
                select_run: 1,
                offset: 0,
                backing: SelectionBacking::Mask,
            },
        ]
        .into_iter()
        .enumerate()
        {
            let fixture = FixtureSpec::default_for(kind, idx == 1);
            mandatory.push(Experiment::new(
                fixture,
                selection,
                if idx == 0 { 1_024 } else { 8_192 },
                ExecutionMode::SyncOracle,
                true,
            ));
        }
    }

    let mut expansion = Vec::new();
    for fixture in fixture_variants(kinds) {
        for selection in &selections {
            for batch_size in [256, 1_024, 8_192] {
                if batch_size <= fixture.rows {
                    expansion.push(Experiment::new(
                        fixture.clone(),
                        selection.clone(),
                        batch_size,
                        ExecutionMode::SyncOracle,
                        false,
                    ));
                }
            }
        }
    }
    seeded_shuffle(&mut expansion, seed ^ 0x51ec_7100_c057_0001);
    expansion.truncate(MAX_PILOT_EXPANSION_POINTS);
    mandatory.extend(expansion);
    mandatory
}

fn fixture_variants(kinds: &[FixtureKind]) -> Vec<FixtureSpec> {
    let mut fixtures = Vec::new();
    for &kind in kinds {
        let widths: &[usize] = match kind {
            FixtureKind::Int32 => &[4],
            FixtureKind::StringView => &[16, 64, 256],
            FixtureKind::Dictionary => &[16, 64],
            FixtureKind::FixedBinary => &[8, 32],
        };
        let cardinalities: &[usize] = match kind {
            FixtureKind::Dictionary => &[16, 1_024],
            _ => &[0],
        };
        for &rows in &[16_384, 65_536] {
            for &page_rows in &[256, 2_048] {
                for &nullable in &[false, true] {
                    for &value_width in widths {
                        for &dictionary_cardinality in cardinalities {
                            fixtures.push(FixtureSpec {
                                kind,
                                rows,
                                nullable,
                                null_every: nullable.then_some(4),
                                value_width,
                                dictionary_cardinality,
                                page_rows,
                            });
                        }
                    }
                }
            }
        }
    }
    fixtures
}

fn refinement_experiments(kinds: &[FixtureKind], seed: u64) -> Vec<Experiment> {
    let shapes = refinement_shapes();
    let mut mandatory = Vec::with_capacity(kinds.len() * shapes.len());
    for (kind_idx, &kind) in kinds.iter().enumerate() {
        for (shape_idx, &(name, skip_run, select_run)) in shapes.iter().enumerate() {
            let backing = if (kind_idx + shape_idx).is_multiple_of(2) {
                SelectionBacking::Selectors
            } else {
                SelectionBacking::Mask
            };
            mandatory.push(Experiment::new(
                FixtureSpec::default_for(kind, false),
                SelectionSpec {
                    name,
                    skip_run,
                    select_run,
                    offset: 0,
                    backing,
                },
                1_024,
                ExecutionMode::SyncOracle,
                true,
            ));
        }
    }

    let selections = shapes
        .into_iter()
        .flat_map(|(name, skip_run, select_run)| {
            [SelectionBacking::Selectors, SelectionBacking::Mask].map(|backing| SelectionSpec {
                name,
                skip_run,
                select_run,
                offset: 0,
                backing,
            })
        })
        .collect::<Vec<_>>();
    let mut expansion = Vec::new();
    for fixture in fixture_variants(kinds) {
        for selection in &selections {
            for batch_size in [256, 1_024, 8_192] {
                if batch_size <= fixture.rows {
                    expansion.push(Experiment::new(
                        fixture.clone(),
                        selection.clone(),
                        batch_size,
                        ExecutionMode::SyncOracle,
                        false,
                    ));
                }
            }
        }
    }
    seeded_shuffle(&mut expansion, seed ^ 0x7ef1_6e00_c057_0001);
    expansion.truncate(MAX_REFINEMENT_EXPANSION_POINTS);
    mandatory.extend(expansion);
    mandatory
}

/// Selection shapes around and between the coarse Pilot anchors. These vary
/// transition density independently from selectivity so the measured data,
/// rather than the legacy threshold, locates the decision boundary.
fn refinement_shapes() -> Vec<(&'static str, usize, usize)> {
    let mut shapes = Vec::new();
    shapes.extend(
        [1, 2, 4, 8, 12, 16, 20, 24, 28, 32, 40, 48, 64]
            .into_iter()
            .map(|run| ("balanced-grid", run, run)),
    );
    shapes.extend(
        [15, 31, 47, 63, 95]
            .into_iter()
            .map(|skip_run| ("sparse-grid", skip_run, 1)),
    );
    shapes.extend(
        [15, 31, 47, 63, 95]
            .into_iter()
            .map(|select_run| ("dense-grid", 1, select_run)),
    );
    shapes.extend(
        [4, 8, 12, 16, 24, 32]
            .into_iter()
            .map(|run| ("quarter-grid", run * 3, run)),
    );
    shapes.extend(
        [4, 8, 12, 16, 24, 32]
            .into_iter()
            .map(|run| ("three-quarter-grid", run, run * 3)),
    );
    shapes
}

fn page_validation_experiments(kinds: &[FixtureKind]) -> Vec<Experiment> {
    if !kinds.contains(&FixtureKind::Int32) {
        return Vec::new();
    }
    let fixture = FixtureSpec {
        kind: FixtureKind::Int32,
        rows: 8_192,
        nullable: false,
        null_every: None,
        value_width: 4,
        dictionary_cardinality: 0,
        page_rows: 128,
    };
    [SelectionBacking::Selectors, SelectionBacking::Mask]
        .into_iter()
        .map(|backing| {
            Experiment::new(
                fixture.clone(),
                SelectionSpec {
                    name: "page-sparse",
                    skip_run: 511,
                    select_run: 1,
                    offset: 0,
                    backing,
                },
                512,
                ExecutionMode::PageValidation,
                true,
            )
        })
        .collect()
}

fn seeded_shuffle<T>(values: &mut [T], seed: u64) {
    let mut rng = StdRng::seed_from_u64(seed);
    for idx in (1..values.len()).rev() {
        let swap_idx = rng.random_range(0..=idx);
        values.swap(idx, swap_idx);
    }
}

pub(crate) fn stable_hash(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}
