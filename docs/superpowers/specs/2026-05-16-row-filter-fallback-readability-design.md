# Row Filter Fallback Readability Refactor Design

## Context

The current branch adds adaptive post-filter fallback for the async Parquet row-filter path. The core design is already split into focused modules:

- `arrow_reader/post_filter.rs` owns post-decode filtering.
- `push_decoder/reader_builder/fallback.rs` owns fallback eligibility and observation.
- `push_decoder/reader_builder/selection_policy.rs` owns row-selection policy resolution.

The remaining readability issue is in `push_decoder/reader_builder/mod.rs`, especially the `WaitingOnData` branch. That branch currently mixes state-machine progression with fallback observation, metrics recording, post-selection handoff, and post-filter state initialization.

## Goal

Reduce the cognitive load in `push_decoder/reader_builder/mod.rs` without changing runtime behavior.

This refactor should make the row-group state machine read in a high-level order:

1. Resolve any fallback transition.
2. Return early if the current row group should switch to post-selection filtering.
3. Wait for missing data if necessary.
4. Build the normal pushdown reader.

## Non-Goals

- Do not change fallback heuristics or thresholds.
- Do not change metrics semantics or counter names.
- Do not change benchmark behavior.
- Do not move the full fallback implementation into a new module in this step.
- Do not broaden fallback eligibility.

## Proposed Design

Add a small private transition enum near `RowGroupReaderBuilder`:

```rust
enum FallbackTransition {
    ContinuePushdown,
    StartPostSelection {
        selection: RowSelection,
    },
    EnablePostFilter,
}
```

Add two private helper methods on `RowGroupReaderBuilder`:

```rust
fn resolve_fallback_transition(
    &mut self,
    row_group_info: &RowGroupInfo,
    cache_info: Option<&CacheInfo>,
) -> Result<FallbackTransition, ParquetError>
```

```rust
fn ensure_post_filter_state(&mut self) -> Result<(), ParquetError>
```

`resolve_fallback_transition` owns the current fallback block from the `WaitingOnData` branch:

- Check whether fallback observation is active and supported.
- Resolve the `RowSelectionStrategyDecision`.
- Capture the observed pushdown selection.
- Call `observe_fallback_candidate`.
- If fallback switches on and there is no base selection, return `StartPostSelection`.
- If fallback switches on and there is a base selection, initialize post-filter state and return `EnablePostFilter`.
- Otherwise record the pushdown fallback metric and return `ContinuePushdown`.

The helper should not consume `data_request` or `cache_info`. If it returns `StartPostSelection`, the caller remains responsible for moving the existing values into:

```rust
self.start_post_selection_filter(
    row_group_info,
    selection,
    cache_info,
    data_request.into_dense_column_chunks(),
)
```

`ensure_post_filter_state` owns only the state transition from `self.filter` into `self.post_filter`. This keeps the main state-machine branch from handling predicate-state ownership directly.

## Naming and Comments

While extracting the helper, improve local names without broad churn:

- Rename `fallback_selection` to `observed_selection` or `pushdown_selection`.
- Clarify that the `post_filter` field stores predicate state reused by later row groups after fallback selects post-filter execution.
- Add a short comment before `StartPostSelection` explaining that the current row group already computed a selection, so it applies that selection after decode instead of re-running predicates.

## Expected Result

The `WaitingOnData` branch should become shorter and easier to scan. Fallback details should be readable in one helper, while the main branch remains focused on the row-group state machine.

The refactor should preserve the existing module structure and keep the diff reviewable.

## Verification

Run these commands after implementation:

```bash
cargo test -p parquet --lib arrow::push_decoder::reader_builder::tests
cargo test -p parquet --test arrow_reader --features arrow,async row_filter::async
cargo fmt --all --check
```

If the second command is too broad or the local test target names differ, run the closest existing async row-filter integration test command and report the exact command used.
