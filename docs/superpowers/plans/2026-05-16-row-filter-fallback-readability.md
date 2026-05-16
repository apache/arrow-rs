# Row Filter Fallback Readability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce the cognitive load in the async Parquet row-filter fallback state machine without changing behavior.

**Architecture:** Keep the existing module layout. Add a small private transition enum and two private helper methods inside `push_decoder/reader_builder/mod.rs`, then replace the inline fallback block in the `WaitingOnData` state with a short high-level match.

**Tech Stack:** Rust, Apache Arrow Rust Parquet reader internals, existing `cargo test` and `cargo fmt`.

---

## File Structure

- Modify: `parquet/src/arrow/push_decoder/reader_builder/mod.rs`
  - Add `FallbackTransition`.
  - Clarify the `post_filter` field comment.
  - Add `ensure_post_filter_state`.
  - Add `resolve_fallback_transition`.
  - Replace the inline fallback block in `RowGroupDecoderState::WaitingOnData`.
- No new production modules.
- No new tests required because this is behavior-preserving refactoring; existing unit and async row-filter tests cover the current behavior.

## Task 1: Add Transition Type and Post-Filter State Helper

**Files:**
- Modify: `parquet/src/arrow/push_decoder/reader_builder/mod.rs:54-62`
- Modify: `parquet/src/arrow/push_decoder/reader_builder/mod.rs:272-276`
- Modify: `parquet/src/arrow/push_decoder/reader_builder/mod.rs:352-356`

- [ ] **Step 1: Add `FallbackTransition` after `RowGroupInfo`**

Insert this enum immediately after `RowGroupInfo`:

```rust
enum FallbackTransition {
    ContinuePushdown,
    StartPostSelection { selection: RowSelection },
    EnablePostFilter,
}
```

- [ ] **Step 2: Clarify the `post_filter` field comment**

Replace the existing field comment:

```rust
/// Shared filter state used once Auto fallback switches to post-filter.
post_filter: Option<Arc<Mutex<RowFilter>>>,
```

with:

```rust
/// Predicate state reused by later row groups once Auto fallback switches to post-filter.
post_filter: Option<Arc<Mutex<RowFilter>>>,
```

- [ ] **Step 3: Add `ensure_post_filter_state`**

Add this private method in `impl RowGroupReaderBuilder`, near `disable_post_filter_fallback`:

```rust
fn ensure_post_filter_state(&mut self) -> Result<(), ParquetError> {
    if self.post_filter.is_some() {
        return Ok(());
    }

    let filter = self.filter.take().ok_or_else(|| {
        ParquetError::General("post-filter fallback selected without a row filter".to_string())
    })?;
    self.post_filter = Some(Arc::new(Mutex::new(filter)));
    Ok(())
}
```

- [ ] **Step 4: Run focused formatting check**

Run:

```bash
cargo fmt --all --check
```

Expected: this may fail until all refactor steps are complete. If it fails only because the new code needs formatting, continue and run `cargo fmt --all` after Task 3.

## Task 2: Extract Fallback Transition Resolution

**Files:**
- Modify: `parquet/src/arrow/push_decoder/reader_builder/mod.rs:358-378`
- Reads existing logic at: `parquet/src/arrow/push_decoder/reader_builder/mod.rs:892-943`

- [ ] **Step 1: Add `resolve_fallback_transition`**

Add this private method in `impl RowGroupReaderBuilder`, after `ensure_post_filter_state`:

```rust
fn resolve_fallback_transition(
    &mut self,
    row_group_info: &RowGroupInfo,
    cache_info: Option<&CacheInfo>,
) -> Result<FallbackTransition, ParquetError> {
    if cache_info.is_none()
        || !matches!(self.fallback_state, RowGroupFallbackState::Observing { .. })
        || !self.post_filter_fallback_supported(row_group_info.budget)
    {
        return Ok(FallbackTransition::ContinuePushdown);
    }

    let decision = row_group_info
        .plan_builder
        .resolve_selection_strategy_decision();
    let observed_selection = row_group_info.plan_builder.selection().cloned();

    self.observe_fallback_candidate(decision, row_group_info.row_count, row_group_info.budget);

    if matches!(
        self.fallback_state,
        RowGroupFallbackState::UsePostFilter { .. }
    ) {
        if row_group_info.base_selection.is_none() {
            let selection = observed_selection.unwrap_or_else(|| {
                RowSelection::from(vec![RowSelector::select(row_group_info.row_count)])
            });
            return Ok(FallbackTransition::StartPostSelection { selection });
        }

        self.ensure_post_filter_state()?;
        self.metrics
            .record_fallback_row_group(RowGroupExecutionMode::Pushdown(decision.strategy));
        return Ok(FallbackTransition::EnablePostFilter);
    }

    self.metrics
        .record_fallback_row_group(RowGroupExecutionMode::Pushdown(decision.strategy));
    Ok(FallbackTransition::ContinuePushdown)
}
```

- [ ] **Step 2: Verify behavior preservation in the helper**

Check these details manually before moving on:

- `StartPostSelection` returns before recording `fallback_pushdown_row_group_count`, matching the old early return.
- `EnablePostFilter` records `Pushdown(decision.strategy)`, matching the old fall-through behavior.
- `ContinuePushdown` records the same metric only after an observation was made.
- The helper does not consume `data_request` or `cache_info`.

## Task 3: Replace the Inline `WaitingOnData` Fallback Block

**Files:**
- Modify: `parquet/src/arrow/push_decoder/reader_builder/mod.rs:887-945`

- [ ] **Step 1: Replace the inline fallback block**

Replace the entire initial fallback-observation block at the top of the `WaitingOnData` arm. It starts with:

```rust
if cache_info.is_some()
    && matches!(self.fallback_state, RowGroupFallbackState::Observing { .. })
    && self.post_filter_fallback_supported(row_group_info.budget)
{
}
```

and ends immediately before:

```rust
let needed_ranges = data_request.needed_ranges(&self.buffers);
```

with:

```rust
match self.resolve_fallback_transition(&row_group_info, cache_info.as_ref())? {
    FallbackTransition::ContinuePushdown | FallbackTransition::EnablePostFilter => {}
    FallbackTransition::StartPostSelection { selection } => {
        let column_chunks = data_request.into_dense_column_chunks();
        // The current row group already computed a pushdown selection. Apply that
        // selection after decode instead of evaluating the predicates again.
        //
        // Sparse predicate chunks may not cover the base selection. Dense chunks
        // are safe to reuse and preserve predicate-cache IO behavior.
        return self.start_post_selection_filter(
            row_group_info,
            selection,
            cache_info,
            column_chunks,
        );
    }
}
```

- [ ] **Step 2: Run formatter**

Run:

```bash
cargo fmt --all
```

Expected: command exits 0.

- [ ] **Step 3: Inspect the resulting `WaitingOnData` branch**

Run:

```bash
nl -ba parquet/src/arrow/push_decoder/reader_builder/mod.rs | sed -n '887,945p'
```

Expected: the branch reads in this order:

1. `resolve_fallback_transition` match.
2. `needed_ranges` check.
3. row group destructuring.
4. normal row group reader build.

## Task 4: Verification

**Files:**
- Verify only; no source edits expected.

- [ ] **Step 1: Run push decoder reader builder tests**

Run:

```bash
cargo test -p parquet --lib arrow::push_decoder::reader_builder::tests
```

Expected: all tests pass.

- [ ] **Step 2: Run async row-filter integration tests**

Run:

```bash
cargo test -p parquet --test arrow_reader --features arrow,async row_filter::async
```

Expected: all matching async row-filter tests pass. If this command does not match tests in this workspace, run:

```bash
cargo test -p parquet --test arrow_reader --features arrow,async row_filter
```

and report the exact command and result.

- [ ] **Step 3: Run formatting check**

Run:

```bash
cargo fmt --all --check
```

Expected: command exits 0.

- [ ] **Step 4: Review diff**

Run:

```bash
git diff -- parquet/src/arrow/push_decoder/reader_builder/mod.rs
```

Expected: diff only introduces the transition enum, helper methods, comment improvements, and the shorter `WaitingOnData` fallback match.

## Task 5: Commit

**Files:**
- Modify: `parquet/src/arrow/push_decoder/reader_builder/mod.rs`
- Include this plan file only if the team wants planning artifacts committed.

- [ ] **Step 1: Check status**

Run:

```bash
git status --short
```

Expected: only intentional files are changed.

- [ ] **Step 2: Stage implementation**

Run:

```bash
git add parquet/src/arrow/push_decoder/reader_builder/mod.rs
```

If committing the plan file, also run:

```bash
git add docs/superpowers/plans/2026-05-16-row-filter-fallback-readability.md
```

- [ ] **Step 3: Commit**

Run:

```bash
git commit -m "refactor(parquet): clarify row filter fallback transition"
```

Expected: commit succeeds.
