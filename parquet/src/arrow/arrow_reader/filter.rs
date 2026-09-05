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

use crate::arrow::ProjectionMask;
use crate::arrow::arrow_reader::selection::RowSelectionStrategy;
use crate::arrow::arrow_reader::{RowSelection, RowSelector};
use crate::schema::types::SchemaDescriptor;
use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_buffer::BooleanBuffer;
use arrow_schema::ArrowError;
use arrow_select::filter::{SlicesIterator, filter_record_batch, prep_null_mask_filter};
use std::fmt::{Debug, Formatter};

/// A predicate operating on [`RecordBatch`]
///
/// See also:
/// * [`RowFilter`] for more information  on applying filters during the
///   Parquet decoding process.
/// * [`ArrowPredicateFn`] for a concrete implementation based on a function
pub trait ArrowPredicate: Send + 'static {
    /// Returns the [`ProjectionMask`] that describes the columns required
    /// to evaluate this predicate.
    ///
    /// All projected columns will be provided in the `batch` passed to
    /// [`evaluate`](Self::evaluate). The projection mask should be as small as
    /// possible because any columns needed for the overall projection mask are
    /// decoded again after a predicate is applied.
    fn projection(&self) -> &ProjectionMask;

    /// Evaluate this predicate for the given [`RecordBatch`] containing the columns
    /// identified by [`Self::projection`]
    ///
    /// Must return a [`BooleanArray`] that has the same length as the input
    /// `batch` where each row indicates whether the row should be returned:
    /// * `true`:the row should be returned
    /// * `false` or `null`: the row should not be returned
    fn evaluate(&mut self, batch: RecordBatch) -> Result<BooleanArray, ArrowError>;
}

/// An [`ArrowPredicate`] created from an [`FnMut`] and a [`ProjectionMask`]
///
/// See [`RowFilter`] for more information on applying filters during the
/// Parquet decoding process.
///
/// The function is passed `RecordBatch`es with only the columns specified in
/// the [`ProjectionMask`].
///
/// The function must return a [`BooleanArray`] that has the same length as the
/// input `batch` where each row indicates whether the row should be returned:
/// * `true`: the row should be returned
/// * `false` or `null`: the row should not be returned
///
/// # Example:
///
/// Given an input schema: `"a:int64", "b:int64"`, you can create a predicate that
/// evaluates `b > 0` like this:
///
/// ```
/// # use std::sync::Arc;
/// # use arrow::compute::kernels::cmp::gt;
/// # use arrow_array::{BooleanArray, Int64Array, RecordBatch};
/// # use arrow_array::cast::AsArray;
/// # use arrow_array::types::Int64Type;
/// # use parquet::arrow::arrow_reader::ArrowPredicateFn;
/// # use parquet::arrow::ProjectionMask;
/// # use parquet::schema::types::{SchemaDescriptor, Type};
/// # use parquet::basic; // note there are two `Type`s that are different
/// # // Schema for a table with one columns: "a" (int64) and "b" (int64)
/// # let descriptor = SchemaDescriptor::new(
/// #  Arc::new(
/// #    Type::group_type_builder("my_schema")
/// #      .with_fields(vec![
/// #        Arc::new(
/// #         Type::primitive_type_builder("a", basic::Type::INT64)
/// #          .build().unwrap()
/// #        ),
/// #        Arc::new(
/// #         Type::primitive_type_builder("b", basic::Type::INT64)
/// #          .build().unwrap()
/// #        ),
/// #     ])
/// #     .build().unwrap()
/// #  )
/// # );
/// // Create a mask for selecting only the second column "b" (index 1)
/// let projection_mask = ProjectionMask::leaves(&descriptor, [1]);
/// // Closure that evaluates "b > 0"
/// let predicate = |batch: RecordBatch| {
///    let scalar_0 = Int64Array::new_scalar(0);
///    let column = batch.column(0).as_primitive::<Int64Type>();
///    // call the gt kernel to compute `>` which returns a BooleanArray
///    gt(column, &scalar_0)
///  };
/// // Create ArrowPredicateFn that can be passed to RowFilter
/// let arrow_predicate = ArrowPredicateFn::new(projection_mask, predicate);
/// ```
pub struct ArrowPredicateFn<F> {
    f: F,
    projection: ProjectionMask,
}

impl<F> ArrowPredicateFn<F>
where
    F: FnMut(RecordBatch) -> Result<BooleanArray, ArrowError> + Send + 'static,
{
    /// Create a new [`ArrowPredicateFn`] that invokes `f` on the columns
    /// specified in `projection`.
    pub fn new(projection: ProjectionMask, f: F) -> Self {
        Self { f, projection }
    }
}

impl<F> ArrowPredicate for ArrowPredicateFn<F>
where
    F: FnMut(RecordBatch) -> Result<BooleanArray, ArrowError> + Send + 'static,
{
    fn projection(&self) -> &ProjectionMask {
        &self.projection
    }

    fn evaluate(&mut self, batch: RecordBatch) -> Result<BooleanArray, ArrowError> {
        (self.f)(batch)
    }
}

/// Filter applied *during* the parquet read process
///
/// See example on [`ArrowReaderBuilder::with_row_filter`]
///
/// [`RowFilter`] applies predicates in order, after decoding only the columns
/// required. As predicates eliminate rows, fewer rows from subsequent columns
/// may be required, thus potentially reducing IO and decode. This process is
/// also known as *push down* filtering and  *late materialization*.
///
/// A `RowFilter` consists of a list of [`ArrowPredicate`]s. Only the rows for which
/// all the predicates evaluate to `true` will be returned.
/// Any [`RowSelection`] provided to the reader will be applied prior
/// to the first predicate, and each predicate in turn will then be used to compute
/// a more refined [`RowSelection`] used when evaluating the subsequent predicates.
///
/// Once all predicates have been evaluated, the final [`RowSelection`] is applied
/// to the top-level [`ProjectionMask`] to produce the final output [`RecordBatch`].
///
/// This design has a couple of implications:
///
/// * [`RowFilter`] can be used to skip entire pages, and thus IO, in addition to CPU decode overheads
/// * Columns may be decoded multiple times if they appear in multiple [`ProjectionMask`]
/// * IO will be deferred until needed by a [`ProjectionMask`]
///
/// As such there is a trade-off between a single large predicate, or multiple predicates,
/// that will depend on the shape of the data. Whilst multiple smaller predicates may
/// minimise the amount of data scanned/decoded, it may not be faster overall.
///
/// For example, if a predicate that needs a single column of data filters out all but
/// 1% of the rows, applying it as one of the early `ArrowPredicateFn` will likely significantly
/// improve performance.
///
/// As a counter example, if a predicate needs several columns of data to evaluate but
/// leaves 99% of the rows, it may be better to not filter the data from parquet and
/// apply the filter after the RecordBatch has been fully decoded.
///
/// Additionally, even if a predicate eliminates a moderate number of rows, it may still be faster
/// to filter the data after the RecordBatch has been fully decoded, if the eliminated rows are
/// not contiguous.
///
/// [`RowSelection`]: crate::arrow::arrow_reader::RowSelection
/// [`ArrowReaderBuilder::with_row_filter`]: crate::arrow::arrow_reader::ArrowReaderBuilder::with_row_filter
pub struct RowFilter {
    /// A list of [`ArrowPredicate`]
    pub(crate) predicates: Vec<Box<dyn ArrowPredicate>>,
}

impl Debug for RowFilter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RowFilter {{ {} predicates: }}", self.predicates.len())
    }
}

impl RowFilter {
    /// Create a new [`RowFilter`] from an array of [`ArrowPredicate`]
    pub fn new(predicates: Vec<Box<dyn ArrowPredicate>>) -> Self {
        Self { predicates }
    }
    /// Returns the inner predicates
    pub fn predicates(&self) -> &Vec<Box<dyn ArrowPredicate>> {
        &self.predicates
    }
    /// Returns the inner predicates, consuming self
    pub fn into_predicates(self) -> Vec<Box<dyn ArrowPredicate>> {
        self.predicates
    }
}

impl RowFilter {
    /// Fuse consecutive predicates on the same single top-level, non-repeated leaf.
    /// This avoids repeated decoding or predicate-cache replay of that column.
    pub(crate) fn fuse_same_projection(self, parquet_schema: &SchemaDescriptor) -> Self {
        let mut fused: Vec<Box<dyn ArrowPredicate>> = Vec::with_capacity(self.predicates.len());
        let mut run: Vec<Box<dyn ArrowPredicate>> = Vec::new();
        let mut flush = |run: &mut Vec<Box<dyn ArrowPredicate>>| {
            if run.len() > 1 && is_fusable_projection(run[0].projection(), parquet_schema) {
                let run = std::mem::take(run);
                fused.push(Box::new(FusedPredicate::new(run)));
            } else {
                fused.append(run);
            }
        };

        for predicate in self.predicates {
            if run
                .last()
                .is_some_and(|last| last.projection() != predicate.projection())
            {
                flush(&mut run);
            }
            run.push(predicate);
        }
        flush(&mut run);

        Self { predicates: fused }
    }
}

/// Restrict fusion to one top-level, non-repeated leaf to limit compaction costs.
fn is_fusable_projection(projection: &ProjectionMask, parquet_schema: &SchemaDescriptor) -> bool {
    let mut selected =
        (0..parquet_schema.num_columns()).filter(|idx| projection.leaf_included(*idx));
    let Some(leaf_idx) = selected.next() else {
        return false;
    };
    if selected.next().is_some() {
        return false;
    }

    let column = parquet_schema.column(leaf_idx);
    column.path().parts().len() == 1 && column.max_rep_level() == 0
}

/// Evaluate same-projection predicates in order on one decoded batch.
/// Later predicates see only surviving rows, sliced when contiguous or compacted
/// otherwise. Selections use runs or bitmaps according to fragmentation.
pub(crate) struct FusedPredicate {
    /// At least two predicates, all with the same projection.
    predicates: Vec<Box<dyn ArrowPredicate>>,
}

/// Heuristic average run length below which bitmaps are preferred.
const FUSION_SELECTION_THRESHOLD: usize = 32;

/// Use bitmaps for short runs and selectors for long runs.
fn adapt_fusion_selection(selection: RowSelection) -> RowSelection {
    match (
        selection.auto_selection_strategy(FUSION_SELECTION_THRESHOLD),
        selection.as_mask().is_some(),
    ) {
        (RowSelectionStrategy::Mask, true) | (RowSelectionStrategy::Selectors, false) => selection,
        (RowSelectionStrategy::Mask, false) => {
            RowSelection::from_boolean_buffer(selection.into_boolean_buffer())
        }
        (RowSelectionStrategy::Selectors, true) => {
            RowSelection::from(Vec::<RowSelector>::from(selection))
        }
    }
}

impl FusedPredicate {
    /// Create a fused predicate from at least two predicates that share one
    /// projection.
    pub(crate) fn new(predicates: Vec<Box<dyn ArrowPredicate>>) -> Self {
        debug_assert!(predicates.len() > 1);
        debug_assert!(
            predicates
                .windows(2)
                .all(|pair| pair[0].projection() == pair[1].projection())
        );
        Self { predicates }
    }
}

impl Debug for FusedPredicate {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FusedPredicate {{ {} predicates }}",
            self.predicates.len()
        )
    }
}

impl ArrowPredicate for FusedPredicate {
    fn projection(&self) -> &ProjectionMask {
        self.predicates[0].projection()
    }

    fn evaluate(&mut self, batch: RecordBatch) -> Result<BooleanArray, ArrowError> {
        let num_rows = batch.num_rows();
        // Positions in the original batch; None until a predicate rejects rows.
        let mut accepted: Option<RowSelection> = None;
        let mut batch = batch;
        let last = self.predicates.len() - 1;

        for (idx, predicate) in self.predicates.iter_mut().enumerate() {
            let filter = evaluate_predicate(predicate.as_mut(), batch.clone())?;
            // No mapping is needed if all preceding predicates accepted every row.
            if idx == last && accepted.is_none() {
                return Ok(filter);
            }
            let true_count = filter.true_count();
            if true_count == 0 {
                return Ok(BooleanArray::new(BooleanBuffer::new_unset(num_rows), None));
            }
            if true_count == filter.len() {
                continue;
            }

            let next =
                adapt_fusion_selection(RowSelection::from_boolean_buffer(filter.values().clone()));
            let combined = match accepted {
                Some(accepted) => accepted.and_then(&next),
                None => next,
            };
            accepted = Some(adapt_fusion_selection(combined));
            if idx != last {
                batch = narrow_batch(&batch, &filter, true_count)?;
            }
        }

        let mask = match accepted {
            Some(accepted) => accepted.into_boolean_buffer(),
            None => BooleanBuffer::new_set(num_rows),
        };
        debug_assert_eq!(mask.len(), num_rows);
        Ok(BooleanArray::new(mask, None))
    }
}

/// Validate the predicate result length and treat nulls as false.
fn evaluate_predicate(
    predicate: &mut dyn ArrowPredicate,
    batch: RecordBatch,
) -> Result<BooleanArray, ArrowError> {
    let input_rows = batch.num_rows();
    let filter = predicate.evaluate(batch)?;
    if filter.len() != input_rows {
        return Err(ArrowError::InvalidArgumentError(format!(
            "ArrowPredicate predicate returned {} rows, expected {input_rows}",
            filter.len()
        )));
    }
    Ok(match filter.null_count() {
        0 => filter,
        _ => prep_null_mask_filter(&filter),
    })
}

/// Restrict `batch` to the rows `filter` accepts, slicing zero-copy when they
/// form one contiguous range and compacting otherwise.
///
/// `filter` must be null-free and accept some but not all rows.
fn narrow_batch(
    batch: &RecordBatch,
    filter: &BooleanArray,
    true_count: usize,
) -> Result<RecordBatch, ArrowError> {
    let mut slices = SlicesIterator::new(filter);
    let (start, end) = slices
        .next()
        .expect("a partially selected filter has a true slice");
    if end - start == true_count {
        Ok(batch.slice(start, end - start))
    } else {
        filter_record_batch(batch, filter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::array_reader::ArrayReader;
    use crate::arrow::array_reader::StructArrayReader;
    use crate::arrow::array_reader::test_util::make_int32_page_reader;
    use crate::arrow::arrow_reader::{ReadPlanBuilder, RowSelectionPolicy};
    use crate::schema::parser::parse_message_type;
    use arrow_array::Int32Array;
    use arrow_array::cast::AsArray;
    use arrow_array::types::Int32Type;
    use arrow_schema::{DataType, Field, Fields, Schema};
    use std::sync::Arc;

    fn test_schema() -> SchemaDescriptor {
        let schema = parse_message_type(
            "message schema {
                REQUIRED INT32 a;
                REQUIRED INT32 b;
                REQUIRED INT32 c;
                REQUIRED GROUP nested { REQUIRED INT32 d; }
                REPEATED INT32 e;
            }",
        )
        .unwrap();
        SchemaDescriptor::new(Arc::new(schema))
    }

    #[test]
    fn fusable_projection_requires_one_top_level_leaf() {
        let schema = test_schema();

        let one_leaf = ProjectionMask::leaves(&schema, [1]);
        let two_leaves = ProjectionMask::leaves(&schema, [0, 2]);
        let nested_leaf = ProjectionMask::leaves(&schema, [3]);
        let repeated_leaf = ProjectionMask::leaves(&schema, [4]);
        assert!(is_fusable_projection(&one_leaf, &schema));
        assert!(!is_fusable_projection(&two_leaves, &schema));
        assert!(!is_fusable_projection(&nested_leaf, &schema));
        assert!(!is_fusable_projection(&repeated_leaf, &schema));
        assert!(!is_fusable_projection(
            &ProjectionMask::none(schema.num_columns()),
            &schema
        ));
    }

    fn accept_all(projection: ProjectionMask) -> Box<dyn ArrowPredicate> {
        Box::new(ArrowPredicateFn::new(projection, |batch| {
            Ok(BooleanArray::from(vec![true; batch.num_rows()]))
        }))
    }

    #[test]
    fn fuse_same_projection_groups_consecutive_fusable_runs() {
        let schema = test_schema();
        let a = ProjectionMask::leaves(&schema, [0]);
        let b = ProjectionMask::leaves(&schema, [1]);
        let ac = ProjectionMask::leaves(&schema, [0, 2]);
        let nested = ProjectionMask::leaves(&schema, [3]);

        let filter = RowFilter::new(vec![
            // fused
            accept_all(a.clone()),
            accept_all(a.clone()),
            // single predicate: kept as-is
            accept_all(b.clone()),
            // two leaves: not fusable
            accept_all(ac.clone()),
            accept_all(ac.clone()),
            // nested leaf: not fusable
            accept_all(nested.clone()),
            accept_all(nested.clone()),
            // fused
            accept_all(a.clone()),
            accept_all(a.clone()),
            accept_all(a.clone()),
        ])
        .fuse_same_projection(&schema);

        let projections: Vec<_> = filter
            .predicates()
            .iter()
            .map(|predicate| predicate.projection().clone())
            .collect();
        assert_eq!(
            projections,
            vec![a.clone(), b, ac.clone(), ac, nested.clone(), nested, a]
        );
        assert_eq!(format!("{filter:?}"), "RowFilter { 7 predicates: }");
    }

    #[test]
    fn fuse_same_projection_keeps_single_predicates_and_empty_filters() {
        let schema = test_schema();
        let a = ProjectionMask::leaves(&schema, [0]);
        let b = ProjectionMask::leaves(&schema, [1]);

        let filter =
            RowFilter::new(vec![accept_all(a), accept_all(b)]).fuse_same_projection(&schema);
        assert_eq!(filter.predicates().len(), 2);

        let filter = RowFilter::new(vec![]).fuse_same_projection(&schema);
        assert!(filter.predicates().is_empty());
    }

    fn int_batch(values: impl IntoIterator<Item = i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        let values = Int32Array::from_iter_values(values);
        RecordBatch::try_new(schema, vec![Arc::new(values)]).unwrap()
    }

    /// A predicate over the `v` column that asserts `precondition` holds for
    /// every row it sees before returning `f(v)`.
    fn int_predicate(
        precondition: impl Fn(i32) -> bool + Send + 'static,
        f: impl Fn(i32) -> Option<bool> + Send + 'static,
    ) -> Box<dyn ArrowPredicate> {
        Box::new(ArrowPredicateFn::new(ProjectionMask::all(), move |batch| {
            let values = batch.column(0).as_primitive::<Int32Type>();
            assert!(
                values.values().iter().all(|v| precondition(*v)),
                "predicate saw rows rejected by a predecessor"
            );
            Ok(values.values().iter().map(|v| f(*v)).collect())
        }))
    }

    /// Fragmented survivors: roughly every other row.
    fn even() -> Box<dyn ArrowPredicate> {
        int_predicate(|_| true, |v| Some(v % 2 == 0))
    }

    /// Clustered survivors: one contiguous range.
    fn in_range() -> Box<dyn ArrowPredicate> {
        int_predicate(|_| true, |v| Some((10..40).contains(&v)))
    }

    /// Applies after `even`: mixes nulls (rejected) into the result.
    fn divisible_by_three_after_even() -> Box<dyn ArrowPredicate> {
        int_predicate(
            |v| v % 2 == 0,
            |v| if v % 7 == 0 { None } else { Some(v % 3 == 0) },
        )
    }

    fn all_true() -> Box<dyn ArrowPredicate> {
        int_predicate(|_| true, |_| Some(true))
    }

    fn all_false() -> Box<dyn ArrowPredicate> {
        int_predicate(|_| true, |_| Some(false))
    }

    fn never_called() -> Box<dyn ArrowPredicate> {
        Box::new(ArrowPredicateFn::new(ProjectionMask::all(), |_| {
            panic!("predicate evaluated after every row was rejected")
        }))
    }

    /// Evaluate `predicates` one after another, each on the rows kept by its
    /// predecessors, the way `RowFilter` applies separate predicates.
    fn sequential(batch: &RecordBatch, predicates: &mut [Box<dyn ArrowPredicate>]) -> Vec<bool> {
        let mut kept = vec![true; batch.num_rows()];
        for predicate in predicates {
            // Like `RowFilter`, stop once every row has been rejected.
            if !kept.contains(&true) {
                break;
            }
            let survivors = filter_record_batch(batch, &BooleanArray::from(kept.clone())).unwrap();
            let filter = predicate.evaluate(survivors).unwrap();
            for (filter_idx, keep) in kept.iter_mut().filter(|keep| **keep).enumerate() {
                *keep = filter.is_valid(filter_idx) && filter.value(filter_idx);
            }
        }
        kept
    }

    const POLICIES: [RowSelectionPolicy; 4] = [
        RowSelectionPolicy::Selectors,
        RowSelectionPolicy::Mask,
        RowSelectionPolicy::Auto { threshold: 32 },
        RowSelectionPolicy::Auto { threshold: 2 },
    ];

    type PredicateChain = fn() -> Vec<Box<dyn ArrowPredicate>>;

    #[test]
    fn fused_predicate_matches_sequential_evaluation() {
        let cases: Vec<PredicateChain> = vec![
            || vec![even(), divisible_by_three_after_even()],
            || vec![even(), in_range(), divisible_by_three_after_even()],
            || vec![in_range(), even()],
            || vec![even(), all_true(), divisible_by_three_after_even()],
            || vec![all_true(), even()],
            || vec![all_true(), all_true()],
            || vec![even(), all_false(), never_called()],
            || vec![all_false(), never_called()],
        ];

        for num_rows in [0, 1, 7, 97, 200] {
            let batch = int_batch(0..num_rows);
            for make_predicates in &cases {
                let expected = sequential(&batch, &mut make_predicates());
                let mut fused = FusedPredicate::new(make_predicates());
                let actual = fused.evaluate(batch.clone()).unwrap();
                assert_eq!(actual.null_count(), 0);
                assert_eq!(
                    actual.values().iter().collect::<Vec<_>>(),
                    expected,
                    "{num_rows} rows"
                );
            }
        }
    }

    #[test]
    fn fused_predicate_rejects_wrong_result_length() {
        let short = Box::new(ArrowPredicateFn::new(ProjectionMask::all(), |_| {
            Ok(BooleanArray::from(vec![true]))
        }));
        let mut fused = FusedPredicate::new(vec![all_true(), short]);
        let err = fused.evaluate(int_batch(0..4)).unwrap_err();
        assert!(
            err.to_string()
                .contains("ArrowPredicate predicate returned 1 rows, expected 4"),
            "{err}"
        );
    }

    #[test]
    fn narrow_batch_slices_contiguous_survivors() {
        let batch = int_batch(0..6);
        let filter = BooleanArray::from(vec![false, false, true, true, true, false]);
        let narrowed = narrow_batch(&batch, &filter, 3).unwrap();
        assert_eq!(
            narrowed.column(0).as_primitive::<Int32Type>().values(),
            &[2, 3, 4]
        );
        // Slicing shares the input buffer instead of copying it.
        assert_eq!(narrowed.column(0).to_data().buffers()[0].as_ptr(), unsafe {
            batch.column(0).to_data().buffers()[0].as_ptr().add(2 * 4)
        });

        let filter = BooleanArray::from(vec![false, true, false, true, false, false]);
        let narrowed = narrow_batch(&batch, &filter, 2).unwrap();
        assert_eq!(
            narrowed.column(0).as_primitive::<Int32Type>().values(),
            &[1, 3]
        );
    }

    #[test]
    fn adaptive_fusion_selection_uses_selectors_for_long_runs() {
        let mask = BooleanBuffer::from_iter((0..1_024).map(|idx| (256..768).contains(&idx)));
        let selection = adapt_fusion_selection(RowSelection::from_boolean_buffer(mask));
        assert!(selection.as_mask().is_none());
    }

    #[test]
    fn adaptive_fusion_selection_keeps_fragmented_masks() {
        let mask = BooleanBuffer::from_iter((0..1_024).map(|idx| idx % 2 == 0));
        let selection = adapt_fusion_selection(RowSelection::from_boolean_buffer(mask));
        assert!(selection.as_mask().is_some());
    }

    /// Fusing predicates into one `ReadPlanBuilder::with_predicate` call
    /// produces the same selection as applying them one at a time.
    #[test]
    fn fused_predicate_matches_predicate_major_read_plan() {
        let data: Vec<i32> = (0..97).collect();
        let make_reader = || {
            let levels = vec![0; data.len()];
            let leaf = make_int32_page_reader(&data, &levels, &levels, 0, 0, None);
            let struct_type =
                DataType::Struct(Fields::from(vec![Field::new("c0", DataType::Int32, false)]));
            Box::new(StructArrayReader::new(
                struct_type,
                vec![leaf],
                0,
                0,
                false,
                None,
            )) as Box<dyn ArrayReader>
        };
        let make_predicates = || vec![even(), divisible_by_three_after_even()];

        let prior = RowSelection::from_filters(&[BooleanArray::from(
            (0..data.len()).map(|idx| idx % 5 != 0).collect::<Vec<_>>(),
        )]);
        for initial in [None, Some(prior)] {
            let mut sequential = ReadPlanBuilder::new(7).with_selection(initial.clone());
            for predicate in &mut make_predicates() {
                sequential = sequential
                    .with_predicate(make_reader(), predicate.as_mut())
                    .unwrap();
            }

            for policy in POLICIES {
                let mut fused = FusedPredicate::new(make_predicates());
                let plan = ReadPlanBuilder::new(7)
                    .with_selection(initial.clone())
                    .with_row_selection_policy(policy)
                    .with_predicate(make_reader(), &mut fused)
                    .unwrap();
                assert_eq!(plan.selection(), sequential.selection(), "{policy:?}");
            }
        }
    }
}
