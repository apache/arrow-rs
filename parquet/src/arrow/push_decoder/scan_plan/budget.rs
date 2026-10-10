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

//! Offset/limit budget shared across the row groups of a scan.

use crate::arrow::arrow_reader::ReadPlanBuilder;

/// Running offset/limit budget shared across row groups.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) struct RowBudget {
    offset: Option<usize>,
    limit: Option<usize>,
}

impl RowBudget {
    pub(crate) fn new(offset: Option<usize>, limit: Option<usize>) -> Self {
        Self { offset, limit }
    }

    pub(crate) fn is_exhausted(self) -> bool {
        matches!(self.limit, Some(0))
    }

    /// The offset still to be skipped before the next readable row group.
    pub(crate) fn offset(self) -> Option<usize> {
        self.offset
    }

    /// The number of output rows still permitted across the remaining row groups.
    pub(crate) fn limit(self) -> Option<usize> {
        self.limit
    }

    /// Returns how many selected rows remain after applying this budget.
    pub(crate) fn rows_after(self, rows_before_budget: usize) -> usize {
        let rows_after_offset = rows_before_budget.saturating_sub(self.offset.unwrap_or(0));
        match self.limit {
            Some(limit) => rows_after_offset.min(limit),
            None => rows_after_offset,
        }
    }

    /// Returns the number of selected rows needed before applying the offset.
    pub(crate) fn selected_row_limit(self) -> Option<usize> {
        self.limit
            .map(|limit| limit.saturating_add(self.offset.unwrap_or(0)))
    }

    pub(crate) fn apply_to_plan(
        self,
        plan_builder: ReadPlanBuilder,
        row_count: usize,
    ) -> BudgetedReadPlan {
        let rows_before_budget = plan_builder.num_rows_selected().unwrap_or(row_count);
        let plan_builder = plan_builder
            .limited(row_count)
            .with_offset(self.offset)
            .with_limit(self.limit)
            .build_limited();
        let rows_after_budget = self.rows_after(rows_before_budget);

        BudgetedReadPlan {
            plan_builder,
            rows_before_budget,
            rows_after_budget,
            remaining_budget: self.advance(rows_before_budget, rows_after_budget),
        }
    }

    /// Advance the budget past one row group.
    ///
    /// `rows_before_budget` is the number of rows selected before applying the
    /// budget, and `rows_after_budget` is the number retained for output from
    /// this row group.
    pub(crate) fn advance(mut self, rows_before_budget: usize, rows_after_budget: usize) -> Self {
        if let Some(offset) = &mut self.offset {
            // Reduction is either because of offset or limit, as limit is applied
            // after offset has been "exhausted" can just use saturating sub here.
            *offset = offset.saturating_sub(rows_before_budget - rows_after_budget);
        }

        if rows_after_budget != 0
            && let Some(limit) = &mut self.limit
        {
            *limit -= rows_after_budget;
        }

        self
    }
}

#[derive(Debug)]
pub(crate) struct BudgetedReadPlan {
    /// Read plan after applying this row group's share of the offset/limit budget.
    pub(crate) plan_builder: ReadPlanBuilder,
    /// Number of rows selected by row selection and predicates before applying
    /// this row group's offset/limit budget.
    pub(crate) rows_before_budget: usize,
    /// Number of selected rows that remain to be read after applying this row
    /// group's offset/limit budget.
    pub(crate) rows_after_budget: usize,
    /// Budget remaining for later row groups.
    pub(crate) remaining_budget: RowBudget,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::arrow_reader::{RowSelection, RowSelector};

    #[test]
    fn test_row_budget_offset_limit_across_row_groups() {
        let first =
            RowBudget::new(Some(225), Some(20)).apply_to_plan(ReadPlanBuilder::new(1024), 200);
        assert_eq!(first.rows_before_budget, 200);
        assert_eq!(first.rows_after_budget, 0);
        assert_eq!(first.remaining_budget, RowBudget::new(Some(25), Some(20)));
        assert_eq!(first.plan_builder.num_rows_selected(), Some(0));

        let second = first
            .remaining_budget
            .apply_to_plan(ReadPlanBuilder::new(1024), 200);
        assert_eq!(second.rows_before_budget, 200);
        assert_eq!(second.rows_after_budget, 20);
        assert_eq!(second.remaining_budget, RowBudget::new(Some(0), Some(0)));
        assert_eq!(second.plan_builder.num_rows_selected(), Some(20));
    }

    #[test]
    fn test_row_budget_limit_only() {
        let budgeted =
            RowBudget::new(None, Some(20)).apply_to_plan(ReadPlanBuilder::new(1024), 200);
        assert_eq!(budgeted.rows_before_budget, 200);
        assert_eq!(budgeted.rows_after_budget, 20);
        assert_eq!(budgeted.remaining_budget, RowBudget::new(None, Some(0)));
        assert_eq!(budgeted.plan_builder.num_rows_selected(), Some(20));
    }

    #[test]
    fn test_row_budget_empty_selection() {
        let empty_selection = RowSelection::from(vec![RowSelector::skip(200)]);
        let budgeted = RowBudget::new(Some(10), Some(20)).apply_to_plan(
            ReadPlanBuilder::new(1024).with_selection(Some(empty_selection)),
            200,
        );
        assert_eq!(budgeted.rows_before_budget, 0);
        assert_eq!(budgeted.rows_after_budget, 0);
        assert_eq!(
            budgeted.remaining_budget,
            RowBudget::new(Some(10), Some(20))
        );
        assert_eq!(budgeted.plan_builder.num_rows_selected(), Some(0));
    }
}
