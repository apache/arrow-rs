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

use std::collections::VecDeque;
use arrow_buffer::BooleanBuffer;
use crate::arrow::arrow_reader::{RowSelection, RowSelector};

/// How to select the next batch of rows to read from the Parquet file
///
/// This is the internal counterpart to [`RowSelector`] except that it
/// also has the `Mask` variant which is used to apply a filter mask
///
/// This allows the reader to dynamically choose between decoding strategies
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ReadStep {
    /// Read n rows
    Read(usize),
    /// Skip n rows
    Skip(usize),
    /// Reads mask.len() rows then applies the filter mask to select just the desired
    /// rows.
    ///
    /// Any row with a 1 value in the mask will be selected and included
    /// in the output batch.
    ///
    /// This is used in situations where the overhead of preferentially decoding
    /// only the selected rows is higher than decoding all rows and then
    /// applying a mask via filter.
    Mask {
        mask: BooleanBuffer,
        num_selected: usize,
    },
}

 impl ReadStep {
     /// returns true if this step selects rows
     pub(crate) fn selects_any(&self) -> bool {
         match self {
             ReadStep::Read(_) => true,
             ReadStep::Skip(_) => false,
             ReadStep::Mask { mask: _, num_selected } => *num_selected > 0
         }
     }
 }


/// A list of [`ReadStep`]s that describe how to read from a Parquet file
///
/// This is the internal counterpart to [`RowSelection`]
#[derive(Debug, Clone, PartialEq)]
pub (crate) struct ReadSteps {
    /// The list of read steps
    steps: Vec<ReadStep>,
}

impl ReadSteps {
    /// Create a new [`ReadSteps`] instance
    pub (crate) fn new(steps: Vec<ReadStep>) -> Self {
        Self { steps }
    }

    /// Create a new empty [`ReadSteps`] instance
    pub (crate) fn empty() -> Self {
        Self::new(vec![])
    }
    
    /// Add a read step to the list
    pub (crate) fn add(&mut self, step: ReadStep) {
        self.steps.push(step);
    }

    /// Get the list of read steps
    pub (crate) fn steps(&self) -> &[ReadStep] {
        &self.steps
    }

    /// Returns true if any step selects rows
    pub(crate) fn selects_any(&self) -> bool {
        self.steps.iter().any(ReadStep::selects_any)
    }

    /// Returns the number of rows selected
    pub(crate) fn num_selected(&self) -> usize {
        self.steps.iter().map(|step| {
                match step {
                    ReadStep::Read(n) => *n,
                    ReadStep::Skip(_) => 0,
                    ReadStep::Mask{ mask: _, num_selected} => *num_selected,
                }
        }).sum()
    }

    /// Applies an offset to this [`ReadSteps`], skipping the first `offset` selected rows
    pub(crate) fn offset(mut self, offset: usize) -> Self {
        if offset == 0 {
            return self;
        }

        let mut selected_count = 0;
        let mut skipped_count = 0;

        // Find the index where the selector exceeds the row count
        let find = self
            .steps
            .iter()
            .position(|step| match step {
                ReadStep::Skip(row_count) => {
                    skipped_count += row_count;
                    false
                }
                ReadStep::Read(row_count) => {
                    selected_count += row_count;
                    selected_count > offset
                }
                ReadStep::Mask {..} => todo!(),
            });

        let split_idx = match find {
            Some(idx) => idx,
            None => {
                self.steps.clear();
                return self;
            }
        };

        let mut steps = Vec::with_capacity(self.steps.len() - split_idx + 1);
        steps.push(ReadStep::Skip(skipped_count + offset));
        steps.push(ReadStep::Read(selected_count - offset));
        steps.extend_from_slice(&self.steps[split_idx + 1..]);

        Self { steps }
    }

    /// Limit this [`ReadSteps`] to only select `limit` rows
    pub(crate) fn limit(mut self, mut limit: usize) -> Self {
        if limit == 0 {
            self.steps.clear();
        }

        for (idx, step) in self.steps.iter_mut().enumerate() {
            match step {
                ReadStep::Read(row_count) =>        {        
                    if *row_count >= limit {
                        *row_count = limit; // update row count
                        self.steps.truncate(idx + 1);
                        break;
                    }
                } 
                ReadStep::Skip(row_count) => {
                    limit -= *row_count;
                }
                ReadStep::Mask { mask: _, num_selected } => {
                    todo!()
                }
            }
        }
        self
    }
    
    /// return the inner steps
    pub(crate) fn into_inner(self) -> Vec<ReadStep> {
        self.steps
    }
}

impl From<Vec<ReadStep>> for ReadSteps {
    fn from(value: Vec<ReadStep>) -> Self {
        Self::new(value)
    }
}

impl From<Vec<RowSelector>> for ReadSteps {
    fn from(selection: Vec<RowSelector>) -> Self {
        let steps = selection
            .into_iter()
            .map(ReadStep::from)
            .collect();
        Self::new(steps)
    }
}

impl From<RowSelection> for ReadSteps {
    fn from(selection: RowSelection) -> Self {
        selection.into_inner().into()
    }
}

impl From<ReadSteps> for Vec<ReadStep> {
    fn from(steps: ReadSteps) -> Self {
        steps.into_inner()
    }
}


/// Incrementally returns [`ReadStep`]s that describe reading from a Parquet file.
///
/// The returned stream of [`ReadStep`]s that is guaranteed to have:
/// 1. No empty selections (that select no rows)
/// 2. No selections that span batch_size boundaries
/// 3. No trailing skip selections
///
/// For example, if the `batch_size` is 100 and we are selecting all 200 rows
/// from a Parquet file, the selectors will be:
/// - `ReadStep::Read(100)  <-- forced break at batch_size boundary`
/// - `ReadStep::Skip(100)`
#[derive(Debug, Clone)]
pub(crate) struct OptimizedReadSteps {
    /// how many rows to read in each batch
    batch_size: usize,
    /// how many records have been read by RowSelection in the "current" batch
    read_records: usize,
    /// Input selectors to read from
    input_steps: VecDeque<ReadStep>,
}

impl Iterator for OptimizedReadSteps {
    type Item = ReadStep;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(mut front) = self.input_steps.pop_front() {
            match front {
                // RowSelectors with row_count = 0 terminate the read, so skip such
                // entries. See https://github.com/apache/arrow-rs/issues/2669
                ReadStep::Read(row_count) if row_count == 0 => {
                    continue;
                }
                ReadStep::Skip(_) => return Some(front),
                ReadStep::Read(mut row_count) => {
                    let need_read = self.batch_size - self.read_records;

                    // if there are more rows in the current RowSelector than needed to
                    // finish the batch, split it up
                    if row_count > need_read {
                        // Part 1: return remaining rows to the front of the queue
                        let remaining = row_count - need_read;
                        self.input_steps
                            .push_front(ReadStep::Read(remaining));
                        // Part 2: adjust the current selector to read the rows we need
                        row_count = need_read;
                    }

                    self.read_records += row_count;
                    // if read enough records to complete a batch, emit
                    if self.read_records == self.batch_size {
                        self.read_records = 0;
                    }

                    return Some(ReadStep::Read(row_count));
                }
                ReadStep::Mask { mask, num_selected } => {
                    todo!()
                }
            }
        }
        // no more selectors to read, end of stream
        None
    }
}

impl OptimizedReadSteps {
    pub(crate) fn new(batch_size: usize, input_steps: ReadSteps) -> Self {
        let mut input_steps = VecDeque::from(input_steps.into_inner());
        // trim any trailing empty selectors
        while input_steps.back().map(|step| !step.selects_any()).unwrap_or(false) {
            input_steps.pop_back();
        }

        Self {
            batch_size,
            read_records: 0,
            input_steps,
        }
    }

    /// Return the number of rows to read in each output batch
    pub(crate) fn batch_size(&self) -> usize {
        self.batch_size
    }
}
