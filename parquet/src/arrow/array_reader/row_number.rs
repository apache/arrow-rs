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

use crate::arrow::array_reader::ArrayReader;
use crate::errors::{ParquetError, Result};
use crate::file::metadata::{ParquetMetaData, RowGroupMetaData};
use arrow_array::{ArrayRef, Int64Array};
use arrow_schema::DataType;
use std::any::Any;
use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::sync::Arc;

/// Tracks row numbers within a Parquet file and emits them as an `Int64Array`.
pub(crate) struct RowNumberReader {
    /// Pre-computed row ranges that are not read yet.
    ///
    /// This reader only keeps track of the ranges of row numbers for each row group. The range is
    /// not materialized into a full array until it's needed.
    remaining_row_ranges: VecDeque<Range<i64>>,
    /// Row ranges read but not emitted.
    ///
    /// These are either full or partial (split) row ranges taken from `remaining_row_ranges`.
    buffered_row_ranges: Vec<Range<i64>>,
}

impl RowNumberReader {
    pub(crate) fn try_new<'a>(
        parquet_metadata: &'a ParquetMetaData,
        row_groups: impl Iterator<Item = &'a RowGroupMetaData>,
    ) -> Result<Self> {
        // Pass 1: Build a map from ordinal to first_row_index
        // This is O(M) where M is the total number of row groups in the file
        let mut ordinal_to_offset: HashMap<i16, i64> = HashMap::new();
        let mut first_row_index: i64 = 0;

        for rg in parquet_metadata.row_groups() {
            if let Some(ordinal) = rg.ordinal() {
                ordinal_to_offset.insert(ordinal, first_row_index);
            }
            first_row_index += rg.num_rows();
        }

        // Pass 2: Build ranges in the order specified by the row_groups iterator
        // This is O(N) where N is the number of selected row groups
        // This preserves the user's requested order instead of sorting by ordinal
        let ranges: VecDeque<_> = row_groups
            .map(|rg| {
                let ordinal = rg.ordinal().ok_or_else(|| {
                    ParquetError::General(
                        "Row group missing ordinal field, required to compute row numbers"
                            .to_string(),
                    )
                })?;

                let offset = ordinal_to_offset.get(&ordinal).ok_or_else(|| {
                    ParquetError::General(format!(
                        "Row group with ordinal {} not found in metadata",
                        ordinal
                    ))
                })?;

                Ok(*offset..*offset + rg.num_rows())
            })
            .collect::<Result<_>>()?;

        Ok(Self {
            buffered_row_ranges: Vec::new(),
            remaining_row_ranges: ranges,
        })
    }

    /// Take up to `count` rows from the first range, splitting it if needed.
    ///
    /// Returns `None` if no ranges remain.
    fn take_range(&mut self, count: usize) -> Option<Range<i64>> {
        let first = self.remaining_row_ranges.front_mut()?;
        if (first.end - first.start) <= count as i64 {
            // take out the full range
            self.remaining_row_ranges.pop_front()
        } else {
            // first range has more rows than we need.
            // so we split the range and put the remaining back.
            let split = first.start + count as i64;
            let taken = first.start..split;
            first.start = split;
            Some(taken)
        }
    }
}

impl ArrayReader for RowNumberReader {
    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        let mut remaining = batch_size;
        while remaining > 0 {
            let Some(range) = self.take_range(remaining) else {
                break;
            };
            remaining -= (range.end - range.start) as usize;
            self.buffered_row_ranges.push(range);
        }
        Ok(batch_size - remaining)
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        let mut remaining = num_records;
        while remaining > 0 {
            let Some(range) = self.take_range(remaining) else {
                break;
            };
            remaining -= (range.end - range.start) as usize;
        }
        Ok(num_records - remaining)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &DataType {
        &DataType::Int64
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        let total_rows: i64 = self
            .buffered_row_ranges
            .iter()
            .map(|range| range.end - range.start)
            .sum();
        let mut result = Vec::with_capacity(total_rows as usize);

        for range in self.buffered_row_ranges.drain(..) {
            result.extend(range);
        }

        Ok(Arc::new(Int64Array::from(result)))
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        None
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::basic::Type as PhysicalType;
    use crate::file::metadata::{
        ColumnChunkMetaData, FileMetaData, ParquetMetaData, RowGroupMetaData,
    };
    use crate::schema::types::{SchemaDescriptor, Type as SchemaType};
    use std::sync::Arc;

    fn create_test_schema() -> Arc<SchemaDescriptor> {
        let schema = SchemaType::group_type_builder("schema")
            .with_fields(vec![Arc::new(
                SchemaType::primitive_type_builder("test_col", PhysicalType::INT32)
                    .build()
                    .unwrap(),
            )])
            .build()
            .unwrap();
        Arc::new(SchemaDescriptor::new(Arc::new(schema)))
    }

    fn create_test_parquet_metadata(row_groups: Vec<(i16, i64)>) -> ParquetMetaData {
        let schema_descr = create_test_schema();

        let mut row_group_metas = vec![];
        for (ordinal, num_rows) in row_groups {
            let columns: Vec<_> = schema_descr
                .columns()
                .iter()
                .map(|col| ColumnChunkMetaData::builder(col.clone()).build().unwrap())
                .collect();

            let row_group = RowGroupMetaData::builder(schema_descr.clone())
                .set_num_rows(num_rows)
                .set_ordinal(ordinal)
                .set_total_byte_size(100)
                .set_column_metadata(columns)
                .build()
                .unwrap();
            row_group_metas.push(row_group);
        }

        let total_rows: i64 = row_group_metas.iter().map(|rg| rg.num_rows()).sum();
        let file_metadata = FileMetaData::new(
            1,            // version
            total_rows,   // num_rows
            None,         // created_by
            None,         // key_value_metadata
            schema_descr, // schema_descr
            None,         // column_orders
        );

        ParquetMetaData::new(file_metadata, row_group_metas)
    }

    fn consume_row_numbers(reader: &mut RowNumberReader) -> Vec<i64> {
        let array = reader.consume_batch().unwrap();
        array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    fn reader_for_all(metadata: &ParquetMetaData) -> RowNumberReader {
        let all_rgs: Vec<_> = metadata.row_groups().iter().collect();
        RowNumberReader::try_new(metadata, all_rgs.into_iter()).unwrap()
    }

    #[test]
    fn test_row_number_reader_reverse_order() {
        // Create metadata with 3 row groups, each with 2 rows
        let metadata = create_test_parquet_metadata(vec![
            (0, 2), // Row group 0: ordinal=0, rows 0-1
            (1, 2), // Row group 1: ordinal=1, rows 2-3
            (2, 2), // Row group 2: ordinal=2, rows 4-5
        ]);

        // Select only row groups with ordinals 2 and 0 (in that order)
        // This means we want row group 2 first, then row group 0, skipping row group 1
        let selected_row_groups: Vec<_> = vec![
            &metadata.row_groups()[2], // ordinal 2
            &metadata.row_groups()[0], // ordinal 0
        ];

        let mut reader =
            RowNumberReader::try_new(&metadata, selected_row_groups.into_iter()).unwrap();

        // Read all row numbers
        let num_read = reader.read_records(6).unwrap();
        assert_eq!(num_read, 4); // Should read 4 rows total (2 from each selected group)

        // Expected: row group 2 first (rows 4-5), then row group 0 (rows 0-1)
        assert_eq!(consume_row_numbers(&mut reader), vec![4, 5, 0, 1]);
    }

    #[test]
    fn test_range_splitting_across_batches() {
        // One row group with 10 rows
        let metadata = create_test_parquet_metadata(vec![(0, 10)]);
        let mut reader = reader_for_all(&metadata);

        assert_eq!(reader.read_records(3).unwrap(), 3);
        assert_eq!(consume_row_numbers(&mut reader), vec![0, 1, 2]);

        assert_eq!(reader.read_records(3).unwrap(), 3);
        assert_eq!(consume_row_numbers(&mut reader), vec![3, 4, 5]);

        assert_eq!(reader.read_records(3).unwrap(), 3);
        assert_eq!(consume_row_numbers(&mut reader), vec![6, 7, 8]);

        // Only 1 row left, requesting 3
        assert_eq!(reader.read_records(3).unwrap(), 1);
        assert_eq!(consume_row_numbers(&mut reader), vec![9]);
    }

    #[test]
    fn test_interleaved_skip_and_read() {
        // Row group 0: rows 0..5
        // Row group 1: rows 5..10
        let metadata = create_test_parquet_metadata(vec![(0, 5), (1, 5)]);
        let mut reader = reader_for_all(&metadata);

        assert_eq!(reader.skip_records(2).unwrap(), 2); // skip [0,1]
        assert_eq!(reader.read_records(2).unwrap(), 2); // read [2,3]
        assert_eq!(reader.skip_records(3).unwrap(), 3); // skip [4] then [5,6]
        assert_eq!(reader.read_records(3).unwrap(), 3); // read [7,8,9]

        assert_eq!(consume_row_numbers(&mut reader), vec![2, 3, 7, 8, 9]);
    }

    #[test]
    fn test_skip_then_read() {
        // One row group with 10 rows
        let metadata = create_test_parquet_metadata(vec![(0, 10)]);
        let mut reader = reader_for_all(&metadata);

        assert_eq!(reader.skip_records(3).unwrap(), 3);
        assert_eq!(reader.read_records(4).unwrap(), 4);
        assert_eq!(consume_row_numbers(&mut reader), vec![3, 4, 5, 6]);
    }
}
