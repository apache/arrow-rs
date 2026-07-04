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
use std::collections::HashMap;
use std::sync::Arc;

pub(crate) struct RowGroupIndexReader {
    buffered_indices: Vec<i64>,
    state: ReaderState,
}

enum ReaderState {
    // fast path: single row group with constant index value
    SingleRowGroup {
        index: i64,
        remaining_rows: usize,
    },
    // general path: multiple row groups with iterator
    MultipleRowGroups {
        remaining_indices: std::iter::Flatten<std::vec::IntoIter<std::iter::RepeatN<i64>>>,
    },
}

impl RowGroupIndexReader {
    pub(crate) fn try_new<'a>(
        parquet_metadata: &'a ParquetMetaData,
        row_groups: impl Iterator<Item = &'a RowGroupMetaData>,
    ) -> Result<Self> {
        let row_groups: Vec<_> = row_groups.collect();

        // optimize for single row group case
        if row_groups.len() == 1 {
            let rg = row_groups[0];
            let ordinal = rg.ordinal().ok_or_else(|| {
                ParquetError::General(
                    "Row group missing ordinal field, required to compute row group indices"
                        .to_string(),
                )
            })?;

            // find the row group index by a linear scan through metadata
            // this is O(n) but only done once, avoiding HashMap allocation
            let index = parquet_metadata
                .row_groups()
                .iter()
                .enumerate()
                .find_map(|(idx, metadata_rg)| {
                    if metadata_rg.ordinal() == Some(ordinal) {
                        Some(idx as i64)
                    } else {
                        None
                    }
                })
                .ok_or_else(|| {
                    ParquetError::General(format!(
                        "Row group with ordinal {} not found in metadata",
                        ordinal
                    ))
                })?;

            return Ok(Self {
                buffered_indices: Vec::new(),
                state: ReaderState::SingleRowGroup {
                    index,
                    remaining_rows: rg.num_rows() as usize,
                },
            });
        }

        // general path: many row groups
        // builds a mapping from ordinal to row group index
        // this is O(n) where n is the total number of row groups in the file
        let ordinal_to_index: HashMap<i16, i64> =
            HashMap::from_iter(parquet_metadata.row_groups().iter().enumerate().filter_map(
                |(row_group_index, rg)| {
                    rg.ordinal()
                        .map(|ordinal| (ordinal, row_group_index as i64))
                },
            ));

        // build repeating iterators in the order specified by the row_groups iterator
        // this is O(m) where m is the number of selected row groups
        let repeated_indices = row_groups
            .iter()
            .map(|rg| {
                let ordinal = rg.ordinal().ok_or_else(|| {
                    ParquetError::General(
                        "Row group missing ordinal field, required to compute row group indices"
                            .to_string(),
                    )
                })?;

                let row_group_index = ordinal_to_index.get(&ordinal).ok_or_else(|| {
                    ParquetError::General(format!(
                        "Row group with ordinal {} not found in metadata",
                        ordinal
                    ))
                })?;

                // repeat row group index for each row in this row group
                Ok(std::iter::repeat_n(
                    *row_group_index,
                    rg.num_rows() as usize,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            buffered_indices: Vec::new(),
            state: ReaderState::MultipleRowGroups {
                remaining_indices: repeated_indices.into_iter().flatten(),
            },
        })
    }
}

impl ArrayReader for RowGroupIndexReader {
    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        match &mut self.state {
            ReaderState::SingleRowGroup {
                index,
                remaining_rows,
            } => {
                let num_to_read = batch_size.min(*remaining_rows);
                self.buffered_indices
                    .resize(self.buffered_indices.len() + num_to_read, *index);
                *remaining_rows -= num_to_read;
                Ok(num_to_read)
            }
            ReaderState::MultipleRowGroups { remaining_indices } => {
                let starting_len = self.buffered_indices.len();
                self.buffered_indices
                    .extend((remaining_indices.by_ref()).take(batch_size));
                Ok(self.buffered_indices.len() - starting_len)
            }
        }
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        match &mut self.state {
            ReaderState::SingleRowGroup { remaining_rows, .. } => {
                let num_to_skip = num_records.min(*remaining_rows);
                *remaining_rows -= num_to_skip;
                Ok(num_to_skip)
            }
            ReaderState::MultipleRowGroups { remaining_indices } => {
                // TODO: Use advance_by when it stabilizes to improve performance
                Ok((remaining_indices.by_ref()).take(num_records).count())
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &DataType {
        &DataType::Int64
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        Ok(Arc::new(Int64Array::from_iter(
            self.buffered_indices.drain(..),
        )))
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
        let file_metadata = FileMetaData::new(1, total_rows, None, None, schema_descr, None);

        ParquetMetaData::new(file_metadata, row_group_metas)
    }

    #[test]
    fn test_row_group_index_reader_basic() {
        // create metadata with 3 row groups, each with varying number of rows
        let metadata = create_test_parquet_metadata(vec![
            (0, 2), // rg: 0, ordinal: 0, 2 rows
            (1, 3), // rg: 1, ordinal: 1, 3 rows
            (2, 1), // rg: 2, ordinal: 2, 1 row
        ]);

        let selected_row_groups: Vec<_> = metadata.row_groups().iter().collect();

        let mut reader =
            RowGroupIndexReader::try_new(&metadata, selected_row_groups.into_iter()).unwrap();

        // 2 rows + 3 rows + 1 row
        let num_read = reader.read_records(6).unwrap();
        assert_eq!(num_read, 6);

        let array = reader.consume_batch().unwrap();
        let indices = array.as_any().downcast_ref::<Int64Array>().unwrap();

        let actual: Vec<i64> = indices.iter().map(|v| v.unwrap()).collect();
        assert_eq!(actual, [0, 0, 1, 1, 1, 2],);
    }

    #[test]
    fn test_row_group_index_reader_reverse_order() {
        // create metadata with 3 row groups, each rg has 2 rows
        let metadata = create_test_parquet_metadata(vec![(0, 2), (1, 2), (2, 2)]);

        // select only rgs with ordinals 2 and 0 (in that order)
        // means select row group 2 first, then row group 0, skipping row group 1
        let selected_row_groups: Vec<_> =
            vec![&metadata.row_groups()[2], &metadata.row_groups()[0]];

        let mut reader =
            RowGroupIndexReader::try_new(&metadata, selected_row_groups.into_iter()).unwrap();

        let num_read = reader.read_records(6).unwrap();
        // 2 rgs * 2 rows each
        assert_eq!(num_read, 4);

        let array = reader.consume_batch().unwrap();
        let indices = array.as_any().downcast_ref::<Int64Array>().unwrap();

        let actual: Vec<i64> = indices.iter().map(|v| v.unwrap()).collect();

        assert_eq!(actual, [2, 2, 0, 0],);
    }

    #[test]
    fn test_row_group_index_reader_skip_records() {
        // rg 0: 3 rows, rg 1: 4 rows, rg 2: 2 rows
        // [0, 0, 0, 1, 1, 1, 1, 2, 2]
        let metadata = create_test_parquet_metadata(vec![(0, 3), (1, 4), (2, 2)]);

        let selected_row_groups = metadata.row_groups().iter().collect::<Vec<_>>();

        let mut reader =
            RowGroupIndexReader::try_new(&metadata, selected_row_groups.into_iter()).unwrap();

        // skip first 5 rows
        // [0, 0, 0, 1, 1, 1, 1, 2, 2]
        // |---- skip ---|
        let num_skipped = reader.skip_records(5).unwrap();
        assert_eq!(num_skipped, 5);

        let num_read = reader.read_records(10).unwrap();
        assert_eq!(num_read, 4);

        let array = reader.consume_batch().unwrap();
        let indices = array.as_any().downcast_ref::<Int64Array>().unwrap();

        let actual = indices.iter().map(|v| v.unwrap()).collect::<Vec<i64>>();
        assert_eq!(actual, [1, 1, 2, 2]);
    }

    #[test]
    fn test_row_group_index_reader_single_row_group() {
        // 3 row groups
        let metadata = create_test_parquet_metadata(vec![(0, 2), (1, 3), (2, 5)]);

        // select last row group [2, 2, 2, 2, 2]
        let selected_row_groups = [&metadata.row_groups()[2]];

        let mut reader =
            RowGroupIndexReader::try_new(&metadata, selected_row_groups.into_iter()).unwrap();

        assert!(matches!(
            reader.state,
            ReaderState::SingleRowGroup { index: 2, .. }
        ));

        let num_read = reader.read_records(10).unwrap();
        assert_eq!(num_read, 5);

        let array = reader.consume_batch().unwrap();
        let indices = array.as_any().downcast_ref::<Int64Array>().unwrap();

        let actual: Vec<i64> = indices.iter().map(|v| v.unwrap()).collect();
        assert_eq!(actual, [2, 2, 2, 2, 2]);
    }

    #[test]
    fn test_row_group_index_reader_single_row_group_with_skip() {
        let metadata = create_test_parquet_metadata(vec![(0, 10), (1, 10), (2, 10)]);

        // second row group (index 1, ordinal 1 with 10 rows)
        let selected_row_groups = vec![&metadata.row_groups()[1]];

        let mut reader =
            RowGroupIndexReader::try_new(&metadata, selected_row_groups.into_iter()).unwrap();

        assert!(matches!(
            reader.state,
            ReaderState::SingleRowGroup { index: 1, .. }
        ));

        // skip first 3 rows
        let num_skipped = reader.skip_records(3).unwrap();
        assert_eq!(num_skipped, 3);

        // read next 5 rows
        let num_read = reader.read_records(5).unwrap();
        assert_eq!(num_read, 5);

        let array = reader.consume_batch().unwrap();
        let indices = array.as_any().downcast_ref::<Int64Array>().unwrap();

        let actual: Vec<i64> = indices.iter().map(|v| v.unwrap()).collect();
        assert_eq!(actual, [1, 1, 1, 1, 1]);

        if let ReaderState::SingleRowGroup { remaining_rows, .. } = reader.state {
            assert_eq!(remaining_rows, 2);
        } else {
            panic!("Expected SingleRowGroup state");
        }
    }
}
