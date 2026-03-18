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

pub(crate) struct RowNumberReader {
    buffered_row_numbers: Vec<i64>,
    remaining_row_numbers: std::iter::Flatten<std::vec::IntoIter<std::ops::Range<i64>>>,
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
        let ranges: Vec<_> = row_groups
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
            buffered_row_numbers: Vec::new(),
            remaining_row_numbers: ranges.into_iter().flatten(),
        })
    }
}

impl ArrayReader for RowNumberReader {
    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        let starting_len = self.buffered_row_numbers.len();
        self.buffered_row_numbers
            .extend((&mut self.remaining_row_numbers).take(batch_size));
        Ok(self.buffered_row_numbers.len() - starting_len)
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        // TODO: Use advance_by when it stabilizes to improve performance
        Ok((&mut self.remaining_row_numbers).take(num_records).count())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &DataType {
        &DataType::Int64
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        Ok(Arc::new(Int64Array::from_iter(
            self.buffered_row_numbers.drain(..),
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

        let array = reader.consume_batch().unwrap();
        let row_numbers = array.as_any().downcast_ref::<Int64Array>().unwrap();

        // Expected: row group 2 first (rows 4-5), then row group 0 (rows 0-1)
        let expected = vec![4, 5, 0, 1];
        let actual: Vec<i64> = row_numbers.iter().map(|v| v.unwrap()).collect();

        assert_eq!(
            actual, expected,
            "Row numbers should match the order of selected row groups, not file order"
        );
    }
}
