use crate::arrow::array_reader::ArrayReader;
use crate::errors::{ParquetError, Result};
use crate::file::metadata::RowGroupMetaData;
use arrow_array::{ArrayRef, Int64Array};
use arrow_schema::DataType;
use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

pub(crate) struct RowNumberReader {
    row_numbers: Vec<i64>,
    row_groups: RowGroupSizeIterator,
}

impl RowNumberReader {
    pub(crate) fn try_new<I>(row_groups: impl IntoIterator<Item = I>) -> Result<Self>
    where
        I: TryInto<RowGroupSize, Error=ParquetError>,
    {
        let row_groups = RowGroupSizeIterator::try_new(row_groups)?;
        Ok(Self {
            row_numbers: Vec::new(),
            row_groups,
        })
    }
}

impl ArrayReader for RowNumberReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &DataType {
        &DataType::Int64
    }

    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        let read = self
            .row_groups
            .read_records(batch_size, &mut self.row_numbers);
        Ok(read)
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        Ok(Arc::new(Int64Array::from_iter(self.row_numbers.drain(..))))
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        let skipped = self.row_groups.skip_records(num_records);
        Ok(skipped)
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        None
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        None
    }
}

struct RowGroupSizeIterator {
    row_groups: VecDeque<RowGroupSize>,
}

impl RowGroupSizeIterator {
    fn try_new<I>(row_groups: impl IntoIterator<Item = I>) -> Result<Self>
    where
        I: TryInto<RowGroupSize, Error=ParquetError>,
    {
        Ok(Self {
            row_groups: VecDeque::from(row_groups.into_iter().map(TryInto::try_into).collect::<Result<Vec<_>>>()?),
        })
    }
}

impl RowGroupSizeIterator {
    fn read_records(&mut self, mut batch_size: usize, row_numbers: &mut Vec<i64>) -> usize {
        let mut read = 0;
        while batch_size > 0 {
            let Some(front) = self.row_groups.front_mut() else {
                return read as usize;
            };
            let to_read = std::cmp::min(front.num_rows, batch_size as i64);
            row_numbers.extend(front.first_row_number..front.first_row_number + to_read);
            front.num_rows -= to_read;
            front.first_row_number += to_read;
            if front.num_rows == 0 {
                self.row_groups.pop_front();
            }
            batch_size -= to_read as usize;
            read += to_read;
        }
        read as usize
    }

    fn skip_records(&mut self, mut num_records: usize) -> usize {
        let mut skipped = 0;
        while num_records > 0 {
            let Some(front) = self.row_groups.front_mut() else {
                return skipped as usize;
            };
            let to_skip = std::cmp::min(front.num_rows, num_records as i64);
            front.num_rows -= to_skip;
            front.first_row_number += to_skip;
            if front.num_rows == 0 {
                self.row_groups.pop_front();
            }
            skipped += to_skip;
            num_records -= to_skip as usize;
        }
        skipped as usize
    }
}

pub(crate) struct RowGroupSize {
    first_row_number: i64,
    num_rows: i64,
}

impl TryFrom<&RowGroupMetaData> for RowGroupSize {
    type Error = ParquetError;

    fn try_from(rg: &RowGroupMetaData) -> Result<Self, Self::Error> {
        Ok(Self {
            first_row_number: rg.first_row_number().ok_or(ParquetError::RowGroupMetaDataMissingRowNumber)?,
            num_rows: rg.num_rows(),
        })
    }
}
