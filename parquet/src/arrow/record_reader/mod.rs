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

use std::cmp::{max, min};

use arrow_buffer::Buffer;
use arrow_data::Bitmap;

use crate::arrow::record_reader::{
    buffer::{BufferQueue, ScalarBuffer, ValuesBuffer},
    definition_levels::{DefinitionLevelBuffer, DefinitionLevelBufferDecoder},
};
use crate::column::{
    page::PageReader,
    reader::{
        decoder::{ColumnLevelDecoderImpl, ColumnValueDecoder, ColumnValueDecoderImpl},
        GenericColumnReader,
    },
};
use crate::data_type::DataType;
use crate::errors::{ParquetError, Result};
use crate::schema::types::ColumnDescPtr;

pub(crate) mod buffer;
mod definition_levels;

/// The minimum number of levels read when reading a repeated field
pub(crate) const MIN_BATCH_SIZE: usize = 1024;

/// A `RecordReader` is a stateful column reader that delimits semantic records.
pub type RecordReader<T> =
    GenericRecordReader<ScalarBuffer<<T as DataType>::T>, ColumnValueDecoderImpl<T>>;

pub(crate) type ColumnReader<CV> =
    GenericColumnReader<ColumnLevelDecoderImpl, DefinitionLevelBufferDecoder, CV>;

/// A generic stateful column reader that delimits semantic records
///
/// This type is hidden from the docs, and relies on private traits with no
/// public implementations. As such this type signature may be changed without
/// breaking downstream users as it can only be constructed through type aliases
pub struct GenericRecordReader<V, CV> {
    column_desc: ColumnDescPtr,

    records: V,
    def_levels: Option<DefinitionLevelBuffer>,
    rep_levels: Option<ScalarBuffer<i16>>,
    column_reader: Option<ColumnReader<CV>>,

    /// Number of records accumulated in records
    num_records: usize,

    /// Number of values `num_records` contains.
    num_values: usize,

    /// Starts from 1, number of values have been written to buffer
    values_written: usize,
}

impl<V, CV> GenericRecordReader<V, CV>
where
    V: ValuesBuffer + Default,
    CV: ColumnValueDecoder<Slice = V::Slice>,
{
    /// Create a new [`GenericRecordReader`]
    pub fn new(desc: ColumnDescPtr) -> Self {
        Self::new_with_records(desc, V::default())
    }
}

impl<V, CV> GenericRecordReader<V, CV>
where
    V: ValuesBuffer,
    CV: ColumnValueDecoder<Slice = V::Slice>,
{
    pub fn new_with_records(desc: ColumnDescPtr, records: V) -> Self {
        let def_levels = (desc.max_def_level() > 0)
            .then(|| DefinitionLevelBuffer::new(&desc, packed_null_mask(&desc)));

        let rep_levels = (desc.max_rep_level() > 0).then(ScalarBuffer::new);

        Self {
            records,
            def_levels,
            rep_levels,
            column_reader: None,
            column_desc: desc,
            num_records: 0,
            num_values: 0,
            values_written: 0,
        }
    }

    /// Set the current page reader.
    pub fn set_page_reader(&mut self, page_reader: Box<dyn PageReader>) -> Result<()> {
        let descr = &self.column_desc;
        let values_decoder = CV::new(descr);

        let def_level_decoder = (descr.max_def_level() != 0).then(|| {
            DefinitionLevelBufferDecoder::new(
                descr.max_def_level(),
                packed_null_mask(descr),
            )
        });

        let rep_level_decoder = (descr.max_rep_level() != 0)
            .then(|| ColumnLevelDecoderImpl::new(descr.max_rep_level()));

        self.column_reader = Some(GenericColumnReader::new_with_decoders(
            self.column_desc.clone(),
            page_reader,
            values_decoder,
            def_level_decoder,
            rep_level_decoder,
        ));
        Ok(())
    }

    /// Try to read `num_records` of column data into internal buffer.
    ///
    /// # Returns
    ///
    /// Number of actual records read.
    pub fn read_records(&mut self, num_records: usize) -> Result<usize> {
        if self.column_reader.is_none() {
            return Ok(0);
        }

        let mut records_read = 0;

        loop {
            // Try to find some records from buffers that has been read into memory
            // but not counted as seen records.

            // Check to see if the column is exhausted. Only peek the next page since in
            // case we are reading to a page boundary and do not actually need to read
            // the next page.
            let end_of_column = !self.column_reader.as_mut().unwrap().peek_next()?;

            let (record_count, value_count) =
                self.count_records(num_records - records_read, end_of_column);

            self.num_records += record_count;
            self.num_values += value_count;
            records_read += record_count;

            if records_read == num_records
                || !self.column_reader.as_mut().unwrap().has_next()?
            {
                break;
            }

            // If repetition levels present, we don't know how much more to read
            // in order to read the requested number of records, therefore read at least
            // MIN_BATCH_SIZE, otherwise read **exactly** what was requested. This helps
            // to avoid a degenerate case where the buffers are never fully drained.
            //
            // Consider the scenario where the user is requesting batches of MIN_BATCH_SIZE.
            //
            // When transitioning across a row group boundary, this will read some remainder
            // from the row group `r`, before reading MIN_BATCH_SIZE from the next row group,
            // leaving `MIN_BATCH_SIZE + r` in the buffer.
            //
            // The client will then only split off the `MIN_BATCH_SIZE` they actually wanted,
            // leaving behind `r`. This will continue indefinitely.
            //
            // Aside from wasting cycles splitting and shuffling buffers unnecessarily, this
            // prevents dictionary preservation from functioning correctly as the buffer
            // will never be emptied, allowing a new dictionary to be registered.
            //
            // This degenerate case can still occur for repeated fields, but
            // it is avoided for the more common case of a non-repeated field
            let batch_size = match &self.rep_levels {
                Some(_) => max(num_records - records_read, MIN_BATCH_SIZE),
                None => num_records - records_read,
            };

            // Try to more value from parquet pages
            self.read_one_batch(batch_size)?;
        }

        Ok(records_read)
    }

    /// Try to skip the next `num_records` rows
    ///
    /// # Returns
    ///
    /// Number of records skipped
    pub fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        // First need to clear the buffer
        let end_of_column = match self.column_reader.as_mut() {
            Some(reader) => !reader.peek_next()?,
            None => return Ok(0),
        };

        let (buffered_records, buffered_values) =
            self.count_records(num_records, end_of_column);

        self.num_records += buffered_records;
        self.num_values += buffered_values;

        let remaining = num_records - buffered_records;

        if remaining == 0 {
            return Ok(buffered_records);
        }

        let skipped = self
            .column_reader
            .as_mut()
            .unwrap()
            .skip_records(remaining)?;

        Ok(skipped + buffered_records)
    }

    /// Returns number of records stored in buffer.
    #[allow(unused)]
    pub fn num_records(&self) -> usize {
        self.num_records
    }

    /// Return number of values stored in buffer.
    /// If the parquet column is not repeated, it should be equals to `num_records`,
    /// otherwise it should be larger than or equal to `num_records`.
    pub fn num_values(&self) -> usize {
        self.num_values
    }

    /// Returns definition level data.
    /// The implementation has side effects. It will create a new buffer to hold those
    /// definition level values that have already been read into memory but not counted
    /// as record values, e.g. those from `self.num_values` to `self.values_written`.
    pub fn consume_def_levels(&mut self) -> Option<Buffer> {
        match self.def_levels.as_mut() {
            Some(x) => x.split_levels(self.num_values),
            None => None,
        }
    }

    /// Return repetition level data.
    /// The side effect is similar to `consume_def_levels`.
    pub fn consume_rep_levels(&mut self) -> Option<Buffer> {
        match self.rep_levels.as_mut() {
            Some(x) => Some(x.split_off(self.num_values)),
            None => None,
        }
    }

    /// Returns currently stored buffer data.
    /// The side effect is similar to `consume_def_levels`.
    pub fn consume_record_data(&mut self) -> V::Output {
        self.records.split_off(self.num_values)
    }

    /// Returns currently stored null bitmap data.
    /// The side effect is similar to `consume_def_levels`.
    pub fn consume_bitmap_buffer(&mut self) -> Option<Buffer> {
        self.consume_bitmap().map(|b| b.into_buffer())
    }

    /// Reset state of record reader.
    /// Should be called after consuming data, e.g. `consume_rep_levels`,
    /// `consume_rep_levels`, `consume_record_data` and `consume_bitmap_buffer`.
    pub fn reset(&mut self) {
        self.values_written -= self.num_values;
        self.num_records = 0;
        self.num_values = 0;
    }

    /// Returns bitmap data.
    pub fn consume_bitmap(&mut self) -> Option<Bitmap> {
        self.def_levels
            .as_mut()
            .map(|levels| levels.split_bitmask(self.num_values))
    }

    /// Try to read one batch of data.
    fn read_one_batch(&mut self, batch_size: usize) -> Result<usize> {
        let rep_levels = self
            .rep_levels
            .as_mut()
            .map(|levels| levels.spare_capacity_mut(batch_size));

        let def_levels = self.def_levels.as_mut();

        let values = self.records.spare_capacity_mut(batch_size);

        let (values_read, levels_read) = self
            .column_reader
            .as_mut()
            .unwrap()
            .read_batch(batch_size, def_levels, rep_levels, values)?;

        if values_read < levels_read {
            let def_levels = self.def_levels.as_ref().ok_or_else(|| {
                general_err!(
                    "Definition levels should exist when data is less than levels!"
                )
            })?;

            self.records.pad_nulls(
                self.values_written,
                values_read,
                levels_read,
                def_levels.nulls().as_slice(),
            );
        }

        let values_read = max(levels_read, values_read);
        self.set_values_written(self.values_written + values_read);
        Ok(values_read)
    }

    /// Inspects the buffered repetition levels in the range `self.num_values..self.values_written`
    /// and returns the number of "complete" records along with the corresponding number of values
    ///
    /// If `end_of_column` is true it indicates that there are no further values for this
    /// column chunk beyond what is currently in the buffers
    ///
    /// A "complete" record is one where the buffer contains a subsequent repetition level of 0
    fn count_records(
        &self,
        records_to_read: usize,
        end_of_column: bool,
    ) -> (usize, usize) {
        match self.rep_levels.as_ref() {
            Some(buf) => {
                let buf = buf.as_slice();

                let mut records_read = 0;
                let mut end_of_last_record = self.num_values;

                for (current, item) in buf
                    .iter()
                    .enumerate()
                    .take(self.values_written)
                    .skip(self.num_values)
                {
                    if *item == 0 && current != self.num_values {
                        records_read += 1;
                        end_of_last_record = current;

                        if records_read == records_to_read {
                            break;
                        }
                    }
                }

                // If reached end of column chunk => end of a record
                if records_read != records_to_read
                    && end_of_column
                    && self.values_written != self.num_values
                {
                    records_read += 1;
                    end_of_last_record = self.values_written;
                }

                (records_read, end_of_last_record - self.num_values)
            }
            None => {
                let records_read =
                    min(records_to_read, self.values_written - self.num_values);

                (records_read, records_read)
            }
        }
    }

    fn set_values_written(&mut self, new_values_written: usize) {
        self.values_written = new_values_written;
        self.records.set_len(self.values_written);

        if let Some(ref mut buf) = self.rep_levels {
            buf.set_len(self.values_written)
        };

        if let Some(ref mut buf) = self.def_levels {
            buf.set_len(self.values_written)
        };
    }
}

/// Returns true if we do not need to unpack the nullability for this column, this is
/// only possible if the max defiition level is 1, and corresponds to nulls at the
/// leaf level, as opposed to a nullable parent nested type
fn packed_null_mask(descr: &ColumnDescPtr) -> bool {
    descr.max_def_level() == 1
        && descr.max_rep_level() == 0
        && descr.self_type().is_optional()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::bitmap::Bitmap;
    use arrow::buffer::Buffer;
    use arrow_array::builder::{Int16BufferBuilder, Int32BufferBuilder};

    use crate::basic::Encoding;
    use crate::data_type::Int32Type;
    use crate::schema::parser::parse_message_type;
    use crate::schema::types::SchemaDescriptor;
    use crate::util::test_common::page_util::{
        DataPageBuilder, DataPageBuilderImpl, InMemoryPageReader,
    };

    use super::RecordReader;

    #[test]
    fn test_read_required_records() {
        // Construct column schema
        let message_type = "
        message test_schema {
          REQUIRED INT32 leaf;
        }
        ";
        let desc = parse_message_type(message_type)
            .map(|t| SchemaDescriptor::new(Arc::new(t)))
            .map(|s| s.column(0))
            .unwrap();

        // Construct record reader
        let mut record_reader = RecordReader::<Int32Type>::new(desc.clone());

        // First page

        // Records data:
        // test_schema
        //   leaf: 4
        // test_schema
        //   leaf: 7
        // test_schema
        //   leaf: 6
        // test_schema
        //   left: 3
        // test_schema
        //   left: 2
        {
            let values = [4, 7, 6, 3, 2];
            let mut pb = DataPageBuilderImpl::new(desc.clone(), 5, true);
            pb.add_values::<Int32Type>(Encoding::PLAIN, &values);
            let page = pb.consume();

            let page_reader = Box::new(InMemoryPageReader::new(vec![page]));
            record_reader.set_page_reader(page_reader).unwrap();
            assert_eq!(2, record_reader.read_records(2).unwrap());
            assert_eq!(2, record_reader.num_records());
            assert_eq!(2, record_reader.num_values());
            assert_eq!(3, record_reader.read_records(3).unwrap());
            assert_eq!(5, record_reader.num_records());
            assert_eq!(5, record_reader.num_values());
        }

        // Second page

        // Records data:
        // test_schema
        //   leaf: 8
        // test_schema
        //   leaf: 9
        {
            let values = [8, 9];
            let mut pb = DataPageBuilderImpl::new(desc, 2, true);
            pb.add_values::<Int32Type>(Encoding::PLAIN, &values);
            let page = pb.consume();

            let page_reader = Box::new(InMemoryPageReader::new(vec![page]));
            record_reader.set_page_reader(page_reader).unwrap();
            assert_eq!(2, record_reader.read_records(10).unwrap());
            assert_eq!(7, record_reader.num_records());
            assert_eq!(7, record_reader.num_values());
        }

        let mut bb = Int32BufferBuilder::new(7);
        bb.append_slice(&[4, 7, 6, 3, 2, 8, 9]);
        let expected_buffer = bb.finish();
        assert_eq!(expected_buffer, record_reader.consume_record_data());
        assert_eq!(None, record_reader.consume_def_levels());
        assert_eq!(None, record_reader.consume_bitmap());
    }

    #[test]
    fn test_read_optional_records() {
        // Construct column schema
        let message_type = "
        message test_schema {
          OPTIONAL Group test_struct {
            OPTIONAL INT32 leaf;
          }
        }
        ";

        let desc = parse_message_type(message_type)
            .map(|t| SchemaDescriptor::new(Arc::new(t)))
            .map(|s| s.column(0))
            .unwrap();

        // Construct record reader
        let mut record_reader = RecordReader::<Int32Type>::new(desc.clone());

        // First page

        // Records data:
        // test_schema
        //   test_struct
        // test_schema
        //   test_struct
        //     left: 7
        // test_schema
        // test_schema
        //   test_struct
        //     leaf: 6
        // test_schema
        //   test_struct
        //     leaf: 6
        {
            let values = [7, 6, 3];
            //empty, non-empty, empty, non-empty, non-empty
            let def_levels = [1i16, 2i16, 0i16, 2i16, 2i16];
            let mut pb = DataPageBuilderImpl::new(desc.clone(), 5, true);
            pb.add_def_levels(2, &def_levels);
            pb.add_values::<Int32Type>(Encoding::PLAIN, &values);
            let page = pb.consume();

            let page_reader = Box::new(InMemoryPageReader::new(vec![page]));
            record_reader.set_page_reader(page_reader).unwrap();
            assert_eq!(2, record_reader.read_records(2).unwrap());
            assert_eq!(2, record_reader.num_records());
            assert_eq!(2, record_reader.num_values());
            assert_eq!(3, record_reader.read_records(3).unwrap());
            assert_eq!(5, record_reader.num_records());
            assert_eq!(5, record_reader.num_values());
        }

        // Second page

        // Records data:
        // test_schema
        // test_schema
        //   test_struct
        //     left: 8
        {
            let values = [8];
            //empty, non-empty
            let def_levels = [0i16, 2i16];
            let mut pb = DataPageBuilderImpl::new(desc, 2, true);
            pb.add_def_levels(2, &def_levels);
            pb.add_values::<Int32Type>(Encoding::PLAIN, &values);
            let page = pb.consume();

            let page_reader = Box::new(InMemoryPageReader::new(vec![page]));
            record_reader.set_page_reader(page_reader).unwrap();
            assert_eq!(2, record_reader.read_records(10).unwrap());
            assert_eq!(7, record_reader.num_records());
            assert_eq!(7, record_reader.num_values());
        }

        // Verify result def levels
        let mut bb = Int16BufferBuilder::new(7);
        bb.append_slice(&[1i16, 2i16, 0i16, 2i16, 2i16, 0i16, 2i16]);
        let expected_def_levels = bb.finish();
        assert_eq!(
            Some(expected_def_levels),
            record_reader.consume_def_levels()
        );

        // Verify bitmap
        let expected_valid = &[false, true, false, true, true, false, true];
        let expected_buffer = Buffer::from_iter(expected_valid.iter().cloned());
        let expected_bitmap = Bitmap::from(expected_buffer);
        assert_eq!(Some(expected_bitmap), record_reader.consume_bitmap());

        // Verify result record data
        let actual = record_reader.consume_record_data();
        let actual_values = actual.typed_data::<i32>();

        let expected = &[0, 7, 0, 6, 3, 0, 8];
        assert_eq!(actual_values.len(), expected.len());

        // Only validate valid values are equal
        let iter = expected_valid.iter().zip(actual_values).zip(expected);
        for ((valid, actual), expected) in iter {
            if *valid {
                assert_eq!(actual, expected)
            }
        }
    }

    #[test]
    fn test_read_repeated_records() {
        // Construct column schema
        let message_type = "
        message test_schema {
          REPEATED Group test_struct {
            REPEATED  INT32 leaf;
          }
        }
        ";

        let desc = parse_message_type(message_type)
            .map(|t| SchemaDescriptor::new(Arc::new(t)))
            .map(|s| s.column(0))
            .unwrap();

        // Construct record reader
        let mut record_reader = RecordReader::<Int32Type>::new(desc.clone());

        // First page

        // Records data:
        // test_schema
        //   test_struct
        //     leaf: 4
        // test_schema
        // test_schema
        //   test_struct
        //   test_struct
        //     leaf: 7
        //     leaf: 6
        //     leaf: 3
        //   test_struct
        //     leaf: 2
        {
            let values = [4, 7, 6, 3, 2];
            let def_levels = [2i16, 0i16, 1i16, 2i16, 2i16, 2i16, 2i16];
            let rep_levels = [0i16, 0i16, 0i16, 1i16, 2i16, 2i16, 1i16];
            let mut pb = DataPageBuilderImpl::new(desc.clone(), 7, true);
            pb.add_rep_levels(2, &rep_levels);
            pb.add_def_levels(2, &def_levels);
            pb.add_values::<Int32Type>(Encoding::PLAIN, &values);
            let page = pb.consume();

            let page_reader = Box::new(InMemoryPageReader::new(vec![page]));
            record_reader.set_page_reader(page_reader).unwrap();

            assert_eq!(1, record_reader.read_records(1).unwrap());
            assert_eq!(1, record_reader.num_records());
            assert_eq!(1, record_reader.num_values());
            assert_eq!(2, record_reader.read_records(3).unwrap());
            assert_eq!(3, record_reader.num_records());
            assert_eq!(7, record_reader.num_values());
        }

        // Second page

        // Records data:
        // test_schema
        //   test_struct
        //     leaf: 8
        //     leaf: 9
        {
            let values = [8, 9];
            let def_levels = [2i16, 2i16];
            let rep_levels = [0i16, 2i16];
            let mut pb = DataPageBuilderImpl::new(desc, 2, true);
            pb.add_rep_levels(2, &rep_levels);
            pb.add_def_levels(2, &def_levels);
            pb.add_values::<Int32Type>(Encoding::PLAIN, &values);
            let page = pb.consume();

            let page_reader = Box::new(InMemoryPageReader::new(vec![page]));
            record_reader.set_page_reader(page_reader).unwrap();

            assert_eq!(1, record_reader.read_records(10).unwrap());
            assert_eq!(4, record_reader.num_records());
            assert_eq!(9, record_reader.num_values());
        }

        // Verify result def levels
        let mut bb = Int16BufferBuilder::new(9);
        bb.append_slice(&[2i16, 0i16, 1i16, 2i16, 2i16, 2i16, 2i16, 2i16, 2i16]);
        let expected_def_levels = bb.finish();
        assert_eq!(
            Some(expected_def_levels),
            record_reader.consume_def_levels()
        );

        // Verify bitmap
        let expected_valid = &[true, false, false, true, true, true, true, true, true];
        let expected_buffer = Buffer::from_iter(expected_valid.iter().cloned());
        let expected_bitmap = Bitmap::from(expected_buffer);
        assert_eq!(Some(expected_bitmap), record_reader.consume_bitmap());

        // Verify result record data
        let actual = record_reader.consume_record_data();
        let actual_values = actual.typed_data::<i32>();
        let expected = &[4, 0, 0, 7, 6, 3, 2, 8, 9];
        assert_eq!(actual_values.len(), expected.len());

        // Only validate valid values are equal
        let iter = expected_valid.iter().zip(actual_values).zip(expected);
        for ((valid, actual), expected) in iter {
            if *valid {
                assert_eq!(actual, expected)
            }
        }
    }

    #[test]
    fn test_read_more_than_one_batch() {
        // Construct column schema
        let message_type = "
        message test_schema {
          REPEATED  INT32 leaf;
        }
        ";

        let desc = parse_message_type(message_type)
            .map(|t| SchemaDescriptor::new(Arc::new(t)))
            .map(|s| s.column(0))
            .unwrap();

        // Construct record reader
        let mut record_reader = RecordReader::<Int32Type>::new(desc.clone());

        {
            let values = [100; 5000];
            let def_levels = [1i16; 5000];
            let mut rep_levels = [1i16; 5000];
            for idx in 0..1000 {
                rep_levels[idx * 5] = 0i16;
            }

            let mut pb = DataPageBuilderImpl::new(desc, 5000, true);
            pb.add_rep_levels(1, &rep_levels);
            pb.add_def_levels(1, &def_levels);
            pb.add_values::<Int32Type>(Encoding::PLAIN, &values);
            let page = pb.consume();

            let page_reader = Box::new(InMemoryPageReader::new(vec![page]));
            record_reader.set_page_reader(page_reader).unwrap();

            assert_eq!(1000, record_reader.read_records(1000).unwrap());
            assert_eq!(1000, record_reader.num_records());
            assert_eq!(5000, record_reader.num_values());
        }
    }

    #[test]
    fn test_row_group_boundary() {
        // Construct column schema
        let message_type = "
        message test_schema {
          REPEATED Group test_struct {
            REPEATED  INT32 leaf;
          }
        }
        ";

        let desc = parse_message_type(message_type)
            .map(|t| SchemaDescriptor::new(Arc::new(t)))
            .map(|s| s.column(0))
            .unwrap();

        let values = [1, 2, 3];
        let def_levels = [1i16, 0i16, 1i16, 2i16, 2i16, 1i16, 2i16];
        let rep_levels = [0i16, 0i16, 0i16, 1i16, 2i16, 0i16, 1i16];
        let mut pb = DataPageBuilderImpl::new(desc.clone(), 7, true);
        pb.add_rep_levels(2, &rep_levels);
        pb.add_def_levels(2, &def_levels);
        pb.add_values::<Int32Type>(Encoding::PLAIN, &values);
        let page = pb.consume();

        let mut record_reader = RecordReader::<Int32Type>::new(desc);
        let page_reader = Box::new(InMemoryPageReader::new(vec![page.clone()]));
        record_reader.set_page_reader(page_reader).unwrap();
        assert_eq!(record_reader.read_records(4).unwrap(), 4);
        assert_eq!(record_reader.num_records(), 4);
        assert_eq!(record_reader.num_values(), 7);

        assert_eq!(record_reader.read_records(4).unwrap(), 0);
        assert_eq!(record_reader.num_records(), 4);
        assert_eq!(record_reader.num_values(), 7);

        record_reader.read_records(4).unwrap();

        let page_reader = Box::new(InMemoryPageReader::new(vec![page]));
        record_reader.set_page_reader(page_reader).unwrap();

        assert_eq!(record_reader.read_records(4).unwrap(), 4);
        assert_eq!(record_reader.num_records(), 8);
        assert_eq!(record_reader.num_values(), 14);

        assert_eq!(record_reader.read_records(4).unwrap(), 0);
        assert_eq!(record_reader.num_records(), 8);
        assert_eq!(record_reader.num_values(), 14);
    }

    #[test]
    fn test_skip_required_records() {
        // Construct column schema
        let message_type = "
        message test_schema {
          REQUIRED INT32 leaf;
        }
        ";
        let desc = parse_message_type(message_type)
            .map(|t| SchemaDescriptor::new(Arc::new(t)))
            .map(|s| s.column(0))
            .unwrap();

        // Construct record reader
        let mut record_reader = RecordReader::<Int32Type>::new(desc.clone());

        // First page

        // Records data:
        // test_schema
        //   leaf: 4
        // test_schema
        //   leaf: 7
        // test_schema
        //   leaf: 6
        // test_schema
        //   left: 3
        // test_schema
        //   left: 2
        {
            let values = [4, 7, 6, 3, 2];
            let mut pb = DataPageBuilderImpl::new(desc.clone(), 5, true);
            pb.add_values::<Int32Type>(Encoding::PLAIN, &values);
            let page = pb.consume();

            let page_reader = Box::new(InMemoryPageReader::new(vec![page]));
            record_reader.set_page_reader(page_reader).unwrap();
            assert_eq!(2, record_reader.skip_records(2).unwrap());
            assert_eq!(0, record_reader.num_records());
            assert_eq!(0, record_reader.num_values());
            assert_eq!(3, record_reader.read_records(3).unwrap());
            assert_eq!(3, record_reader.num_records());
            assert_eq!(3, record_reader.num_values());
        }

        // Second page

        // Records data:
        // test_schema
        //   leaf: 8
        // test_schema
        //   leaf: 9
        {
            let values = [8, 9];
            let mut pb = DataPageBuilderImpl::new(desc, 2, true);
            pb.add_values::<Int32Type>(Encoding::PLAIN, &values);
            let page = pb.consume();

            let page_reader = Box::new(InMemoryPageReader::new(vec![page]));
            record_reader.set_page_reader(page_reader).unwrap();
            assert_eq!(2, record_reader.skip_records(10).unwrap());
            assert_eq!(3, record_reader.num_records());
            assert_eq!(3, record_reader.num_values());
            assert_eq!(0, record_reader.read_records(10).unwrap());
        }

        let mut bb = Int32BufferBuilder::new(3);
        bb.append_slice(&[6, 3, 2]);
        let expected_buffer = bb.finish();
        assert_eq!(expected_buffer, record_reader.consume_record_data());
        assert_eq!(None, record_reader.consume_def_levels());
        assert_eq!(None, record_reader.consume_bitmap());
    }

    #[test]
    fn test_skip_optional_records() {
        // Construct column schema
        let message_type = "
        message test_schema {
          OPTIONAL Group test_struct {
            OPTIONAL INT32 leaf;
          }
        }
        ";

        let desc = parse_message_type(message_type)
            .map(|t| SchemaDescriptor::new(Arc::new(t)))
            .map(|s| s.column(0))
            .unwrap();

        // Construct record reader
        let mut record_reader = RecordReader::<Int32Type>::new(desc.clone());

        // First page

        // Records data:
        // test_schema
        //   test_struct
        // test_schema
        //   test_struct
        //     leaf: 7
        // test_schema
        // test_schema
        //   test_struct
        //     leaf: 6
        // test_schema
        //   test_struct
        //     leaf: 6
        {
            let values = [7, 6, 3];
            //empty, non-empty, empty, non-empty, non-empty
            let def_levels = [1i16, 2i16, 0i16, 2i16, 2i16];
            let mut pb = DataPageBuilderImpl::new(desc.clone(), 5, true);
            pb.add_def_levels(2, &def_levels);
            pb.add_values::<Int32Type>(Encoding::PLAIN, &values);
            let page = pb.consume();

            let page_reader = Box::new(InMemoryPageReader::new(vec![page]));
            record_reader.set_page_reader(page_reader).unwrap();
            assert_eq!(2, record_reader.skip_records(2).unwrap());
            assert_eq!(0, record_reader.num_records());
            assert_eq!(0, record_reader.num_values());
            assert_eq!(3, record_reader.read_records(3).unwrap());
            assert_eq!(3, record_reader.num_records());
            assert_eq!(3, record_reader.num_values());
        }

        // Second page

        // Records data:
        // test_schema
        // test_schema
        //   test_struct
        //     left: 8
        {
            let values = [8];
            //empty, non-empty
            let def_levels = [0i16, 2i16];
            let mut pb = DataPageBuilderImpl::new(desc, 2, true);
            pb.add_def_levels(2, &def_levels);
            pb.add_values::<Int32Type>(Encoding::PLAIN, &values);
            let page = pb.consume();

            let page_reader = Box::new(InMemoryPageReader::new(vec![page]));
            record_reader.set_page_reader(page_reader).unwrap();
            assert_eq!(2, record_reader.skip_records(10).unwrap());
            assert_eq!(3, record_reader.num_records());
            assert_eq!(3, record_reader.num_values());
            assert_eq!(0, record_reader.read_records(10).unwrap());
        }

        // Verify result def levels
        let mut bb = Int16BufferBuilder::new(7);
        bb.append_slice(&[0i16, 2i16, 2i16]);
        let expected_def_levels = bb.finish();
        assert_eq!(
            Some(expected_def_levels),
            record_reader.consume_def_levels()
        );

        // Verify bitmap
        let expected_valid = &[false, true, true];
        let expected_buffer = Buffer::from_iter(expected_valid.iter().cloned());
        let expected_bitmap = Bitmap::from(expected_buffer);
        assert_eq!(Some(expected_bitmap), record_reader.consume_bitmap());

        // Verify result record data
        let actual = record_reader.consume_record_data();
        let actual_values = actual.typed_data::<i32>();

        let expected = &[0, 6, 3];
        assert_eq!(actual_values.len(), expected.len());

        // Only validate valid values are equal
        let iter = expected_valid.iter().zip(actual_values).zip(expected);
        for ((valid, actual), expected) in iter {
            if *valid {
                assert_eq!(actual, expected)
            }
        }
    }
}
