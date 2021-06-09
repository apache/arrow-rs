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
use std::mem::{replace, size_of};

use crate::column::{page::PageReader, reader::ColumnReaderImpl};
use crate::data_type::DataType;
use crate::errors::{ParquetError, Result};
use crate::schema::types::ColumnDescPtr;
use arrow::array::BooleanBufferBuilder;
use arrow::bitmap::Bitmap;
use arrow::buffer::{Buffer, MutableBuffer};

const MIN_BATCH_SIZE: usize = 1024;

/// A `RecordReader` is a stateful column reader that delimits semantic records.
pub struct RecordReader<T: DataType> {
    column_desc: ColumnDescPtr,

    records: MutableBuffer,
    def_levels: Option<MutableBuffer>,
    rep_levels: Option<MutableBuffer>,
    null_bitmap: Option<BooleanBufferBuilder>,
    column_reader: Option<ColumnReaderImpl<T>>,

    /// Number of records accumulated in records
    num_records: usize,
    /// Number of values `num_records` contains.
    num_values: usize,

    values_seen: usize,
    /// Starts from 1, number of values have been written to buffer
    values_written: usize,
    in_middle_of_record: bool,
}

impl<T: DataType> RecordReader<T> {
    pub fn new(column_schema: ColumnDescPtr) -> Self {
        let (def_levels, null_map) = if column_schema.max_def_level() > 0 {
            (
                Some(MutableBuffer::new(MIN_BATCH_SIZE)),
                Some(BooleanBufferBuilder::new(0)),
            )
        } else {
            (None, None)
        };

        let rep_levels = if column_schema.max_rep_level() > 0 {
            Some(MutableBuffer::new(MIN_BATCH_SIZE))
        } else {
            None
        };

        Self {
            records: MutableBuffer::new(MIN_BATCH_SIZE),
            def_levels,
            rep_levels,
            null_bitmap: null_map,
            column_reader: None,
            column_desc: column_schema,
            num_records: 0,
            num_values: 0,
            values_seen: 0,
            values_written: 0,
            in_middle_of_record: false,
        }
    }

    /// Set the current page reader.
    pub fn set_page_reader(&mut self, page_reader: Box<dyn PageReader>) -> Result<()> {
        self.column_reader =
            Some(ColumnReaderImpl::new(self.column_desc.clone(), page_reader));
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

        // Used to mark whether we have reached the end of current
        // column chunk
        let mut end_of_column = false;

        loop {
            // Try to find some records from buffers that has been read into memory
            // but not counted as seen records.
            records_read += self.split_records(num_records - records_read)?;

            // Since page reader contains complete records, so if we reached end of a
            // page reader, we should reach the end of a record
            if end_of_column
                && self.values_seen >= self.values_written
                && self.in_middle_of_record
            {
                self.num_records += 1;
                self.num_values = self.values_seen;
                self.in_middle_of_record = false;
                records_read += 1;
            }

            if (records_read >= num_records) || end_of_column {
                break;
            }

            let batch_size = max(num_records - records_read, MIN_BATCH_SIZE);

            // Try to more value from parquet pages
            let values_read = self.read_one_batch(batch_size)?;
            if values_read < batch_size {
                end_of_column = true;
            }
        }

        Ok(records_read)
    }

    /// Returns number of records stored in buffer.
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
    pub fn consume_def_levels(&mut self) -> Result<Option<Buffer>> {
        let new_buffer = if let Some(ref mut def_levels_buf) = &mut self.def_levels {
            let num_left_values = self.values_written - self.num_values;
            // create an empty buffer, as it will be resized below
            let mut new_buffer = MutableBuffer::new(0);
            let num_bytes = num_left_values * size_of::<i16>();
            let new_len = self.num_values * size_of::<i16>();

            new_buffer.resize(num_bytes, 0);

            let new_def_levels = new_buffer.as_slice_mut();
            let left_def_levels = &def_levels_buf.as_slice_mut()[new_len..];

            new_def_levels[0..num_bytes].copy_from_slice(&left_def_levels[0..num_bytes]);

            def_levels_buf.resize(new_len, 0);
            Some(new_buffer)
        } else {
            None
        };

        Ok(replace(&mut self.def_levels, new_buffer).map(|x| x.into()))
    }

    /// Return repetition level data.
    /// The side effect is similar to `consume_def_levels`.
    pub fn consume_rep_levels(&mut self) -> Result<Option<Buffer>> {
        // TODO: Optimize to reduce the copy
        let new_buffer = if let Some(ref mut rep_levels_buf) = &mut self.rep_levels {
            let num_left_values = self.values_written - self.num_values;
            // create an empty buffer, as it will be resized below
            let mut new_buffer = MutableBuffer::new(0);
            let num_bytes = num_left_values * size_of::<i16>();
            let new_len = self.num_values * size_of::<i16>();

            new_buffer.resize(num_bytes, 0);

            let new_rep_levels = new_buffer.as_slice_mut();
            let left_rep_levels = &rep_levels_buf.as_slice_mut()[new_len..];

            new_rep_levels[0..num_bytes].copy_from_slice(&left_rep_levels[0..num_bytes]);

            rep_levels_buf.resize(new_len, 0);

            Some(new_buffer)
        } else {
            None
        };

        Ok(replace(&mut self.rep_levels, new_buffer).map(|x| x.into()))
    }

    /// Returns currently stored buffer data.
    /// The side effect is similar to `consume_def_levels`.
    pub fn consume_record_data(&mut self) -> Result<Buffer> {
        // TODO: Optimize to reduce the copy
        let num_left_values = self.values_written - self.num_values;
        // create an empty buffer, as it will be resized below
        let mut new_buffer = MutableBuffer::new(0);
        let num_bytes = num_left_values * T::get_type_size();
        let new_len = self.num_values * T::get_type_size();

        new_buffer.resize(num_bytes, 0);

        let new_records = new_buffer.as_slice_mut();
        let left_records = &mut self.records.as_slice_mut()[new_len..];

        new_records[0..num_bytes].copy_from_slice(&left_records[0..num_bytes]);

        self.records.resize(new_len, 0);

        Ok(replace(&mut self.records, new_buffer).into())
    }

    /// Returns currently stored null bitmap data.
    /// The side effect is similar to `consume_def_levels`.
    pub fn consume_bitmap_buffer(&mut self) -> Result<Option<Buffer>> {
        // TODO: Optimize to reduce the copy
        if self.column_desc.max_def_level() > 0 {
            assert!(self.null_bitmap.is_some());
            let num_left_values = self.values_written - self.num_values;
            let new_bitmap_builder = Some(BooleanBufferBuilder::new(max(
                MIN_BATCH_SIZE,
                num_left_values,
            )));

            let old_bitmap = replace(&mut self.null_bitmap, new_bitmap_builder)
                .map(|mut builder| builder.finish())
                .unwrap();

            let old_bitmap = Bitmap::from(old_bitmap);

            for i in self.num_values..self.values_written {
                self.null_bitmap
                    .as_mut()
                    .unwrap()
                    .append(old_bitmap.is_set(i));
            }

            Ok(Some(old_bitmap.into_buffer()))
        } else {
            Ok(None)
        }
    }

    /// Reset state of record reader.
    /// Should be called after consuming data, e.g. `consume_rep_levels`,
    /// `consume_rep_levels`, `consume_record_data` and `consume_bitmap_buffer`.
    pub fn reset(&mut self) {
        self.values_written -= self.num_values;
        self.num_records = 0;
        self.num_values = 0;
        self.values_seen = 0;
        self.in_middle_of_record = false;
    }

    /// Returns bitmap data.
    pub fn consume_bitmap(&mut self) -> Result<Option<Bitmap>> {
        self.consume_bitmap_buffer()
            .map(|buffer| buffer.map(Bitmap::from))
    }

    /// Try to read one batch of data.
    fn read_one_batch(&mut self, batch_size: usize) -> Result<usize> {
        // Reserve spaces
        self.records
            .resize(self.records.len() + batch_size * T::get_type_size(), 0);
        if let Some(ref mut buf) = self.rep_levels {
            buf.resize(buf.len() + batch_size * size_of::<i16>(), 0);
        }
        if let Some(ref mut buf) = self.def_levels {
            buf.resize(buf.len() + batch_size * size_of::<i16>(), 0);
        }

        let values_written = self.values_written;

        // Convert mutable buffer spaces to mutable slices
        let (prefix, values, suffix) =
            unsafe { self.records.as_slice_mut().align_to_mut::<T::T>() };
        assert!(prefix.is_empty() && suffix.is_empty());
        let values = &mut values[values_written..];

        let def_levels = self.def_levels.as_mut().map(|buf| {
            let (prefix, def_levels, suffix) =
                unsafe { buf.as_slice_mut().align_to_mut::<i16>() };
            assert!(prefix.is_empty() && suffix.is_empty());
            &mut def_levels[values_written..]
        });

        let rep_levels = self.rep_levels.as_mut().map(|buf| {
            let (prefix, rep_levels, suffix) =
                unsafe { buf.as_slice_mut().align_to_mut::<i16>() };
            assert!(prefix.is_empty() && suffix.is_empty());
            &mut rep_levels[values_written..]
        });

        let (values_read, levels_read) = self
            .column_reader
            .as_mut()
            .unwrap()
            .read_batch(batch_size, def_levels, rep_levels, values)?;

        // get new references for the def levels.
        let def_levels = self.def_levels.as_ref().map(|buf| {
            let (prefix, def_levels, suffix) =
                unsafe { buf.as_slice().align_to::<i16>() };
            assert!(prefix.is_empty() && suffix.is_empty());
            &def_levels[values_written..]
        });

        let max_def_level = self.column_desc.max_def_level();

        if values_read < levels_read {
            let def_levels = def_levels.ok_or_else(|| {
                general_err!(
                    "Definition levels should exist when data is less than levels!"
                )
            })?;

            // Fill spaces in column data with default values
            let mut values_pos = values_read;
            let mut level_pos = levels_read;

            while level_pos > values_pos {
                if def_levels[level_pos - 1] == max_def_level {
                    // This values is not empty
                    // We use swap rather than assign here because T::T doesn't
                    // implement Copy
                    values.swap(level_pos - 1, values_pos - 1);
                    values_pos -= 1;
                } else {
                    values[level_pos - 1] = T::T::default();
                }

                level_pos -= 1;
            }
        }

        // Fill in bitmap data
        if let Some(null_buffer) = self.null_bitmap.as_mut() {
            let def_levels = def_levels.ok_or_else(|| {
                general_err!(
                    "Definition levels should exist when data is less than levels!"
                )
            })?;
            (0..levels_read)
                .for_each(|idx| null_buffer.append(def_levels[idx] == max_def_level));
        }

        let values_read = max(values_read, levels_read);
        self.set_values_written(self.values_written + values_read)?;
        Ok(values_read)
    }

    /// Split values into records according repetition definition and returns number of
    /// records read.
    #[allow(clippy::unnecessary_wraps)]
    fn split_records(&mut self, records_to_read: usize) -> Result<usize> {
        let rep_levels = self.rep_levels.as_ref().map(|buf| {
            let (prefix, rep_levels, suffix) =
                unsafe { buf.as_slice().align_to::<i16>() };
            assert!(prefix.is_empty() && suffix.is_empty());
            rep_levels
        });

        match rep_levels {
            Some(buf) => {
                let mut records_read = 0;

                while (self.values_seen < self.values_written)
                    && (records_read < records_to_read)
                {
                    if buf[self.values_seen] == 0 {
                        if self.in_middle_of_record {
                            records_read += 1;
                            self.num_records += 1;
                            self.num_values = self.values_seen;
                        }
                        self.in_middle_of_record = true;
                    }
                    self.values_seen += 1;
                }

                Ok(records_read)
            }
            None => {
                let records_read =
                    min(records_to_read, self.values_written - self.values_seen);
                self.num_records += records_read;
                self.num_values += records_read;
                self.values_seen += records_read;
                self.in_middle_of_record = false;

                Ok(records_read)
            }
        }
    }

    #[allow(clippy::unnecessary_wraps)]
    fn set_values_written(&mut self, new_values_written: usize) -> Result<()> {
        self.values_written = new_values_written;
        self.records
            .resize(self.values_written * T::get_type_size(), 0);

        let new_levels_len = self.values_written * size_of::<i16>();

        if let Some(ref mut buf) = self.rep_levels {
            buf.resize(new_levels_len, 0)
        };

        if let Some(ref mut buf) = self.def_levels {
            buf.resize(new_levels_len, 0)
        };

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::RecordReader;
    use crate::basic::Encoding;
    use crate::column::page::Page;
    use crate::column::page::PageReader;
    use crate::data_type::Int32Type;
    use crate::errors::Result;
    use crate::schema::parser::parse_message_type;
    use crate::schema::types::SchemaDescriptor;
    use crate::util::test_common::page_util::{DataPageBuilder, DataPageBuilderImpl};
    use arrow::array::{BooleanBufferBuilder, Int16BufferBuilder, Int32BufferBuilder};
    use arrow::bitmap::Bitmap;
    use std::sync::Arc;

    struct TestPageReader {
        pages: Box<dyn Iterator<Item = Page>>,
    }

    impl TestPageReader {
        pub fn new(pages: Vec<Page>) -> Self {
            Self {
                pages: Box::new(pages.into_iter()),
            }
        }
    }

    impl PageReader for TestPageReader {
        fn get_next_page(&mut self) -> Result<Option<Page>> {
            Ok(self.pages.next())
        }
    }

    impl Iterator for TestPageReader {
        type Item = Result<Page>;

        fn next(&mut self) -> Option<Self::Item> {
            self.get_next_page().transpose()
        }
    }

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

            let page_reader = Box::new(TestPageReader::new(vec![page]));
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

            let page_reader = Box::new(TestPageReader::new(vec![page]));
            record_reader.set_page_reader(page_reader).unwrap();
            assert_eq!(2, record_reader.read_records(10).unwrap());
            assert_eq!(7, record_reader.num_records());
            assert_eq!(7, record_reader.num_values());
        }

        let mut bb = Int32BufferBuilder::new(7);
        bb.append_slice(&[4, 7, 6, 3, 2, 8, 9]);
        let expected_buffer = bb.finish();
        assert_eq!(
            expected_buffer,
            record_reader.consume_record_data().unwrap()
        );
        assert_eq!(None, record_reader.consume_def_levels().unwrap());
        assert_eq!(None, record_reader.consume_bitmap().unwrap());
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

            let page_reader = Box::new(TestPageReader::new(vec![page]));
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

            let page_reader = Box::new(TestPageReader::new(vec![page]));
            record_reader.set_page_reader(page_reader).unwrap();
            assert_eq!(2, record_reader.read_records(10).unwrap());
            assert_eq!(7, record_reader.num_records());
            assert_eq!(7, record_reader.num_values());
        }

        // Verify result record data
        let mut bb = Int32BufferBuilder::new(7);
        bb.append_slice(&[0, 7, 0, 6, 3, 0, 8]);
        let expected_buffer = bb.finish();
        assert_eq!(
            expected_buffer,
            record_reader.consume_record_data().unwrap()
        );

        // Verify result def levels
        let mut bb = Int16BufferBuilder::new(7);
        bb.append_slice(&[1i16, 2i16, 0i16, 2i16, 2i16, 0i16, 2i16]);
        let expected_def_levels = bb.finish();
        assert_eq!(
            Some(expected_def_levels),
            record_reader.consume_def_levels().unwrap()
        );

        // Verify bitmap
        let mut bb = BooleanBufferBuilder::new(7);
        bb.append_slice(&[false, true, false, true, true, false, true]);
        let expected_bitmap = Bitmap::from(bb.finish());
        assert_eq!(
            Some(expected_bitmap),
            record_reader.consume_bitmap().unwrap()
        );
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

            let page_reader = Box::new(TestPageReader::new(vec![page]));
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

            let page_reader = Box::new(TestPageReader::new(vec![page]));
            record_reader.set_page_reader(page_reader).unwrap();

            assert_eq!(1, record_reader.read_records(10).unwrap());
            assert_eq!(4, record_reader.num_records());
            assert_eq!(9, record_reader.num_values());
        }

        // Verify result record data
        let mut bb = Int32BufferBuilder::new(9);
        bb.append_slice(&[4, 0, 0, 7, 6, 3, 2, 8, 9]);
        let expected_buffer = bb.finish();
        assert_eq!(
            expected_buffer,
            record_reader.consume_record_data().unwrap()
        );

        // Verify result def levels
        let mut bb = Int16BufferBuilder::new(9);
        bb.append_slice(&[2i16, 0i16, 1i16, 2i16, 2i16, 2i16, 2i16, 2i16, 2i16]);
        let expected_def_levels = bb.finish();
        assert_eq!(
            Some(expected_def_levels),
            record_reader.consume_def_levels().unwrap()
        );

        // Verify bitmap
        let mut bb = BooleanBufferBuilder::new(9);
        bb.append_slice(&[true, false, false, true, true, true, true, true, true]);
        let expected_bitmap = Bitmap::from(bb.finish());
        assert_eq!(
            Some(expected_bitmap),
            record_reader.consume_bitmap().unwrap()
        );
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

            let page_reader = Box::new(TestPageReader::new(vec![page]));
            record_reader.set_page_reader(page_reader).unwrap();

            assert_eq!(1000, record_reader.read_records(1000).unwrap());
            assert_eq!(1000, record_reader.num_records());
            assert_eq!(5000, record_reader.num_values());
        }
    }
}
