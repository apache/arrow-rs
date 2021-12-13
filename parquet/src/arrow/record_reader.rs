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
use std::marker::PhantomData;
use std::mem::replace;
use std::ops::Range;

use crate::arrow::record_reader::private::{
    DefinitionLevels, RecordBuffer, RepetitionLevels,
};
use crate::column::{
    page::PageReader,
    reader::{
        private::{
            ColumnLevelDecoder, ColumnLevelDecoderImpl, ColumnValueDecoder,
            ColumnValueDecoderImpl,
        },
        GenericColumnReader,
    },
};
use crate::data_type::DataType;
use crate::errors::Result;
use crate::schema::types::ColumnDescPtr;
use arrow::array::BooleanBufferBuilder;
use arrow::bitmap::Bitmap;
use arrow::buffer::{Buffer, MutableBuffer};

pub(crate) mod private {
    use super::*;

    pub trait RecordBuffer: Sized + Default {
        type Output: Sized;

        type Writer: ?Sized;

        /// Split out `len` items
        fn split(&mut self, len: usize) -> Self::Output;

        /// Get a writer with `batch_size` capacity
        fn writer(&mut self, batch_size: usize) -> &mut Self::Writer;

        /// Record a write of `len` items
        fn commit(&mut self, len: usize);
    }

    pub trait RepetitionLevels: RecordBuffer {
        /// Inspects the buffered repetition levels in `range` and returns the number of
        /// "complete" records along with the corresponding number of values
        ///
        /// A "complete" record is one where the buffer contains a subsequent repetition level of 0
        fn count_records(
            &self,
            range: Range<usize>,
            max_records: usize,
        ) -> (usize, usize);
    }

    pub trait DefinitionLevels: RecordBuffer {
        /// Update the provided validity mask based on contained levels
        fn update_valid_mask(
            &self,
            valid: &mut BooleanBufferBuilder,
            range: Range<usize>,
            max_level: i16,
        );
    }

    pub struct TypedBuffer<T> {
        buffer: MutableBuffer,

        /// Length in elements of size T
        len: usize,

        /// Placeholder to allow `T` as an invariant generic parameter
        _phantom: PhantomData<*mut T>,
    }

    impl<T> Default for TypedBuffer<T> {
        fn default() -> Self {
            Self {
                buffer: MutableBuffer::new(0),
                len: 0,
                _phantom: Default::default(),
            }
        }
    }

    impl<T> RecordBuffer for TypedBuffer<T> {
        type Output = Buffer;

        type Writer = [T];

        fn split(&mut self, len: usize) -> Self::Output {
            let num_bytes = len * std::mem::size_of::<T>();
            let remaining_bytes = self.buffer.len() - num_bytes;
            // TODO: Optimize to reduce the copy
            // create an empty buffer, as it will be resized below
            let mut remaining = MutableBuffer::new(0);
            remaining.resize(remaining_bytes, 0);

            let new_records = remaining.as_slice_mut();

            new_records[0..remaining_bytes]
                .copy_from_slice(&self.buffer.as_slice()[num_bytes..]);

            self.buffer.resize(num_bytes, 0);

            replace(&mut self.buffer, remaining).into()
        }

        fn writer(&mut self, batch_size: usize) -> &mut Self::Writer {
            self.buffer
                .resize((self.len + batch_size) * std::mem::size_of::<T>(), 0);

            let (prefix, values, suffix) =
                unsafe { self.buffer.as_slice_mut().align_to_mut::<T>() };
            assert!(prefix.is_empty() && suffix.is_empty());

            &mut values[self.len..self.len + batch_size]
        }

        fn commit(&mut self, len: usize) {
            self.len = len;

            let new_bytes = self.len * std::mem::size_of::<T>();
            assert!(new_bytes <= self.buffer.len());
            self.buffer.resize(new_bytes, 0);
        }
    }

    impl RepetitionLevels for TypedBuffer<i16> {
        fn count_records(
            &self,
            range: Range<usize>,
            max_records: usize,
        ) -> (usize, usize) {
            let (prefix, buf, suffix) =
                unsafe { self.buffer.as_slice().align_to::<i16>() };
            assert!(prefix.is_empty() && suffix.is_empty());

            let start = range.start;
            let mut records_read = 0;
            let mut end_of_last_record = start;

            for current in range {
                if buf[current] == 0 && current != start {
                    records_read += 1;
                    end_of_last_record = current;

                    if records_read == max_records {
                        break;
                    }
                }
            }

            (records_read, end_of_last_record - start)
        }
    }

    impl DefinitionLevels for TypedBuffer<i16> {
        fn update_valid_mask(
            &self,
            null_mask: &mut BooleanBufferBuilder,
            range: Range<usize>,
            max_level: i16,
        ) {
            let (prefix, buf, suffix) =
                unsafe { self.buffer.as_slice().align_to::<i16>() };
            assert!(prefix.is_empty() && suffix.is_empty());

            for i in &buf[range] {
                null_mask.append(*i == max_level)
            }
        }
    }
}

const MIN_BATCH_SIZE: usize = 1024;

/// A `RecordReader` is a stateful column reader that delimits semantic records.
pub type RecordReader<T> = GenericRecordReader<
    private::TypedBuffer<i16>,
    private::TypedBuffer<i16>,
    private::TypedBuffer<<T as DataType>::T>,
    ColumnLevelDecoderImpl,
    ColumnLevelDecoderImpl,
    ColumnValueDecoderImpl<T>,
>;

#[doc(hidden)]
pub struct GenericRecordReader<R, D, V, CR, CD, CV> {
    column_desc: ColumnDescPtr,

    records: V,
    def_levels: Option<D>,
    rep_levels: Option<R>,
    null_bitmap: Option<BooleanBufferBuilder>,
    column_reader: Option<GenericColumnReader<CR, CD, CV>>,

    /// Number of records accumulated in records
    num_records: usize,
    /// Number of values `num_records` contains.
    num_values: usize,

    /// Starts from 1, number of values have been written to buffer
    values_written: usize,
}

impl<R, D, V, CR, CD, CV> GenericRecordReader<R, D, V, CR, CD, CV>
where
    R: RepetitionLevels,
    D: DefinitionLevels,
    V: RecordBuffer,
    CR: ColumnLevelDecoder<Writer = R::Writer>,
    CD: ColumnLevelDecoder<Writer = D::Writer>,
    CV: ColumnValueDecoder<Writer = V::Writer>,
{
    pub fn new(column_schema: ColumnDescPtr) -> Self {
        let (def_levels, null_map) = if column_schema.max_def_level() > 0 {
            (Some(Default::default()), Some(BooleanBufferBuilder::new(0)))
        } else {
            (None, None)
        };

        let rep_levels = (column_schema.max_rep_level() > 0).then(Default::default);

        Self {
            records: Default::default(),
            def_levels,
            rep_levels,
            null_bitmap: null_map,
            column_reader: None,
            column_desc: column_schema,
            num_records: 0,
            num_values: 0,
            values_written: 0,
        }
    }

    /// Set the current page reader.
    pub fn set_page_reader(&mut self, page_reader: Box<dyn PageReader>) -> Result<()> {
        self.column_reader = Some(GenericColumnReader::new_null_padding(
            self.column_desc.clone(),
            page_reader,
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

        // Used to mark whether we have reached the end of current
        // column chunk
        let mut end_of_column = false;

        loop {
            // Try to find some records from buffers that has been read into memory
            // but not counted as seen records.
            let (record_count, value_count) =
                self.count_records(num_records - records_read);

            self.num_records += record_count;
            self.num_values += value_count;
            records_read += record_count;

            if records_read == num_records {
                break;
            }

            if end_of_column {
                // Since page reader contains complete records, if we reached end of a
                // page reader, we should reach the end of a record
                if self.rep_levels.is_some() {
                    self.num_records += 1;
                    self.num_values = self.values_written;
                    records_read += 1;
                }
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
    pub fn consume_def_levels(&mut self) -> Result<Option<D::Output>> {
        Ok(match self.def_levels.as_mut() {
            Some(x) => Some(x.split(self.num_values)),
            None => None,
        })
    }

    /// Return repetition level data.
    /// The side effect is similar to `consume_def_levels`.
    pub fn consume_rep_levels(&mut self) -> Result<Option<R::Output>> {
        Ok(match self.rep_levels.as_mut() {
            Some(x) => Some(x.split(self.num_values)),
            None => None,
        })
    }

    /// Returns currently stored buffer data.
    /// The side effect is similar to `consume_def_levels`.
    pub fn consume_record_data(&mut self) -> Result<V::Output> {
        Ok(self.records.split(self.num_values))
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
    }

    /// Returns bitmap data.
    pub fn consume_bitmap(&mut self) -> Result<Option<Bitmap>> {
        self.consume_bitmap_buffer()
            .map(|buffer| buffer.map(Bitmap::from))
    }

    /// Try to read one batch of data.
    fn read_one_batch(&mut self, batch_size: usize) -> Result<usize> {
        let values_written = self.values_written;

        let rep_levels = self
            .rep_levels
            .as_mut()
            .map(|levels| levels.writer(batch_size));

        let def_levels = self
            .def_levels
            .as_mut()
            .map(|levels| levels.writer(batch_size));

        let values = self.records.writer(batch_size);

        let (values_read, levels_read) = self
            .column_reader
            .as_mut()
            .unwrap()
            .read_batch(batch_size, def_levels, rep_levels, values)?;

        if let Some(null_bitmap) = self.null_bitmap.as_mut() {
            let def_levels = self
                .def_levels
                .as_mut()
                .expect("definition levels should exist");

            def_levels.update_valid_mask(
                null_bitmap,
                values_written..values_written + levels_read,
                self.column_desc.max_def_level(),
            )
        }

        let values_read = max(levels_read, values_read);
        self.set_values_written(self.values_written + values_read)?;
        Ok(values_read)
    }

    /// Inspects the buffered repetition levels in the range `self.num_values..self.values_written`
    /// and returns the number of "complete" records along with the corresponding number of values
    ///
    /// A "complete" record is one where the buffer contains a subsequent repetition level of 0
    fn count_records(&self, records_to_read: usize) -> (usize, usize) {
        match self.rep_levels.as_ref() {
            Some(buf) => {
                buf.count_records(self.num_values..self.values_written, records_to_read)
            }
            None => {
                let records_read =
                    min(records_to_read, self.values_written - self.num_values);

                (records_read, records_read)
            }
        }
    }

    #[allow(clippy::unnecessary_wraps)]
    fn set_values_written(&mut self, new_values_written: usize) -> Result<()> {
        self.values_written = new_values_written;
        self.records.commit(self.values_written);

        if let Some(ref mut buf) = self.rep_levels {
            buf.commit(self.values_written)
        };

        if let Some(ref mut buf) = self.def_levels {
            buf.commit(self.values_written)
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
