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

use arrow_buffer::Buffer;

use crate::arrow::record_reader::{
    buffer::ValuesBuffer,
    definition_levels::{DefinitionLevelBuffer, DefinitionLevelBufferDecoder},
};
use crate::column::reader::decoder::RepetitionLevelDecoderImpl;
use crate::column::{
    page::PageReader,
    reader::{
        decoder::{ColumnValueDecoder, ColumnValueDecoderImpl},
        GenericColumnReader,
    },
};
use crate::data_type::DataType;
use crate::errors::{ParquetError, Result};
use crate::schema::types::ColumnDescPtr;

pub(crate) mod buffer;
mod definition_levels;

/// A `RecordReader` is a stateful column reader that delimits semantic records.
pub type RecordReader<T> = GenericRecordReader<Vec<<T as DataType>::T>, ColumnValueDecoderImpl<T>>;

pub(crate) type ColumnReader<CV> =
    GenericColumnReader<RepetitionLevelDecoderImpl, DefinitionLevelBufferDecoder, CV>;

/// A generic stateful column reader that delimits semantic records
///
/// This type is hidden from the docs, and relies on private traits with no
/// public implementations. As such this type signature may be changed without
/// breaking downstream users as it can only be constructed through type aliases
pub struct GenericRecordReader<V, CV> {
    column_desc: ColumnDescPtr,

    values: V,
    def_levels: Option<DefinitionLevelBuffer>,
    rep_levels: Option<Vec<i16>>,
    column_reader: Option<ColumnReader<CV>>,
    /// Number of buffered levels / null-padded values
    num_values: usize,
    /// Number of buffered records
    num_records: usize,
}

impl<V, CV> GenericRecordReader<V, CV>
where
    V: ValuesBuffer,
    CV: ColumnValueDecoder<Buffer = V>,
{
    /// Create a new [`GenericRecordReader`]
    pub fn new(desc: ColumnDescPtr) -> Self {
        let def_levels = (desc.max_def_level() > 0)
            .then(|| DefinitionLevelBuffer::new(&desc, packed_null_mask(&desc)));

        let rep_levels = (desc.max_rep_level() > 0).then(Vec::new);

        Self {
            values: V::default(),
            def_levels,
            rep_levels,
            column_reader: None,
            column_desc: desc,
            num_values: 0,
            num_records: 0,
        }
    }

    /// Set the current page reader.
    pub fn set_page_reader(&mut self, page_reader: Box<dyn PageReader>) -> Result<()> {
        let descr = &self.column_desc;
        let values_decoder = CV::new(descr);

        let def_level_decoder = (descr.max_def_level() != 0).then(|| {
            DefinitionLevelBufferDecoder::new(descr.max_def_level(), packed_null_mask(descr))
        });

        let rep_level_decoder = (descr.max_rep_level() != 0)
            .then(|| RepetitionLevelDecoderImpl::new(descr.max_rep_level()));

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
            let records_to_read = num_records - records_read;
            records_read += self.read_one_batch(records_to_read)?;
            if records_read == num_records || !self.column_reader.as_mut().unwrap().has_next()? {
                break;
            }
        }
        Ok(records_read)
    }

    /// Try to skip the next `num_records` rows
    ///
    /// # Returns
    ///
    /// Number of records skipped
    pub fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        match self.column_reader.as_mut() {
            Some(reader) => reader.skip_records(num_records),
            None => Ok(0),
        }
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
    pub fn consume_def_levels(&mut self) -> Option<Vec<i16>> {
        self.def_levels.as_mut().and_then(|x| x.consume_levels())
    }

    /// Return repetition level data.
    /// The side effect is similar to `consume_def_levels`.
    pub fn consume_rep_levels(&mut self) -> Option<Vec<i16>> {
        self.rep_levels.as_mut().map(std::mem::take)
    }

    /// Returns currently stored buffer data.
    /// The side effect is similar to `consume_def_levels`.
    pub fn consume_record_data(&mut self) -> V {
        std::mem::take(&mut self.values)
    }

    /// Returns currently stored null bitmap data for nullable columns.
    /// For non-nullable columns, the bitmap is discarded.
    /// The side effect is similar to `consume_def_levels`.
    pub fn consume_bitmap_buffer(&mut self) -> Option<Buffer> {
        self.consume_bitmap()
    }

    /// Reset state of record reader.
    /// Should be called after consuming data, e.g. `consume_rep_levels`,
    /// `consume_rep_levels`, `consume_record_data` and `consume_bitmap_buffer`.
    pub fn reset(&mut self) {
        self.num_values = 0;
        self.num_records = 0;
    }

    /// Returns bitmap data for nullable columns.
    /// For non-nullable columns, the bitmap is discarded.
    pub fn consume_bitmap(&mut self) -> Option<Buffer> {
        let mask = self
            .def_levels
            .as_mut()
            .map(|levels| levels.consume_bitmask());

        // While we always consume the bitmask here, we only want to return
        // the bitmask for nullable arrays. (Marking nulls on a non-nullable
        // array may fail validations, even if those nulls are masked off at
        // a higher level.)
        if self.column_desc.self_type().is_optional() {
            mask
        } else {
            None
        }
    }

    /// Try to read one batch of data returning the number of records read
    fn read_one_batch(&mut self, batch_size: usize) -> Result<usize> {
        let (records_read, values_read, levels_read) =
            self.column_reader.as_mut().unwrap().read_records(
                batch_size,
                self.def_levels.as_mut(),
                self.rep_levels.as_mut(),
                &mut self.values,
            )?;

        if values_read < levels_read {
            let def_levels = self.def_levels.as_ref().ok_or_else(|| {
                general_err!("Definition levels should exist when data is less than levels!")
            })?;

            self.values.pad_nulls(
                self.num_values,
                values_read,
                levels_read,
                def_levels.nulls().as_slice(),
            );
        }

        self.num_records += records_read;
        self.num_values += levels_read;
        Ok(records_read)
    }
}

/// Returns true if we do not need to unpack the nullability for this column, this is
/// only possible if the max definition level is 1, and corresponds to nulls at the
/// leaf level, as opposed to a nullable parent nested type
fn packed_null_mask(descr: &ColumnDescPtr) -> bool {
    descr.max_def_level() == 1 && descr.max_rep_level() == 0 && descr.self_type().is_optional()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::buffer::Buffer;

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

        assert_eq!(record_reader.consume_record_data(), &[4, 7, 6, 3, 2, 8, 9]);
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
        assert_eq!(
            Some(vec![1i16, 2i16, 0i16, 2i16, 2i16, 0i16, 2i16]),
            record_reader.consume_def_levels()
        );

        // Verify bitmap
        let expected_valid = &[false, true, false, true, true, false, true];
        let expected_buffer = Buffer::from_iter(expected_valid.iter().cloned());
        assert_eq!(Some(expected_buffer), record_reader.consume_bitmap());

        // Verify result record data
        let actual = record_reader.consume_record_data();

        let expected = &[0, 7, 0, 6, 3, 0, 8];
        assert_eq!(actual.len(), expected.len());

        // Only validate valid values are equal
        let iter = expected_valid.iter().zip(&actual).zip(expected);
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
        assert_eq!(
            Some(vec![2i16, 0i16, 1i16, 2i16, 2i16, 2i16, 2i16, 2i16, 2i16]),
            record_reader.consume_def_levels()
        );

        // Verify bitmap
        let expected_valid = &[true, false, false, true, true, true, true, true, true];
        let expected_buffer = Buffer::from_iter(expected_valid.iter().cloned());
        assert_eq!(Some(expected_buffer), record_reader.consume_bitmap());

        // Verify result record data
        let actual = record_reader.consume_record_data();
        let expected = &[4, 0, 0, 7, 6, 3, 2, 8, 9];
        assert_eq!(actual.len(), expected.len());

        // Only validate valid values are equal
        let iter = expected_valid.iter().zip(&actual).zip(expected);
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

        assert_eq!(record_reader.consume_record_data(), &[6, 3, 2]);
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
        assert_eq!(
            Some(vec![0i16, 2i16, 2i16]),
            record_reader.consume_def_levels()
        );

        // Verify bitmap
        let expected_valid = &[false, true, true];
        let expected_buffer = Buffer::from_iter(expected_valid.iter().cloned());
        assert_eq!(Some(expected_buffer), record_reader.consume_bitmap());

        // Verify result record data
        let actual = record_reader.consume_record_data();

        let expected = &[0, 6, 3];
        assert_eq!(actual.len(), expected.len());

        // Only validate valid values are equal
        let iter = expected_valid.iter().zip(&actual).zip(expected);
        for ((valid, actual), expected) in iter {
            if *valid {
                assert_eq!(actual, expected)
            }
        }
    }
}
