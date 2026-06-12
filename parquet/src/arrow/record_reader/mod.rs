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

use arrow_buffer::{BooleanBufferBuilder, Buffer};

use crate::arrow::record_reader::{
    buffer::ValuesBuffer,
    definition_levels::{DefinitionLevelBuffer, DefinitionLevelBufferDecoder},
};
use crate::column::reader::decoder::RepetitionLevelDecoderImpl;
use crate::column::{
    page::PageReader,
    reader::{
        GenericColumnReader,
        decoder::{ColumnValueDecoder, ColumnValueDecoderImpl},
    },
};
use crate::data_type::DataType;
use crate::errors::{ParquetError, Result};
use crate::schema::types::ColumnDescPtr;

pub(crate) mod buffer;
pub(crate) mod definition_levels;

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

    /// Values buffer, lazily initialized on first read to avoid
    /// allocating a buffer that may never be used (e.g., after the last batch)
    values: Option<V>,
    def_levels: Option<DefinitionLevelBuffer>,
    rep_levels: Option<Vec<i16>>,
    column_reader: Option<ColumnReader<CV>>,
    /// Number of buffered levels / null-padded values
    num_values: usize,
    /// Number of buffered records
    num_records: usize,
    /// Capacity hint for pre-allocating buffers based on batch size
    capacity_hint: usize,
    /// Number of values in the values buffer (may differ from num_values when
    /// padding_threshold is set, since list-level padding is excluded).
    values_written: usize,
    /// When set, `pad_nulls` only pads item-level nulls (def >= threshold)
    /// and skips list-level padding (def < threshold). The values buffer has
    /// `item_count` entries instead of `levels_read`.
    padding_threshold: Option<i16>,
    /// Compact bitmap accumulated during selective padding. Each bit
    /// corresponds to an item-level entry (def >= threshold): set when the
    /// value is real (def >= max_def), unset for item-level nulls. Used both
    /// as the valid_mask for `pad_nulls` (via `as_slice()`) and as the null
    /// bitmap consumed by the leaf reader (via `consume_compact_bitmap`).
    compact_bitmap: Option<BooleanBufferBuilder>,
}

impl<V, CV> GenericRecordReader<V, CV>
where
    V: ValuesBuffer,
    CV: ColumnValueDecoder<Buffer = V>,
{
    /// Create a new [`GenericRecordReader`]
    ///
    /// The capacity is used to pre-allocate internal buffers for full-padding
    /// reads, avoiding reallocations when reading fragmented row selections.
    pub fn new(desc: ColumnDescPtr, capacity: usize) -> Self {
        let def_levels = (desc.max_def_level() > 0)
            .then(|| DefinitionLevelBuffer::new(&desc, packed_null_mask(&desc)));

        let rep_levels = (desc.max_rep_level() > 0).then(Vec::new);

        Self {
            values: None, // Lazily initialized on first read
            def_levels,
            rep_levels,
            column_reader: None,
            column_desc: desc,
            num_values: 0,
            num_records: 0,
            capacity_hint: capacity,
            values_written: 0,
            padding_threshold: None,
            compact_bitmap: None,
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
        // Take the buffer, leaving None. The next read will lazily allocate a new buffer.
        // This avoids allocating a buffer that may never be used (e.g., after the last batch).
        self.values.take().unwrap_or_else(|| V::with_capacity(0))
    }

    /// Reset state of record reader.
    /// Should be called after consuming data, e.g. `consume_rep_levels`,
    /// `consume_rep_levels`, `consume_record_data` and `consume_compact_bitmap`.
    pub fn reset(&mut self) {
        self.num_values = 0;
        self.num_records = 0;
        self.values_written = 0;
        self.compact_bitmap = None;
    }

    /// Returns the maximum definition level for the column being read.
    pub fn max_def_level(&self) -> i16 {
        self.column_desc.max_def_level()
    }

    /// Set the padding threshold. When set, `pad_nulls` only pads entries
    /// where `def >= threshold` (item-level nulls within non-null lists),
    /// skipping list-level padding entries (def < threshold).
    pub fn set_padding_threshold(&mut self, threshold: i16) {
        self.padding_threshold = Some(threshold);
    }

    /// Returns the number of values in the values buffer.
    /// When padding_threshold is None, this equals `num_values` (full padding).
    /// When padding_threshold is set, this is the item_count (selective padding).
    pub fn values_written(&self) -> usize {
        if self.padding_threshold.is_some() {
            self.values_written
        } else {
            self.num_values
        }
    }

    /// Consume the compact null bitmap built during selective padding.
    /// Returns the full bitmap when not using selective padding.
    pub fn consume_compact_bitmap(&mut self) -> Option<Buffer> {
        if self.padding_threshold.is_some() {
            if let Some(levels) = self.def_levels.as_mut() {
                levels.consume_bitmask();
            }
            self.compact_bitmap
                .as_mut()
                .map(|b| b.finish().into_inner())
        } else {
            self.consume_bitmap()
        }
    }

    /// Returns bitmap data for nullable columns.
    /// For non-nullable columns, the bitmap is discarded.
    pub fn consume_bitmap(&mut self) -> Option<Buffer> {
        let mask = self
            .def_levels
            .as_mut()
            .and_then(|levels| levels.consume_bitmask());

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
        if batch_size == 0 {
            return Ok(0);
        }
        // Update capacity hint to the largest batch size seen.
        if batch_size > self.capacity_hint {
            self.capacity_hint = batch_size;
        }

        let padding_threshold = self.padding_threshold;
        let max_def = self.column_desc.max_def_level();
        let mut physical_values_to_read = 0usize;
        let mut output_slots_to_read = 0usize;

        let values = self.values.get_or_insert_with(|| V::with_capacity(0));
        let compact_bitmap = &mut self.compact_bitmap;

        if padding_threshold.is_none() {
            values.reserve_exact(self.capacity_hint.saturating_sub(self.num_values));
        }

        let (records_read, values_read, levels_read) = self
            .column_reader
            .as_mut()
            .unwrap()
            .read_records_with_reservation(
                batch_size,
                self.def_levels.as_mut(),
                self.rep_levels.as_mut(),
                values,
                |values, values_to_read, levels_to_read, def_levels| {
                    let output_slots = if let Some(threshold) = padding_threshold {
                        let def_levels = def_levels.ok_or_else(|| {
                            general_err!(
                                "Definition levels should exist when data is less than levels!"
                            )
                        })?;
                        let all_levels = def_levels.levels().ok_or_else(|| {
                            general_err!(
                                "Raw definition levels must be available for selective padding"
                            )
                        })?;
                        let batch_levels = &all_levels[all_levels.len() - levels_to_read..];
                        let bitmap =
                            compact_bitmap.get_or_insert_with(|| BooleanBufferBuilder::new(0));

                        definition_levels::build_filtered_validity_bitmap(
                            batch_levels,
                            None,
                            Some(threshold),
                            max_def,
                            bitmap,
                        )
                    } else {
                        levels_to_read
                    };

                    let additional = output_slots_to_read
                        .saturating_add(output_slots)
                        .saturating_sub(physical_values_to_read);
                    values.reserve_exact(additional);

                    output_slots_to_read += output_slots;
                    physical_values_to_read += values_to_read;
                    Ok(())
                },
            )?;

        if self.padding_threshold.is_some() {
            debug_assert_eq!(
                physical_values_to_read, values_read,
                "reservation accounting must match decoded values"
            );
            let item_count = output_slots_to_read;
            let bitmap = compact_bitmap.get_or_insert_with(|| BooleanBufferBuilder::new(0));

            // Pad values to item_count positions (only if there are gaps).
            if values_read < item_count {
                values.reserve_exact(item_count - values_read);
                values.pad_nulls(
                    self.values_written,
                    values_read,
                    item_count,
                    bitmap.as_slice(),
                );
            }

            debug_assert_eq!(
                bitmap.len(),
                self.values_written + item_count,
                "compact bitmap length must equal total items written"
            );
            self.values_written += item_count;
        } else if values_read < levels_read {
            debug_assert_eq!(
                output_slots_to_read, levels_read,
                "reservation accounting must match decoded levels"
            );
            // Full padding: pad all null positions.
            let def_levels = self.def_levels.as_ref().ok_or_else(|| {
                general_err!("Definition levels should exist when data is less than levels!")
            })?;

            values.reserve_exact(levels_read - values_read);
            values.pad_nulls(
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

    use crate::arrow::arrow_reader::DEFAULT_BATCH_SIZE;
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
        let mut record_reader = RecordReader::<Int32Type>::new(desc.clone(), DEFAULT_BATCH_SIZE);

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
    fn test_capacity_hint_preserved_across_fragmented_reads() {
        let message_type = "
        message test_schema {
          REQUIRED INT32 leaf;
        }
        ";
        let desc = parse_message_type(message_type)
            .map(|t| SchemaDescriptor::new(Arc::new(t)))
            .map(|s| s.column(0))
            .unwrap();

        let mut record_reader = RecordReader::<Int32Type>::new(desc.clone(), 8);
        let values = [1, 2, 3, 4];
        let mut pb = DataPageBuilderImpl::new(desc, 4, true);
        pb.add_values::<Int32Type>(Encoding::PLAIN, &values);
        let page = pb.consume();

        let page_reader = Box::new(InMemoryPageReader::new(vec![page]));
        record_reader.set_page_reader(page_reader).unwrap();

        assert_eq!(1, record_reader.read_records(1).unwrap());
        assert_eq!(1, record_reader.read_records(1).unwrap());

        let record_data = record_reader.consume_record_data();
        assert_eq!(record_data, &[1, 2]);
        assert!(
            record_data.capacity() >= 8,
            "capacity hint should survive fragmented reads, got {}",
            record_data.capacity()
        );
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
        let mut record_reader = RecordReader::<Int32Type>::new(desc.clone(), DEFAULT_BATCH_SIZE);

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
        let mut record_reader = RecordReader::<Int32Type>::new(desc.clone(), DEFAULT_BATCH_SIZE);

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
    fn test_selective_padding_consumes_full_bitmap() {
        let message_type = "
        message test_schema {
          OPTIONAL GROUP my_list (LIST) {
            REPEATED GROUP list {
              OPTIONAL INT32 element;
            }
          }
        }
        ";

        let desc = parse_message_type(message_type)
            .map(|t| SchemaDescriptor::new(Arc::new(t)))
            .map(|s| s.column(0))
            .unwrap();

        let mut record_reader = RecordReader::<Int32Type>::new(desc.clone(), DEFAULT_BATCH_SIZE);
        record_reader.set_padding_threshold(2);

        let values = [10, 20];
        let def_levels = [3i16, 2i16, 0i16, 2i16, 3i16];
        let rep_levels = [0i16, 1i16, 0i16, 0i16, 1i16];
        let mut pb = DataPageBuilderImpl::new(desc, 5, true);
        pb.add_rep_levels(1, &rep_levels);
        pb.add_def_levels(3, &def_levels);
        pb.add_values::<Int32Type>(Encoding::PLAIN, &values);

        let page_reader = Box::new(InMemoryPageReader::new(vec![pb.consume()]));
        record_reader.set_page_reader(page_reader).unwrap();
        assert_eq!(3, record_reader.read_records(3).unwrap());

        let expected_compact = Buffer::from_iter([true, false, false, true]);
        assert_eq!(
            Some(expected_compact),
            record_reader.consume_compact_bitmap()
        );
        assert_eq!(None, record_reader.consume_bitmap());
    }

    #[test]
    fn test_selective_padding_reserves_compact_capacity() {
        let message_type = "
        message test_schema {
          OPTIONAL GROUP my_list (LIST) {
            REPEATED GROUP list {
              OPTIONAL INT32 element;
            }
          }
        }
        ";

        let desc = parse_message_type(message_type)
            .map(|t| SchemaDescriptor::new(Arc::new(t)))
            .map(|s| s.column(0))
            .unwrap();

        let mut record_reader = RecordReader::<Int32Type>::new(desc.clone(), DEFAULT_BATCH_SIZE);
        record_reader.set_padding_threshold(2);

        let values = [10];
        let mut def_levels = vec![0i16; 30];
        def_levels.extend([3i16, 2i16]);
        let rep_levels = vec![0i16; def_levels.len()];

        let mut pb = DataPageBuilderImpl::new(desc, def_levels.len() as u32, true);
        pb.add_rep_levels(1, &rep_levels);
        pb.add_def_levels(3, &def_levels);
        pb.add_values::<Int32Type>(Encoding::PLAIN, &values);

        let page_reader = Box::new(InMemoryPageReader::new(vec![pb.consume()]));
        record_reader.set_page_reader(page_reader).unwrap();
        assert_eq!(def_levels.len(), record_reader.read_records(128).unwrap());

        let expected_compact = Buffer::from_iter([true, false]);
        assert_eq!(
            Some(expected_compact),
            record_reader.consume_compact_bitmap()
        );

        let actual = record_reader.consume_record_data();
        assert_eq!(actual.len(), 2);
        assert_eq!(actual[0], 10);
        assert!(
            actual.capacity() < def_levels.len(),
            "selective padding should not reserve one child slot per list-level NULL"
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
        let mut record_reader = RecordReader::<Int32Type>::new(desc.clone(), DEFAULT_BATCH_SIZE);

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

        let mut record_reader = RecordReader::<Int32Type>::new(desc, DEFAULT_BATCH_SIZE);
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
        let mut record_reader = RecordReader::<Int32Type>::new(desc.clone(), DEFAULT_BATCH_SIZE);

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
        let mut record_reader = RecordReader::<Int32Type>::new(desc.clone(), DEFAULT_BATCH_SIZE);

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
