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
use crate::arrow::buffer::converter::Converter;
use crate::arrow::schema::parquet_to_arrow_field;
use crate::column::page::PageIterator;
use crate::column::reader::ColumnReaderImpl;
use crate::data_type::DataType;
use crate::errors::Result;
use crate::schema::types::ColumnDescPtr;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType as ArrowType;
use std::any::Any;
use std::marker::PhantomData;

/// Primitive array readers are leaves of array reader tree. They accept page iterator
/// and read them into primitive arrays.
pub struct ComplexObjectArrayReader<T, C>
where
    T: DataType,
    C: Converter<Vec<Option<T::T>>, ArrayRef> + 'static,
{
    data_type: ArrowType,
    pages: Box<dyn PageIterator>,
    def_levels_buffer: Option<Vec<i16>>,
    rep_levels_buffer: Option<Vec<i16>>,
    data_buffer: Vec<T::T>,
    column_desc: ColumnDescPtr,
    column_reader: Option<ColumnReaderImpl<T>>,
    converter: C,
    in_progress_def_levels_buffer: Option<Vec<i16>>,
    in_progress_rep_levels_buffer: Option<Vec<i16>>,
    before_consume: bool,
    _parquet_type_marker: PhantomData<T>,
    _converter_marker: PhantomData<C>,
}

impl<T, C> ArrayReader for ComplexObjectArrayReader<T, C>
where
    T: DataType,
    C: Converter<Vec<Option<T::T>>, ArrayRef> + Send + 'static,
{
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        if !self.before_consume {
            self.before_consume = true;
        }
        // Try to initialize column reader
        if self.column_reader.is_none() {
            self.next_column_reader()?;
        }

        let mut data_buffer: Vec<T::T> = Vec::with_capacity(batch_size);
        data_buffer.resize_with(batch_size, T::T::default);

        let mut def_levels_buffer = if self.column_desc.max_def_level() > 0 {
            let mut buf: Vec<i16> = Vec::with_capacity(batch_size);
            buf.resize_with(batch_size, || 0);
            Some(buf)
        } else {
            None
        };

        let mut rep_levels_buffer = if self.column_desc.max_rep_level() > 0 {
            let mut buf: Vec<i16> = Vec::with_capacity(batch_size);
            buf.resize_with(batch_size, || 0);
            Some(buf)
        } else {
            None
        };

        let mut num_read = 0;

        while self.column_reader.is_some() && num_read < batch_size {
            let num_to_read = batch_size - num_read;
            let cur_data_buf = &mut data_buffer[num_read..];
            let cur_def_levels_buf =
                def_levels_buffer.as_mut().map(|b| &mut b[num_read..]);
            let cur_rep_levels_buf =
                rep_levels_buffer.as_mut().map(|b| &mut b[num_read..]);
            let (data_read, levels_read) =
                self.column_reader.as_mut().unwrap().read_batch(
                    num_to_read,
                    cur_def_levels_buf,
                    cur_rep_levels_buf,
                    cur_data_buf,
                )?;

            // Fill space
            if levels_read > data_read {
                def_levels_buffer.iter().for_each(|def_levels_buffer| {
                    let (mut level_pos, mut data_pos) = (levels_read, data_read);
                    while level_pos > 0 && data_pos > 0 {
                        if def_levels_buffer[num_read + level_pos - 1]
                            == self.column_desc.max_def_level()
                        {
                            cur_data_buf.swap(level_pos - 1, data_pos - 1);
                            level_pos -= 1;
                            data_pos -= 1;
                        } else {
                            level_pos -= 1;
                        }
                    }
                });
            }

            let values_read = levels_read.max(data_read);
            num_read += values_read;
            // current page exhausted && page iterator exhausted
            if values_read < num_to_read && !self.next_column_reader()? {
                break;
            }
        }
        data_buffer.truncate(num_read);
        def_levels_buffer
            .iter_mut()
            .for_each(|buf| buf.truncate(num_read));
        rep_levels_buffer
            .iter_mut()
            .for_each(|buf| buf.truncate(num_read));

        if let Some(mut def_levels_buffer) = def_levels_buffer {
            match &mut self.in_progress_def_levels_buffer {
                None => {
                    self.in_progress_def_levels_buffer = Some(def_levels_buffer);
                }
                Some(buf) => buf.append(&mut def_levels_buffer),
            }
        }

        if let Some(mut rep_levels_buffer) = rep_levels_buffer {
            match &mut self.in_progress_rep_levels_buffer {
                None => {
                    self.in_progress_rep_levels_buffer = Some(rep_levels_buffer);
                }
                Some(buf) => buf.append(&mut rep_levels_buffer),
            }
        }

        self.data_buffer.append(&mut data_buffer);

        Ok(num_read)
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        let data: Vec<Option<T::T>> = if self.in_progress_def_levels_buffer.is_some() {
            let data_buffer = std::mem::take(&mut self.data_buffer);
            data_buffer
                .into_iter()
                .zip(self.in_progress_def_levels_buffer.as_ref().unwrap().iter())
                .map(|(t, def_level)| {
                    if *def_level == self.column_desc.max_def_level() {
                        Some(t)
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            self.data_buffer.iter().map(|x| Some(x.clone())).collect()
        };

        let mut array = self.converter.convert(data)?;

        if let ArrowType::Dictionary(_, _) = self.data_type {
            array = arrow::compute::cast(&array, &self.data_type)?;
        }

        self.data_buffer = vec![];
        self.def_levels_buffer = std::mem::take(&mut self.in_progress_def_levels_buffer);
        self.rep_levels_buffer = std::mem::take(&mut self.in_progress_rep_levels_buffer);
        self.before_consume = false;

        Ok(array)
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        let mut num_read = 0;
        while (self.column_reader.is_some() || self.next_column_reader()?)
            && num_read < num_records
        {
            let remain_to_skip = num_records - num_read;
            let skip = self
                .column_reader
                .as_mut()
                .unwrap()
                .skip_records(remain_to_skip)?;
            num_read += skip;
            //  skip < remain_to_skip means end of row group
            //  self.next_column_reader() == false means end of file
            if skip < remain_to_skip && !self.next_column_reader()? {
                break;
            }
        }
        Ok(num_read)
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        if self.before_consume {
            self.in_progress_def_levels_buffer.as_deref()
        } else {
            self.def_levels_buffer.as_deref()
        }
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        if self.before_consume {
            self.in_progress_rep_levels_buffer.as_deref()
        } else {
            self.rep_levels_buffer.as_deref()
        }
    }
}

impl<T, C> ComplexObjectArrayReader<T, C>
where
    T: DataType,
    C: Converter<Vec<Option<T::T>>, ArrayRef> + 'static,
{
    pub fn new(
        pages: Box<dyn PageIterator>,
        column_desc: ColumnDescPtr,
        converter: C,
        arrow_type: Option<ArrowType>,
    ) -> Result<Self> {
        let data_type = match arrow_type {
            Some(t) => t,
            None => parquet_to_arrow_field(column_desc.as_ref())?
                .data_type()
                .clone(),
        };

        Ok(Self {
            data_type,
            pages,
            def_levels_buffer: None,
            rep_levels_buffer: None,
            data_buffer: vec![],
            column_desc,
            column_reader: None,
            converter,
            in_progress_def_levels_buffer: None,
            in_progress_rep_levels_buffer: None,
            before_consume: true,
            _parquet_type_marker: PhantomData,
            _converter_marker: PhantomData,
        })
    }

    fn next_column_reader(&mut self) -> Result<bool> {
        Ok(match self.pages.next() {
            Some(page) => {
                self.column_reader =
                    Some(ColumnReaderImpl::<T>::new(self.column_desc.clone(), page?));
                true
            }
            None => false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::arrow::buffer::converter::{Utf8ArrayConverter, Utf8Converter};
    use crate::basic::Encoding;
    use crate::column::page::Page;
    use crate::data_type::{ByteArray, ByteArrayType};
    use crate::schema::parser::parse_message_type;
    use crate::schema::types::SchemaDescriptor;
    use crate::util::{DataPageBuilder, DataPageBuilderImpl, InMemoryPageIterator};
    use arrow::array::StringArray;
    use rand::{thread_rng, Rng};
    use std::sync::Arc;

    #[test]
    fn test_complex_array_reader_no_pages() {
        let message_type = "
        message test_schema {
            REPEATED Group test_mid {
                OPTIONAL BYTE_ARRAY leaf (UTF8);
            }
        }
        ";
        let schema = parse_message_type(message_type)
            .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
            .unwrap();
        let column_desc = schema.column(0);
        let pages: Vec<Vec<Page>> = Vec::new();
        let page_iterator = InMemoryPageIterator::new(schema, column_desc.clone(), pages);

        let converter = Utf8Converter::new(Utf8ArrayConverter {});
        let mut array_reader =
            ComplexObjectArrayReader::<ByteArrayType, Utf8Converter>::new(
                Box::new(page_iterator),
                column_desc,
                converter,
                None,
            )
            .unwrap();

        let values_per_page = 100; // this value is arbitrary in this test - the result should always be an array of 0 length
        let array = array_reader.next_batch(values_per_page).unwrap();
        assert_eq!(array.len(), 0);
    }

    #[test]
    fn test_complex_array_reader_def_and_rep_levels() {
        // Construct column schema
        let message_type = "
        message test_schema {
            REPEATED Group test_mid {
                OPTIONAL BYTE_ARRAY leaf (UTF8);
            }
        }
        ";
        let num_pages = 2;
        let values_per_page = 100;
        let str_base = "Hello World";

        let schema = parse_message_type(message_type)
            .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
            .unwrap();

        let max_def_level = schema.column(0).max_def_level();
        let max_rep_level = schema.column(0).max_rep_level();

        assert_eq!(max_def_level, 2);
        assert_eq!(max_rep_level, 1);

        let mut rng = thread_rng();
        let column_desc = schema.column(0);
        let mut pages: Vec<Vec<Page>> = Vec::new();

        let mut rep_levels = Vec::with_capacity(num_pages * values_per_page);
        let mut def_levels = Vec::with_capacity(num_pages * values_per_page);
        let mut all_values = Vec::with_capacity(num_pages * values_per_page);

        for i in 0..num_pages {
            let mut values = Vec::with_capacity(values_per_page);

            for _ in 0..values_per_page {
                let def_level = rng.gen_range(0..max_def_level + 1);
                let rep_level = rng.gen_range(0..max_rep_level + 1);
                if def_level == max_def_level {
                    let len = rng.gen_range(1..str_base.len());
                    let slice = &str_base[..len];
                    values.push(ByteArray::from(slice));
                    all_values.push(Some(slice.to_string()));
                } else {
                    all_values.push(None)
                }
                rep_levels.push(rep_level);
                def_levels.push(def_level)
            }

            let range = i * values_per_page..(i + 1) * values_per_page;
            let mut pb =
                DataPageBuilderImpl::new(column_desc.clone(), values.len() as u32, true);

            pb.add_rep_levels(max_rep_level, &rep_levels.as_slice()[range.clone()]);
            pb.add_def_levels(max_def_level, &def_levels.as_slice()[range]);
            pb.add_values::<ByteArrayType>(Encoding::PLAIN, values.as_slice());

            let data_page = pb.consume();
            pages.push(vec![data_page]);
        }

        let page_iterator = InMemoryPageIterator::new(schema, column_desc.clone(), pages);

        let converter = Utf8Converter::new(Utf8ArrayConverter {});
        let mut array_reader =
            ComplexObjectArrayReader::<ByteArrayType, Utf8Converter>::new(
                Box::new(page_iterator),
                column_desc,
                converter,
                None,
            )
            .unwrap();

        let mut accu_len: usize = 0;

        let len = array_reader.read_records(values_per_page / 2).unwrap();
        assert_eq!(len, values_per_page / 2);
        assert_eq!(
            Some(&def_levels[accu_len..(accu_len + len)]),
            array_reader.get_def_levels()
        );
        assert_eq!(
            Some(&rep_levels[accu_len..(accu_len + len)]),
            array_reader.get_rep_levels()
        );
        accu_len += len;
        array_reader.consume_batch().unwrap();

        // Read next values_per_page values, the first values_per_page/2 ones are from the first column chunk,
        // and the last values_per_page/2 ones are from the second column chunk
        let len = array_reader.read_records(values_per_page).unwrap();
        assert_eq!(len, values_per_page);
        assert_eq!(
            Some(&def_levels[accu_len..(accu_len + len)]),
            array_reader.get_def_levels()
        );
        assert_eq!(
            Some(&rep_levels[accu_len..(accu_len + len)]),
            array_reader.get_rep_levels()
        );
        let array = array_reader.consume_batch().unwrap();
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..array.len() {
            if array.is_valid(i) {
                assert_eq!(
                    all_values[i + accu_len].as_ref().unwrap().as_str(),
                    strings.value(i)
                )
            } else {
                assert_eq!(all_values[i + accu_len], None)
            }
        }
        accu_len += len;

        // Try to read values_per_page values, however there are only values_per_page/2 values
        let len = array_reader.read_records(values_per_page).unwrap();
        assert_eq!(len, values_per_page / 2);
        assert_eq!(
            Some(&def_levels[accu_len..(accu_len + len)]),
            array_reader.get_def_levels()
        );
        assert_eq!(
            Some(&rep_levels[accu_len..(accu_len + len)]),
            array_reader.get_rep_levels()
        );
        array_reader.consume_batch().unwrap();
    }

    #[test]
    fn test_complex_array_reader_dict_enc_string() {
        use crate::encodings::encoding::{DictEncoder, Encoder};
        // Construct column schema
        let message_type = "
        message test_schema {
            REPEATED Group test_mid {
                OPTIONAL BYTE_ARRAY leaf (UTF8);
            }
        }
        ";
        let num_pages = 2;
        let values_per_page = 100;
        let str_base = "Hello World";

        let schema = parse_message_type(message_type)
            .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
            .unwrap();
        let column_desc = schema.column(0);
        let max_def_level = column_desc.max_def_level();
        let max_rep_level = column_desc.max_rep_level();

        assert_eq!(max_def_level, 2);
        assert_eq!(max_rep_level, 1);

        let mut rng = thread_rng();
        let mut pages: Vec<Vec<Page>> = Vec::new();

        let mut rep_levels = Vec::with_capacity(num_pages * values_per_page);
        let mut def_levels = Vec::with_capacity(num_pages * values_per_page);
        let mut all_values = Vec::with_capacity(num_pages * values_per_page);

        for i in 0..num_pages {
            let mut dict_encoder = DictEncoder::<ByteArrayType>::new(column_desc.clone());
            // add data page
            let mut values = Vec::with_capacity(values_per_page);

            for _ in 0..values_per_page {
                let def_level = rng.gen_range(0..max_def_level + 1);
                let rep_level = rng.gen_range(0..max_rep_level + 1);
                if def_level == max_def_level {
                    let len = rng.gen_range(1..str_base.len());
                    let slice = &str_base[..len];
                    values.push(ByteArray::from(slice));
                    all_values.push(Some(slice.to_string()));
                } else {
                    all_values.push(None)
                }
                rep_levels.push(rep_level);
                def_levels.push(def_level)
            }

            let range = i * values_per_page..(i + 1) * values_per_page;
            let mut pb =
                DataPageBuilderImpl::new(column_desc.clone(), values.len() as u32, true);
            pb.add_rep_levels(max_rep_level, &rep_levels.as_slice()[range.clone()]);
            pb.add_def_levels(max_def_level, &def_levels.as_slice()[range]);
            let _ = dict_encoder.put(&values);
            let indices = dict_encoder
                .write_indices()
                .expect("write_indices() should be OK");
            pb.add_indices(indices);
            let data_page = pb.consume();
            // for each page log num_values vs actual values in page
            // println!("page num_values: {}, values.len(): {}", data_page.num_values(), values.len());
            // add dictionary page
            let dict = dict_encoder
                .write_dict()
                .expect("write_dict() should be OK");
            let dict_page = Page::DictionaryPage {
                buf: dict,
                num_values: dict_encoder.num_entries() as u32,
                encoding: Encoding::RLE_DICTIONARY,
                is_sorted: false,
            };
            pages.push(vec![dict_page, data_page]);
        }

        let page_iterator = InMemoryPageIterator::new(schema, column_desc.clone(), pages);
        let converter = Utf8Converter::new(Utf8ArrayConverter {});
        let mut array_reader =
            ComplexObjectArrayReader::<ByteArrayType, Utf8Converter>::new(
                Box::new(page_iterator),
                column_desc,
                converter,
                None,
            )
            .unwrap();

        let mut accu_len: usize = 0;

        // println!("---------- reading a batch of {} values ----------", values_per_page / 2);
        let len = array_reader.read_records(values_per_page / 2).unwrap();
        assert_eq!(len, values_per_page / 2);
        assert_eq!(
            Some(&def_levels[accu_len..(accu_len + len)]),
            array_reader.get_def_levels()
        );
        assert_eq!(
            Some(&rep_levels[accu_len..(accu_len + len)]),
            array_reader.get_rep_levels()
        );
        accu_len += len;
        array_reader.consume_batch().unwrap();

        // Read next values_per_page values, the first values_per_page/2 ones are from the first column chunk,
        // and the last values_per_page/2 ones are from the second column chunk
        // println!("---------- reading a batch of {} values ----------", values_per_page);
        //let array = array_reader.next_batch(values_per_page).unwrap();
        let len = array_reader.read_records(values_per_page).unwrap();
        assert_eq!(len, values_per_page);
        assert_eq!(
            Some(&def_levels[accu_len..(accu_len + len)]),
            array_reader.get_def_levels()
        );
        assert_eq!(
            Some(&rep_levels[accu_len..(accu_len + len)]),
            array_reader.get_rep_levels()
        );
        let array = array_reader.consume_batch().unwrap();
        let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
        for i in 0..array.len() {
            if array.is_valid(i) {
                assert_eq!(
                    all_values[i + accu_len].as_ref().unwrap().as_str(),
                    strings.value(i)
                )
            } else {
                assert_eq!(all_values[i + accu_len], None)
            }
        }
        accu_len += len;

        // Try to read values_per_page values, however there are only values_per_page/2 values
        // println!("---------- reading a batch of {} values ----------", values_per_page);
        let len = array_reader.read_records(values_per_page).unwrap();
        assert_eq!(len, values_per_page / 2);
        assert_eq!(
            Some(&def_levels[accu_len..(accu_len + len)]),
            array_reader.get_def_levels()
        );
        assert_eq!(
            Some(&rep_levels[accu_len..(accu_len + len)]),
            array_reader.get_rep_levels()
        );
        array_reader.consume_batch().unwrap();
    }
}
