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

use std::{any::Any, collections::VecDeque, marker::PhantomData};
use std::{rc::Rc, cell::RefCell};
use arrow::{array::{ArrayRef, Int16Array}, buffer::MutableBuffer, datatypes::{DataType as ArrowType, ToByteSlice}};
use crate::{column::page::{Page, PageIterator}, memory::ByteBufferPtr, schema::types::{ColumnDescPtr, ColumnDescriptor}};
use crate::arrow::schema::parquet_to_arrow_field;
use crate::errors::{ParquetError, Result};
use crate::basic::Encoding;
use super::array_reader::ArrayReader;

struct UnzipIter<Source, Target, State>
{
    shared_state: Rc<RefCell<State>>,
    select_item_buffer: fn(&mut State) -> &mut VecDeque<Target>,
    consume_source_item: fn(source_item: Source, state: &mut State) -> Target,
}

impl<Source, Target, State> UnzipIter<Source, Target, State>
{
    fn new(
        shared_state: Rc<RefCell<State>>, 
        item_buffer_selector: fn(&mut State) -> &mut VecDeque<Target>, 
        source_item_consumer: fn(source_item: Source, state: &mut State) -> Target
    ) -> Self {
        Self {
            shared_state,
            select_item_buffer: item_buffer_selector,
            consume_source_item: source_item_consumer,
        }
    }
}

trait UnzipIterState<T> {
    type SourceIter: Iterator<Item = T>;
    fn source_iter(&mut self) -> &mut Self::SourceIter;
}

impl<Source, Target, State: UnzipIterState<Source>> Iterator for UnzipIter<Source, Target, State> {
    type Item = Target;

    fn next(&mut self) -> Option<Self::Item> {
        let mut inner = self.shared_state.borrow_mut();
        // try to get one from the stored data
        (self.select_item_buffer)(&mut *inner).pop_front().or_else(|| 
            // nothing stored, we need a new element.
            inner.source_iter().next().map(|s| {
                (self.consume_source_item)(s, &mut inner)
            }))
    }
}

struct PageBufferUnzipIterState<V, L, It> {
    iter: It,
    value_iter_buffer: VecDeque<V>,
    def_level_iter_buffer: VecDeque<L>,
    rep_level_iter_buffer: VecDeque<L>,
}

impl<V, L, It: Iterator<Item = (V, L, L)>> UnzipIterState<(V, L, L)> for PageBufferUnzipIterState<V, L, It> {
    type SourceIter = It;

    #[inline]
    fn source_iter(&mut self) -> &mut Self::SourceIter {
        &mut self.iter
    }
}

fn unzip_iter<V, L, It: Iterator<Item = (V, L, L)>>(it: It) -> (
    UnzipIter<(V, L, L), V, PageBufferUnzipIterState<V, L, It>>, 
    UnzipIter<(V, L, L), L, PageBufferUnzipIterState<V, L, It>>,
    UnzipIter<(V, L, L), L, PageBufferUnzipIterState<V, L, It>>,
) {
    let shared_data = Rc::new(RefCell::new(PageBufferUnzipIterState { 
        iter: it,
        value_iter_buffer: VecDeque::new(),
        def_level_iter_buffer: VecDeque::new(),
        rep_level_iter_buffer: VecDeque::new(),
    }));

    let value_iter = UnzipIter::new(
        shared_data.clone(),
        |state| &mut state.value_iter_buffer,
        |(v, d, r), state| { 
            state.def_level_iter_buffer.push_back(d); 
            state.rep_level_iter_buffer.push_back(r);
            v
        }, 
    );

    let def_level_iter = UnzipIter::new(
        shared_data.clone(),
        |state| &mut state.def_level_iter_buffer,
        |(v, d, r), state| {
            state.value_iter_buffer.push_back(v);
            state.rep_level_iter_buffer.push_back(r);
            d
        }, 
    );

    let rep_level_iter = UnzipIter::new(
        shared_data,
        |state| &mut state.rep_level_iter_buffer,
        |(v, d, r), state| {
            state.value_iter_buffer.push_back(v);
            state.def_level_iter_buffer.push_back(d);
            r
        }, 
    );

    (value_iter, def_level_iter, rep_level_iter)
}

pub trait ArrayConverter {
    fn convert_value_bytes(&self, value_decoder: &mut impl ValueDecoder, num_values: usize) -> Result<arrow::array::ArrayData>;
}

pub struct ArrowArrayReader<'a, C: ArrayConverter + 'a> {
    column_desc: ColumnDescPtr,
    data_type: ArrowType,
    def_level_decoder: Box<dyn ValueDecoder + 'a>,
    rep_level_decoder: Box<dyn ValueDecoder + 'a>,
    value_decoder: Box<dyn ValueDecoder + 'a>,
    last_def_levels: Option<Int16Array>,
    last_rep_levels: Option<Int16Array>,
    array_converter: C,
}

pub(crate) struct ColumnChunkContext {
    dictionary_values: Option<Vec<ByteBufferPtr>>,
}

impl ColumnChunkContext {
    fn new() -> Self {
        Self {
            dictionary_values: None,
        }
    }

    fn set_dictionary(&mut self, dictionary_values: Vec<ByteBufferPtr>) {
        self.dictionary_values = Some(dictionary_values);
    }
}

impl<'a, C: ArrayConverter + 'a> ArrowArrayReader<'a, C> {
    pub fn try_new<P: PageIterator + 'a>(column_chunk_iterator: P, column_desc: ColumnDescPtr, array_converter: C, arrow_type: Option<ArrowType>) -> Result<Self> {
        let data_type = match arrow_type {
            Some(t) => t,
            None => parquet_to_arrow_field(column_desc.as_ref())?
                .data_type()
                .clone(),
        };
        // println!("ArrowArrayReader::try_new, column: {}, data_type: {}", column_desc.path(), data_type);
        let page_iter = column_chunk_iterator
            // build iterator of pages across column chunks
            .flat_map(|x| -> Box<dyn Iterator<Item = Result<(Page, Rc<RefCell<ColumnChunkContext>>)>>> {
                // attach column chunk context
                let context = Rc::new(RefCell::new(ColumnChunkContext::new()));
                match x {
                    Ok(page_reader) => Box::new(page_reader.map(move |pr| pr.and_then(|p| Ok((p, context.clone()))))),
                    // errors from reading column chunks / row groups are propagated to page level
                    Err(e) => Box::new(std::iter::once(Err(e)))
                }
            });
        // capture a clone of column_desc in closure so that it can outlive current function
        let map_page_fn = (|column_desc: ColumnDescPtr| {
            // move |x: Result<Page>|  match x {
            //     Ok(p) => Self::map_page(p, column_desc.as_ref()),
            //     Err(e) => Err(e),
            // }
            move |x: Result<(Page, Rc<RefCell<ColumnChunkContext>>)>| x.and_then(
                |(page, context)| Self::map_page(page, context, column_desc.as_ref())
            )
        })(column_desc.clone());
        // map page iterator into tuple of buffer iterators for (values, def levels, rep levels)
        // errors from lower levels are surfaced through the value decoder iterator
        let decoder_iter = page_iter
            .map(map_page_fn)
            .map(|x| match x {
                Ok(iter_tuple) => iter_tuple,
                // errors from reading pages are propagated to decoder iterator level
                Err(e) => Self::map_page_error(e)
            });
        // split tuple iterator into separate iterators for (values, def levels, rep levels)
        let (value_iter, def_level_iter, rep_level_iter) = unzip_iter(decoder_iter);
        
        Ok(Self {
            column_desc,
            data_type,
            def_level_decoder: Box::new(CompositeValueDecoder::new(def_level_iter)),
            rep_level_decoder: Box::new(CompositeValueDecoder::new(rep_level_iter)),
            value_decoder: Box::new(CompositeValueDecoder::new(value_iter)),
            last_def_levels: None,
            last_rep_levels: None,
            array_converter,
        })
    }

    #[inline]
    fn def_levels_available(column_desc: &ColumnDescriptor) -> bool {
        column_desc.max_def_level() > 0
    }

    #[inline]
    fn rep_levels_available(column_desc: &ColumnDescriptor) -> bool {
        column_desc.max_rep_level() > 0
    }

    fn map_page_error(err: ParquetError) -> (Box<dyn ValueDecoder>, Box<dyn ValueDecoder>, Box<dyn ValueDecoder>)
    {
        (
            Box::new(<dyn ValueDecoder>::once(Err(err.clone()))),
            Box::new(<dyn ValueDecoder>::once(Err(err.clone()))),
            Box::new(<dyn ValueDecoder>::once(Err(err.clone()))),
        )
    }

    // Split Result<Page> into Result<(Iterator<Values>, Iterator<DefLevels>, Iterator<RepLevels>)>
    // this method could fail, e.g. if the page encoding is not supported
    fn map_page(page: Page, column_chunk_context: Rc<RefCell<ColumnChunkContext>>, column_desc: &ColumnDescriptor) -> Result<(Box<dyn ValueDecoder>, Box<dyn ValueDecoder>, Box<dyn ValueDecoder>)> 
    {
        // println!(
        //     "ArrowArrayReader::map_page, column: {}, page: {:?}, encoding: {:?}, num values: {:?}", 
        //     column_desc.path(), page.page_type(), page.encoding(), page.num_values()
        // );
        use crate::encodings::levels::LevelDecoder;
        match page {
            Page::DictionaryPage {
                buf,
                num_values,
                encoding,
                ..
            } => {
                let mut column_chunk_context = column_chunk_context.borrow_mut();
                if column_chunk_context.dictionary_values.is_some() {
                    return Err(general_err!("Column chunk cannot have more than one dictionary"));
                }
                // create plain decoder for dictionary values
                let mut dict_decoder = Self::get_dictionary_page_decoder(buf, num_values as usize, encoding, column_desc)?;
                // decode and cache dictionary values
                let dictionary_values = dict_decoder.read_dictionary_values()?;
                column_chunk_context.set_dictionary(dictionary_values);

                // a dictionary page doesn't return any values
                Ok((
                    Box::new(<dyn ValueDecoder>::empty()),
                    Box::new(<dyn ValueDecoder>::empty()),
                    Box::new(<dyn ValueDecoder>::empty()),
                ))
            }
            Page::DataPage {
                buf,
                num_values,
                encoding,
                def_level_encoding,
                rep_level_encoding,
                statistics: _,
            } => {
                let mut buffer_ptr = buf;
                // create rep level decoder iterator
                let rep_level_iter: Box<dyn ValueDecoder> = if Self::rep_levels_available(&column_desc) {
                    let mut rep_decoder =
                        LevelDecoder::v1(rep_level_encoding, column_desc.max_rep_level());
                    let rep_level_byte_len = rep_decoder.set_data(
                        num_values as usize,
                        buffer_ptr.all(),
                    );
                    // advance buffer pointer
                    buffer_ptr = buffer_ptr.start_from(rep_level_byte_len);
                    Box::new(LevelValueDecoder::new(rep_decoder))
                }
                else {
                    Box::new(<dyn ValueDecoder>::once(Err(ParquetError::General(format!("rep levels are not available")))))
                };
                // create def level decoder iterator
                let def_level_iter: Box<dyn ValueDecoder> = if Self::def_levels_available(&column_desc) {
                    let mut def_decoder = LevelDecoder::v1(
                        def_level_encoding,
                        column_desc.max_def_level(),
                    );
                    let def_levels_byte_len = def_decoder.set_data(
                        num_values as usize,
                        buffer_ptr.all(),
                    );
                    // advance buffer pointer
                    buffer_ptr = buffer_ptr.start_from(def_levels_byte_len);
                    Box::new(LevelValueDecoder::new(def_decoder))
                }
                else {
                    Box::new(<dyn ValueDecoder>::once(Err(ParquetError::General(format!("def levels are not available")))))
                };
                // create value decoder iterator
                let value_iter = Self::get_value_decoder(
                    buffer_ptr, num_values as usize, encoding, column_desc, column_chunk_context
                )?;
                Ok((
                    value_iter,
                    def_level_iter,
                    rep_level_iter
                ))
            }
            Page::DataPageV2 {
                buf,
                num_values,
                encoding,
                num_nulls: _,
                num_rows: _,
                def_levels_byte_len,
                rep_levels_byte_len,
                is_compressed: _,
                statistics: _,
            } => {
                let mut offset = 0;
                // create rep level decoder iterator
                let rep_level_iter: Box<dyn ValueDecoder> = if Self::rep_levels_available(&column_desc) {
                    let rep_levels_byte_len = rep_levels_byte_len as usize;
                    let mut rep_decoder =
                        LevelDecoder::v2(column_desc.max_rep_level());
                    rep_decoder.set_data_range(
                        num_values as usize,
                        &buf,
                        offset,
                        rep_levels_byte_len,
                    );
                    offset += rep_levels_byte_len;
                    Box::new(LevelValueDecoder::new(rep_decoder))
                }
                else {
                    Box::new(<dyn ValueDecoder>::once(Err(ParquetError::General(format!("rep levels are not available")))))
                };
                // create def level decoder iterator
                let def_level_iter: Box<dyn ValueDecoder> = if Self::def_levels_available(&column_desc) {
                    let def_levels_byte_len = def_levels_byte_len as usize;
                    let mut def_decoder =
                        LevelDecoder::v2(column_desc.max_def_level());
                    def_decoder.set_data_range(
                        num_values as usize,
                        &buf,
                        offset,
                        def_levels_byte_len,
                    );
                    offset += def_levels_byte_len;
                    Box::new(LevelValueDecoder::new(def_decoder))
                }
                else {
                    Box::new(<dyn ValueDecoder>::once(Err(ParquetError::General(format!("def levels are not available")))))
                };

                // create value decoder iterator
                let values_buffer = buf.start_from(offset);
                let value_iter = Self::get_value_decoder(
                    values_buffer, num_values as usize, encoding, column_desc, column_chunk_context
                )?;
                Ok((
                    value_iter,
                    def_level_iter,
                    rep_level_iter
                ))
            }
        }
    }

    fn get_dictionary_page_decoder(values_buffer: ByteBufferPtr, num_values: usize, mut encoding: Encoding, column_desc: &ColumnDescriptor) -> Result<Box<dyn DictionaryValueDecoder>> {
        if encoding == Encoding::PLAIN || encoding == Encoding::PLAIN_DICTIONARY {
            encoding = Encoding::RLE_DICTIONARY
        }

        if encoding == Encoding::RLE_DICTIONARY {
            Ok(Self::get_plain_value_decoder(values_buffer, num_values, column_desc).into_dictionary_decoder())
        } else {
            Err(nyi_err!(
                "Invalid/Unsupported encoding type for dictionary: {}",
                encoding
            ))
        }
    }

    fn get_value_decoder(values_buffer: ByteBufferPtr, num_values: usize, mut encoding: Encoding, column_desc: &ColumnDescriptor, column_chunk_context: Rc<RefCell<ColumnChunkContext>>) -> Result<Box<dyn ValueDecoder>> {
        if encoding == Encoding::PLAIN_DICTIONARY {
            encoding = Encoding::RLE_DICTIONARY;
        }

        match encoding {
            Encoding::PLAIN => Ok(Self::get_plain_value_decoder(values_buffer, num_values, column_desc).into_value_decoder()),
            Encoding::RLE_DICTIONARY => {
                if column_chunk_context.borrow().dictionary_values.is_some() {
                    let value_bit_len = Self::get_column_physical_bit_len(column_desc);
                    let dictionary_decoder: Box<dyn ValueDecoder> = if value_bit_len == 0 {
                        Box::new(VariableLenDictionaryDecoder::new(
                            column_chunk_context, values_buffer, num_values
                        ))
                    }
                    else {
                        Box::new(FixedLenDictionaryDecoder::new(
                            column_chunk_context, values_buffer, num_values, value_bit_len
                        ))
                    };
                    Ok(dictionary_decoder)
                }
                else {
                    Err(general_err!(
                        "Dictionary values have not been initialized."
                    ))
                }
            }
            // Encoding::RLE => Box::new(RleValueDecoder::new()),
            // Encoding::DELTA_BINARY_PACKED => Box::new(DeltaBitPackDecoder::new()),
            // Encoding::DELTA_LENGTH_BYTE_ARRAY => Box::new(DeltaLengthByteArrayDecoder::new()),
            // Encoding::DELTA_BYTE_ARRAY => Box::new(DeltaByteArrayDecoder::new()),
            e => return Err(nyi_err!("Encoding {} is not supported", e)),
        }
    }

    fn get_column_physical_bit_len(column_desc: &ColumnDescriptor) -> usize {
        use crate::basic::Type as PhysicalType;
        // parquet only supports a limited number of physical types
        // later converters cast to a more specific arrow / logical type if necessary
        match column_desc.physical_type() {
            PhysicalType::BOOLEAN => 1,
            PhysicalType::INT32 | PhysicalType::FLOAT => 32,
            PhysicalType::INT64 | PhysicalType::DOUBLE => 64,
            PhysicalType::INT96 => 96,
            PhysicalType::BYTE_ARRAY => 0,
            PhysicalType::FIXED_LEN_BYTE_ARRAY => column_desc.type_length() as usize * 8,
        }
    }

    fn get_plain_value_decoder(values_buffer: ByteBufferPtr, num_values: usize, column_desc: &ColumnDescriptor) -> Box<dyn PlainValueDecoder> {
        let value_bit_len = Self::get_column_physical_bit_len(column_desc);
        if value_bit_len == 0 {
            Box::new(VariableLenPlainDecoder::new(values_buffer, num_values))
        }
        else {
            Box::new(FixedLenPlainDecoder::new(values_buffer, num_values, value_bit_len))
        }
    }

    fn build_level_array(level_decoder: &mut impl ValueDecoder, batch_size: usize) -> Result<Int16Array> {
        use arrow::datatypes::Int16Type;
        let level_converter = PrimitiveArrayConverter::<Int16Type>::new();
        let array_data = level_converter.convert_value_bytes(level_decoder, batch_size)?;
        Ok(Int16Array::from(array_data))
    }
}

impl<C: ArrayConverter> ArrayReader for ArrowArrayReader<'static, C> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn next_batch(&mut self, batch_size: usize) -> Result<ArrayRef> {
        if Self::rep_levels_available(&self.column_desc) {
            // read rep levels if available
            let rep_level_array = Self::build_level_array(&mut self.rep_level_decoder, batch_size)?;
            self.last_rep_levels = Some(rep_level_array);
        }
        
        // check if def levels are available
        let (values_to_read, null_bitmap_array) = if !Self::def_levels_available(&self.column_desc) {
            // if no def levels - just read (up to) batch_size values
            (batch_size, None)
        }
        else {
            // if def levels are available - they determine how many values will be read
            // decode def levels, return first error if any
            let def_level_array = Self::build_level_array(&mut self.def_level_decoder, batch_size)?;
            let def_level_count = def_level_array.len();
            // use eq_scalar to efficiently build null bitmap array from def levels
            let null_bitmap_array = arrow::compute::eq_scalar(&def_level_array, self.column_desc.max_def_level())?;
            self.last_def_levels = Some(def_level_array);
            // efficiently calculate values to read
            let values_to_read = null_bitmap_array.values().count_set_bits_offset(0, def_level_count);
            let maybe_null_bitmap = if values_to_read != null_bitmap_array.len() {
                Some(null_bitmap_array)
            }
            else {
                // shortcut if no NULLs
                None
            };
            (values_to_read, maybe_null_bitmap)
        };

        // read a batch of values
        // println!("ArrowArrayReader::next_batch, batch_size: {}, values_to_read: {}", batch_size, values_to_read);

        // converter only creates a no-null / all value array data
        let mut value_array_data = self.array_converter.convert_value_bytes(&mut self.value_decoder, values_to_read)?;

        if let Some(null_bitmap_array) = null_bitmap_array {
            // Only if def levels are available - insert null values efficiently using MutableArrayData.
            // This will require value bytes to be copied again, but converter requirements are reduced.
            // With a small number of NULLs, this will only be a few copies of large byte sequences.
            let actual_batch_size = null_bitmap_array.len();
            // use_nulls is false, because null_bitmap_array is already calculated and re-used
            let mut mutable = arrow::array::MutableArrayData::new(vec![&value_array_data], false, actual_batch_size);
            // SlicesIterator slices only the true values, NULLs are inserted to fill any gaps
            arrow::compute::SlicesIterator::new(&null_bitmap_array).for_each(|(start, end)| {
                // the gap needs to be filled with NULLs
                if start > mutable.len() {
                    let nulls_to_add = start - mutable.len();
                    mutable.extend_nulls(nulls_to_add);
                }
                // fill values, adjust start and end with NULL count so far
                let nulls_added = mutable.null_count();
                mutable.extend(0, start - nulls_added, end - nulls_added);
            });
            // any remaining part is NULLs
            if mutable.len() < actual_batch_size {
                let nulls_to_add = actual_batch_size - mutable.len();
                mutable.extend_nulls(nulls_to_add);
            }
            
            value_array_data = mutable
                .into_builder()
                .null_bit_buffer(null_bitmap_array.values().clone())
                .build();
        }
        let mut array = arrow::array::make_array(value_array_data);
        if array.data_type() != &self.data_type {
            // cast array to self.data_type if necessary
            array = arrow::compute::cast(&array, &self.data_type)?
        }
        Ok(array)
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.last_def_levels.as_ref().map(|x| x.values())
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.last_rep_levels.as_ref().map(|x| x.values())
    }
}

use crate::encodings::rle::RleDecoder;

pub trait ValueDecoder {
    fn read_value_bytes(&mut self, num_values: usize, read_bytes: &mut dyn FnMut(&[u8], usize)) -> Result<usize>;
}

trait DictionaryValueDecoder {
    fn read_dictionary_values(&mut self) -> Result<Vec<ByteBufferPtr>>;
}

trait PlainValueDecoder: ValueDecoder + DictionaryValueDecoder {
    fn into_value_decoder(self: Box<Self>) -> Box<dyn ValueDecoder>;
    fn into_dictionary_decoder(self: Box<Self>) -> Box<dyn DictionaryValueDecoder>;
}

impl<T> PlainValueDecoder for T 
where T: ValueDecoder + DictionaryValueDecoder + 'static
{
    fn into_value_decoder(self: Box<T>) -> Box<dyn ValueDecoder> {
        self
    }

    fn into_dictionary_decoder(self: Box<T>) -> Box<dyn DictionaryValueDecoder> {
        self
    }
}

impl dyn ValueDecoder {
    fn empty() -> impl ValueDecoder {
        SingleValueDecoder::new(Ok(0))
    }

    fn once(value: Result<usize>) -> impl ValueDecoder {
        SingleValueDecoder::new(value)
    }
}

impl ValueDecoder for Box<dyn ValueDecoder> {
    #[inline]
    fn read_value_bytes(&mut self, num_values: usize, read_bytes: &mut dyn FnMut(&[u8], usize)) -> Result<usize> {
        self.as_mut().read_value_bytes(num_values, read_bytes)
    }
}

struct SingleValueDecoder {
    value: Result<usize>,
}

impl SingleValueDecoder {
    fn new(value: Result<usize>) -> Self {
        Self {
            value,
        }
    }
}

impl ValueDecoder for SingleValueDecoder {
    fn read_value_bytes(&mut self, _num_values: usize, _read_bytes: &mut dyn FnMut(&[u8], usize)) -> Result<usize> {
        self.value.clone()
    }
}

struct CompositeValueDecoder<I: Iterator<Item = Box<dyn ValueDecoder>>> {
    current_decoder: Option<Box<dyn ValueDecoder>>,
    decoder_iter: I,
}

impl<I: Iterator<Item = Box<dyn ValueDecoder>>> CompositeValueDecoder<I> {
    fn new(mut decoder_iter: I) -> Self {
        let current_decoder = decoder_iter.next();
        Self {
            decoder_iter,
            current_decoder,
        }
    }
}

impl<I: Iterator<Item = Box<dyn ValueDecoder>>> ValueDecoder for CompositeValueDecoder<I> {
    fn read_value_bytes(&mut self, num_values: usize, read_bytes: &mut dyn FnMut(&[u8], usize)) -> Result<usize> {
        let mut values_to_read = num_values;
        while values_to_read > 0 {
            let value_decoder = match self.current_decoder.as_mut() {
                Some(d) => d,
                // no more decoders
                None => break,
            };
            while values_to_read > 0 {
                let values_read = value_decoder.read_value_bytes(values_to_read, read_bytes)?;
                if values_read > 0 {
                    values_to_read -= values_read;
                }
                else {
                    // no more values in current decoder
                    self.current_decoder = self.decoder_iter.next();
                    break;
                }
            }
        }

        Ok(num_values - values_to_read)
    }
}

struct LevelValueDecoder {
    level_decoder: crate::encodings::levels::LevelDecoder,
    level_value_buffer: Vec<i16>,
}

impl LevelValueDecoder {
    fn new(level_decoder: crate::encodings::levels::LevelDecoder) -> Self {
        Self {
            level_decoder,
            level_value_buffer: vec![0i16; 2048],
        }
    }
}

impl ValueDecoder for LevelValueDecoder {
    fn read_value_bytes(&mut self, num_values: usize, read_bytes: &mut dyn FnMut(&[u8], usize)) -> Result<usize> {
        let value_size = std::mem::size_of::<i16>();
        let mut total_values_read = 0;
        while total_values_read < num_values {
            let values_to_read = std::cmp::min(num_values - total_values_read, self.level_value_buffer.len());
            let values_read = match self.level_decoder.get(&mut self.level_value_buffer[..values_to_read]) {
                Ok(values_read) => values_read,
                Err(e) => return Err(e),
            };
            if values_read > 0 {
                let level_value_bytes = &self.level_value_buffer.to_byte_slice()[..values_read * value_size];
                read_bytes(level_value_bytes, values_read);
                total_values_read += values_read;
            }
            else {
                break;
            }
        }
        Ok(total_values_read)
    }
}

pub(crate) struct FixedLenPlainDecoder {
    data: ByteBufferPtr,
    num_values: usize,
    value_bit_len: usize,
}

impl FixedLenPlainDecoder {
    pub(crate) fn new(data: ByteBufferPtr, num_values: usize, value_bit_len: usize) -> Self {
        Self {
            data,
            num_values,
            value_bit_len,
        }
    }
}

impl DictionaryValueDecoder for FixedLenPlainDecoder {
    fn read_dictionary_values(&mut self) -> Result<Vec<ByteBufferPtr>> {
        let value_byte_len = self.value_bit_len / 8;
        let available_values = self.data.len() / value_byte_len;
        let values_to_read = std::cmp::min(available_values, self.num_values);
        let byte_len = values_to_read * value_byte_len;
        let values = vec![self.data.range(0, byte_len)];
        self.num_values = 0;
        self.data.set_range(self.data.start(), 0);
        Ok(values)
    }
}

impl ValueDecoder for FixedLenPlainDecoder {
    fn read_value_bytes(&mut self, num_values: usize, read_bytes: &mut dyn FnMut(&[u8], usize)) -> Result<usize> {
        if self.data.len() > 0 {
            let available_values = self.data.len() * 8 / self.value_bit_len;
            let values_to_read = std::cmp::min(available_values, num_values);
            let byte_len = values_to_read * self.value_bit_len / 8;
            read_bytes(&self.data.data()[..byte_len], values_to_read);
            self.data.set_range(
                self.data.start() + byte_len,
                self.data.len() - byte_len
            );
            Ok(values_to_read)
        }
        else {
            Ok(0)
        }
    }
}

pub(crate) struct VariableLenPlainDecoder {
    data: ByteBufferPtr,
    num_values: usize,
    position: usize,
}

impl VariableLenPlainDecoder {
    pub(crate) fn new(data: ByteBufferPtr, num_values: usize) -> Self {
        Self {
            data,
            num_values,
            position: 0,
        }
    }
}

impl DictionaryValueDecoder for VariableLenPlainDecoder {
    fn read_dictionary_values(&mut self) -> Result<Vec<ByteBufferPtr>> {
        const LEN_SIZE: usize = std::mem::size_of::<u32>();
        let data = self.data.data();
        let data_len = data.len();
        let values_to_read = self.num_values;
        let mut values = Vec::with_capacity(values_to_read);
        let mut values_read = 0;
        while self.position < data_len && values_read < values_to_read {
            let len: usize = 
                read_num_bytes!(u32, LEN_SIZE, data[self.position..])
                as usize;
            self.position += LEN_SIZE;
            if data_len < self.position + len {
                return Err(eof_err!("Not enough bytes to decode"));
            }
            values.push(self.data.range(self.position, len));
            self.position += len;
            values_read += 1;
        }
        self.num_values -= values_read;
        Ok(values)
    }
}

impl ValueDecoder for VariableLenPlainDecoder {
    fn read_value_bytes(&mut self, num_values: usize, read_bytes: &mut dyn FnMut(&[u8], usize)) -> Result<usize> {
        const LEN_SIZE: usize = std::mem::size_of::<u32>();
        let data = self.data.data();
        let data_len = data.len();
        let values_to_read = std::cmp::min(self.num_values, num_values);
        let mut values_read = 0;
        while self.position < data_len && values_read < values_to_read {
            let len: usize = 
                read_num_bytes!(u32, LEN_SIZE, data[self.position..])
                as usize;
            self.position += LEN_SIZE;
            if data_len < self.position + len {
                return Err(eof_err!("Not enough bytes to decode"));
            }
            read_bytes(&data[self.position..][..len], 1);
            self.position += len;
            values_read += 1;
        }
        self.num_values -= values_read;
        Ok(values_read)
    }
}

pub(crate) struct FixedLenDictionaryDecoder {
    context_ref: Rc<RefCell<ColumnChunkContext>>,
    key_data_bufer: ByteBufferPtr,
    num_values: usize,
    rle_decoder: RleDecoder,
    value_byte_len: usize,
    keys_buffer: Vec<i32>,
}

impl FixedLenDictionaryDecoder {
    pub(crate) fn new(column_chunk_context: Rc<RefCell<ColumnChunkContext>>, key_data_bufer: ByteBufferPtr, num_values: usize, value_bit_len: usize) -> Self {
        assert!(value_bit_len % 8 == 0, "value_bit_size must be a multiple of 8");
        // First byte in `data` is bit width
        let bit_width = key_data_bufer.data()[0];
        let mut rle_decoder = RleDecoder::new(bit_width);
        rle_decoder.set_data(key_data_bufer.start_from(1));
        
        Self {
            context_ref: column_chunk_context,
            key_data_bufer,
            num_values,
            rle_decoder,
            value_byte_len: value_bit_len / 8,
            keys_buffer: vec![0; 2048],
        }
    }
}

impl ValueDecoder for FixedLenDictionaryDecoder {
    fn read_value_bytes(&mut self, num_values: usize, read_bytes: &mut dyn FnMut(&[u8], usize)) -> Result<usize> {
        if self.num_values <= 0 {
            return Ok(0);
        }
        let context = self.context_ref.borrow();
        let values = context.dictionary_values.as_ref().unwrap();
        let input_value_bytes = values[0].data();
        // read no more than available values or requested values
        let values_to_read = std::cmp::min(self.num_values, num_values);
        let mut values_read = 0;
        while values_read < values_to_read {
            // read values in batches of up to self.keys_buffer.len()
            let keys_to_read = std::cmp::min(values_to_read - values_read, self.keys_buffer.len());
            let keys_read = match self.rle_decoder.get_batch(&mut self.keys_buffer[..keys_to_read]) {
                Ok(keys_read) => keys_read,
                Err(e) => return Err(e),
            };
            if keys_read == 0 {
                self.num_values = 0;
                return Ok(values_read);
            }
            for i in 0..keys_read {
                let key = self.keys_buffer[i] as usize;
                read_bytes(&input_value_bytes[key * self.value_byte_len..][..self.value_byte_len], 1);
            }
            values_read += keys_read;
        }
        self.num_values -= values_read;
        Ok(values_read)
    }
}

pub(crate) struct VariableLenDictionaryDecoder {
    context_ref: Rc<RefCell<ColumnChunkContext>>,
    key_data_bufer: ByteBufferPtr,
    num_values: usize,
    rle_decoder: RleDecoder,
    keys_buffer: Vec<i32>,
}

impl VariableLenDictionaryDecoder {
    pub(crate) fn new(column_chunk_context: Rc<RefCell<ColumnChunkContext>>, key_data_bufer: ByteBufferPtr, num_values: usize) -> Self {
        // First byte in `data` is bit width
        let bit_width = key_data_bufer.data()[0];
        let mut rle_decoder = RleDecoder::new(bit_width);
        rle_decoder.set_data(key_data_bufer.start_from(1));
        
        Self {
            context_ref: column_chunk_context,
            key_data_bufer,
            num_values,
            rle_decoder,
            keys_buffer: vec![0; 2048],
        }
    }
}

impl ValueDecoder for VariableLenDictionaryDecoder {
    fn read_value_bytes(&mut self, num_values: usize, read_bytes: &mut dyn FnMut(&[u8], usize)) -> Result<usize> {
        if self.num_values <= 0 {
            return Ok(0);
        }
        let context = self.context_ref.borrow();
        let values = context.dictionary_values.as_ref().unwrap();
        let values_to_read = std::cmp::min(self.num_values, num_values);
        let mut values_read = 0;
        while values_read < values_to_read {
            // read values in batches of up to self.keys_buffer.len()
            let keys_to_read = std::cmp::min(values_to_read - values_read, self.keys_buffer.len());
            let keys_read = match self.rle_decoder.get_batch(&mut self.keys_buffer[..keys_to_read]) {
                Ok(keys_read) => keys_read,
                Err(e) => return Err(e),
            };
            if keys_read == 0 {
                self.num_values = 0;
                return Ok(values_read);
            }
            for i in 0..keys_read {
                let key = self.keys_buffer[i] as usize;
                read_bytes(values[key].data(), 1);
            }
            values_read += keys_read;
        }
        self.num_values -= values_read;
        Ok(values_read)
    }
}

use arrow::datatypes::ArrowPrimitiveType;

pub struct PrimitiveArrayConverter<T: ArrowPrimitiveType> {
    _phantom_data: PhantomData<T>,
}

impl<T: ArrowPrimitiveType> PrimitiveArrayConverter<T> {
    pub fn new() -> Self {
        Self {
            _phantom_data: PhantomData,
        }
    }
}

impl<T: ArrowPrimitiveType> ArrayConverter for PrimitiveArrayConverter<T> {
    fn convert_value_bytes(&self, value_decoder: &mut impl ValueDecoder, num_values: usize) -> Result<arrow::array::ArrayData> {
        let value_size = T::get_byte_width();
        let values_byte_capacity = num_values * value_size;
        let mut values_buffer = MutableBuffer::new(values_byte_capacity);

        value_decoder.read_value_bytes(num_values, &mut |value_bytes, _| {
            values_buffer.extend_from_slice(value_bytes);
        })?;

        // calculate actual data_len, which may be different from the iterator's upper bound
        let value_count = values_buffer.len() / value_size;
        let array_data = arrow::array::ArrayData::builder(T::DATA_TYPE)
            .len(value_count)
            .add_buffer(values_buffer.into())
            .build();
        Ok(array_data)
    }
}

pub struct StringArrayConverter {}

impl StringArrayConverter {
    pub fn new() -> Self {
        Self {}
    }
}

impl ArrayConverter for StringArrayConverter {
    fn convert_value_bytes(&self, value_decoder: &mut impl ValueDecoder, num_values: usize) -> Result<arrow::array::ArrayData> {
        use arrow::datatypes::ArrowNativeType;
        let offset_size = std::mem::size_of::<i32>();
        let mut offsets_buffer = MutableBuffer::new((num_values + 1) * offset_size);
        // allocate initial capacity of 1 byte for each item
        let values_byte_capacity = num_values;
        let mut values_buffer = MutableBuffer::new(values_byte_capacity);

        let mut length_so_far = i32::default();
        offsets_buffer.push(length_so_far);

        value_decoder.read_value_bytes(num_values,&mut |value_bytes, values_read| {
            debug_assert_eq!(
                values_read, 1,
                "offset length value buffers can only contain bytes for a single value"
            );
            length_so_far += <i32 as ArrowNativeType>::from_usize(value_bytes.len()).unwrap();
            // this should be safe because a ValueDecoder should not read more than num_values
            unsafe { offsets_buffer.push_unchecked(length_so_far); }
            values_buffer.extend_from_slice(value_bytes);
        })?;
        // calculate actual data_len, which may be different from the iterator's upper bound
        let data_len = (offsets_buffer.len() / offset_size) - 1;
        let array_data = arrow::array::ArrayData::builder(ArrowType::Utf8)
            .len(data_len)
            .add_buffer(offsets_buffer.into())
            .add_buffer(values_buffer.into())
            .build();
        Ok(array_data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{basic::{Encoding}, column::page::PageReader, schema::types::SchemaDescPtr};
    use crate::column::page::{Page};
    use crate::data_type::{ByteArray};
    use crate::data_type::{ByteArrayType};
    use crate::schema::parser::parse_message_type;
    use crate::schema::types::{SchemaDescriptor};
    use crate::util::test_common::page_util::{
        DataPageBuilder, DataPageBuilderImpl, InMemoryPageIterator,
    };
    use arrow::array::{PrimitiveArray, StringArray};
    use arrow::datatypes::{Int32Type as ArrowInt32};
    use rand::{Rng, distributions::uniform::SampleUniform, thread_rng};
    use std::sync::Arc;

    /// Iterator for testing reading empty columns
    struct EmptyPageIterator {
        schema: SchemaDescPtr,
    }

    impl EmptyPageIterator {
        fn new(schema: SchemaDescPtr) -> Self {
            EmptyPageIterator { schema }
        }
    }

    impl Iterator for EmptyPageIterator {
        type Item = Result<Box<dyn PageReader>>;

        fn next(&mut self) -> Option<Self::Item> {
            None
        }
    }

    impl PageIterator for EmptyPageIterator {
        fn schema(&mut self) -> Result<SchemaDescPtr> {
            Ok(self.schema.clone())
        }

        fn column_schema(&mut self) -> Result<ColumnDescPtr> {
            Ok(self.schema.column(0))
        }
    }

    #[test]
    fn test_array_reader_empty_pages() {
        // Construct column schema
        let message_type = "
        message test_schema {
          REQUIRED INT32 leaf;
        }
        ";

        let schema = parse_message_type(message_type)
            .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
            .unwrap();

        let column_desc = schema.column(0);
        let page_iterator = EmptyPageIterator::new(schema);

        let converter = PrimitiveArrayConverter::<arrow::datatypes::Int32Type>::new();
        let mut array_reader = ArrowArrayReader::try_new(
            page_iterator, column_desc, converter, None
        ).unwrap();

        // expect no values to be read
        let array = array_reader.next_batch(50).unwrap();
        assert!(array.is_empty());
    }

    fn make_column_chunks<T: crate::data_type::DataType>(
        column_desc: ColumnDescPtr,
        encoding: Encoding,
        num_levels: usize,
        min_value: T::T,
        max_value: T::T,
        def_levels: &mut Vec<i16>,
        rep_levels: &mut Vec<i16>,
        values: &mut Vec<T::T>,
        page_lists: &mut Vec<Vec<Page>>,
        use_v2: bool,
        num_chunks: usize,
    ) where
        T::T: PartialOrd + SampleUniform + Copy,
    {
        for _i in 0..num_chunks {
            let mut pages = VecDeque::new();
            let mut data = Vec::new();
            let mut page_def_levels = Vec::new();
            let mut page_rep_levels = Vec::new();

            crate::util::test_common::make_pages::<T>(
                column_desc.clone(),
                encoding,
                1,
                num_levels,
                min_value,
                max_value,
                &mut page_def_levels,
                &mut page_rep_levels,
                &mut data,
                &mut pages,
                use_v2,
            );

            def_levels.append(&mut page_def_levels);
            rep_levels.append(&mut page_rep_levels);
            values.append(&mut data);
            page_lists.push(Vec::from(pages));
        }
    }

    #[test]
    fn test_primitive_array_reader_data() {
        // Construct column schema
        let message_type = "
        message test_schema {
          REQUIRED INT32 leaf;
        }
        ";

        let schema = parse_message_type(message_type)
            .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
            .unwrap();

        let column_desc = schema.column(0);

        // Construct page iterator
        {
            let mut data = Vec::new();
            let mut page_lists = Vec::new();
            make_column_chunks::<crate::data_type::Int32Type>(
                column_desc.clone(),
                Encoding::PLAIN,
                100,
                1,
                200,
                &mut Vec::new(),
                &mut Vec::new(),
                &mut data,
                &mut page_lists,
                true,
                2,
            );
            let page_iterator =
                InMemoryPageIterator::new(schema, column_desc.clone(), page_lists);

            let converter = PrimitiveArrayConverter::<arrow::datatypes::Int32Type>::new();
            let mut array_reader = ArrowArrayReader::try_new(
                page_iterator, column_desc, converter, None
            ).unwrap();

            // Read first 50 values, which are all from the first column chunk
            let array = array_reader.next_batch(50).unwrap();
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<ArrowInt32>>()
                .unwrap();

            assert_eq!(
                &PrimitiveArray::<ArrowInt32>::from(data[0..50].to_vec()),
                array
            );

            // Read next 100 values, the first 50 ones are from the first column chunk,
            // and the last 50 ones are from the second column chunk
            let array = array_reader.next_batch(100).unwrap();
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<ArrowInt32>>()
                .unwrap();

            assert_eq!(
                &PrimitiveArray::<ArrowInt32>::from(data[50..150].to_vec()),
                array
            );

            // Try to read 100 values, however there are only 50 values
            let array = array_reader.next_batch(100).unwrap();
            let array = array
                .as_any()
                .downcast_ref::<PrimitiveArray<ArrowInt32>>()
                .unwrap();

            assert_eq!(
                &PrimitiveArray::<ArrowInt32>::from(data[150..200].to_vec()),
                array
            );
        }
    }

    #[test]
    fn test_primitive_array_reader_def_and_rep_levels() {
        // Construct column schema
        let message_type = "
        message test_schema {
            REPEATED Group test_mid {
                OPTIONAL INT32 leaf;
            }
        }
        ";

        let schema = parse_message_type(message_type)
            .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
            .unwrap();

        let column_desc = schema.column(0);

        // Construct page iterator
        {
            let mut def_levels = Vec::new();
            let mut rep_levels = Vec::new();
            let mut page_lists = Vec::new();
            make_column_chunks::<crate::data_type::Int32Type>(
                column_desc.clone(),
                Encoding::PLAIN,
                100,
                1,
                200,
                &mut def_levels,
                &mut rep_levels,
                &mut Vec::new(),
                &mut page_lists,
                true,
                2,
            );

            let page_iterator =
                InMemoryPageIterator::new(schema, column_desc.clone(), page_lists);

            let converter = PrimitiveArrayConverter::<arrow::datatypes::Int32Type>::new();
            let mut array_reader = ArrowArrayReader::try_new(
                page_iterator, column_desc, converter, None
            ).unwrap();

            let mut accu_len: usize = 0;

            // Read first 50 values, which are all from the first column chunk
            let array = array_reader.next_batch(50).unwrap();
            assert_eq!(
                Some(&def_levels[accu_len..(accu_len + array.len())]),
                array_reader.get_def_levels()
            );
            assert_eq!(
                Some(&rep_levels[accu_len..(accu_len + array.len())]),
                array_reader.get_rep_levels()
            );
            accu_len += array.len();

            // Read next 100 values, the first 50 ones are from the first column chunk,
            // and the last 50 ones are from the second column chunk
            let array = array_reader.next_batch(100).unwrap();
            assert_eq!(
                Some(&def_levels[accu_len..(accu_len + array.len())]),
                array_reader.get_def_levels()
            );
            assert_eq!(
                Some(&rep_levels[accu_len..(accu_len + array.len())]),
                array_reader.get_rep_levels()
            );
            accu_len += array.len();

            // Try to read 100 values, however there are only 50 values
            let array = array_reader.next_batch(100).unwrap();
            assert_eq!(
                Some(&def_levels[accu_len..(accu_len + array.len())]),
                array_reader.get_def_levels()
            );
            assert_eq!(
                Some(&rep_levels[accu_len..(accu_len + array.len())]),
                array_reader.get_rep_levels()
            );
        }
    }

    #[test]
    fn test_arrow_array_reader_string() {
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
        let converter = StringArrayConverter::new();
        let mut array_reader = ArrowArrayReader::try_new(
            page_iterator, column_desc, converter, None
        ).unwrap();

        let mut accu_len: usize = 0;

        let array = array_reader.next_batch(values_per_page / 2).unwrap();
        assert_eq!(array.len(), values_per_page / 2);
        assert_eq!(
            Some(&def_levels[accu_len..(accu_len + array.len())]),
            array_reader.get_def_levels()
        );
        assert_eq!(
            Some(&rep_levels[accu_len..(accu_len + array.len())]),
            array_reader.get_rep_levels()
        );
        accu_len += array.len();

        // Read next values_per_page values, the first values_per_page/2 ones are from the first column chunk,
        // and the last values_per_page/2 ones are from the second column chunk
        let array = array_reader.next_batch(values_per_page).unwrap();
        assert_eq!(array.len(), values_per_page);
        assert_eq!(
            Some(&def_levels[accu_len..(accu_len + array.len())]),
            array_reader.get_def_levels()
        );
        assert_eq!(
            Some(&rep_levels[accu_len..(accu_len + array.len())]),
            array_reader.get_rep_levels()
        );
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
        accu_len += array.len();

        // Try to read values_per_page values, however there are only values_per_page/2 values
        let array = array_reader.next_batch(values_per_page).unwrap();
        assert_eq!(array.len(), values_per_page / 2);
        assert_eq!(
            Some(&def_levels[accu_len..(accu_len + array.len())]),
            array_reader.get_def_levels()
        );
        assert_eq!(
            Some(&rep_levels[accu_len..(accu_len + array.len())]),
            array_reader.get_rep_levels()
        );
    }
}