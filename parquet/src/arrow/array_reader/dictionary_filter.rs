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

//! Decoder-level dictionary filter pushdown for Parquet reader.
//!
//! Evaluates predicates on dictionary values during decoding, producing a
//! `BooleanArray` directly from integer keys — no intermediate `DictionaryArray`
//! or `StringViewArray` is created for data rows.

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, StringViewArray, StructArray};
use arrow_buffer::BooleanBuffer;
use arrow_buffer::bit_util;
use arrow_schema::{DataType as ArrowType, Field, Fields, Schema};
use bytes::Bytes;

use crate::arrow::ProjectionMask;
use crate::arrow::array_reader::byte_view_array::ByteViewArrayDecoderPlain;
use crate::arrow::array_reader::{ArrayReader, RowGroups};
use crate::arrow::arrow_reader::ArrowPredicate;
use crate::arrow::buffer::view_buffer::ViewBuffer;
use crate::arrow::decoder::DictIndexDecoder;
use crate::arrow::record_reader::{DefinitionLevelBuffer, DefinitionLevelBufferDecoder};
use crate::basic::{ConvertedType, Encoding};
use crate::column::page::{Page, PageIterator, PageReader};
use crate::column::reader::GenericColumnReader;
use crate::column::reader::decoder::{ColumnValueDecoder, RepetitionLevelDecoderImpl};
use crate::errors::{ParquetError, Result};
use crate::file::metadata::ParquetMetaData;
use crate::schema::types::ColumnDescPtr;

/// A [`ColumnValueDecoder`] that maps dictionary indices to pre-computed
/// boolean filter results.
///
/// Phase 1 (in [`make_dictionary_filter_reader`]) evaluates the predicate on
/// dictionary values and produces `matching_keys: Vec<bool>`. Phase 2 uses
/// this decoder to read RLE-encoded integer keys and look up the boolean
/// result for each key.
struct DictFilterDecoder {
    /// Pre-computed filter results indexed by dictionary key.
    /// `matching_keys[key]` is `true` if that dictionary value matched the predicate.
    matching_keys: Vec<bool>,
    /// Current data page decoder for dictionary indices.
    decoder: Option<DictIndexDecoder>,
}

impl ColumnValueDecoder for DictFilterDecoder {
    type Buffer = Vec<bool>;

    fn new(_col: &ColumnDescPtr) -> Self {
        Self {
            matching_keys: Vec::new(),
            decoder: None,
        }
    }

    fn set_dict(
        &mut self,
        _buf: Bytes,
        _num_values: u32,
        encoding: Encoding,
        _is_sorted: bool,
    ) -> Result<()> {
        // matching_keys was already computed in Phase 1. Just validate encoding.
        if !matches!(
            encoding,
            Encoding::PLAIN | Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY
        ) {
            return Err(nyi_err!(
                "Invalid/Unsupported encoding type for dictionary: {}",
                encoding
            ));
        }
        Ok(())
    }

    fn set_data(
        &mut self,
        encoding: Encoding,
        data: Bytes,
        num_levels: usize,
        num_values: Option<usize>,
    ) -> Result<()> {
        match encoding {
            Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY => {
                self.decoder = Some(DictIndexDecoder::new(data, num_levels, num_values)?);
                Ok(())
            }
            other => Err(nyi_err!(
                "DictFilterDecoder does not support encoding: {}",
                other
            )),
        }
    }

    fn read(&mut self, out: &mut Self::Buffer, num_values: usize) -> Result<usize> {
        let decoder = self
            .decoder
            .as_mut()
            .ok_or_else(|| general_err!("no decoder set"))?;

        let matching_keys = &self.matching_keys;
        decoder.read(num_values, |keys: &[i32]| {
            out.extend(
                keys.iter()
                    .map(|&k| matching_keys.get(k as usize).copied().unwrap_or(false)),
            );
            Ok(())
        })
    }

    fn skip_values(&mut self, num_values: usize) -> Result<usize> {
        let decoder = self
            .decoder
            .as_mut()
            .ok_or_else(|| general_err!("no decoder set"))?;
        decoder.skip(num_values)
    }
}

impl DictFilterDecoder {
    fn with_matching_keys(matching_keys: Vec<bool>) -> Self {
        Self {
            matching_keys,
            decoder: None,
        }
    }
}

/// An [`ArrayReader`] that produces `BooleanArray` from dictionary-encoded
/// columns by mapping pre-computed dictionary filter results to row-level
/// booleans.
///
/// This avoids constructing any `DictionaryArray`, `StringViewArray`, or
/// other intermediate arrays for data rows.
struct DictionaryFilterArrayReader {
    /// The struct data type wrapping a single Boolean field.
    data_type: ArrowType,
    /// Page iterator for reading data pages.
    pages: Box<dyn PageIterator>,
    /// Column descriptor.
    column_desc: ColumnDescPtr,
    /// Pre-computed matching keys per row group, consumed in order.
    matching_keys_per_rg: VecDeque<Vec<bool>>,
    /// Current column reader (created per row group).
    column_reader: Option<
        GenericColumnReader<
            RepetitionLevelDecoderImpl,
            DefinitionLevelBufferDecoder,
            DictFilterDecoder,
        >,
    >,
    /// Buffer for decoded boolean values.
    bool_buffer: Vec<bool>,
    /// Definition level buffer (for nullable columns).
    def_level_buffer: Option<DefinitionLevelBuffer>,
    /// Definition levels from the last consume_batch call.
    def_levels_buffer: Option<Vec<i16>>,
    /// Repetition levels from the last consume_batch call.
    rep_levels_buffer: Option<Vec<i16>>,
    /// Number of levels (null + non-null) written into bool_buffer.
    num_levels: usize,
    /// Number of records read.
    num_records: usize,
}

impl DictionaryFilterArrayReader {
    fn new(
        pages: Box<dyn PageIterator>,
        column_desc: ColumnDescPtr,
        matching_keys_per_rg: VecDeque<Vec<bool>>,
    ) -> Self {
        let has_def_levels = column_desc.max_def_level() > 0;
        let data_type = ArrowType::Struct(Fields::from(vec![Field::new(
            "_filter",
            ArrowType::Boolean,
            false,
        )]));
        Self {
            data_type,
            pages,
            column_desc: column_desc.clone(),
            matching_keys_per_rg,
            column_reader: None,
            bool_buffer: Vec::new(),
            def_level_buffer: if has_def_levels {
                Some(DefinitionLevelBuffer::new(&column_desc, true))
            } else {
                None
            },
            def_levels_buffer: None,
            rep_levels_buffer: None,
            num_levels: 0,
            num_records: 0,
        }
    }

    /// Ensure we have a column_reader, advancing to the next row group if needed.
    /// Returns false if no more data is available.
    fn ensure_column_reader(&mut self) -> Result<bool> {
        if self.column_reader.is_some() {
            return Ok(true);
        }

        let matching_keys = match self.matching_keys_per_rg.pop_front() {
            Some(keys) => keys,
            None => return Ok(false),
        };
        let page_reader = match self.pages.next() {
            Some(reader) => reader?,
            None => return Ok(false),
        };

        let values_decoder = DictFilterDecoder::with_matching_keys(matching_keys);

        let desc = &self.column_desc;
        let def_level_decoder = (desc.max_def_level() != 0)
            .then(|| DefinitionLevelBufferDecoder::new(desc.max_def_level(), true));
        let rep_level_decoder = (desc.max_rep_level() != 0)
            .then(|| RepetitionLevelDecoderImpl::new(desc.max_rep_level()));

        self.column_reader = Some(GenericColumnReader::new_with_decoders(
            desc.clone(),
            page_reader,
            values_decoder,
            def_level_decoder,
            rep_level_decoder,
        ));

        Ok(true)
    }
}

impl ArrayReader for DictionaryFilterArrayReader {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_data_type(&self) -> &ArrowType {
        &self.data_type
    }

    fn read_records(&mut self, batch_size: usize) -> Result<usize> {
        let mut records_read = 0usize;
        while records_read < batch_size {
            if !self.ensure_column_reader()? {
                break;
            }

            let column_reader = self.column_reader.as_mut().unwrap();
            let records_to_read = batch_size - records_read;

            let (records, values_read, levels_read) = column_reader.read_records(
                records_to_read,
                self.def_level_buffer.as_mut(),
                None,
                &mut self.bool_buffer,
            )?;

            // Expand packed non-null values to sparse positions, placing
            // `false` at null positions. The BooleanArray we produce has no
            // null buffer, so nulls must be materialized as `false` values.
            if values_read < levels_read {
                let def_levels = self.def_level_buffer.as_ref().ok_or_else(|| {
                    general_err!("Definition levels should exist when data is less than levels!")
                })?;
                let null_mask = def_levels.nulls().as_slice();
                let start = self.num_levels;
                self.bool_buffer.resize(start + levels_read, false);

                // Work backwards to avoid overwriting unprocessed values.
                // null_mask is cumulative across iterations, so bit positions
                // are offset by `start`.
                let mut val_idx = values_read;
                for level_idx in (0..levels_read).rev() {
                    if bit_util::get_bit(null_mask, start + level_idx) {
                        val_idx -= 1;
                        self.bool_buffer[start + level_idx] = self.bool_buffer[start + val_idx];
                    } else {
                        self.bool_buffer[start + level_idx] = false;
                    }
                }
                debug_assert_eq!(val_idx, 0);
            }

            self.num_levels += levels_read;
            records_read += records;
            self.num_records += records;

            if records < records_to_read {
                self.column_reader = None;
            }
        }
        Ok(records_read)
    }

    fn consume_batch(&mut self) -> Result<ArrayRef> {
        let values = std::mem::take(&mut self.bool_buffer);
        let boolean_buffer = BooleanBuffer::from(values);
        let bool_array: ArrayRef = Arc::new(BooleanArray::new(boolean_buffer, None));

        if let Some(def_buf) = self.def_level_buffer.as_mut() {
            self.def_levels_buffer = def_buf.consume_levels();
            let _ = def_buf.consume_bitmask();
        }

        self.num_levels = 0;
        self.num_records = 0;

        // Wrap in StructArray for ParquetRecordBatchReader compatibility
        let struct_array = StructArray::new(
            Fields::from(vec![Field::new("_filter", ArrowType::Boolean, false)]),
            vec![bool_array],
            None,
        );
        Ok(Arc::new(struct_array))
    }

    fn skip_records(&mut self, num_records: usize) -> Result<usize> {
        let mut records_skipped = 0usize;
        while records_skipped < num_records {
            if !self.ensure_column_reader()? {
                break;
            }

            let column_reader = self.column_reader.as_mut().unwrap();
            let records_to_skip = num_records - records_skipped;

            let skipped = column_reader.skip_records(records_to_skip)?;
            records_skipped += skipped;

            if skipped < records_to_skip {
                self.column_reader = None;
            }
        }
        Ok(records_skipped)
    }

    fn get_def_levels(&self) -> Option<&[i16]> {
        self.def_levels_buffer.as_deref()
    }

    fn get_rep_levels(&self) -> Option<&[i16]> {
        self.rep_levels_buffer.as_deref()
    }
}

/// Creates a [`DictionaryFilterArrayReader`] that evaluates a predicate on
/// dictionary values and produces row-level `BooleanArray` filters.
///
/// **Phase 1:** For each row group, reads the dictionary page, decodes it to
/// a `StringViewArray`, evaluates the predicate, and collects `matching_keys`.
///
/// **Phase 2:** Creates a `DictionaryFilterArrayReader` with a second page
/// iterator that reads integer keys and maps them to booleans via the
/// pre-computed `matching_keys`.
pub(crate) fn make_dictionary_filter_reader(
    row_groups: &dyn RowGroups,
    projection: &ProjectionMask,
    predicate: &mut dyn ArrowPredicate,
    metadata: &ParquetMetaData,
) -> Result<Box<dyn ArrayReader>> {
    let schema_descr = metadata.file_metadata().schema_descr();
    let num_columns = schema_descr.num_columns();
    let col_idx = (0..num_columns)
        .find(|&i| projection.leaf_included(i))
        .ok_or_else(|| {
            ParquetError::General("No column found in projection for dictionary filter".to_string())
        })?;

    let column_desc = schema_descr.column(col_idx);
    let col_name = column_desc.name().to_string();
    let is_utf8 = column_desc.converted_type() == ConvertedType::UTF8
        || matches!(
            column_desc.logical_type_ref(),
            Some(crate::basic::LogicalType::String) | Some(crate::basic::LogicalType::Json)
        );

    // Phase 1: Pre-evaluate predicate on dictionary values for each row group
    let mut matching_keys_per_rg = VecDeque::new();
    let mut dict_pages = row_groups.column_chunks(col_idx)?;

    for _rg in row_groups.row_groups() {
        let page_reader = dict_pages.next().ok_or_else(|| {
            ParquetError::General("Missing page reader for row group".to_string())
        })??;

        let matching_keys = evaluate_dict_predicate(page_reader, predicate, &col_name, is_utf8)?;
        matching_keys_per_rg.push_back(matching_keys);
    }

    // Phase 2: Create a second page iterator for reading data pages
    let data_pages = row_groups.column_chunks(col_idx)?;

    Ok(Box::new(DictionaryFilterArrayReader::new(
        data_pages,
        schema_descr.column(col_idx).clone(),
        matching_keys_per_rg,
    )))
}

/// Read the dictionary page from a page reader, decode it, evaluate the
/// predicate, and return a `Vec<bool>` where `matching_keys[i]` indicates
/// whether dictionary value `i` matched the predicate.
fn evaluate_dict_predicate(
    mut page_reader: Box<dyn PageReader>,
    predicate: &mut dyn ArrowPredicate,
    col_name: &str,
    is_utf8: bool,
) -> Result<Vec<bool>> {
    let (buf, num_values) = match page_reader.get_next_page()? {
        Some(Page::DictionaryPage {
            buf, num_values, ..
        }) => (buf, num_values),
        // Per Parquet spec, dictionary page always comes first.
        // If we see a data page or no page, there is no dictionary.
        _ => return Ok(Vec::new()),
    };

    let validate_utf8 = is_utf8;
    let mut view_buffer = ViewBuffer::default();
    let mut decoder = ByteViewArrayDecoderPlain::new(
        buf,
        num_values as usize,
        Some(num_values as usize),
        validate_utf8,
    );
    decoder.read(&mut view_buffer, usize::MAX)?;

    let array: ArrayRef = if is_utf8 {
        view_buffer.into_array(None, &ArrowType::Utf8View)
    } else {
        // Binary columns: reinterpret BinaryView as Utf8View (identical layout).
        // Safety: matches the non-dict decoder which uses new_unchecked for
        // ConvertedType::NONE columns without UTF-8 validation.
        let binary_array = view_buffer.into_array(None, &ArrowType::BinaryView);
        let binary_view = binary_array
            .as_any()
            .downcast_ref::<arrow_array::BinaryViewArray>()
            .unwrap();
        let data = binary_view.to_data();
        let (_, len, nulls, offset, buffers, _) = data.into_parts();
        debug_assert_eq!(
            offset, 0,
            "Expected zero offset from freshly created BinaryViewArray"
        );
        let builder = arrow_data::ArrayDataBuilder::new(ArrowType::Utf8View)
            .len(len)
            .buffers(buffers)
            .nulls(nulls);
        let string_data = unsafe { builder.build_unchecked() };
        Arc::new(StringViewArray::from(string_data))
    };

    let field = Field::new(col_name, ArrowType::Utf8View, false);
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(schema, vec![array]).map_err(|e| {
        ParquetError::ArrowError(format!("Failed to create dict predicate batch: {e}"))
    })?;

    let result = predicate
        .evaluate(batch)
        .map_err(|e| ParquetError::ArrowError(format!("Dict predicate evaluation failed: {e}")))?;

    // Treat nulls in predicate result as false (non-matching)
    Ok((0..result.len())
        .map(|i| result.is_valid(i) && result.value(i))
        .collect::<Vec<bool>>())
}
