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

//! Dictionary page pruning for row filters
//!
//! When a predicate targets a dictionary-encoded column, we can evaluate the
//! predicate against the dictionary values before decoding any data pages.
//! If no dictionary values match, the entire column chunk can be skipped.

use crate::arrow::ProjectionMask;
use crate::arrow::array_reader::RowGroups;
use crate::arrow::arrow_reader::ArrowPredicate;
use crate::arrow::arrow_reader::filter::DictionaryPredicateResult;
use crate::arrow::in_memory_row_group::InMemoryRowGroup;
use crate::basic::{Encoding, Type as PhysicalType};
use crate::errors::Result;
use crate::file::metadata::ParquetMetaData;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

/// Try to prune a predicate using dictionary pages from the in-memory row group.
///
/// Returns `Some(DictionaryPredicateResult)` if dictionary pruning was performed,
/// `None` if the column is not dictionary-encoded or pruning is not applicable.
pub(crate) fn try_dictionary_prune_in_memory(
    predicate: &mut dyn ArrowPredicate,
    row_group: &InMemoryRowGroup<'_>,
    metadata: &ParquetMetaData,
    fields: Option<&crate::arrow::schema::ParquetField>,
) -> Result<Option<DictionaryPredicateResult>> {
    let projection = predicate.projection();
    let schema_descr = metadata.file_metadata().schema_descr();

    // Only support single-column predicates
    let col_idx = single_leaf_column(projection, schema_descr.num_columns());
    let Some(col_idx) = col_idx else {
        return Ok(None);
    };

    let row_group_meta = metadata.row_group(row_group.row_group_idx);
    let col_meta = row_group_meta.column(col_idx);

    // Only proceed if the column has a dictionary page
    if col_meta.dictionary_page_offset().is_none() {
        return Ok(None);
    }

    // Only safe to prune if ALL data pages are dictionary-encoded.
    // If some pages fell back to plain encoding, the dictionary doesn't
    // cover all values and we can't safely skip based on dictionary alone.
    if !is_all_dictionary_encoded(col_meta) {
        return Ok(None);
    }

    let physical_type = schema_descr.column(col_idx).physical_type();

    // Only support BYTE_ARRAY and INT32/INT64 columns
    if !matches!(
        physical_type,
        PhysicalType::BYTE_ARRAY | PhysicalType::INT32 | PhysicalType::INT64
    ) {
        return Ok(None);
    }

    // Get the arrow type for this column from the ParquetField tree.
    // Only supports top-level primitive columns (not nested in structs/lists).
    let Some(arrow_type) = fields.and_then(|f| find_top_level_leaf_arrow_type(f, col_idx)) else {
        return Ok(None);
    };

    // Create a page reader for this column
    let mut page_iter = row_group.column_chunks(col_idx)?;
    let Some(page_reader) = page_iter.next() else {
        return Ok(None);
    };
    let mut page_reader = page_reader?;

    // Read the first page - should be the dictionary page
    let first_page = page_reader.get_next_page()?;
    let Some(page) = first_page else {
        return Ok(None);
    };

    if !page.is_dictionary_page() {
        return Ok(None);
    }

    let crate::column::page::Page::DictionaryPage {
        buf, num_values, ..
    } = page
    else {
        return Ok(None);
    };

    // Decode PLAIN-encoded dictionary values based on physical type,
    // then cast to the target arrow type if needed
    let array: ArrayRef = match physical_type {
        PhysicalType::BYTE_ARRAY => {
            decode_plain_byte_array(&buf, num_values as usize, &arrow_type)?
        }
        PhysicalType::INT32 => decode_plain_int32_as(&buf, num_values as usize, &arrow_type)?,
        PhysicalType::INT64 => decode_plain_int64_as(&buf, num_values as usize, &arrow_type)?,
        _ => return Ok(None),
    };

    // Build a RecordBatch with the dictionary values using a synthetic field
    let col_name = schema_descr.column(col_idx).name().to_string();
    let field = Field::new(&col_name, arrow_type, true);
    let schema = Arc::new(Schema::new(vec![field]));
    let batch = RecordBatch::try_new(schema, vec![array]).map_err(|e| {
        crate::errors::ParquetError::General(format!("Failed to create dictionary batch: {}", e))
    })?;

    // Evaluate the predicate against dictionary values
    let result = predicate.evaluate_dictionary(batch).map_err(|e| {
        crate::errors::ParquetError::General(format!(
            "Failed to evaluate dictionary predicate: {}",
            e
        ))
    })?;

    Ok(Some(result))
}

/// Decode PLAIN-encoded BYTE_ARRAY values into a string/binary array
/// matching the target arrow type.
fn decode_plain_byte_array(
    buf: &[u8],
    num_values: usize,
    arrow_type: &DataType,
) -> Result<ArrayRef> {
    // Parse all byte array values
    let mut values: Vec<&[u8]> = Vec::with_capacity(num_values);
    let mut offset = 0;
    for _ in 0..num_values {
        if offset + 4 > buf.len() {
            return Err(crate::errors::ParquetError::EOF(
                "eof decoding dictionary byte array".into(),
            ));
        }
        let len = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if offset + len > buf.len() {
            return Err(crate::errors::ParquetError::EOF(
                "eof decoding dictionary byte array".into(),
            ));
        }
        values.push(&buf[offset..offset + len]);
        offset += len;
    }

    match arrow_type {
        DataType::Utf8View => {
            let mut builder = arrow_array::builder::StringViewBuilder::with_capacity(num_values);
            for v in &values {
                // SAFETY: parquet BYTE_ARRAY dictionary values for string columns are valid UTF-8
                let s = unsafe { std::str::from_utf8_unchecked(v) };
                builder.append_value(s);
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            let strs: Vec<&str> = values
                .iter()
                // SAFETY: parquet BYTE_ARRAY dictionary values for string columns are valid UTF-8
                .map(|v| unsafe { std::str::from_utf8_unchecked(v) })
                .collect();
            Ok(Arc::new(arrow_array::StringArray::from(strs)))
        }
        DataType::BinaryView => {
            let mut builder = arrow_array::builder::BinaryViewBuilder::with_capacity(num_values);
            for v in &values {
                builder.append_value(v);
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => {
            // Default to BinaryArray for unknown types
            let binary_values: Vec<&[u8]> = values;
            Ok(Arc::new(arrow_array::BinaryArray::from(binary_values)))
        }
    }
}

/// Check if all data pages in a column chunk are dictionary-encoded.
///
/// Uses page encoding stats if available, otherwise falls back to checking
/// column-level encodings. Returns false if we can't determine conclusively.
fn is_all_dictionary_encoded(col_meta: &crate::file::metadata::ColumnChunkMetaData) -> bool {
    // No dictionary page -> definitely not all dictionary encoded
    if col_meta.dictionary_page_offset().is_none() {
        return false;
    }

    // Method 1: Use page encoding stats mask if available (most reliable)
    if let Some(mask) = col_meta.page_encoding_stats_mask() {
        return mask.is_only(Encoding::PLAIN_DICTIONARY) || mask.is_only(Encoding::RLE_DICTIONARY);
    }

    // Method 1a: Use full page encoding stats if the mask form wasn't used
    // (i.e. ParquetMetaDataOptions::with_encoding_stats_as_mask was set to false)
    if let Some(stats) = col_meta.page_encoding_stats() {
        return stats
            .iter()
            .filter(|s| {
                matches!(
                    s.page_type,
                    crate::basic::PageType::DATA_PAGE | crate::basic::PageType::DATA_PAGE_V2
                )
            })
            .all(|s| {
                matches!(
                    s.encoding,
                    Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY
                )
            });
    }

    // Method 2: Check column-level encodings.
    // Dictionary-encoded columns have PLAIN_DICTIONARY or RLE_DICTIONARY for data,
    // plus PLAIN and/or RLE for definition/repetition levels.
    // If PLAIN appears as a data encoding (fallback), the column also has
    // PLAIN_DICTIONARY/RLE_DICTIONARY, so we'd see both.
    // Safe heuristic: only dictionary + level encodings = all dict encoded.
    let mut has_dict_data_encoding = false;
    let mut has_non_dict_data_encoding = false;
    for enc in col_meta.encodings() {
        match enc {
            Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY => {
                has_dict_data_encoding = true;
            }
            // These are used for definition/repetition levels, not data
            #[allow(deprecated)]
            Encoding::RLE | Encoding::BIT_PACKED => {}
            // PLAIN is ambiguous - used for def/rep levels in V1 pages AND
            // as a fallback data encoding. We allow ONE PLAIN if there's also
            // a dictionary encoding, since the PLAIN is likely for levels.
            Encoding::PLAIN => {
                // PLAIN is expected for def/rep levels. We can't distinguish
                // from data fallback here, so we allow it if dict encoding exists.
                // This is a heuristic that works for most files.
            }
            // Any other encoding (DELTA_*, etc.) means non-dictionary data
            _ => {
                has_non_dict_data_encoding = true;
            }
        }
    }

    has_dict_data_encoding && !has_non_dict_data_encoding
}

/// Decode PLAIN-encoded INT32 values, casting to the target arrow type
fn decode_plain_int32_as(buf: &[u8], num_values: usize, arrow_type: &DataType) -> Result<ArrayRef> {
    if buf.len() < num_values * 4 {
        return Err(crate::errors::ParquetError::EOF(
            "eof decoding dictionary int32".into(),
        ));
    }
    let values: Vec<i32> = (0..num_values)
        .map(|i| i32::from_le_bytes(buf[i * 4..(i + 1) * 4].try_into().unwrap()))
        .collect();
    match arrow_type {
        DataType::Int8 => Ok(Arc::new(arrow_array::Int8Array::from(
            values.into_iter().map(|v| v as i8).collect::<Vec<_>>(),
        ))),
        DataType::Int16 => Ok(Arc::new(arrow_array::Int16Array::from(
            values.into_iter().map(|v| v as i16).collect::<Vec<_>>(),
        ))),
        DataType::UInt8 => Ok(Arc::new(arrow_array::UInt8Array::from(
            values.into_iter().map(|v| v as u8).collect::<Vec<_>>(),
        ))),
        DataType::UInt16 => Ok(Arc::new(arrow_array::UInt16Array::from(
            values.into_iter().map(|v| v as u16).collect::<Vec<_>>(),
        ))),
        DataType::UInt32 => Ok(Arc::new(arrow_array::UInt32Array::from(
            values.into_iter().map(|v| v as u32).collect::<Vec<_>>(),
        ))),
        _ => Ok(Arc::new(arrow_array::Int32Array::from(values))),
    }
}

/// Decode PLAIN-encoded INT64 values, casting to the target arrow type
fn decode_plain_int64_as(buf: &[u8], num_values: usize, arrow_type: &DataType) -> Result<ArrayRef> {
    if buf.len() < num_values * 8 {
        return Err(crate::errors::ParquetError::EOF(
            "eof decoding dictionary int64".into(),
        ));
    }
    let values: Vec<i64> = (0..num_values)
        .map(|i| i64::from_le_bytes(buf[i * 8..(i + 1) * 8].try_into().unwrap()))
        .collect();
    match arrow_type {
        DataType::Int32 => Ok(Arc::new(arrow_array::Int32Array::from(
            values.into_iter().map(|v| v as i32).collect::<Vec<_>>(),
        ))),
        DataType::Timestamp(unit, tz) => {
            use arrow_array::TimestampMicrosecondArray;
            use arrow_array::TimestampMillisecondArray;
            use arrow_array::TimestampNanosecondArray;
            use arrow_array::TimestampSecondArray;
            use arrow_schema::TimeUnit;
            match unit {
                TimeUnit::Second => Ok(Arc::new(
                    TimestampSecondArray::from(values).with_timezone_opt(tz.clone()),
                )),
                TimeUnit::Millisecond => Ok(Arc::new(
                    TimestampMillisecondArray::from(values).with_timezone_opt(tz.clone()),
                )),
                TimeUnit::Microsecond => Ok(Arc::new(
                    TimestampMicrosecondArray::from(values).with_timezone_opt(tz.clone()),
                )),
                TimeUnit::Nanosecond => Ok(Arc::new(
                    TimestampNanosecondArray::from(values).with_timezone_opt(tz.clone()),
                )),
            }
        }
        _ => Ok(Arc::new(arrow_array::Int64Array::from(values))),
    }
}

/// Find the arrow DataType for a specific leaf column index, but only if it's
/// a direct child of the root (not nested inside a struct/list/map).
/// Returns None for nested columns to avoid applying dictionary pruning to them.
fn find_top_level_leaf_arrow_type(
    root: &crate::arrow::schema::ParquetField,
    target_col_idx: usize,
) -> Option<DataType> {
    use crate::arrow::schema::ParquetFieldType;
    // root must be a group (the schema root)
    let ParquetFieldType::Group { children } = &root.field_type else {
        return None;
    };
    // Only check direct children — skip nested fields
    for child in children {
        if let ParquetFieldType::Primitive { col_idx, .. } = &child.field_type {
            if *col_idx == target_col_idx {
                return Some(child.arrow_type.clone());
            }
        }
        // If the child is a Group (struct/list), don't recurse —
        // we only support top-level primitives for dictionary pruning
    }
    None
}

/// Returns the single leaf column index if the projection mask selects exactly one leaf.
fn single_leaf_column(mask: &ProjectionMask, num_columns: usize) -> Option<usize> {
    let mut found = None;
    for i in 0..num_columns {
        if mask.leaf_included(i) {
            if found.is_some() {
                return None; // more than one column
            }
            found = Some(i);
        }
    }
    found
}
