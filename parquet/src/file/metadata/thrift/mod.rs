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

//! This module is the bridge between a Parquet file's thrift encoded metadata
//! and this crate's [Parquet metadata API]. It contains objects and functions used
//! to serialize/deserialize metadata objects into/from the Thrift compact protocol
//! format as defined by the [Parquet specification].
//!
//! [Parquet metadata API]: crate::file::metadata
//! [Parquet specification]: https://github.com/apache/parquet-format/tree/master

use std::io::Write;
use std::sync::Arc;

#[cfg(feature = "encryption")]
pub(crate) mod encryption;

#[cfg(feature = "encryption")]
use crate::file::{
    column_crypto_metadata::ColumnCryptoMetaData, metadata::thrift::encryption::EncryptionAlgorithm,
};
use crate::{
    basic::{
        ColumnOrder, Compression, ConvertedType, Encoding, EncodingMask, LogicalType, PageType,
        Repetition, Type,
    },
    data_type::{ByteArray, FixedLenByteArray, Int96},
    errors::{ParquetError, Result},
    file::{
        metadata::{
            ColumnChunkMetaData, ColumnChunkMetaDataBuilder, KeyValue, LevelHistogram,
            PageEncodingStats, ParquetMetaData, ParquetMetaDataOptions, ParquetPageEncodingStats,
            RowGroupMetaData, RowGroupMetaDataBuilder, SortingColumn,
        },
        statistics::ValueStatistics,
    },
    parquet_thrift::{
        ElementType, FieldType, ReadThrift, ThriftCompactInputProtocol,
        ThriftCompactOutputProtocol, ThriftSliceInputProtocol, WriteThrift, WriteThriftField,
        read_thrift_vec,
    },
    schema::types::{
        ColumnDescriptor, SchemaDescriptor, TypePtr, num_nodes, parquet_schema_from_array,
    },
    thrift_struct,
    util::bit_util::FromBytes,
    write_thrift_field,
};

// this needs to be visible to the schema conversion code
thrift_struct!(
pub(crate) struct SchemaElement<'a> {
  /// Data type for this field. Not set if the current element is a non-leaf node
  1: optional Type r#type;
  /// If type is FIXED_LEN_BYTE_ARRAY, this is the byte length of the values.
  /// Otherwise, if specified, this is the maximum bit length to store any of the values.
  /// (e.g. a low cardinality INT col could have this set to 3).  Note that this is
  /// in the schema, and therefore fixed for the entire file.
  2: optional i32 type_length;
  /// Repetition of the field. The root of the schema does not have a repetition_type.
  /// All other nodes must have one.
  3: optional Repetition repetition_type;
  /// Name of the field in the schema
  4: required string<'a> name;
  /// Nested fields. Since thrift does not support nested fields,
  /// the nesting is flattened to a single list by a depth-first traversal.
  /// The children count is used to construct the nested relationship.
  /// This field is not set when the element is a primitive type.
  5: optional i32 num_children;
  /// DEPRECATED: When the schema is the result of a conversion from another model.
  /// Used to record the original type to help with cross conversion.
  ///
  /// This is superseded by logical_type.
  6: optional ConvertedType converted_type;
  /// DEPRECATED: Used when this column contains decimal data.
  /// See the DECIMAL converted type for more details.
  ///
  /// This is superseded by using the DecimalType annotation in logical_type.
  7: optional i32 scale
  8: optional i32 precision
  /// When the original schema supports field ids, this will save the
  /// original field id in the parquet schema
  9: optional i32 field_id;
  /// The logical type of this SchemaElement
  ///
  /// LogicalType replaces ConvertedType, but ConvertedType is still required
  /// for some logical types to ensure forward-compatibility in format v1.
  10: optional LogicalType logical_type
}
);

thrift_struct!(
struct Statistics<'a> {
   1: optional binary<'a> max;
   2: optional binary<'a> min;
   3: optional i64 null_count;
   4: optional i64 distinct_count;
   5: optional binary<'a> max_value;
   6: optional binary<'a> min_value;
   7: optional bool is_max_value_exact;
   8: optional bool is_min_value_exact;
}
);

thrift_struct!(
struct BoundingBox {
  1: required double xmin;
  2: required double xmax;
  3: required double ymin;
  4: required double ymax;
  5: optional double zmin;
  6: optional double zmax;
  7: optional double mmin;
  8: optional double mmax;
}
);

thrift_struct!(
struct GeospatialStatistics {
  1: optional BoundingBox bbox;
  2: optional list<i32> geospatial_types;
}
);

thrift_struct!(
struct SizeStatistics {
   1: optional i64 unencoded_byte_array_data_bytes;
   2: optional list<i64> repetition_level_histogram;
   3: optional list<i64> definition_level_histogram;
}
);

fn convert_geo_stats(
    stats: Option<GeospatialStatistics>,
) -> Option<Box<crate::geospatial::statistics::GeospatialStatistics>> {
    stats.map(|st| {
        let bbox = convert_bounding_box(st.bbox);
        let geospatial_types: Option<Vec<i32>> = st.geospatial_types.filter(|v| !v.is_empty());
        Box::new(crate::geospatial::statistics::GeospatialStatistics::new(
            bbox,
            geospatial_types,
        ))
    })
}

fn convert_bounding_box(
    bbox: Option<BoundingBox>,
) -> Option<crate::geospatial::bounding_box::BoundingBox> {
    bbox.map(|bb| {
        let mut newbb = crate::geospatial::bounding_box::BoundingBox::new(
            bb.xmin.into(),
            bb.xmax.into(),
            bb.ymin.into(),
            bb.ymax.into(),
        );

        newbb = match (bb.zmin, bb.zmax) {
            (Some(zmin), Some(zmax)) => newbb.with_zrange(zmin.into(), zmax.into()),
            // If either None or mismatch, leave it as None and don't error
            _ => newbb,
        };

        newbb = match (bb.mmin, bb.mmax) {
            (Some(mmin), Some(mmax)) => newbb.with_mrange(mmin.into(), mmax.into()),
            // If either None or mismatch, leave it as None and don't error
            _ => newbb,
        };

        newbb
    })
}

/// Create a [`crate::file::statistics::Statistics`] from a thrift [`Statistics`] object.
fn convert_stats(
    column_descr: &Arc<ColumnDescriptor>,
    thrift_stats: Option<Statistics>,
) -> Result<Option<crate::file::statistics::Statistics>> {
    use crate::file::statistics::Statistics as FStatistics;
    Ok(match thrift_stats {
        Some(stats) => {
            // Number of nulls recorded, when it is not available, we just mark it as 0.
            // TODO this should be `None` if there is no information about NULLS.
            // see https://github.com/apache/arrow-rs/pull/6216/files
            let null_count = stats.null_count.unwrap_or(0);

            if null_count < 0 {
                return Err(general_err!(
                    "Statistics null count is negative {}",
                    null_count
                ));
            }

            // Generic null count.
            let null_count = Some(null_count as u64);
            // Generic distinct count (count of distinct values occurring)
            let distinct_count = stats.distinct_count.map(|value| value as u64);
            // Whether or not statistics use deprecated min/max fields.
            let old_format = stats.min_value.is_none() && stats.max_value.is_none();
            // Generic min value as bytes.
            let min = if old_format {
                stats.min
            } else {
                stats.min_value
            };
            // Generic max value as bytes.
            let max = if old_format {
                stats.max
            } else {
                stats.max_value
            };

            fn check_len(min: &Option<&[u8]>, max: &Option<&[u8]>, len: usize) -> Result<()> {
                if let Some(min) = min {
                    if min.len() < len {
                        return Err(general_err!("Insufficient bytes to parse min statistic",));
                    }
                }
                if let Some(max) = max {
                    if max.len() < len {
                        return Err(general_err!("Insufficient bytes to parse max statistic",));
                    }
                }
                Ok(())
            }

            let physical_type = column_descr.physical_type();
            match physical_type {
                Type::BOOLEAN => check_len(&min, &max, 1),
                Type::INT32 | Type::FLOAT => check_len(&min, &max, 4),
                Type::INT64 | Type::DOUBLE => check_len(&min, &max, 8),
                Type::INT96 => check_len(&min, &max, 12),
                _ => Ok(()),
            }?;

            // Values are encoded using PLAIN encoding definition, except that
            // variable-length byte arrays do not include a length prefix.
            //
            // Instead of using actual decoder, we manually convert values.
            let res = match physical_type {
                Type::BOOLEAN => FStatistics::boolean(
                    min.map(|data| data[0] != 0),
                    max.map(|data| data[0] != 0),
                    distinct_count,
                    null_count,
                    old_format,
                ),
                Type::INT32 => FStatistics::int32(
                    min.map(|data| i32::from_le_bytes(data[..4].try_into().unwrap())),
                    max.map(|data| i32::from_le_bytes(data[..4].try_into().unwrap())),
                    distinct_count,
                    null_count,
                    old_format,
                ),
                Type::INT64 => FStatistics::int64(
                    min.map(|data| i64::from_le_bytes(data[..8].try_into().unwrap())),
                    max.map(|data| i64::from_le_bytes(data[..8].try_into().unwrap())),
                    distinct_count,
                    null_count,
                    old_format,
                ),
                Type::INT96 => {
                    // INT96 statistics may not be correct, because comparison is signed
                    let min = if let Some(data) = min {
                        assert_eq!(data.len(), 12);
                        Some(Int96::try_from_le_slice(data)?)
                    } else {
                        None
                    };
                    let max = if let Some(data) = max {
                        assert_eq!(data.len(), 12);
                        Some(Int96::try_from_le_slice(data)?)
                    } else {
                        None
                    };
                    FStatistics::int96(min, max, distinct_count, null_count, old_format)
                }
                Type::FLOAT => FStatistics::float(
                    min.map(|data| f32::from_le_bytes(data[..4].try_into().unwrap())),
                    max.map(|data| f32::from_le_bytes(data[..4].try_into().unwrap())),
                    distinct_count,
                    null_count,
                    old_format,
                ),
                Type::DOUBLE => FStatistics::double(
                    min.map(|data| f64::from_le_bytes(data[..8].try_into().unwrap())),
                    max.map(|data| f64::from_le_bytes(data[..8].try_into().unwrap())),
                    distinct_count,
                    null_count,
                    old_format,
                ),
                Type::BYTE_ARRAY => FStatistics::ByteArray(
                    ValueStatistics::new(
                        min.map(ByteArray::from),
                        max.map(ByteArray::from),
                        distinct_count,
                        null_count,
                        old_format,
                    )
                    .with_max_is_exact(stats.is_max_value_exact.unwrap_or(false))
                    .with_min_is_exact(stats.is_min_value_exact.unwrap_or(false)),
                ),
                Type::FIXED_LEN_BYTE_ARRAY => FStatistics::FixedLenByteArray(
                    ValueStatistics::new(
                        min.map(ByteArray::from).map(FixedLenByteArray::from),
                        max.map(ByteArray::from).map(FixedLenByteArray::from),
                        distinct_count,
                        null_count,
                        old_format,
                    )
                    .with_max_is_exact(stats.is_max_value_exact.unwrap_or(false))
                    .with_min_is_exact(stats.is_min_value_exact.unwrap_or(false)),
                ),
            };

            Some(res)
        }
        None => None,
    })
}

// bit positions for required fields in the Thrift ColumnMetaData struct
const COL_META_TYPE: u16 = 1 << 1;
const COL_META_ENCODINGS: u16 = 1 << 2;
const COL_META_CODEC: u16 = 1 << 4;
const COL_META_NUM_VALUES: u16 = 1 << 5;
const COL_META_TOTAL_UNCOMP_SZ: u16 = 1 << 6;
const COL_META_TOTAL_COMP_SZ: u16 = 1 << 7;
const COL_META_DATA_PAGE_OFFSET: u16 = 1 << 9;

// a mask where all required fields' bits are set
const COL_META_ALL_REQUIRED: u16 = COL_META_TYPE
    | COL_META_ENCODINGS
    | COL_META_CODEC
    | COL_META_NUM_VALUES
    | COL_META_TOTAL_UNCOMP_SZ
    | COL_META_TOTAL_COMP_SZ
    | COL_META_DATA_PAGE_OFFSET;

// check mask to see if all required fields are set. return an appropriate error if
// any are missing.
fn validate_column_metadata(mask: u16) -> Result<()> {
    if mask != COL_META_ALL_REQUIRED {
        if mask & COL_META_ENCODINGS == 0 {
            return Err(general_err!("Required field encodings is missing"));
        }

        if mask & COL_META_CODEC == 0 {
            return Err(general_err!("Required field codec is missing"));
        }
        if mask & COL_META_NUM_VALUES == 0 {
            return Err(general_err!("Required field num_values is missing"));
        }
        if mask & COL_META_TOTAL_UNCOMP_SZ == 0 {
            return Err(general_err!(
                "Required field total_uncompressed_size is missing"
            ));
        }
        if mask & COL_META_TOTAL_COMP_SZ == 0 {
            return Err(general_err!(
                "Required field total_compressed_size is missing"
            ));
        }
        if mask & COL_META_DATA_PAGE_OFFSET == 0 {
            return Err(general_err!("Required field data_page_offset is missing"));
        }
    }

    Ok(())
}

fn read_encoding_stats_as_mask<'a>(
    prot: &mut ThriftSliceInputProtocol<'a>,
) -> Result<EncodingMask> {
    // read the vector of stats, setting mask bits for data pages
    let mut mask = 0i32;
    let list_ident = prot.read_list_begin()?;
    for _ in 0..list_ident.size {
        let pes = PageEncodingStats::read_thrift(prot)?;
        match pes.page_type {
            PageType::DATA_PAGE | PageType::DATA_PAGE_V2 => mask |= 1 << pes.encoding as i32,
            _ => {}
        }
    }
    EncodingMask::try_new(mask)
}

// Decode `ColumnMetaData`. Returns a mask of all required fields that were observed.
// This mask can be passed to `validate_column_metadata`.
fn read_column_metadata<'a>(
    prot: &mut ThriftSliceInputProtocol<'a>,
    column: &mut ColumnChunkMetaData,
    col_index: usize,
    options: Option<&ParquetMetaDataOptions>,
) -> Result<u16> {
    // mask for seen required fields in ColumnMetaData
    let mut seen_mask = 0u16;

    let mut skip_pes = false;
    let mut pes_mask = false;

    if let Some(opts) = options {
        skip_pes = opts.skip_encoding_stats(col_index);
        pes_mask = opts.encoding_stats_as_mask();
    }

    // struct ColumnMetaData {
    //   1: required Type type
    //   2: required list<Encoding> encodings
    //   3: required list<string> path_in_schema
    //   4: required CompressionCodec codec
    //   5: required i64 num_values
    //   6: required i64 total_uncompressed_size
    //   7: required i64 total_compressed_size
    //   8: optional list<KeyValue> key_value_metadata
    //   9: required i64 data_page_offset
    //   10: optional i64 index_page_offset
    //   11: optional i64 dictionary_page_offset
    //   12: optional Statistics statistics;
    //   13: optional list<PageEncodingStats> encoding_stats;
    //   14: optional i64 bloom_filter_offset;
    //   15: optional i32 bloom_filter_length;
    //   16: optional SizeStatistics size_statistics;
    //   17: optional GeospatialStatistics geospatial_statistics;
    // }
    let column_descr = &column.column_descr;

    let mut last_field_id = 0i16;
    loop {
        let field_ident = prot.read_field_begin(last_field_id)?;
        if field_ident.field_type == FieldType::Stop {
            break;
        }
        match field_ident.id {
            // 1: type is never used, we can use the column descriptor
            1 => {
                // read for error handling
                Type::read_thrift(&mut *prot)?;
                seen_mask |= COL_META_TYPE;
            }
            2 => {
                column.encodings = EncodingMask::read_thrift(&mut *prot)?;
                seen_mask |= COL_META_ENCODINGS;
            }
            // 3: path_in_schema is redundant
            4 => {
                column.compression = Compression::read_thrift(&mut *prot)?;
                seen_mask |= COL_META_CODEC;
            }
            5 => {
                column.num_values = i64::read_thrift(&mut *prot)?;
                seen_mask |= COL_META_NUM_VALUES;
            }
            6 => {
                column.total_uncompressed_size = i64::read_thrift(&mut *prot)?;
                seen_mask |= COL_META_TOTAL_UNCOMP_SZ;
            }
            7 => {
                column.total_compressed_size = i64::read_thrift(&mut *prot)?;
                seen_mask |= COL_META_TOTAL_COMP_SZ;
            }
            // 8: we don't expose this key value
            9 => {
                column.data_page_offset = i64::read_thrift(&mut *prot)?;
                seen_mask |= COL_META_DATA_PAGE_OFFSET;
            }
            10 => {
                column.index_page_offset = Some(i64::read_thrift(&mut *prot)?);
            }
            11 => {
                column.dictionary_page_offset = Some(i64::read_thrift(&mut *prot)?);
            }
            12 => {
                column.statistics =
                    convert_stats(column_descr, Some(Statistics::read_thrift(&mut *prot)?))?;
            }
            13 if !skip_pes => {
                if pes_mask {
                    let val = read_encoding_stats_as_mask(&mut *prot)?;
                    column.encoding_stats = Some(ParquetPageEncodingStats::Mask(val));
                } else {
                    let val =
                        read_thrift_vec::<PageEncodingStats, ThriftSliceInputProtocol>(&mut *prot)?;
                    column.encoding_stats = Some(ParquetPageEncodingStats::Full(val));
                }
            }
            14 => {
                column.bloom_filter_offset = Some(i64::read_thrift(&mut *prot)?);
            }
            15 => {
                column.bloom_filter_length = Some(i32::read_thrift(&mut *prot)?);
            }
            16 => {
                let val = SizeStatistics::read_thrift(&mut *prot)?;
                column.unencoded_byte_array_data_bytes = val.unencoded_byte_array_data_bytes;
                column.repetition_level_histogram =
                    val.repetition_level_histogram.map(LevelHistogram::from);
                column.definition_level_histogram =
                    val.definition_level_histogram.map(LevelHistogram::from);
            }
            17 => {
                let val = GeospatialStatistics::read_thrift(&mut *prot)?;
                column.geo_statistics = convert_geo_stats(Some(val));
            }
            _ => {
                prot.skip(field_ident.field_type)?;
            }
        };
        last_field_id = field_ident.id;
    }

    Ok(seen_mask)
}

// using ThriftSliceInputProtocol rather than ThriftCompactInputProtocl trait because
// these are all internal and operate on slices.
fn read_column_chunk<'a>(
    prot: &mut ThriftSliceInputProtocol<'a>,
    column_descr: &Arc<ColumnDescriptor>,
    col_index: usize,
    options: Option<&ParquetMetaDataOptions>,
) -> Result<ColumnChunkMetaData> {
    // create a default initialized ColumnMetaData
    let mut col = ColumnChunkMetaDataBuilder::new(column_descr.clone()).build()?;

    // seen flag for file_offset
    let mut has_file_offset = false;

    // mask of seen flags for ColumnMetaData
    let mut col_meta_mask = 0u16;

    // struct ColumnChunk {
    //   1: optional string file_path
    //   2: required i64 file_offset = 0
    //   3: optional ColumnMetaData meta_data
    //   4: optional i64 offset_index_offset
    //   5: optional i32 offset_index_length
    //   6: optional i64 column_index_offset
    //   7: optional i32 column_index_length
    //   8: optional ColumnCryptoMetaData crypto_metadata
    //   9: optional binary encrypted_column_metadata
    // }
    let mut last_field_id = 0i16;
    loop {
        let field_ident = prot.read_field_begin(last_field_id)?;
        if field_ident.field_type == FieldType::Stop {
            break;
        }
        match field_ident.id {
            1 => {
                col.file_path = Some(String::read_thrift(&mut *prot)?);
            }
            2 => {
                col.file_offset = i64::read_thrift(&mut *prot)?;
                has_file_offset = true;
            }
            3 => {
                col_meta_mask = read_column_metadata(&mut *prot, &mut col, col_index, options)?;
            }
            4 => {
                col.offset_index_offset = Some(i64::read_thrift(&mut *prot)?);
            }
            5 => {
                col.offset_index_length = Some(i32::read_thrift(&mut *prot)?);
            }
            6 => {
                col.column_index_offset = Some(i64::read_thrift(&mut *prot)?);
            }
            7 => {
                col.column_index_length = Some(i32::read_thrift(&mut *prot)?);
            }
            #[cfg(feature = "encryption")]
            8 => {
                let val = ColumnCryptoMetaData::read_thrift(&mut *prot)?;
                col.column_crypto_metadata = Some(Box::new(val));
            }
            #[cfg(feature = "encryption")]
            9 => {
                col.encrypted_column_metadata = Some(<&[u8]>::read_thrift(&mut *prot)?.to_vec());
            }
            _ => {
                prot.skip(field_ident.field_type)?;
            }
        };
        last_field_id = field_ident.id;
    }

    // the only required field from ColumnChunk
    if !has_file_offset {
        return Err(general_err!("Required field file_offset is missing"));
    };

    // if encrypted just return. we'll decrypt after finishing the footer and populate the rest.
    #[cfg(feature = "encryption")]
    if col.encrypted_column_metadata.is_some() {
        return Ok(col);
    }

    // not encrypted, so make sure all required fields were read
    validate_column_metadata(col_meta_mask)?;

    Ok(col)
}

fn read_row_group(
    prot: &mut ThriftSliceInputProtocol,
    schema_descr: &Arc<SchemaDescriptor>,
    options: Option<&ParquetMetaDataOptions>,
) -> Result<RowGroupMetaData> {
    // create default initialized RowGroupMetaData
    let mut row_group = RowGroupMetaDataBuilder::new(schema_descr.clone()).build_unchecked();

    // mask values for required fields
    const RG_COLUMNS: u8 = 1 << 1;
    const RG_TOT_BYTE_SIZE: u8 = 1 << 2;
    const RG_NUM_ROWS: u8 = 1 << 3;
    const RG_ALL_REQUIRED: u8 = RG_COLUMNS | RG_TOT_BYTE_SIZE | RG_NUM_ROWS;

    let mut mask = 0u8;

    // struct RowGroup {
    //   1: required list<ColumnChunk> columns
    //   2: required i64 total_byte_size
    //   3: required i64 num_rows
    //   4: optional list<SortingColumn> sorting_columns
    //   5: optional i64 file_offset
    //   6: optional i64 total_compressed_size
    //   7: optional i16 ordinal
    // }
    let mut last_field_id = 0i16;
    loop {
        let field_ident = prot.read_field_begin(last_field_id)?;
        if field_ident.field_type == FieldType::Stop {
            break;
        }
        match field_ident.id {
            1 => {
                let list_ident = prot.read_list_begin()?;
                if schema_descr.num_columns() != list_ident.size as usize {
                    return Err(general_err!(
                        "Column count mismatch. Schema has {} columns while Row Group has {}",
                        schema_descr.num_columns(),
                        list_ident.size
                    ));
                }
                for i in 0..list_ident.size as usize {
                    let col = read_column_chunk(prot, &schema_descr.columns()[i], i, options)?;
                    row_group.columns.push(col);
                }
                mask |= RG_COLUMNS;
            }
            2 => {
                row_group.total_byte_size = i64::read_thrift(&mut *prot)?;
                mask |= RG_TOT_BYTE_SIZE;
            }
            3 => {
                row_group.num_rows = i64::read_thrift(&mut *prot)?;
                mask |= RG_NUM_ROWS;
            }
            4 => {
                let val = read_thrift_vec::<SortingColumn, ThriftSliceInputProtocol>(&mut *prot)?;
                row_group.sorting_columns = Some(val);
            }
            5 => {
                row_group.file_offset = Some(i64::read_thrift(&mut *prot)?);
            }
            // 6: we don't expose total_compressed_size
            7 => {
                row_group.ordinal = Some(i16::read_thrift(&mut *prot)?);
            }
            _ => {
                prot.skip(field_ident.field_type)?;
            }
        };
        last_field_id = field_ident.id;
    }

    if mask != RG_ALL_REQUIRED {
        if mask & RG_COLUMNS == 0 {
            return Err(general_err!("Required field columns is missing"));
        }
        if mask & RG_TOT_BYTE_SIZE == 0 {
            return Err(general_err!("Required field total_byte_size is missing"));
        }
        if mask & RG_NUM_ROWS == 0 {
            return Err(general_err!("Required field num_rows is missing"));
        }
    }

    Ok(row_group)
}

/// Create a [`SchemaDescriptor`] from thrift input. The input buffer must contain a complete
/// Parquet footer.
pub(crate) fn parquet_schema_from_bytes(buf: &[u8]) -> Result<SchemaDescriptor> {
    let mut prot = ThriftSliceInputProtocol::new(buf);

    let mut last_field_id = 0i16;
    loop {
        let field_ident = prot.read_field_begin(last_field_id)?;
        if field_ident.field_type == FieldType::Stop {
            break;
        }
        match field_ident.id {
            2 => {
                // read schema and convert to SchemaDescriptor for use when reading row groups
                let val = read_thrift_vec::<SchemaElement, ThriftSliceInputProtocol>(&mut prot)?;
                let val = parquet_schema_from_array(val)?;
                return Ok(SchemaDescriptor::new(val));
            }
            _ => prot.skip(field_ident.field_type)?,
        }
        last_field_id = field_ident.id;
    }
    Err(general_err!("Input does not contain a schema"))
}

/// Create [`ParquetMetaData`] from thrift input. Note that this only decodes the file metadata in
/// the Parquet footer. Page indexes will need to be added later.
pub(crate) fn parquet_metadata_from_bytes(
    buf: &[u8],
    options: Option<&ParquetMetaDataOptions>,
) -> Result<ParquetMetaData> {
    let mut prot = ThriftSliceInputProtocol::new(buf);

    // begin reading the file metadata
    let mut version: Option<i32> = None;
    let mut num_rows: Option<i64> = None;
    let mut row_groups: Option<Vec<RowGroupMetaData>> = None;
    let mut key_value_metadata: Option<Vec<KeyValue>> = None;
    let mut created_by: Option<&str> = None;
    let mut column_orders: Option<Vec<ColumnOrder>> = None;
    #[cfg(feature = "encryption")]
    let mut encryption_algorithm: Option<EncryptionAlgorithm> = None;
    #[cfg(feature = "encryption")]
    let mut footer_signing_key_metadata: Option<&[u8]> = None;

    // this will need to be set before parsing row groups
    let mut schema_descr: Option<Arc<SchemaDescriptor>> = None;

    // see if we already have a schema.
    if let Some(options) = options {
        schema_descr = options.schema().cloned();
    }

    // struct FileMetaData {
    //   1: required i32 version
    //   2: required list<SchemaElement> schema;
    //   3: required i64 num_rows
    //   4: required list<RowGroup> row_groups
    //   5: optional list<KeyValue> key_value_metadata
    //   6: optional string created_by
    //   7: optional list<ColumnOrder> column_orders;
    //   8: optional EncryptionAlgorithm encryption_algorithm
    //   9: optional binary footer_signing_key_metadata
    // }
    let mut last_field_id = 0i16;
    loop {
        let field_ident = prot.read_field_begin(last_field_id)?;
        if field_ident.field_type == FieldType::Stop {
            break;
        }
        match field_ident.id {
            1 => {
                version = Some(i32::read_thrift(&mut prot)?);
            }
            2 => {
                // If schema was passed in, skip parsing it
                if schema_descr.is_some() {
                    prot.skip(field_ident.field_type)?;
                } else {
                    // read schema and convert to SchemaDescriptor for use when reading row groups
                    let val =
                        read_thrift_vec::<SchemaElement, ThriftSliceInputProtocol>(&mut prot)?;
                    let val = parquet_schema_from_array(val)?;
                    schema_descr = Some(Arc::new(SchemaDescriptor::new(val)));
                }
            }
            3 => {
                num_rows = Some(i64::read_thrift(&mut prot)?);
            }
            4 => {
                if schema_descr.is_none() {
                    return Err(general_err!("Required field schema is missing"));
                }
                let schema_descr = schema_descr.as_ref().unwrap();
                let list_ident = prot.read_list_begin()?;
                let mut rg_vec = Vec::with_capacity(list_ident.size as usize);

                // Read row groups and handle ordinal assignment
                let mut assigner = OrdinalAssigner::new();
                for ordinal in 0..list_ident.size {
                    let ordinal: i16 = ordinal.try_into().map_err(|_| {
                        ParquetError::General(format!(
                            "Row group ordinal {ordinal} exceeds i16 max value",
                        ))
                    })?;
                    let rg = read_row_group(&mut prot, schema_descr, options)?;
                    rg_vec.push(assigner.ensure(ordinal, rg)?);
                }
                row_groups = Some(rg_vec);
            }
            5 => {
                let val = read_thrift_vec::<KeyValue, ThriftSliceInputProtocol>(&mut prot)?;
                key_value_metadata = Some(val);
            }
            6 => {
                created_by = Some(<&str>::read_thrift(&mut prot)?);
            }
            7 => {
                let val = read_thrift_vec::<ColumnOrder, ThriftSliceInputProtocol>(&mut prot)?;
                column_orders = Some(val);
            }
            #[cfg(feature = "encryption")]
            8 => {
                let val = EncryptionAlgorithm::read_thrift(&mut prot)?;
                encryption_algorithm = Some(val);
            }
            #[cfg(feature = "encryption")]
            9 => {
                footer_signing_key_metadata = Some(<&[u8]>::read_thrift(&mut prot)?);
            }
            _ => {
                prot.skip(field_ident.field_type)?;
            }
        };
        last_field_id = field_ident.id;
    }
    let Some(version) = version else {
        return Err(general_err!("Required field version is missing"));
    };
    let Some(num_rows) = num_rows else {
        return Err(general_err!("Required field num_rows is missing"));
    };
    let Some(row_groups) = row_groups else {
        return Err(general_err!("Required field row_groups is missing"));
    };

    let created_by = created_by.map(|c| c.to_owned());

    // we've tested for `None` by now so this is safe
    let schema_descr = schema_descr.unwrap();

    // need to map read column orders to actual values based on the schema
    if column_orders
        .as_ref()
        .is_some_and(|cos| cos.len() != schema_descr.num_columns())
    {
        return Err(general_err!("Column order length mismatch"));
    }
    // replace default type defined column orders with ones having the correct sort order
    // TODO(ets): this could instead be done above when decoding
    let column_orders = column_orders.map(|mut cos| {
        for (i, column) in schema_descr.columns().iter().enumerate() {
            if let ColumnOrder::TYPE_DEFINED_ORDER(_) = cos[i] {
                let sort_order = ColumnOrder::sort_order_for_type(
                    column.logical_type_ref(),
                    column.converted_type(),
                    column.physical_type(),
                );
                cos[i] = ColumnOrder::TYPE_DEFINED_ORDER(sort_order);
            }
        }
        cos
    });

    #[cfg(not(feature = "encryption"))]
    let fmd = crate::file::metadata::FileMetaData::new(
        version,
        num_rows,
        created_by,
        key_value_metadata,
        schema_descr,
        column_orders,
    );
    #[cfg(feature = "encryption")]
    let fmd = crate::file::metadata::FileMetaData::new(
        version,
        num_rows,
        created_by,
        key_value_metadata,
        schema_descr,
        column_orders,
    )
    .with_encryption_algorithm(encryption_algorithm)
    .with_footer_signing_key_metadata(footer_signing_key_metadata.map(|v| v.to_vec()));

    Ok(ParquetMetaData::new(fmd, row_groups))
}

/// Assign [`RowGroupMetaData::ordinal`]  if it is missing.
#[derive(Debug, Default)]
pub(crate) struct OrdinalAssigner {
    first_has_ordinal: Option<bool>,
}

impl OrdinalAssigner {
    fn new() -> Self {
        Default::default()
    }

    /// Sets [`RowGroupMetaData::ordinal`] if it is missing.
    ///
    /// # Arguments
    /// - actual_ordinal: The ordinal (index) of the row group being processed
    ///   in the file metadata.
    /// - rg: The [`RowGroupMetaData`] to potentially modify.
    ///
    /// Ensures:
    /// 1. If the first row group has an ordinal, all subsequent row groups must
    ///    also have ordinals.
    /// 2. If the first row group does NOT have an ordinal, all subsequent row
    ///    groups must also not have ordinals.
    fn ensure(
        &mut self,
        actual_ordinal: i16,
        mut rg: RowGroupMetaData,
    ) -> Result<RowGroupMetaData> {
        let rg_has_ordinal = rg.ordinal.is_some();

        // Only set first_has_ordinal if it's None (first row group that arrives)
        if self.first_has_ordinal.is_none() {
            self.first_has_ordinal = Some(rg_has_ordinal);
        }

        // assign ordinal if missing and consistent with first row group
        let first_has_ordinal = self.first_has_ordinal.unwrap();
        if !first_has_ordinal && !rg_has_ordinal {
            rg.ordinal = Some(actual_ordinal);
        } else if first_has_ordinal != rg_has_ordinal {
            return Err(general_err!(
                "Inconsistent ordinal assignment: first_has_ordinal is set to \
                {} but row-group with actual ordinal {} has rg_has_ordinal set to {}",
                first_has_ordinal,
                actual_ordinal,
                rg_has_ordinal
            ));
        }
        Ok(rg)
    }
}

thrift_struct!(
    pub(crate) struct IndexPageHeader {}
);

thrift_struct!(
pub(crate) struct DictionaryPageHeader {
  /// Number of values in the dictionary
  1: required i32 num_values;

  /// Encoding using this dictionary page
  2: required Encoding encoding

  /// If true, the entries in the dictionary are sorted in ascending order
  3: optional bool is_sorted;
}
);

thrift_struct!(
/// Statistics for the page header.
///
/// This is a duplicate of the [`Statistics`] struct above. Because the page reader uses
/// the [`Read`] API, we cannot read the min/max values as slices. This should not be
/// a huge problem since this crate no longer reads the page header statistics by default.
///
/// [`Read`]: crate::parquet_thrift::ThriftReadInputProtocol
pub(crate) struct PageStatistics {
   1: optional binary max;
   2: optional binary min;
   3: optional i64 null_count;
   4: optional i64 distinct_count;
   5: optional binary max_value;
   6: optional binary min_value;
   7: optional bool is_max_value_exact;
   8: optional bool is_min_value_exact;
}
);

thrift_struct!(
pub(crate) struct DataPageHeader {
  1: required i32 num_values
  2: required Encoding encoding
  3: required Encoding definition_level_encoding;
  4: required Encoding repetition_level_encoding;
  5: optional PageStatistics statistics;
}
);

impl DataPageHeader {
    // reader that skips decoding page statistics
    fn read_thrift_without_stats<'a, R>(prot: &mut R) -> Result<Self>
    where
        R: ThriftCompactInputProtocol<'a>,
    {
        let mut num_values: Option<i32> = None;
        let mut encoding: Option<Encoding> = None;
        let mut definition_level_encoding: Option<Encoding> = None;
        let mut repetition_level_encoding: Option<Encoding> = None;
        let statistics: Option<PageStatistics> = None;
        let mut last_field_id = 0i16;
        loop {
            let field_ident = prot.read_field_begin(last_field_id)?;
            if field_ident.field_type == FieldType::Stop {
                break;
            }
            match field_ident.id {
                1 => {
                    let val = i32::read_thrift(&mut *prot)?;
                    num_values = Some(val);
                }
                2 => {
                    let val = Encoding::read_thrift(&mut *prot)?;
                    encoding = Some(val);
                }
                3 => {
                    let val = Encoding::read_thrift(&mut *prot)?;
                    definition_level_encoding = Some(val);
                }
                4 => {
                    let val = Encoding::read_thrift(&mut *prot)?;
                    repetition_level_encoding = Some(val);
                }
                _ => {
                    prot.skip(field_ident.field_type)?;
                }
            };
            last_field_id = field_ident.id;
        }
        let Some(num_values) = num_values else {
            return Err(general_err!("Required field num_values is missing"));
        };
        let Some(encoding) = encoding else {
            return Err(general_err!("Required field encoding is missing"));
        };
        let Some(definition_level_encoding) = definition_level_encoding else {
            return Err(general_err!(
                "Required field definition_level_encoding is missing"
            ));
        };
        let Some(repetition_level_encoding) = repetition_level_encoding else {
            return Err(general_err!(
                "Required field repetition_level_encoding is missing"
            ));
        };
        Ok(Self {
            num_values,
            encoding,
            definition_level_encoding,
            repetition_level_encoding,
            statistics,
        })
    }
}

thrift_struct!(
pub(crate) struct DataPageHeaderV2 {
  1: required i32 num_values
  2: required i32 num_nulls
  3: required i32 num_rows
  4: required Encoding encoding
  5: required i32 definition_levels_byte_length;
  6: required i32 repetition_levels_byte_length;
  7: optional bool is_compressed = true;
  8: optional PageStatistics statistics;
}
);

impl DataPageHeaderV2 {
    // reader that skips decoding page statistics
    fn read_thrift_without_stats<'a, R>(prot: &mut R) -> Result<Self>
    where
        R: ThriftCompactInputProtocol<'a>,
    {
        let mut num_values: Option<i32> = None;
        let mut num_nulls: Option<i32> = None;
        let mut num_rows: Option<i32> = None;
        let mut encoding: Option<Encoding> = None;
        let mut definition_levels_byte_length: Option<i32> = None;
        let mut repetition_levels_byte_length: Option<i32> = None;
        let mut is_compressed: Option<bool> = None;
        let statistics: Option<PageStatistics> = None;
        let mut last_field_id = 0i16;
        loop {
            let field_ident = prot.read_field_begin(last_field_id)?;
            if field_ident.field_type == FieldType::Stop {
                break;
            }
            match field_ident.id {
                1 => {
                    let val = i32::read_thrift(&mut *prot)?;
                    num_values = Some(val);
                }
                2 => {
                    let val = i32::read_thrift(&mut *prot)?;
                    num_nulls = Some(val);
                }
                3 => {
                    let val = i32::read_thrift(&mut *prot)?;
                    num_rows = Some(val);
                }
                4 => {
                    let val = Encoding::read_thrift(&mut *prot)?;
                    encoding = Some(val);
                }
                5 => {
                    let val = i32::read_thrift(&mut *prot)?;
                    definition_levels_byte_length = Some(val);
                }
                6 => {
                    let val = i32::read_thrift(&mut *prot)?;
                    repetition_levels_byte_length = Some(val);
                }
                7 => {
                    let val = field_ident.bool_val.unwrap();
                    is_compressed = Some(val);
                }
                _ => {
                    prot.skip(field_ident.field_type)?;
                }
            };
            last_field_id = field_ident.id;
        }
        let Some(num_values) = num_values else {
            return Err(general_err!("Required field num_values is missing"));
        };
        let Some(num_nulls) = num_nulls else {
            return Err(general_err!("Required field num_nulls is missing"));
        };
        let Some(num_rows) = num_rows else {
            return Err(general_err!("Required field num_rows is missing"));
        };
        let Some(encoding) = encoding else {
            return Err(general_err!("Required field encoding is missing"));
        };
        let Some(definition_levels_byte_length) = definition_levels_byte_length else {
            return Err(general_err!(
                "Required field definition_levels_byte_length is missing"
            ));
        };
        let Some(repetition_levels_byte_length) = repetition_levels_byte_length else {
            return Err(general_err!(
                "Required field repetition_levels_byte_length is missing"
            ));
        };
        Ok(Self {
            num_values,
            num_nulls,
            num_rows,
            encoding,
            definition_levels_byte_length,
            repetition_levels_byte_length,
            is_compressed,
            statistics,
        })
    }
}

thrift_struct!(
pub(crate) struct PageHeader {
  /// the type of the page: indicates which of the *_header fields is set
  1: required PageType r#type

  /// Uncompressed page size in bytes (not including this header)
  2: required i32 uncompressed_page_size

  /// Compressed (and potentially encrypted) page size in bytes, not including this header
  3: required i32 compressed_page_size

  /// The 32-bit CRC checksum for the page, to be be calculated as follows:
  4: optional i32 crc

  // Headers for page specific data.  One only will be set.
  5: optional DataPageHeader data_page_header;
  6: optional IndexPageHeader index_page_header;
  7: optional DictionaryPageHeader dictionary_page_header;
  8: optional DataPageHeaderV2 data_page_header_v2;
}
);

impl PageHeader {
    // reader that skips reading page statistics. obtained by running
    // `cargo expand -p parquet --all-features --lib file::metadata::thrift`
    // and modifying the impl of `read_thrift`
    pub(crate) fn read_thrift_without_stats<'a, R>(prot: &mut R) -> Result<Self>
    where
        R: ThriftCompactInputProtocol<'a>,
    {
        let mut type_: Option<PageType> = None;
        let mut uncompressed_page_size: Option<i32> = None;
        let mut compressed_page_size: Option<i32> = None;
        let mut crc: Option<i32> = None;
        let mut data_page_header: Option<DataPageHeader> = None;
        let mut index_page_header: Option<IndexPageHeader> = None;
        let mut dictionary_page_header: Option<DictionaryPageHeader> = None;
        let mut data_page_header_v2: Option<DataPageHeaderV2> = None;
        let mut last_field_id = 0i16;
        loop {
            let field_ident = prot.read_field_begin(last_field_id)?;
            if field_ident.field_type == FieldType::Stop {
                break;
            }
            match field_ident.id {
                1 => {
                    let val = PageType::read_thrift(&mut *prot)?;
                    type_ = Some(val);
                }
                2 => {
                    let val = i32::read_thrift(&mut *prot)?;
                    uncompressed_page_size = Some(val);
                }
                3 => {
                    let val = i32::read_thrift(&mut *prot)?;
                    compressed_page_size = Some(val);
                }
                4 => {
                    let val = i32::read_thrift(&mut *prot)?;
                    crc = Some(val);
                }
                5 => {
                    let val = DataPageHeader::read_thrift_without_stats(&mut *prot)?;
                    data_page_header = Some(val);
                }
                6 => {
                    let val = IndexPageHeader::read_thrift(&mut *prot)?;
                    index_page_header = Some(val);
                }
                7 => {
                    let val = DictionaryPageHeader::read_thrift(&mut *prot)?;
                    dictionary_page_header = Some(val);
                }
                8 => {
                    let val = DataPageHeaderV2::read_thrift_without_stats(&mut *prot)?;
                    data_page_header_v2 = Some(val);
                }
                _ => {
                    prot.skip(field_ident.field_type)?;
                }
            };
            last_field_id = field_ident.id;
        }
        let Some(type_) = type_ else {
            return Err(general_err!("Required field type_ is missing"));
        };
        let Some(uncompressed_page_size) = uncompressed_page_size else {
            return Err(general_err!(
                "Required field uncompressed_page_size is missing"
            ));
        };
        let Some(compressed_page_size) = compressed_page_size else {
            return Err(general_err!(
                "Required field compressed_page_size is missing"
            ));
        };
        Ok(Self {
            r#type: type_,
            uncompressed_page_size,
            compressed_page_size,
            crc,
            data_page_header,
            index_page_header,
            dictionary_page_header,
            data_page_header_v2,
        })
    }
}

/////////////////////////////////////////////////
// helper functions for writing file meta data

#[cfg(feature = "encryption")]
fn should_write_column_stats(column_chunk: &ColumnChunkMetaData) -> bool {
    // If there is encrypted column metadata present,
    // the column is encrypted with a different key to the footer or a plaintext footer is used,
    // so the statistics are sensitive and shouldn't be written.
    column_chunk.encrypted_column_metadata.is_none()
}

#[cfg(not(feature = "encryption"))]
fn should_write_column_stats(_column_chunk: &ColumnChunkMetaData) -> bool {
    true
}

// serialize the bits of the column chunk needed for a thrift ColumnMetaData
// struct ColumnMetaData {
//   1: required Type type
//   2: required list<Encoding> encodings
//   3: required list<string> path_in_schema
//   4: required CompressionCodec codec
//   5: required i64 num_values
//   6: required i64 total_uncompressed_size
//   7: required i64 total_compressed_size
//   8: optional list<KeyValue> key_value_metadata
//   9: required i64 data_page_offset
//   10: optional i64 index_page_offset
//   11: optional i64 dictionary_page_offset
//   12: optional Statistics statistics;
//   13: optional list<PageEncodingStats> encoding_stats;
//   14: optional i64 bloom_filter_offset;
//   15: optional i32 bloom_filter_length;
//   16: optional SizeStatistics size_statistics;
//   17: optional GeospatialStatistics geospatial_statistics;
// }
pub(super) fn serialize_column_meta_data<W: Write>(
    column_chunk: &ColumnChunkMetaData,
    w: &mut ThriftCompactOutputProtocol<W>,
) -> Result<()> {
    use crate::file::statistics::page_stats_to_thrift;

    column_chunk.column_type().write_thrift_field(w, 1, 0)?;
    column_chunk
        .encodings()
        .collect::<Vec<_>>()
        .write_thrift_field(w, 2, 1)?;
    let path = column_chunk.column_descr.path().parts();
    let path: Vec<&str> = path.iter().map(|v| v.as_str()).collect();
    path.write_thrift_field(w, 3, 2)?;
    column_chunk.compression.write_thrift_field(w, 4, 3)?;
    column_chunk.num_values.write_thrift_field(w, 5, 4)?;
    column_chunk
        .total_uncompressed_size
        .write_thrift_field(w, 6, 5)?;
    column_chunk
        .total_compressed_size
        .write_thrift_field(w, 7, 6)?;
    // no key_value_metadata here
    let mut last_field_id = column_chunk.data_page_offset.write_thrift_field(w, 9, 7)?;
    if let Some(index_page_offset) = column_chunk.index_page_offset {
        last_field_id = index_page_offset.write_thrift_field(w, 10, last_field_id)?;
    }
    if let Some(dictionary_page_offset) = column_chunk.dictionary_page_offset {
        last_field_id = dictionary_page_offset.write_thrift_field(w, 11, last_field_id)?;
    }

    if should_write_column_stats(column_chunk) {
        // PageStatistics is the same as thrift Statistics, but writable
        let stats = page_stats_to_thrift(column_chunk.statistics());
        if let Some(stats) = stats {
            last_field_id = stats.write_thrift_field(w, 12, last_field_id)?;
        }
        if let Some(page_encoding_stats) = column_chunk.page_encoding_stats() {
            last_field_id = page_encoding_stats.write_thrift_field(w, 13, last_field_id)?;
        }
        if let Some(bloom_filter_offset) = column_chunk.bloom_filter_offset {
            last_field_id = bloom_filter_offset.write_thrift_field(w, 14, last_field_id)?;
        }
        if let Some(bloom_filter_length) = column_chunk.bloom_filter_length {
            last_field_id = bloom_filter_length.write_thrift_field(w, 15, last_field_id)?;
        }

        // SizeStatistics
        let size_stats = if column_chunk.unencoded_byte_array_data_bytes.is_some()
            || column_chunk.repetition_level_histogram.is_some()
            || column_chunk.definition_level_histogram.is_some()
        {
            let repetition_level_histogram = column_chunk
                .repetition_level_histogram()
                .map(|hist| hist.clone().into_inner());

            let definition_level_histogram = column_chunk
                .definition_level_histogram()
                .map(|hist| hist.clone().into_inner());

            Some(SizeStatistics {
                unencoded_byte_array_data_bytes: column_chunk.unencoded_byte_array_data_bytes,
                repetition_level_histogram,
                definition_level_histogram,
            })
        } else {
            None
        };
        if let Some(size_stats) = size_stats {
            last_field_id = size_stats.write_thrift_field(w, 16, last_field_id)?;
        }

        if let Some(geo_stats) = column_chunk.geo_statistics() {
            geo_stats.write_thrift_field(w, 17, last_field_id)?;
        }
    }

    w.write_struct_end()
}

// temp struct used for writing
pub(super) struct FileMeta<'a> {
    pub(super) file_metadata: &'a crate::file::metadata::FileMetaData,
    pub(super) row_groups: &'a Vec<RowGroupMetaData>,
}

// struct FileMetaData {
//   1: required i32 version
//   2: required list<SchemaElement> schema;
//   3: required i64 num_rows
//   4: required list<RowGroup> row_groups
//   5: optional list<KeyValue> key_value_metadata
//   6: optional string created_by
//   7: optional list<ColumnOrder> column_orders;
//   8: optional EncryptionAlgorithm encryption_algorithm
//   9: optional binary footer_signing_key_metadata
// }
impl<'a> WriteThrift for FileMeta<'a> {
    const ELEMENT_TYPE: ElementType = ElementType::Struct;

    // needed for last_field_id w/o encryption
    #[allow(unused_assignments)]
    fn write_thrift<W: Write>(&self, writer: &mut ThriftCompactOutputProtocol<W>) -> Result<()> {
        self.file_metadata
            .version
            .write_thrift_field(writer, 1, 0)?;

        // field 2 is schema. do depth-first traversal of tree, converting to SchemaElement and
        // writing along the way.
        let root = self.file_metadata.schema_descr().root_schema_ptr();
        let schema_len = num_nodes(&root)?;
        writer.write_field_begin(FieldType::List, 2, 1)?;
        writer.write_list_begin(ElementType::Struct, schema_len)?;
        // recursively write Type nodes as SchemaElements
        write_schema(&root, writer)?;

        self.file_metadata
            .num_rows
            .write_thrift_field(writer, 3, 2)?;

        // this will call RowGroupMetaData::write_thrift
        let mut last_field_id = self.row_groups.write_thrift_field(writer, 4, 3)?;

        if let Some(kv_metadata) = self.file_metadata.key_value_metadata() {
            last_field_id = kv_metadata.write_thrift_field(writer, 5, last_field_id)?;
        }
        if let Some(created_by) = self.file_metadata.created_by() {
            last_field_id = created_by.write_thrift_field(writer, 6, last_field_id)?;
        }
        if let Some(column_orders) = self.file_metadata.column_orders() {
            last_field_id = column_orders.write_thrift_field(writer, 7, last_field_id)?;
        }
        #[cfg(feature = "encryption")]
        if let Some(algo) = self.file_metadata.encryption_algorithm.as_ref() {
            last_field_id = algo.write_thrift_field(writer, 8, last_field_id)?;
        }
        #[cfg(feature = "encryption")]
        if let Some(key) = self.file_metadata.footer_signing_key_metadata.as_ref() {
            key.as_slice()
                .write_thrift_field(writer, 9, last_field_id)?;
        }

        writer.write_struct_end()
    }
}

fn write_schema<W: Write>(
    schema: &TypePtr,
    writer: &mut ThriftCompactOutputProtocol<W>,
) -> Result<()> {
    if !schema.is_group() {
        return Err(general_err!("Root schema must be Group type"));
    }
    write_schema_helper(schema, writer)
}

fn write_schema_helper<W: Write>(
    node: &TypePtr,
    writer: &mut ThriftCompactOutputProtocol<W>,
) -> Result<()> {
    match node.as_ref() {
        crate::schema::types::Type::PrimitiveType {
            basic_info,
            physical_type,
            type_length,
            scale,
            precision,
        } => {
            let element = SchemaElement {
                r#type: Some(*physical_type),
                type_length: if *type_length >= 0 {
                    Some(*type_length)
                } else {
                    None
                },
                repetition_type: Some(basic_info.repetition()),
                name: basic_info.name(),
                num_children: None,
                converted_type: match basic_info.converted_type() {
                    ConvertedType::NONE => None,
                    other => Some(other),
                },
                scale: if *scale >= 0 { Some(*scale) } else { None },
                precision: if *precision >= 0 {
                    Some(*precision)
                } else {
                    None
                },
                field_id: if basic_info.has_id() {
                    Some(basic_info.id())
                } else {
                    None
                },
                logical_type: basic_info.logical_type_ref().cloned(),
            };
            element.write_thrift(writer)
        }
        crate::schema::types::Type::GroupType { basic_info, fields } => {
            let repetition = if basic_info.has_repetition() {
                Some(basic_info.repetition())
            } else {
                None
            };

            let element = SchemaElement {
                r#type: None,
                type_length: None,
                repetition_type: repetition,
                name: basic_info.name(),
                num_children: Some(fields.len().try_into()?),
                converted_type: match basic_info.converted_type() {
                    ConvertedType::NONE => None,
                    other => Some(other),
                },
                scale: None,
                precision: None,
                field_id: if basic_info.has_id() {
                    Some(basic_info.id())
                } else {
                    None
                },
                logical_type: basic_info.logical_type_ref().cloned(),
            };

            element.write_thrift(writer)?;

            // Add child elements for a group
            for field in fields {
                write_schema_helper(field, writer)?;
            }
            Ok(())
        }
    }
}

// struct RowGroup {
//   1: required list<ColumnChunk> columns
//   2: required i64 total_byte_size
//   3: required i64 num_rows
//   4: optional list<SortingColumn> sorting_columns
//   5: optional i64 file_offset
//   6: optional i64 total_compressed_size
//   7: optional i16 ordinal
// }
impl WriteThrift for RowGroupMetaData {
    const ELEMENT_TYPE: ElementType = ElementType::Struct;

    fn write_thrift<W: Write>(&self, writer: &mut ThriftCompactOutputProtocol<W>) -> Result<()> {
        // this will call ColumnChunkMetaData::write_thrift
        self.columns.write_thrift_field(writer, 1, 0)?;
        self.total_byte_size.write_thrift_field(writer, 2, 1)?;
        let mut last_field_id = self.num_rows.write_thrift_field(writer, 3, 2)?;
        if let Some(sorting_columns) = self.sorting_columns() {
            last_field_id = sorting_columns.write_thrift_field(writer, 4, last_field_id)?;
        }
        if let Some(file_offset) = self.file_offset() {
            last_field_id = file_offset.write_thrift_field(writer, 5, last_field_id)?;
        }
        // this is optional, but we'll always write it
        last_field_id = self
            .compressed_size()
            .write_thrift_field(writer, 6, last_field_id)?;
        if let Some(ordinal) = self.ordinal() {
            ordinal.write_thrift_field(writer, 7, last_field_id)?;
        }
        writer.write_struct_end()
    }
}

// struct ColumnChunk {
//   1: optional string file_path
//   2: required i64 file_offset = 0
//   3: optional ColumnMetaData meta_data
//   4: optional i64 offset_index_offset
//   5: optional i32 offset_index_length
//   6: optional i64 column_index_offset
//   7: optional i32 column_index_length
//   8: optional ColumnCryptoMetaData crypto_metadata
//   9: optional binary encrypted_column_metadata
// }
impl WriteThrift for ColumnChunkMetaData {
    const ELEMENT_TYPE: ElementType = ElementType::Struct;

    #[allow(unused_assignments)]
    fn write_thrift<W: Write>(&self, writer: &mut ThriftCompactOutputProtocol<W>) -> Result<()> {
        let mut last_field_id = 0i16;
        if let Some(file_path) = self.file_path() {
            last_field_id = file_path.write_thrift_field(writer, 1, last_field_id)?;
        }
        last_field_id = self
            .file_offset()
            .write_thrift_field(writer, 2, last_field_id)?;

        #[cfg(feature = "encryption")]
        let write_meta_data =
            self.encrypted_column_metadata.is_none() || self.plaintext_footer_mode;
        #[cfg(not(feature = "encryption"))]
        let write_meta_data = true;

        // When the footer is encrypted and encrypted_column_metadata is present,
        // skip writing the plaintext meta_data field to reduce footer size.
        // When the footer is plaintext (plaintext_footer_mode=true), we still write
        // meta_data for backward compatibility with readers that expect it, but with
        // sensitive fields (statistics, bloom filter info, etc.) stripped out.
        if write_meta_data {
            writer.write_field_begin(FieldType::Struct, 3, last_field_id)?;
            serialize_column_meta_data(self, writer)?;
            last_field_id = 3;
        }

        if let Some(offset_idx_off) = self.offset_index_offset() {
            last_field_id = offset_idx_off.write_thrift_field(writer, 4, last_field_id)?;
        }
        if let Some(offset_idx_len) = self.offset_index_length() {
            last_field_id = offset_idx_len.write_thrift_field(writer, 5, last_field_id)?;
        }
        if let Some(column_idx_off) = self.column_index_offset() {
            last_field_id = column_idx_off.write_thrift_field(writer, 6, last_field_id)?;
        }
        if let Some(column_idx_len) = self.column_index_length() {
            last_field_id = column_idx_len.write_thrift_field(writer, 7, last_field_id)?;
        }
        #[cfg(feature = "encryption")]
        {
            if let Some(crypto_metadata) = self.crypto_metadata() {
                last_field_id = crypto_metadata.write_thrift_field(writer, 8, last_field_id)?;
            }
            if let Some(encrypted_meta) = self.encrypted_column_metadata.as_ref() {
                encrypted_meta
                    .as_slice()
                    .write_thrift_field(writer, 9, last_field_id)?;
            }
        }

        writer.write_struct_end()
    }
}

// struct GeospatialStatistics {
//   1: optional BoundingBox bbox;
//   2: optional list<i32> geospatial_types;
// }
impl WriteThrift for crate::geospatial::statistics::GeospatialStatistics {
    const ELEMENT_TYPE: ElementType = ElementType::Struct;

    fn write_thrift<W: Write>(&self, writer: &mut ThriftCompactOutputProtocol<W>) -> Result<()> {
        let mut last_field_id = 0i16;
        if let Some(bbox) = self.bounding_box() {
            last_field_id = bbox.write_thrift_field(writer, 1, last_field_id)?;
        }
        if let Some(geo_types) = self.geospatial_types() {
            geo_types.write_thrift_field(writer, 2, last_field_id)?;
        }

        writer.write_struct_end()
    }
}

// macro cannot handle qualified names
use crate::geospatial::statistics::GeospatialStatistics as RustGeospatialStatistics;
write_thrift_field!(RustGeospatialStatistics, FieldType::Struct);

// struct BoundingBox {
//   1: required double xmin;
//   2: required double xmax;
//   3: required double ymin;
//   4: required double ymax;
//   5: optional double zmin;
//   6: optional double zmax;
//   7: optional double mmin;
//   8: optional double mmax;
// }
impl WriteThrift for crate::geospatial::bounding_box::BoundingBox {
    const ELEMENT_TYPE: ElementType = ElementType::Struct;

    fn write_thrift<W: Write>(&self, writer: &mut ThriftCompactOutputProtocol<W>) -> Result<()> {
        self.get_xmin().write_thrift_field(writer, 1, 0)?;
        self.get_xmax().write_thrift_field(writer, 2, 1)?;
        self.get_ymin().write_thrift_field(writer, 3, 2)?;
        let mut last_field_id = self.get_ymax().write_thrift_field(writer, 4, 3)?;

        if let Some(zmin) = self.get_zmin() {
            last_field_id = zmin.write_thrift_field(writer, 5, last_field_id)?;
        }
        if let Some(zmax) = self.get_zmax() {
            last_field_id = zmax.write_thrift_field(writer, 6, last_field_id)?;
        }
        if let Some(mmin) = self.get_mmin() {
            last_field_id = mmin.write_thrift_field(writer, 7, last_field_id)?;
        }
        if let Some(mmax) = self.get_mmax() {
            mmax.write_thrift_field(writer, 8, last_field_id)?;
        }

        writer.write_struct_end()
    }
}

// macro cannot handle qualified names
use crate::geospatial::bounding_box::BoundingBox as RustBoundingBox;
write_thrift_field!(RustBoundingBox, FieldType::Struct);

#[cfg(test)]
pub(crate) mod tests {
    use crate::errors::Result;
    use crate::file::metadata::thrift::{BoundingBox, SchemaElement, write_schema};
    use crate::file::metadata::{ColumnChunkMetaData, RowGroupMetaData};
    use crate::parquet_thrift::tests::test_roundtrip;
    use crate::parquet_thrift::{
        ElementType, ThriftCompactOutputProtocol, ThriftSliceInputProtocol, read_thrift_vec,
    };
    use crate::schema::types::{
        ColumnDescriptor, SchemaDescriptor, TypePtr, num_nodes, parquet_schema_from_array,
    };
    use std::sync::Arc;

    // for testing. decode thrift encoded RowGroup
    pub(crate) fn read_row_group(
        buf: &mut [u8],
        schema_descr: Arc<SchemaDescriptor>,
    ) -> Result<RowGroupMetaData> {
        let mut reader = ThriftSliceInputProtocol::new(buf);
        crate::file::metadata::thrift::read_row_group(&mut reader, &schema_descr, None)
    }

    pub(crate) fn read_column_chunk(
        buf: &mut [u8],
        column_descr: Arc<ColumnDescriptor>,
    ) -> Result<ColumnChunkMetaData> {
        let mut reader = ThriftSliceInputProtocol::new(buf);
        crate::file::metadata::thrift::read_column_chunk(&mut reader, &column_descr, 0, None)
    }

    pub(crate) fn roundtrip_schema(schema: TypePtr) -> Result<TypePtr> {
        let num_nodes = num_nodes(&schema)?;
        let mut buf = Vec::new();
        let mut writer = ThriftCompactOutputProtocol::new(&mut buf);

        // kick off writing list
        writer.write_list_begin(ElementType::Struct, num_nodes)?;

        // write SchemaElements
        write_schema(&schema, &mut writer)?;

        let mut prot = ThriftSliceInputProtocol::new(&buf);
        let se: Vec<SchemaElement> = read_thrift_vec(&mut prot)?;
        parquet_schema_from_array(se)
    }

    pub(crate) fn schema_to_buf(schema: &TypePtr) -> Result<Vec<u8>> {
        let num_nodes = num_nodes(schema)?;
        let mut buf = Vec::new();
        let mut writer = ThriftCompactOutputProtocol::new(&mut buf);

        // kick off writing list
        writer.write_list_begin(ElementType::Struct, num_nodes)?;

        // write SchemaElements
        write_schema(schema, &mut writer)?;
        Ok(buf)
    }

    pub(crate) fn buf_to_schema_list<'a>(buf: &'a mut Vec<u8>) -> Result<Vec<SchemaElement<'a>>> {
        let mut prot = ThriftSliceInputProtocol::new(buf.as_mut_slice());
        read_thrift_vec(&mut prot)
    }

    #[test]
    fn test_bounding_box_roundtrip() {
        test_roundtrip(BoundingBox {
            xmin: 0.1.into(),
            xmax: 10.3.into(),
            ymin: 0.001.into(),
            ymax: 128.5.into(),
            zmin: None,
            zmax: None,
            mmin: None,
            mmax: None,
        });

        test_roundtrip(BoundingBox {
            xmin: 0.1.into(),
            xmax: 10.3.into(),
            ymin: 0.001.into(),
            ymax: 128.5.into(),
            zmin: Some(11.0.into()),
            zmax: Some(1300.0.into()),
            mmin: None,
            mmax: None,
        });

        test_roundtrip(BoundingBox {
            xmin: 0.1.into(),
            xmax: 10.3.into(),
            ymin: 0.001.into(),
            ymax: 128.5.into(),
            zmin: Some(11.0.into()),
            zmax: Some(1300.0.into()),
            mmin: Some(3.7.into()),
            mmax: Some(42.0.into()),
        });
    }
}
