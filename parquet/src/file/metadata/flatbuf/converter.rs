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

//! Converter between Parquet metadata and FlatBuffers representation.
//!
//! This module provides functionality to convert Parquet metadata to/from
//! the FlatBuffers format defined in `parquet3.fbs`.

use std::sync::Arc;

use flatbuffers::{FlatBufferBuilder, WIPOffset};

use crate::basic::{ColumnOrder, Compression, Encoding, LogicalType, Repetition, Type};
use crate::errors::{ParquetError, Result};
use crate::file::metadata::{
    ColumnChunkMetaData, ColumnChunkMetaDataBuilder, FileMetaData, KeyValue,
    ParquetMetaData, RowGroupMetaData, RowGroupMetaDataBuilder, SortingColumn,
};
use crate::file::statistics::Statistics;
use crate::schema::types::{
    ColumnDescriptor, SchemaDescPtr, SchemaDescriptor, Type as SchemaType,
};

use super::parquet3_generated::parquet::format_3 as fb;


/// Packed min/max statistics for FlatBuffers format
#[derive(Debug, Default, Clone)]
struct PackedStats {
    lo4: u32,
    lo8: u64,
    hi8: u64,
    len: i8,
}

/// Min/max statistics with optional prefix for string types
#[derive(Debug, Default)]
struct MinMax {
    min: PackedStats,
    max: PackedStats,
    prefix: String,
}

/// Pack statistics into the FlatBuffers format based on physical type.
///
/// Statistics are stored in integral types if their size is fixed:
/// - BOOLEAN: none
/// - INT32/FLOAT: lo4 (little-endian)
/// - INT64/DOUBLE: lo8 (little-endian)
/// - INT96: lo4+lo8 (little-endian)
/// - BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY: prefix + lo8+hi8 (big-endian)
fn pack_statistics(
    physical_type: Type,
    min: &[u8],
    is_min_exact: bool,
    max: &[u8],
    is_max_exact: bool,
) -> MinMax {
    match physical_type {
        Type::BOOLEAN => MinMax::default(),
        Type::INT32 | Type::FLOAT => {
            let load = |v: &[u8], is_exact: bool| -> PackedStats {
                if v.len() >= 4 {
                    PackedStats {
                        lo4: u32::from_le_bytes(v[..4].try_into().unwrap()),
                        len: if is_exact { 4 } else { -4 },
                        ..Default::default()
                    }
                } else {
                    PackedStats::default()
                }
            };
            MinMax {
                min: load(min, is_min_exact),
                max: load(max, is_max_exact),
                prefix: String::new(),
            }
        }
        Type::INT64 | Type::DOUBLE => {
            let load = |v: &[u8], is_exact: bool| -> PackedStats {
                if v.len() >= 8 {
                    PackedStats {
                        lo8: u64::from_le_bytes(v[..8].try_into().unwrap()),
                        len: if is_exact { 8 } else { -8 },
                        ..Default::default()
                    }
                } else {
                    PackedStats::default()
                }
            };
            MinMax {
                min: load(min, is_min_exact),
                max: load(max, is_max_exact),
                prefix: String::new(),
            }
        }
        Type::INT96 => {
            let load = |v: &[u8], is_exact: bool| -> PackedStats {
                if v.len() >= 12 {
                    PackedStats {
                        lo4: u32::from_le_bytes(v[..4].try_into().unwrap()),
                        lo8: u64::from_le_bytes(v[4..12].try_into().unwrap()),
                        len: if is_exact { 12 } else { -12 },
                        ..Default::default()
                    }
                } else {
                    PackedStats::default()
                }
            };
            MinMax {
                min: load(min, is_min_exact),
                max: load(max, is_max_exact),
                prefix: String::new(),
            }
        }
        Type::FIXED_LEN_BYTE_ARRAY => {
            // Special case for decimal16
            if min.len() == 16 && max.len() == 16 && is_min_exact && is_max_exact {
                let load = |v: &[u8]| -> PackedStats {
                    PackedStats {
                        lo8: u64::from_be_bytes(v[8..16].try_into().unwrap()),
                        hi8: u64::from_be_bytes(v[0..8].try_into().unwrap()),
                        len: 16,
                        ..Default::default()
                    }
                };
                return MinMax {
                    min: load(min),
                    max: load(max),
                    prefix: String::new(),
                };
            }
            pack_byte_array_stats(min, is_min_exact, max, is_max_exact)
        }
        Type::BYTE_ARRAY => pack_byte_array_stats(min, is_min_exact, max, is_max_exact),
    }
}

/// Pack byte array statistics with common prefix extraction
fn pack_byte_array_stats(
    min: &[u8],
    is_min_exact: bool,
    max: &[u8],
    is_max_exact: bool,
) -> MinMax {
    // Find common prefix
    let prefix_len = min
        .iter()
        .zip(max.iter())
        .take_while(|(a, b)| a == b)
        .count();

    let prefix = if prefix_len > 0 {
        String::from_utf8_lossy(&max[..prefix_len]).to_string()
    } else {
        String::new()
    };

    let load = |v: &[u8], is_exact: bool, is_max: bool| -> PackedStats {
        let suffix = &v[prefix_len..];
        if suffix.len() <= 4 {
            let mut buf = [0u8; 4];
            buf[..suffix.len()].copy_from_slice(suffix);
            PackedStats {
                lo4: u32::from_be_bytes(buf),
                len: if is_exact {
                    suffix.len() as i8
                } else {
                    -(suffix.len() as i8)
                },
                ..Default::default()
            }
        } else {
            // For longer values, we store the first 4 bytes adjusted
            let first4 = u32::from_be_bytes(suffix[..4].try_into().unwrap());
            PackedStats {
                lo4: if is_max {
                    first4.saturating_add(1)
                } else {
                    first4.saturating_sub(1)
                },
                len: -4,
                ..Default::default()
            }
        }
    };

    MinMax {
        min: load(min, is_min_exact, false),
        max: load(max, is_max_exact, true),
        prefix,
    }
}

/// Unpack statistics from FlatBuffers format back to byte arrays
fn unpack_statistics(
    physical_type: Type,
    packed: &MinMax,
) -> Option<(Vec<u8>, bool, Vec<u8>, bool)> {
    match physical_type {
        Type::BOOLEAN => None,
        Type::INT32 | Type::FLOAT => {
            let min = packed.min.lo4.to_le_bytes().to_vec();
            let max = packed.max.lo4.to_le_bytes().to_vec();
            Some((min, packed.min.len > 0, max, packed.max.len > 0))
        }
        Type::INT64 | Type::DOUBLE => {
            let min = packed.min.lo8.to_le_bytes().to_vec();
            let max = packed.max.lo8.to_le_bytes().to_vec();
            Some((min, packed.min.len > 0, max, packed.max.len > 0))
        }
        Type::INT96 => {
            let mut min = Vec::with_capacity(12);
            min.extend_from_slice(&packed.min.lo4.to_le_bytes());
            min.extend_from_slice(&packed.min.lo8.to_le_bytes());
            let mut max = Vec::with_capacity(12);
            max.extend_from_slice(&packed.max.lo4.to_le_bytes());
            max.extend_from_slice(&packed.max.lo8.to_le_bytes());
            Some((min, packed.min.len > 0, max, packed.max.len > 0))
        }
        Type::BYTE_ARRAY | Type::FIXED_LEN_BYTE_ARRAY => {
            let unpack = |p: &PackedStats, prefix: &str| -> (Vec<u8>, bool) {
                let mut result = prefix.as_bytes().to_vec();
                if p.len == 16 {
                    // Decimal16 case
                    result.extend_from_slice(&p.hi8.to_be_bytes());
                    result.extend_from_slice(&p.lo8.to_be_bytes());
                    (result, true)
                } else {
                    result.extend_from_slice(&p.lo4.to_be_bytes()[..(p.len.unsigned_abs() as usize).min(4)]);
                    (result, p.len >= 0)
                }
            };
            let (min, min_exact) = unpack(&packed.min, &packed.prefix);
            let (max, max_exact) = unpack(&packed.max, &packed.prefix);
            Some((min, min_exact, max, max_exact))
        }
    }
}

/// Converter from Parquet metadata to FlatBuffers format
pub struct ThriftToFlatBufferConverter<'a> {
    metadata: &'a ParquetMetaData,
    builder: FlatBufferBuilder<'a>,
}

impl<'a> ThriftToFlatBufferConverter<'a> {
    /// Create a new converter from ParquetMetaData
    pub fn new(metadata: &'a ParquetMetaData) -> Self {
        Self {
            metadata,
            builder: FlatBufferBuilder::with_capacity(64 * 1024),
        }
    }

    /// Convert the metadata to FlatBuffers format and return the serialized bytes
    pub fn convert(mut self) -> Vec<u8> {
        let file_metadata = self.metadata.file_metadata();
        let schema_descr = file_metadata.schema_descr();

        // Build schema elements
        let schema_offsets = self.build_schema(schema_descr);
        let schema = self.builder.create_vector(&schema_offsets);

        // Build row groups
        let row_group_offsets: Vec<_> = self
            .metadata
            .row_groups()
            .iter()
            .enumerate()
            .map(|(rg_idx, rg)| self.build_row_group(rg, rg_idx, schema_descr))
            .collect();
        let row_groups = self.builder.create_vector(&row_group_offsets);

        // Build key-value metadata
        let kv = file_metadata.key_value_metadata().map(|kvs| {
            let kv_offsets: Vec<_> = kvs.iter().map(|kv| self.build_kv(kv)).collect();
            self.builder.create_vector(&kv_offsets)
        });

        // Build created_by
        let created_by = file_metadata
            .created_by()
            .map(|s| self.builder.create_string(s));

        // Build FileMetaData
        let file_meta_data = fb::FileMetaData::create(
            &mut self.builder,
            &fb::FileMetaDataArgs {
                version: file_metadata.version(),
                schema: Some(schema),
                num_rows: file_metadata.num_rows(),
                row_groups: Some(row_groups),
                kv,
                created_by,
            },
        );

        self.builder.finish(file_meta_data, None);
        self.builder.finished_data().to_vec()
    }

    fn build_schema(
        &mut self,
        schema_descr: &SchemaDescriptor,
    ) -> Vec<WIPOffset<fb::SchemaElement<'a>>> {
        let root_schema = schema_descr.root_schema();
        let mut elements = Vec::new();
        self.build_schema_element(root_schema, &mut elements, true, schema_descr);
        elements
    }

    fn build_schema_element(
        &mut self,
        schema_type: &SchemaType,
        elements: &mut Vec<WIPOffset<fb::SchemaElement<'a>>>,
        is_root: bool,
        _schema_descr: &SchemaDescriptor,
    ) {
        let name = self.builder.create_string(schema_type.name());

        let (physical_type, type_length) = match schema_type {
            SchemaType::PrimitiveType {
                physical_type,
                type_length,
                ..
            } => (
                Some(convert_type_to_fb(*physical_type)),
                if *type_length > 0 {
                    Some(*type_length)
                } else {
                    None
                },
            ),
            SchemaType::GroupType { .. } => (None, None),
        };

        let repetition_type = if is_root {
            fb::FieldRepetitionType::REQUIRED
        } else {
            convert_repetition_to_fb(schema_type.get_basic_info().repetition())
        };

        let num_children = if schema_type.is_group() {
            schema_type.get_fields().len() as i32
        } else {
            0
        };

        let field_id_opt = if schema_type.get_basic_info().has_id() {
            Some(schema_type.get_basic_info().id())
        } else {
            None
        };

        // Build logical type if present
        let (logical_type_type, logical_type) =
            self.build_logical_type(schema_type.get_basic_info().logical_type_ref());

        // Build column order for leaf nodes
        let (column_order_type, column_order) = if !schema_type.is_group() {
            // For leaf nodes, add column order
            (
                fb::ColumnOrder::TypeDefinedOrder,
                Some(
                    fb::Empty::create(&mut self.builder, &fb::EmptyArgs {})
                        .as_union_value(),
                ),
            )
        } else {
            (fb::ColumnOrder::NONE, None)
        };

        let element = fb::SchemaElement::create(
            &mut self.builder,
            &fb::SchemaElementArgs {
                name: Some(name),
                type_: physical_type,
                repetition_type,
                logical_type_type,
                logical_type,
                type_length,
                num_children,
                field_id: field_id_opt,
                column_order_type,
                column_order,
            },
        );
        elements.push(element);

        // Recursively add children
        if schema_type.is_group() {
            for field in schema_type.get_fields() {
                self.build_schema_element(field, elements, false, _schema_descr);
            }
        }
    }

    fn build_logical_type(
        &mut self,
        logical_type: Option<&LogicalType>,
    ) -> (fb::LogicalType, Option<WIPOffset<flatbuffers::UnionWIPOffset>>) {
        match logical_type {
            None => (fb::LogicalType::NONE, None),
            Some(lt) => match lt {
                LogicalType::String => (
                    fb::LogicalType::StringType,
                    Some(fb::Empty::create(&mut self.builder, &fb::EmptyArgs {}).as_union_value()),
                ),
                LogicalType::Map => (
                    fb::LogicalType::MapType,
                    Some(fb::Empty::create(&mut self.builder, &fb::EmptyArgs {}).as_union_value()),
                ),
                LogicalType::List => (
                    fb::LogicalType::ListType,
                    Some(fb::Empty::create(&mut self.builder, &fb::EmptyArgs {}).as_union_value()),
                ),
                LogicalType::Enum => (
                    fb::LogicalType::EnumType,
                    Some(fb::Empty::create(&mut self.builder, &fb::EmptyArgs {}).as_union_value()),
                ),
                LogicalType::Decimal { precision, scale } => (
                    fb::LogicalType::DecimalType,
                    Some(
                        fb::DecimalOpts::create(
                            &mut self.builder,
                            &fb::DecimalOptsArgs {
                                precision: *precision,
                                scale: *scale,
                            },
                        )
                        .as_union_value(),
                    ),
                ),
                LogicalType::Date => (
                    fb::LogicalType::DateType,
                    Some(fb::Empty::create(&mut self.builder, &fb::EmptyArgs {}).as_union_value()),
                ),
                LogicalType::Time {
                    is_adjusted_to_u_t_c,
                    unit,
                } => (
                    fb::LogicalType::TimeType,
                    Some(
                        fb::TimeOpts::create(
                            &mut self.builder,
                            &fb::TimeOptsArgs {
                                is_adjusted_to_utc: *is_adjusted_to_u_t_c,
                                unit: convert_time_unit_to_fb(unit),
                            },
                        )
                        .as_union_value(),
                    ),
                ),
                LogicalType::Timestamp {
                    is_adjusted_to_u_t_c,
                    unit,
                } => (
                    fb::LogicalType::TimestampType,
                    Some(
                        fb::TimeOpts::create(
                            &mut self.builder,
                            &fb::TimeOptsArgs {
                                is_adjusted_to_utc: *is_adjusted_to_u_t_c,
                                unit: convert_time_unit_to_fb(unit),
                            },
                        )
                        .as_union_value(),
                    ),
                ),
                LogicalType::Integer {
                    bit_width,
                    is_signed,
                } => (
                    fb::LogicalType::IntType,
                    Some(
                        fb::IntOpts::create(
                            &mut self.builder,
                            &fb::IntOptsArgs {
                                bit_width: *bit_width as i8,
                                is_signed: *is_signed,
                            },
                        )
                        .as_union_value(),
                    ),
                ),
                LogicalType::Unknown => (
                    fb::LogicalType::NullType,
                    Some(fb::Empty::create(&mut self.builder, &fb::EmptyArgs {}).as_union_value()),
                ),
                LogicalType::Json => (
                    fb::LogicalType::JsonType,
                    Some(fb::Empty::create(&mut self.builder, &fb::EmptyArgs {}).as_union_value()),
                ),
                LogicalType::Bson => (
                    fb::LogicalType::BsonType,
                    Some(fb::Empty::create(&mut self.builder, &fb::EmptyArgs {}).as_union_value()),
                ),
                LogicalType::Uuid => (
                    fb::LogicalType::UUIDType,
                    Some(fb::Empty::create(&mut self.builder, &fb::EmptyArgs {}).as_union_value()),
                ),
                LogicalType::Float16 => (
                    fb::LogicalType::Float16Type,
                    Some(fb::Empty::create(&mut self.builder, &fb::EmptyArgs {}).as_union_value()),
                ),
                LogicalType::Variant { .. } => (
                    fb::LogicalType::VariantType,
                    Some(fb::Empty::create(&mut self.builder, &fb::EmptyArgs {}).as_union_value()),
                ),
                LogicalType::Geometry { crs } => {
                    let crs_str = crs.as_ref().map(|s| self.builder.create_string(s));
                    (
                        fb::LogicalType::GeometryType,
                        Some(
                            fb::GeometryType::create(
                                &mut self.builder,
                                &fb::GeometryTypeArgs { crs: crs_str },
                            )
                            .as_union_value(),
                        ),
                    )
                }
                LogicalType::Geography { crs, algorithm } => {
                    let crs_str = crs.as_ref().map(|s| self.builder.create_string(s));
                    let algo = algorithm.as_ref().map(|a| convert_edge_interpolation_to_fb(*a)).unwrap_or(fb::EdgeInterpolationAlgorithm::SPHERICAL);
                    (
                        fb::LogicalType::GeographyType,
                        Some(
                            fb::GeographyType::create(
                                &mut self.builder,
                                &fb::GeographyTypeArgs {
                                    crs: crs_str,
                                    algorithm: algo,
                                },
                            )
                            .as_union_value(),
                        ),
                    )
                }
                LogicalType::_Unknown { .. } => (fb::LogicalType::NONE, None),
            },
        }
    }

    fn build_row_group(
        &mut self,
        rg: &RowGroupMetaData,
        _rg_idx: usize,
        _schema_descr: &SchemaDescriptor,
    ) -> WIPOffset<fb::RowGroup<'a>> {
        // Build column chunks
        let column_offsets: Vec<_> = rg
            .columns()
            .iter()
            .enumerate()
            .map(|(col_idx, cc)| self.build_column_chunk(cc, rg, col_idx))
            .collect();
        let columns = self.builder.create_vector(&column_offsets);

        // Build sorting columns
        let sorting_columns = rg.sorting_columns().map(|scs| {
            let sc_offsets: Vec<_> = scs.iter().map(|sc| self.build_sorting_column(sc)).collect();
            self.builder.create_vector(&sc_offsets)
        });

        // Calculate total compressed size
        let total_compressed_size: i64 = rg.columns().iter().map(|c| c.compressed_size()).sum();

        fb::RowGroup::create(
            &mut self.builder,
            &fb::RowGroupArgs {
                columns: Some(columns),
                total_byte_size: rg.total_byte_size(),
                num_rows: rg.num_rows(),
                sorting_columns,
                file_offset: rg.file_offset().unwrap_or(0),
                total_compressed_size,
                ordinal: rg.ordinal(),
            },
        )
    }

    fn build_column_chunk(
        &mut self,
        cc: &ColumnChunkMetaData,
        rg: &RowGroupMetaData,
        _col_idx: usize,
    ) -> WIPOffset<fb::ColumnChunk<'a>> {
        let meta_data = self.build_column_metadata(cc, rg);

        let file_path = cc.file_path().map(|s| self.builder.create_string(s));

        fb::ColumnChunk::create(
            &mut self.builder,
            &fb::ColumnChunkArgs {
                file_path,
                meta_data: Some(meta_data),
            },
        )
    }

    fn build_column_metadata(
        &mut self,
        cc: &ColumnChunkMetaData,
        rg: &RowGroupMetaData,
    ) -> WIPOffset<fb::ColumnMetadata<'a>> {
        // Build key-value metadata if present
        let key_value_metadata = None; // Column-level KV metadata not commonly used

        // Build statistics
        let statistics = cc.statistics().map(|stats| self.build_statistics(stats, cc.column_type()));

        // Determine if fully dictionary encoded
        let is_fully_dict_encoded = cc.dictionary_page_offset().is_some()
            && cc.page_encoding_stats_mask().is_some_and(|mask| {
                mask.is_only(Encoding::PLAIN_DICTIONARY) || mask.is_only(Encoding::RLE_DICTIONARY)
            });

        // Only store num_values if different from row group num_rows
        let num_values = if cc.num_values() != rg.num_rows() {
            Some(cc.num_values())
        } else {
            None
        };

        fb::ColumnMetadata::create(
            &mut self.builder,
            &fb::ColumnMetadataArgs {
                codec: convert_compression_to_fb(cc.compression()),
                num_values,
                total_uncompressed_size: cc.uncompressed_size(),
                total_compressed_size: cc.compressed_size(),
                key_value_metadata,
                data_page_offset: cc.data_page_offset(),
                index_page_offset: cc.index_page_offset(),
                dictionary_page_offset: cc.dictionary_page_offset(),
                statistics,
                is_fully_dict_encoded,
                bloom_filter_offset: cc.bloom_filter_offset(),
                bloom_filter_length: cc.bloom_filter_length(),
            },
        )
    }

    fn build_statistics(
        &mut self,
        stats: &Statistics,
        physical_type: Type,
    ) -> WIPOffset<fb::Statistics<'a>> {
        let null_count = stats.null_count_opt().map(|n| n as i32);

        // Pack min/max values
        let (min_stats, max_stats, prefix_str) = if let (Some(min), Some(max)) =
            (stats.min_bytes_opt(), stats.max_bytes_opt())
        {
            let packed = pack_statistics(
                physical_type,
                min,
                stats.is_min_max_backwards_compatible(),
                max,
                stats.is_min_max_backwards_compatible(),
            );
            (Some(packed.min), Some(packed.max), packed.prefix)
        } else {
            (None, None, String::new())
        };

        let prefix = if !prefix_str.is_empty() {
            Some(self.builder.create_string(&prefix_str))
        } else {
            None
        };

        fb::Statistics::create(
            &mut self.builder,
            &fb::StatisticsArgs {
                null_count,
                min_lo4: min_stats.as_ref().map(|s| s.lo4).unwrap_or(0),
                min_lo8: min_stats.as_ref().map(|s| s.lo8).unwrap_or(0),
                min_hi8: min_stats.as_ref().map(|s| s.hi8).unwrap_or(0),
                min_len: min_stats.map(|s| s.len),
                max_lo4: max_stats.as_ref().map(|s| s.lo4).unwrap_or(0),
                max_lo8: max_stats.as_ref().map(|s| s.lo8).unwrap_or(0),
                max_hi8: max_stats.as_ref().map(|s| s.hi8).unwrap_or(0),
                max_len: max_stats.map(|s| s.len),
                prefix,
            },
        )
    }

    fn build_sorting_column(&mut self, sc: &SortingColumn) -> WIPOffset<fb::SortingColumn<'a>> {
        fb::SortingColumn::create(
            &mut self.builder,
            &fb::SortingColumnArgs {
                column_idx: sc.column_idx,
                descending: sc.descending,
                nulls_first: sc.nulls_first,
            },
        )
    }

    fn build_kv(&mut self, kv: &KeyValue) -> WIPOffset<fb::KV<'a>> {
        let key = self.builder.create_string(&kv.key);
        let val = kv.value.as_ref().map(|v| self.builder.create_string(v));

        fb::KV::create(
            &mut self.builder,
            &fb::KVArgs {
                key: Some(key),
                val,
            },
        )
    }
}

/// Converter from FlatBuffers format back to Parquet metadata
pub struct FlatBufferConverter;

impl FlatBufferConverter {
    /// Parse FlatBuffers metadata from bytes and convert to ParquetMetaData
    pub fn convert(buf: &[u8], schema_descr: SchemaDescPtr) -> Result<ParquetMetaData> {
        let fb_meta = fb::root_as_file_meta_data(buf)
            .map_err(|e| ParquetError::General(format!("Invalid FlatBuffer: {}", e)))?;

        let file_metadata = Self::convert_file_metadata(&fb_meta, schema_descr.clone())?;
        let row_groups = Self::convert_row_groups(&fb_meta, schema_descr)?;

        Ok(ParquetMetaData::new(file_metadata, row_groups))
    }

    fn convert_file_metadata(
        fb_meta: &fb::FileMetaData,
        schema_descr: SchemaDescPtr,
    ) -> Result<FileMetaData> {
        let version = fb_meta.version();
        let num_rows = fb_meta.num_rows();

        let created_by = fb_meta.created_by().map(|s| s.to_string());

        let key_value_metadata = fb_meta.kv().map(|kvs| {
            kvs.iter()
                .map(|kv| KeyValue {
                    key: kv.key().unwrap_or("").to_string(),
                    value: kv.val().map(|v| v.to_string()),
                })
                .collect()
        });

        // Extract column orders from schema elements
        let column_orders = fb_meta.schema().map(|schema| {
            schema
                .iter()
                .filter(|e| e.num_children() == 0) // Only leaf nodes
                .map(|e| {
                    if e.column_order_type() == fb::ColumnOrder::TypeDefinedOrder {
                        ColumnOrder::TYPE_DEFINED_ORDER(crate::basic::SortOrder::SIGNED)
                    } else {
                        ColumnOrder::UNDEFINED
                    }
                })
                .collect()
        });

        Ok(FileMetaData::new(
            version,
            num_rows,
            created_by,
            key_value_metadata,
            schema_descr,
            column_orders,
        ))
    }

    fn convert_row_groups(
        fb_meta: &fb::FileMetaData,
        schema_descr: SchemaDescPtr,
    ) -> Result<Vec<RowGroupMetaData>> {
        let row_groups = fb_meta.row_groups().ok_or_else(|| {
            ParquetError::General("FlatBuffer metadata missing row_groups".to_string())
        })?;

        row_groups
            .iter()
            .map(|rg| Self::convert_row_group(&rg, schema_descr.clone()))
            .collect()
    }

    fn convert_row_group(
        fb_rg: &fb::RowGroup,
        schema_descr: SchemaDescPtr,
    ) -> Result<RowGroupMetaData> {
        let columns = fb_rg.columns().ok_or_else(|| {
            ParquetError::General("FlatBuffer row group missing columns".to_string())
        })?;

        let mut builder = RowGroupMetaDataBuilder::new(schema_descr.clone())
            .set_num_rows(fb_rg.num_rows())
            .set_total_byte_size(fb_rg.total_byte_size());

        if fb_rg.file_offset() != 0 {
            builder = builder.set_file_offset(fb_rg.file_offset());
        }

        if let Some(ordinal) = fb_rg.ordinal() {
            builder = builder.set_ordinal(ordinal);
        }

        // Convert sorting columns
        if let Some(sorting_cols) = fb_rg.sorting_columns() {
            let sorting: Vec<_> = sorting_cols
                .iter()
                .map(|sc| SortingColumn {
                    column_idx: sc.column_idx(),
                    descending: sc.descending(),
                    nulls_first: sc.nulls_first(),
                })
                .collect();
            builder = builder.set_sorting_columns(Some(sorting));
        }

        // Convert column chunks
        for (col_idx, fb_cc) in columns.iter().enumerate() {
            let column_descr = schema_descr.column(col_idx);
            let cc = Self::convert_column_chunk(&fb_cc, fb_rg, column_descr)?;
            builder = builder.add_column_metadata(cc);
        }

        builder.build()
    }

    fn convert_column_chunk(
        fb_cc: &fb::ColumnChunk,
        fb_rg: &fb::RowGroup,
        column_descr: Arc<ColumnDescriptor>,
    ) -> Result<ColumnChunkMetaData> {
        let fb_meta = fb_cc.meta_data().ok_or_else(|| {
            ParquetError::General("FlatBuffer column chunk missing metadata".to_string())
        })?;

        let physical_type = column_descr.physical_type();
        let mut builder = ColumnChunkMetaDataBuilder::new(column_descr)
            .set_compression(convert_compression_from_fb(fb_meta.codec()))
            .set_num_values(fb_meta.num_values().unwrap_or(fb_rg.num_rows()))
            .set_total_compressed_size(fb_meta.total_compressed_size())
            .set_total_uncompressed_size(fb_meta.total_uncompressed_size())
            .set_data_page_offset(fb_meta.data_page_offset());

        if let Some(offset) = fb_meta.index_page_offset() {
            builder = builder.set_index_page_offset(Some(offset));
        }

        if let Some(offset) = fb_meta.dictionary_page_offset() {
            builder = builder.set_dictionary_page_offset(Some(offset));
        }

        if let Some(offset) = fb_meta.bloom_filter_offset() {
            builder = builder.set_bloom_filter_offset(Some(offset));
        }

        if let Some(length) = fb_meta.bloom_filter_length() {
            builder = builder.set_bloom_filter_length(Some(length));
        }

        // Convert statistics
        if let Some(fb_stats) = fb_meta.statistics() {
            let stats = Self::convert_statistics(&fb_stats, physical_type)?;
            if let Some(s) = stats {
                builder = builder.set_statistics(s);
            }
        }

        // Set file path if present
        if let Some(path) = fb_cc.file_path() {
            builder = builder.set_file_path(path.to_string());
        }

        builder.build()
    }

    fn convert_statistics(fb_stats: &fb::Statistics, physical_type: Type) -> Result<Option<Statistics>> {
        let null_count = fb_stats.null_count().map(|n| n as u64);

        // Unpack min/max values
        let (min_bytes, max_bytes) = if fb_stats.min_len().is_some() && fb_stats.max_len().is_some()
        {
            let packed = MinMax {
                min: PackedStats {
                    lo4: fb_stats.min_lo4(),
                    lo8: fb_stats.min_lo8(),
                    hi8: fb_stats.min_hi8(),
                    len: fb_stats.min_len().unwrap_or(0),
                },
                max: PackedStats {
                    lo4: fb_stats.max_lo4(),
                    lo8: fb_stats.max_lo8(),
                    hi8: fb_stats.max_hi8(),
                    len: fb_stats.max_len().unwrap_or(0),
                },
                prefix: fb_stats.prefix().unwrap_or("").to_string(),
            };

            match unpack_statistics(physical_type, &packed) {
                Some((min, _min_exact, max, _max_exact)) => (Some(min), Some(max)),
                None => (None, None),
            }
        } else {
            (None, None)
        };

        // Create statistics based on physical type
        let stats = match physical_type {
            Type::BOOLEAN => {
                if let (Some(min), Some(max)) = (min_bytes.as_ref(), max_bytes.as_ref()) {
                    Statistics::boolean(
                        min.first().map(|&b| b != 0),
                        max.first().map(|&b| b != 0),
                        None,
                        null_count,
                        false,
                    )
                } else {
                    return Ok(None);
                }
            }
            Type::INT32 => {
                if let (Some(min), Some(max)) = (min_bytes.as_ref(), max_bytes.as_ref()) {
                    Statistics::int32(
                        Some(i32::from_le_bytes(min[..4].try_into().unwrap())),
                        Some(i32::from_le_bytes(max[..4].try_into().unwrap())),
                        None,
                        null_count,
                        false,
                    )
                } else {
                    return Ok(None);
                }
            }
            Type::INT64 => {
                if let (Some(min), Some(max)) = (min_bytes.as_ref(), max_bytes.as_ref()) {
                    Statistics::int64(
                        Some(i64::from_le_bytes(min[..8].try_into().unwrap())),
                        Some(i64::from_le_bytes(max[..8].try_into().unwrap())),
                        None,
                        null_count,
                        false,
                    )
                } else {
                    return Ok(None);
                }
            }
            Type::FLOAT => {
                if let (Some(min), Some(max)) = (min_bytes.as_ref(), max_bytes.as_ref()) {
                    Statistics::float(
                        Some(f32::from_le_bytes(min[..4].try_into().unwrap())),
                        Some(f32::from_le_bytes(max[..4].try_into().unwrap())),
                        None,
                        null_count,
                        false,
                    )
                } else {
                    return Ok(None);
                }
            }
            Type::DOUBLE => {
                if let (Some(min), Some(max)) = (min_bytes.as_ref(), max_bytes.as_ref()) {
                    Statistics::double(
                        Some(f64::from_le_bytes(min[..8].try_into().unwrap())),
                        Some(f64::from_le_bytes(max[..8].try_into().unwrap())),
                        None,
                        null_count,
                        false,
                    )
                } else {
                    return Ok(None);
                }
            }
            Type::INT96 | Type::BYTE_ARRAY | Type::FIXED_LEN_BYTE_ARRAY => {
                // For these types, we don't fully reconstruct statistics
                // as they require more context
                return Ok(None);
            }
        };

        Ok(Some(stats))
    }
}

// Conversion helper functions

fn convert_type_to_fb(t: Type) -> fb::Type {
    match t {
        Type::BOOLEAN => fb::Type::BOOLEAN,
        Type::INT32 => fb::Type::INT32,
        Type::INT64 => fb::Type::INT64,
        Type::INT96 => fb::Type::INT96,
        Type::FLOAT => fb::Type::FLOAT,
        Type::DOUBLE => fb::Type::DOUBLE,
        Type::BYTE_ARRAY => fb::Type::BYTE_ARRAY,
        Type::FIXED_LEN_BYTE_ARRAY => fb::Type::FIXED_LEN_BYTE_ARRAY,
    }
}

#[allow(dead_code)]
fn convert_type_from_fb(t: fb::Type) -> Type {
    match t {
        fb::Type::BOOLEAN => Type::BOOLEAN,
        fb::Type::INT32 => Type::INT32,
        fb::Type::INT64 => Type::INT64,
        fb::Type::INT96 => Type::INT96,
        fb::Type::FLOAT => Type::FLOAT,
        fb::Type::DOUBLE => Type::DOUBLE,
        fb::Type::BYTE_ARRAY => Type::BYTE_ARRAY,
        fb::Type::FIXED_LEN_BYTE_ARRAY => Type::FIXED_LEN_BYTE_ARRAY,
        _ => Type::BYTE_ARRAY, // Default fallback
    }
}

fn convert_repetition_to_fb(r: Repetition) -> fb::FieldRepetitionType {
    match r {
        Repetition::REQUIRED => fb::FieldRepetitionType::REQUIRED,
        Repetition::OPTIONAL => fb::FieldRepetitionType::OPTIONAL,
        Repetition::REPEATED => fb::FieldRepetitionType::REPEATED,
    }
}

#[allow(dead_code)]
fn convert_repetition_from_fb(r: fb::FieldRepetitionType) -> Repetition {
    match r {
        fb::FieldRepetitionType::REQUIRED => Repetition::REQUIRED,
        fb::FieldRepetitionType::OPTIONAL => Repetition::OPTIONAL,
        fb::FieldRepetitionType::REPEATED => Repetition::REPEATED,
        _ => Repetition::OPTIONAL, // Default fallback
    }
}

fn convert_compression_to_fb(c: Compression) -> fb::CompressionCodec {
    match c {
        Compression::UNCOMPRESSED => fb::CompressionCodec::UNCOMPRESSED,
        Compression::SNAPPY => fb::CompressionCodec::SNAPPY,
        Compression::GZIP(_) => fb::CompressionCodec::GZIP,
        Compression::LZO => fb::CompressionCodec::LZO,
        Compression::BROTLI(_) => fb::CompressionCodec::BROTLI,
        Compression::ZSTD(_) => fb::CompressionCodec::ZSTD,
        Compression::LZ4_RAW => fb::CompressionCodec::LZ4_RAW,
        Compression::LZ4 => fb::CompressionCodec::LZ4_RAW, // LZ4 maps to LZ4_RAW
    }
}

fn convert_compression_from_fb(c: fb::CompressionCodec) -> Compression {
    match c {
        fb::CompressionCodec::UNCOMPRESSED => Compression::UNCOMPRESSED,
        fb::CompressionCodec::SNAPPY => Compression::SNAPPY,
        fb::CompressionCodec::GZIP => Compression::GZIP(Default::default()),
        fb::CompressionCodec::LZO => Compression::LZO,
        fb::CompressionCodec::BROTLI => Compression::BROTLI(Default::default()),
        fb::CompressionCodec::ZSTD => Compression::ZSTD(Default::default()),
        fb::CompressionCodec::LZ4_RAW => Compression::LZ4_RAW,
        _ => Compression::UNCOMPRESSED, // Default fallback
    }
}

fn convert_time_unit_to_fb(unit: &crate::basic::TimeUnit) -> fb::TimeUnit {
    match unit {
        crate::basic::TimeUnit::MILLIS => fb::TimeUnit::MS,
        crate::basic::TimeUnit::MICROS => fb::TimeUnit::US,
        crate::basic::TimeUnit::NANOS => fb::TimeUnit::NS,
    }
}

fn convert_edge_interpolation_to_fb(
    algo: crate::basic::EdgeInterpolationAlgorithm,
) -> fb::EdgeInterpolationAlgorithm {
    match algo {
        crate::basic::EdgeInterpolationAlgorithm::SPHERICAL => {
            fb::EdgeInterpolationAlgorithm::SPHERICAL
        }
        crate::basic::EdgeInterpolationAlgorithm::VINCENTY => {
            fb::EdgeInterpolationAlgorithm::VINCENTY
        }
        crate::basic::EdgeInterpolationAlgorithm::THOMAS => fb::EdgeInterpolationAlgorithm::THOMAS,
        crate::basic::EdgeInterpolationAlgorithm::ANDOYER => {
            fb::EdgeInterpolationAlgorithm::ANDOYER
        }
        crate::basic::EdgeInterpolationAlgorithm::KARNEY => fb::EdgeInterpolationAlgorithm::KARNEY,
        crate::basic::EdgeInterpolationAlgorithm::_Unknown(_) => {
            fb::EdgeInterpolationAlgorithm::SPHERICAL
        }
    }
}

/// Convert ParquetMetaData to FlatBuffers format
pub fn parquet_metadata_to_flatbuf(metadata: &ParquetMetaData) -> Vec<u8> {
    ThriftToFlatBufferConverter::new(metadata).convert()
}

/// Convert FlatBuffers format back to ParquetMetaData
pub fn flatbuf_to_parquet_metadata(
    buf: &[u8],
    schema_descr: SchemaDescPtr,
) -> Result<ParquetMetaData> {
    FlatBufferConverter::convert(buf, schema_descr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::basic::Type as PhysicalType;
    use crate::schema::parser::parse_message_type;

    fn test_schema() -> SchemaDescPtr {
        let schema_str = "
            message test_schema {
                REQUIRED INT32 id;
                OPTIONAL BYTE_ARRAY name (UTF8);
                REQUIRED INT64 timestamp;
            }
        ";
        let schema = parse_message_type(schema_str).unwrap();
        Arc::new(SchemaDescriptor::new(Arc::new(schema)))
    }

    #[test]
    fn test_pack_unpack_int32_stats() {
        let min = 42i32.to_le_bytes().to_vec();
        let max = 100i32.to_le_bytes().to_vec();

        let packed = pack_statistics(PhysicalType::INT32, &min, true, &max, true);

        assert_eq!(packed.min.lo4, 42);
        assert_eq!(packed.max.lo4, 100);
        assert_eq!(packed.min.len, 4);
        assert_eq!(packed.max.len, 4);

        let unpacked = unpack_statistics(PhysicalType::INT32, &packed).unwrap();
        assert_eq!(unpacked.0, min);
        assert_eq!(unpacked.2, max);
    }

    #[test]
    fn test_pack_unpack_int64_stats() {
        let min = 42i64.to_le_bytes().to_vec();
        let max = 100i64.to_le_bytes().to_vec();

        let packed = pack_statistics(PhysicalType::INT64, &min, true, &max, true);

        assert_eq!(packed.min.lo8, 42);
        assert_eq!(packed.max.lo8, 100);
        assert_eq!(packed.min.len, 8);
        assert_eq!(packed.max.len, 8);

        let unpacked = unpack_statistics(PhysicalType::INT64, &packed).unwrap();
        assert_eq!(unpacked.0, min);
        assert_eq!(unpacked.2, max);
    }

    #[test]
    fn test_type_conversion() {
        assert_eq!(convert_type_from_fb(convert_type_to_fb(Type::BOOLEAN)), Type::BOOLEAN);
        assert_eq!(convert_type_from_fb(convert_type_to_fb(Type::INT32)), Type::INT32);
        assert_eq!(convert_type_from_fb(convert_type_to_fb(Type::INT64)), Type::INT64);
        assert_eq!(convert_type_from_fb(convert_type_to_fb(Type::FLOAT)), Type::FLOAT);
        assert_eq!(convert_type_from_fb(convert_type_to_fb(Type::DOUBLE)), Type::DOUBLE);
    }

    #[test]
    fn test_compression_conversion() {
        assert_eq!(
            convert_compression_from_fb(convert_compression_to_fb(Compression::UNCOMPRESSED)),
            Compression::UNCOMPRESSED
        );
        assert_eq!(
            convert_compression_from_fb(convert_compression_to_fb(Compression::SNAPPY)),
            Compression::SNAPPY
        );
    }

    #[test]
    fn test_metadata_roundtrip() {
        use crate::file::metadata::{
            ColumnChunkMetaDataBuilder, FileMetaData, ParquetMetaData,
            ParquetMetaDataBuilder, RowGroupMetaDataBuilder,
        };

        // Create a test schema
        let schema_descr = test_schema();

        // Build a simple FileMetaData
        let file_meta = FileMetaData::new(
            2,          // version
            1000,       // num_rows
            Some("test-created-by".to_string()),
            Some(vec![
                KeyValue {
                    key: "test-key".to_string(),
                    value: Some("test-value".to_string()),
                },
            ]),
            schema_descr.clone(),
            None,       // column_orders
        );

        // Build a row group with column chunks
        let mut rg_builder = RowGroupMetaDataBuilder::new(schema_descr.clone())
            .set_num_rows(1000)
            .set_total_byte_size(10000);

        // Add column chunks for each column
        for i in 0..schema_descr.num_columns() {
            let col_descr = schema_descr.column(i);
            let cc = ColumnChunkMetaDataBuilder::new(col_descr)
                .set_compression(Compression::SNAPPY)
                .set_num_values(1000)
                .set_total_compressed_size(1000)
                .set_total_uncompressed_size(2000)
                .set_data_page_offset(1000 + (i as i64 * 1000))
                .build()
                .unwrap();
            rg_builder = rg_builder.add_column_metadata(cc);
        }
        let row_group = rg_builder.build().unwrap();

        // Create ParquetMetaData
        let original_metadata = ParquetMetaDataBuilder::new(file_meta)
            .add_row_group(row_group)
            .build();

        // Convert to FlatBuffers
        let fb_bytes = parquet_metadata_to_flatbuf(&original_metadata);

        // Verify it was serialized
        assert!(!fb_bytes.is_empty());

        // Convert back from FlatBuffers
        let converted_metadata =
            flatbuf_to_parquet_metadata(&fb_bytes, schema_descr.clone()).unwrap();

        // Verify basic metadata fields
        assert_eq!(
            original_metadata.file_metadata().version(),
            converted_metadata.file_metadata().version()
        );
        assert_eq!(
            original_metadata.file_metadata().num_rows(),
            converted_metadata.file_metadata().num_rows()
        );
        assert_eq!(
            original_metadata.file_metadata().created_by(),
            converted_metadata.file_metadata().created_by()
        );

        // Verify row group count
        assert_eq!(
            original_metadata.num_row_groups(),
            converted_metadata.num_row_groups()
        );

        // Verify row group details
        let orig_rg = original_metadata.row_group(0);
        let conv_rg = converted_metadata.row_group(0);
        assert_eq!(orig_rg.num_rows(), conv_rg.num_rows());
        assert_eq!(orig_rg.num_columns(), conv_rg.num_columns());

        // Verify column chunk details
        for i in 0..orig_rg.num_columns() {
            let orig_cc = orig_rg.column(i);
            let conv_cc = conv_rg.column(i);
            assert_eq!(orig_cc.compression(), conv_cc.compression());
            assert_eq!(orig_cc.num_values(), conv_cc.num_values());
            assert_eq!(orig_cc.compressed_size(), conv_cc.compressed_size());
            assert_eq!(orig_cc.uncompressed_size(), conv_cc.uncompressed_size());
            assert_eq!(orig_cc.data_page_offset(), conv_cc.data_page_offset());
        }
    }
}
