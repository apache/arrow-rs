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

//! [`StatisticsConverter`] to convert statistics in parquet format to arrow [`ArrayRef`].

/// Notice that all the corresponding tests are in
/// `arrow-rs/parquet/tests/arrow_reader/statistics.rs`.
use crate::arrow::buffer::bit_util::sign_extend_be;
use crate::arrow::parquet_column;
use crate::data_type::{ByteArray, FixedLenByteArray};
use crate::errors::{ParquetError, Result};
use crate::file::metadata::{ParquetColumnIndex, ParquetOffsetIndex, RowGroupMetaData};
use crate::file::page_index::index::{Index, PageIndex};
use crate::file::statistics::Statistics as ParquetStatistics;
use crate::schema::types::SchemaDescriptor;
use arrow_array::builder::{
    BinaryViewBuilder, BooleanBuilder, FixedSizeBinaryBuilder, LargeStringBuilder, StringBuilder,
    StringViewBuilder,
};
use arrow_array::{
    new_empty_array, new_null_array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array,
    Decimal128Array, Decimal256Array, Float16Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, LargeBinaryArray, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow_buffer::i256;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use half::f16;
use paste::paste;
use std::sync::Arc;

// Convert the bytes array to i128.
// The endian of the input bytes array must be big-endian.
pub(crate) fn from_bytes_to_i128(b: &[u8]) -> i128 {
    // The bytes array are from parquet file and must be the big-endian.
    // The endian is defined by parquet format, and the reference document
    // https://github.com/apache/parquet-format/blob/54e53e5d7794d383529dd30746378f19a12afd58/src/main/thrift/parquet.thrift#L66
    i128::from_be_bytes(sign_extend_be::<16>(b))
}

// Convert the bytes array to i256.
// The endian of the input bytes array must be big-endian.
pub(crate) fn from_bytes_to_i256(b: &[u8]) -> i256 {
    i256::from_be_bytes(sign_extend_be::<32>(b))
}

// Convert the bytes array to f16
pub(crate) fn from_bytes_to_f16(b: &[u8]) -> Option<f16> {
    match b {
        [low, high] => Some(f16::from_be_bytes([*high, *low])),
        _ => None,
    }
}

/// Define an adapter iterator for extracting statistics from an iterator of
/// `ParquetStatistics`
///
///
/// Handles checking if the statistics are present and valid with the correct type.
///
/// Parameters:
/// * `$iterator_type` is the name of the iterator type (e.g. `MinBooleanStatsIterator`)
/// * `$func` is the function to call to get the value (e.g. `min` or `max`)
/// * `$parquet_statistics_type` is the type of the statistics (e.g. `ParquetStatistics::Boolean`)
/// * `$stat_value_type` is the type of the statistics value (e.g. `bool`)
macro_rules! make_stats_iterator {
    ($iterator_type:ident, $func:ident, $parquet_statistics_type:path, $stat_value_type:ty) => {
        /// Maps an iterator of `ParquetStatistics` into an iterator of
        /// `&$stat_value_type``
        ///
        /// Yielded elements:
        /// * Some(stats) if valid
        /// * None if the statistics are not present, not valid, or not $stat_value_type
        struct $iterator_type<'a, I>
        where
            I: Iterator<Item = Option<&'a ParquetStatistics>>,
        {
            iter: I,
        }

        impl<'a, I> $iterator_type<'a, I>
        where
            I: Iterator<Item = Option<&'a ParquetStatistics>>,
        {
            /// Create a new iterator to extract the statistics
            fn new(iter: I) -> Self {
                Self { iter }
            }
        }

        /// Implement the Iterator trait for the iterator
        impl<'a, I> Iterator for $iterator_type<'a, I>
        where
            I: Iterator<Item = Option<&'a ParquetStatistics>>,
        {
            type Item = Option<&'a $stat_value_type>;

            /// return the next statistics value
            fn next(&mut self) -> Option<Self::Item> {
                let next = self.iter.next();
                next.map(|x| {
                    x.and_then(|stats| match stats {
                        $parquet_statistics_type(s) => s.$func(),
                        _ => None,
                    })
                })
            }

            fn size_hint(&self) -> (usize, Option<usize>) {
                self.iter.size_hint()
            }
        }
    };
}

make_stats_iterator!(
    MinBooleanStatsIterator,
    min_opt,
    ParquetStatistics::Boolean,
    bool
);
make_stats_iterator!(
    MaxBooleanStatsIterator,
    max_opt,
    ParquetStatistics::Boolean,
    bool
);
make_stats_iterator!(
    MinInt32StatsIterator,
    min_opt,
    ParquetStatistics::Int32,
    i32
);
make_stats_iterator!(
    MaxInt32StatsIterator,
    max_opt,
    ParquetStatistics::Int32,
    i32
);
make_stats_iterator!(
    MinInt64StatsIterator,
    min_opt,
    ParquetStatistics::Int64,
    i64
);
make_stats_iterator!(
    MaxInt64StatsIterator,
    max_opt,
    ParquetStatistics::Int64,
    i64
);
make_stats_iterator!(
    MinFloatStatsIterator,
    min_opt,
    ParquetStatistics::Float,
    f32
);
make_stats_iterator!(
    MaxFloatStatsIterator,
    max_opt,
    ParquetStatistics::Float,
    f32
);
make_stats_iterator!(
    MinDoubleStatsIterator,
    min_opt,
    ParquetStatistics::Double,
    f64
);
make_stats_iterator!(
    MaxDoubleStatsIterator,
    max_opt,
    ParquetStatistics::Double,
    f64
);
make_stats_iterator!(
    MinByteArrayStatsIterator,
    min_bytes_opt,
    ParquetStatistics::ByteArray,
    [u8]
);
make_stats_iterator!(
    MaxByteArrayStatsIterator,
    max_bytes_opt,
    ParquetStatistics::ByteArray,
    [u8]
);
make_stats_iterator!(
    MinFixedLenByteArrayStatsIterator,
    min_bytes_opt,
    ParquetStatistics::FixedLenByteArray,
    [u8]
);
make_stats_iterator!(
    MaxFixedLenByteArrayStatsIterator,
    max_bytes_opt,
    ParquetStatistics::FixedLenByteArray,
    [u8]
);

/// Special iterator adapter for extracting i128 values from from an iterator of
/// `ParquetStatistics`
///
/// Handles checking if the statistics are present and valid with the correct type.
///
/// Depending on the parquet file, the statistics for `Decimal128` can be stored as
/// `Int32`, `Int64` or `ByteArray` or `FixedSizeByteArray` :mindblown:
///
/// This iterator handles all cases, extracting the values
/// and converting it to `stat_value_type`.
///
/// Parameters:
/// * `$iterator_type` is the name of the iterator type (e.g. `MinBooleanStatsIterator`)
/// * `$func` is the function to call to get the value (e.g. `min` or `max`)
/// * `$bytes_func` is the function to call to get the value as bytes (e.g. `min_bytes` or `max_bytes`)
/// * `$stat_value_type` is the type of the statistics value (e.g. `i128`)
/// * `convert_func` is the function to convert the bytes to stats value (e.g. `from_bytes_to_i128`)
macro_rules! make_decimal_stats_iterator {
    ($iterator_type:ident, $func:ident, $bytes_func:ident, $stat_value_type:ident, $convert_func: ident) => {
        struct $iterator_type<'a, I>
        where
            I: Iterator<Item = Option<&'a ParquetStatistics>>,
        {
            iter: I,
        }

        impl<'a, I> $iterator_type<'a, I>
        where
            I: Iterator<Item = Option<&'a ParquetStatistics>>,
        {
            fn new(iter: I) -> Self {
                Self { iter }
            }
        }

        impl<'a, I> Iterator for $iterator_type<'a, I>
        where
            I: Iterator<Item = Option<&'a ParquetStatistics>>,
        {
            type Item = Option<$stat_value_type>;

            fn next(&mut self) -> Option<Self::Item> {
                let next = self.iter.next();
                next.map(|x| {
                    x.and_then(|stats| match stats {
                        ParquetStatistics::Int32(s) => {
                            s.$func().map(|x| $stat_value_type::from(*x))
                        }
                        ParquetStatistics::Int64(s) => {
                            s.$func().map(|x| $stat_value_type::from(*x))
                        }
                        ParquetStatistics::ByteArray(s) => s.$bytes_func().map($convert_func),
                        ParquetStatistics::FixedLenByteArray(s) => {
                            s.$bytes_func().map($convert_func)
                        }
                        _ => None,
                    })
                })
            }

            fn size_hint(&self) -> (usize, Option<usize>) {
                self.iter.size_hint()
            }
        }
    };
}

make_decimal_stats_iterator!(
    MinDecimal128StatsIterator,
    min_opt,
    min_bytes_opt,
    i128,
    from_bytes_to_i128
);
make_decimal_stats_iterator!(
    MaxDecimal128StatsIterator,
    max_opt,
    max_bytes_opt,
    i128,
    from_bytes_to_i128
);
make_decimal_stats_iterator!(
    MinDecimal256StatsIterator,
    min_opt,
    min_bytes_opt,
    i256,
    from_bytes_to_i256
);
make_decimal_stats_iterator!(
    MaxDecimal256StatsIterator,
    max_opt,
    max_bytes_opt,
    i256,
    from_bytes_to_i256
);

/// Special macro to combine the statistics iterators for min and max using the [`mod@paste`] macro.
/// This is used to avoid repeating the same code for min and max statistics extractions
///
/// Parameters:
/// stat_type_prefix: The prefix of the statistics iterator type (e.g. `Min` or `Max`)
/// data_type: The data type of the statistics (e.g. `DataType::Int32`)
/// iterator: The iterator of [`ParquetStatistics`] to extract the statistics from.
macro_rules! get_statistics {
    ($stat_type_prefix: ident, $data_type: ident, $iterator: ident) => {
        paste! {
        match $data_type {
            DataType::Boolean => Ok(Arc::new(BooleanArray::from_iter(
                [<$stat_type_prefix BooleanStatsIterator>]::new($iterator).map(|x| x.copied()),
            ))),
            DataType::Int8 => Ok(Arc::new(Int8Array::from_iter(
                [<$stat_type_prefix Int32StatsIterator>]::new($iterator).map(|x| {
                    x.and_then(|x| i8::try_from(*x).ok())
                }),
            ))),
            DataType::Int16 => Ok(Arc::new(Int16Array::from_iter(
                [<$stat_type_prefix Int32StatsIterator>]::new($iterator).map(|x| {
                    x.and_then(|x| i16::try_from(*x).ok())
                }),
            ))),
            DataType::Int32 => Ok(Arc::new(Int32Array::from_iter(
                [<$stat_type_prefix Int32StatsIterator>]::new($iterator).map(|x| x.copied()),
            ))),
            DataType::Int64 => Ok(Arc::new(Int64Array::from_iter(
                [<$stat_type_prefix Int64StatsIterator>]::new($iterator).map(|x| x.copied()),
            ))),
            DataType::UInt8 => Ok(Arc::new(UInt8Array::from_iter(
                [<$stat_type_prefix Int32StatsIterator>]::new($iterator).map(|x| {
                    x.and_then(|x| u8::try_from(*x).ok())
                }),
            ))),
            DataType::UInt16 => Ok(Arc::new(UInt16Array::from_iter(
                [<$stat_type_prefix Int32StatsIterator>]::new($iterator).map(|x| {
                    x.and_then(|x| u16::try_from(*x).ok())
                }),
            ))),
            DataType::UInt32 => Ok(Arc::new(UInt32Array::from_iter(
                [<$stat_type_prefix Int32StatsIterator>]::new($iterator).map(|x| x.map(|x| *x as u32)),
            ))),
            DataType::UInt64 => Ok(Arc::new(UInt64Array::from_iter(
                [<$stat_type_prefix Int64StatsIterator>]::new($iterator).map(|x| x.map(|x| *x as u64)),
            ))),
            DataType::Float16 => Ok(Arc::new(Float16Array::from_iter(
                [<$stat_type_prefix FixedLenByteArrayStatsIterator>]::new($iterator).map(|x| x.and_then(|x| {
                    from_bytes_to_f16(x)
                })),
            ))),
            DataType::Float32 => Ok(Arc::new(Float32Array::from_iter(
                [<$stat_type_prefix FloatStatsIterator>]::new($iterator).map(|x| x.copied()),
            ))),
            DataType::Float64 => Ok(Arc::new(Float64Array::from_iter(
                [<$stat_type_prefix DoubleStatsIterator>]::new($iterator).map(|x| x.copied()),
            ))),
            DataType::Date32 => Ok(Arc::new(Date32Array::from_iter(
                [<$stat_type_prefix Int32StatsIterator>]::new($iterator).map(|x| x.copied()),
            ))),
            DataType::Date64 => Ok(Arc::new(Date64Array::from_iter(
                [<$stat_type_prefix Int32StatsIterator>]::new($iterator)
                    .map(|x| x.map(|x| i64::from(*x) * 24 * 60 * 60 * 1000)),
            ))),
            DataType::Timestamp(unit, timezone) =>{
                let iter = [<$stat_type_prefix Int64StatsIterator>]::new($iterator).map(|x| x.copied());
                Ok(match unit {
                    TimeUnit::Second => Arc::new(TimestampSecondArray::from_iter(iter).with_timezone_opt(timezone.clone())),
                    TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from_iter(iter).with_timezone_opt(timezone.clone())),
                    TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from_iter(iter).with_timezone_opt(timezone.clone())),
                    TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from_iter(iter).with_timezone_opt(timezone.clone())),
                })
            },
            DataType::Time32(unit) => {
                Ok(match unit {
                    TimeUnit::Second =>  Arc::new(Time32SecondArray::from_iter(
                        [<$stat_type_prefix Int32StatsIterator>]::new($iterator).map(|x| x.copied()),
                    )),
                    TimeUnit::Millisecond => Arc::new(Time32MillisecondArray::from_iter(
                        [<$stat_type_prefix Int32StatsIterator>]::new($iterator).map(|x| x.copied()),
                    )),
                    _ => {
                        let len = $iterator.count();
                        // don't know how to extract statistics, so return a null array
                        new_null_array($data_type, len)
                    }
                })
            },
            DataType::Time64(unit) => {
                Ok(match unit {
                    TimeUnit::Microsecond =>  Arc::new(Time64MicrosecondArray::from_iter(
                        [<$stat_type_prefix Int64StatsIterator>]::new($iterator).map(|x| x.copied()),
                    )),
                    TimeUnit::Nanosecond => Arc::new(Time64NanosecondArray::from_iter(
                        [<$stat_type_prefix Int64StatsIterator>]::new($iterator).map(|x| x.copied()),
                    )),
                    _ => {
                        let len = $iterator.count();
                        // don't know how to extract statistics, so return a null array
                        new_null_array($data_type, len)
                    }
                })
            },
            DataType::Binary => Ok(Arc::new(BinaryArray::from_iter(
                [<$stat_type_prefix ByteArrayStatsIterator>]::new($iterator)
            ))),
            DataType::LargeBinary => Ok(Arc::new(LargeBinaryArray::from_iter(
                [<$stat_type_prefix ByteArrayStatsIterator>]::new($iterator)
            ))),
            DataType::Utf8 => {
                let iterator = [<$stat_type_prefix ByteArrayStatsIterator>]::new($iterator);
                let mut builder = StringBuilder::new();
                for x in iterator {
                    let Some(x) = x else {
                        builder.append_null(); // no statistics value
                        continue;
                    };

                    let Ok(x) = std::str::from_utf8(x) else {
                        builder.append_null();
                        continue;
                    };

                    builder.append_value(x);
                }
                Ok(Arc::new(builder.finish()))
            },
            DataType::LargeUtf8 => {
                let iterator = [<$stat_type_prefix ByteArrayStatsIterator>]::new($iterator);
                let mut builder = LargeStringBuilder::new();
                for x in iterator {
                    let Some(x) = x else {
                        builder.append_null(); // no statistics value
                        continue;
                    };

                    let Ok(x) = std::str::from_utf8(x) else {
                        builder.append_null();
                        continue;
                    };

                    builder.append_value(x);
                }
                Ok(Arc::new(builder.finish()))
            },
            DataType::FixedSizeBinary(size) => {
                let iterator = [<$stat_type_prefix FixedLenByteArrayStatsIterator>]::new($iterator);
                let mut builder = FixedSizeBinaryBuilder::new(*size);
                for x in iterator {
                    let Some(x) = x else {
                        builder.append_null(); // no statistics value
                        continue;
                    };

                    // ignore invalid values
                    if x.len().try_into() != Ok(*size){
                        builder.append_null();
                        continue;
                    }

                    builder.append_value(x).expect("ensure to append successfully here, because size have been checked before");
                }
                Ok(Arc::new(builder.finish()))
            },
            DataType::Decimal128(precision, scale) => {
                let arr = Decimal128Array::from_iter(
                    [<$stat_type_prefix Decimal128StatsIterator>]::new($iterator)
                ).with_precision_and_scale(*precision, *scale)?;
                Ok(Arc::new(arr))
            },
            DataType::Decimal256(precision, scale) => {
                let arr = Decimal256Array::from_iter(
                    [<$stat_type_prefix Decimal256StatsIterator>]::new($iterator)
                ).with_precision_and_scale(*precision, *scale)?;
                Ok(Arc::new(arr))
            },
            DataType::Dictionary(_, value_type) => {
                [<$stat_type_prefix:lower _ statistics>](value_type, $iterator)
            },
            DataType::Utf8View => {
                let iterator = [<$stat_type_prefix ByteArrayStatsIterator>]::new($iterator);
                let mut builder = StringViewBuilder::new();
                for x in iterator {
                    let Some(x) = x else {
                        builder.append_null(); // no statistics value
                        continue;
                    };

                    let Ok(x) = std::str::from_utf8(x) else {
                        builder.append_null();
                        continue;
                    };

                    builder.append_value(x);
                }
                Ok(Arc::new(builder.finish()))
            },
            DataType::BinaryView => {
                let iterator = [<$stat_type_prefix ByteArrayStatsIterator>]::new($iterator);
                let mut builder = BinaryViewBuilder::new();
                for x in iterator {
                    let Some(x) = x else {
                        builder.append_null(); // no statistics value
                        continue;
                    };

                    builder.append_value(x);
                }
                Ok(Arc::new(builder.finish()))
            }

            DataType::Map(_,_) |
            DataType::Duration(_) |
            DataType::Interval(_) |
            DataType::Null |
            DataType::List(_) |
            DataType::ListView(_) |
            DataType::FixedSizeList(_, _) |
            DataType::LargeList(_) |
            DataType::LargeListView(_) |
            DataType::Struct(_) |
            DataType::Union(_, _) |
            DataType::RunEndEncoded(_, _) => {
                let len = $iterator.count();
                // don't know how to extract statistics, so return a null array
                Ok(new_null_array($data_type, len))
            }
        }}}
}

macro_rules! make_data_page_stats_iterator {
    ($iterator_type: ident, $func: expr, $index_type: path, $stat_value_type: ty) => {
        struct $iterator_type<'a, I>
        where
            I: Iterator<Item = (usize, &'a Index)>,
        {
            iter: I,
        }

        impl<'a, I> $iterator_type<'a, I>
        where
            I: Iterator<Item = (usize, &'a Index)>,
        {
            fn new(iter: I) -> Self {
                Self { iter }
            }
        }

        impl<'a, I> Iterator for $iterator_type<'a, I>
        where
            I: Iterator<Item = (usize, &'a Index)>,
        {
            type Item = Vec<Option<$stat_value_type>>;

            fn next(&mut self) -> Option<Self::Item> {
                let next = self.iter.next();
                match next {
                    Some((len, index)) => match index {
                        $index_type(native_index) => {
                            Some(native_index.indexes.iter().map($func).collect::<Vec<_>>())
                        }
                        // No matching `Index` found;
                        // thus no statistics that can be extracted.
                        // We return vec![None; len] to effectively
                        // create an arrow null-array with the length
                        // corresponding to the number of entries in
                        // `ParquetOffsetIndex` per row group per column.
                        _ => Some(vec![None; len]),
                    },
                    _ => None,
                }
            }

            fn size_hint(&self) -> (usize, Option<usize>) {
                self.iter.size_hint()
            }
        }
    };
}

make_data_page_stats_iterator!(
    MinBooleanDataPageStatsIterator,
    |x: &PageIndex<bool>| { x.min },
    Index::BOOLEAN,
    bool
);
make_data_page_stats_iterator!(
    MaxBooleanDataPageStatsIterator,
    |x: &PageIndex<bool>| { x.max },
    Index::BOOLEAN,
    bool
);
make_data_page_stats_iterator!(
    MinInt32DataPageStatsIterator,
    |x: &PageIndex<i32>| { x.min },
    Index::INT32,
    i32
);
make_data_page_stats_iterator!(
    MaxInt32DataPageStatsIterator,
    |x: &PageIndex<i32>| { x.max },
    Index::INT32,
    i32
);
make_data_page_stats_iterator!(
    MinInt64DataPageStatsIterator,
    |x: &PageIndex<i64>| { x.min },
    Index::INT64,
    i64
);
make_data_page_stats_iterator!(
    MaxInt64DataPageStatsIterator,
    |x: &PageIndex<i64>| { x.max },
    Index::INT64,
    i64
);
make_data_page_stats_iterator!(
    MinFloat16DataPageStatsIterator,
    |x: &PageIndex<FixedLenByteArray>| { x.min.clone() },
    Index::FIXED_LEN_BYTE_ARRAY,
    FixedLenByteArray
);
make_data_page_stats_iterator!(
    MaxFloat16DataPageStatsIterator,
    |x: &PageIndex<FixedLenByteArray>| { x.max.clone() },
    Index::FIXED_LEN_BYTE_ARRAY,
    FixedLenByteArray
);
make_data_page_stats_iterator!(
    MinFloat32DataPageStatsIterator,
    |x: &PageIndex<f32>| { x.min },
    Index::FLOAT,
    f32
);
make_data_page_stats_iterator!(
    MaxFloat32DataPageStatsIterator,
    |x: &PageIndex<f32>| { x.max },
    Index::FLOAT,
    f32
);
make_data_page_stats_iterator!(
    MinFloat64DataPageStatsIterator,
    |x: &PageIndex<f64>| { x.min },
    Index::DOUBLE,
    f64
);
make_data_page_stats_iterator!(
    MaxFloat64DataPageStatsIterator,
    |x: &PageIndex<f64>| { x.max },
    Index::DOUBLE,
    f64
);
make_data_page_stats_iterator!(
    MinByteArrayDataPageStatsIterator,
    |x: &PageIndex<ByteArray>| { x.min.clone() },
    Index::BYTE_ARRAY,
    ByteArray
);
make_data_page_stats_iterator!(
    MaxByteArrayDataPageStatsIterator,
    |x: &PageIndex<ByteArray>| { x.max.clone() },
    Index::BYTE_ARRAY,
    ByteArray
);
make_data_page_stats_iterator!(
    MaxFixedLenByteArrayDataPageStatsIterator,
    |x: &PageIndex<FixedLenByteArray>| { x.max.clone() },
    Index::FIXED_LEN_BYTE_ARRAY,
    FixedLenByteArray
);

make_data_page_stats_iterator!(
    MinFixedLenByteArrayDataPageStatsIterator,
    |x: &PageIndex<FixedLenByteArray>| { x.min.clone() },
    Index::FIXED_LEN_BYTE_ARRAY,
    FixedLenByteArray
);

macro_rules! get_decimal_page_stats_iterator {
    ($iterator_type: ident, $func: ident, $stat_value_type: ident, $convert_func: ident) => {
        struct $iterator_type<'a, I>
        where
            I: Iterator<Item = (usize, &'a Index)>,
        {
            iter: I,
        }

        impl<'a, I> $iterator_type<'a, I>
        where
            I: Iterator<Item = (usize, &'a Index)>,
        {
            fn new(iter: I) -> Self {
                Self { iter }
            }
        }

        impl<'a, I> Iterator for $iterator_type<'a, I>
        where
            I: Iterator<Item = (usize, &'a Index)>,
        {
            type Item = Vec<Option<$stat_value_type>>;

            fn next(&mut self) -> Option<Self::Item> {
                let next = self.iter.next();
                match next {
                    Some((len, index)) => match index {
                        Index::INT32(native_index) => Some(
                            native_index
                                .indexes
                                .iter()
                                .map(|x| x.$func.and_then(|x| Some($stat_value_type::from(x))))
                                .collect::<Vec<_>>(),
                        ),
                        Index::INT64(native_index) => Some(
                            native_index
                                .indexes
                                .iter()
                                .map(|x| x.$func.and_then(|x| Some($stat_value_type::from(x))))
                                .collect::<Vec<_>>(),
                        ),
                        Index::BYTE_ARRAY(native_index) => Some(
                            native_index
                                .indexes
                                .iter()
                                .map(|x| {
                                    x.clone().$func.and_then(|x| Some($convert_func(x.data())))
                                })
                                .collect::<Vec<_>>(),
                        ),
                        Index::FIXED_LEN_BYTE_ARRAY(native_index) => Some(
                            native_index
                                .indexes
                                .iter()
                                .map(|x| {
                                    x.clone().$func.and_then(|x| Some($convert_func(x.data())))
                                })
                                .collect::<Vec<_>>(),
                        ),
                        _ => Some(vec![None; len]),
                    },
                    _ => None,
                }
            }

            fn size_hint(&self) -> (usize, Option<usize>) {
                self.iter.size_hint()
            }
        }
    };
}

get_decimal_page_stats_iterator!(
    MinDecimal128DataPageStatsIterator,
    min,
    i128,
    from_bytes_to_i128
);

get_decimal_page_stats_iterator!(
    MaxDecimal128DataPageStatsIterator,
    max,
    i128,
    from_bytes_to_i128
);

get_decimal_page_stats_iterator!(
    MinDecimal256DataPageStatsIterator,
    min,
    i256,
    from_bytes_to_i256
);

get_decimal_page_stats_iterator!(
    MaxDecimal256DataPageStatsIterator,
    max,
    i256,
    from_bytes_to_i256
);

macro_rules! get_data_page_statistics {
    ($stat_type_prefix: ident, $data_type: ident, $iterator: ident) => {
        paste! {
            match $data_type {
                DataType::Boolean => {
                    let iterator = [<$stat_type_prefix BooleanDataPageStatsIterator>]::new($iterator);
                    let mut builder = BooleanBuilder::new();
                    for x in iterator {
                        for x in x.into_iter() {
                            let Some(x) = x else {
                                builder.append_null(); // no statistics value
                                continue;
                            };
                            builder.append_value(x);
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                },
                DataType::UInt8 => Ok(Arc::new(
                    UInt8Array::from_iter(
                        [<$stat_type_prefix Int32DataPageStatsIterator>]::new($iterator)
                            .map(|x| {
                                x.into_iter().map(|x| {
                                    x.and_then(|x| u8::try_from(x).ok())
                                })
                            })
                            .flatten()
                    )
                )),
                DataType::UInt16 => Ok(Arc::new(
                    UInt16Array::from_iter(
                        [<$stat_type_prefix Int32DataPageStatsIterator>]::new($iterator)
                            .map(|x| {
                                x.into_iter().map(|x| {
                                    x.and_then(|x| u16::try_from(x).ok())
                                })
                            })
                            .flatten()
                    )
                )),
                DataType::UInt32 => Ok(Arc::new(
                    UInt32Array::from_iter(
                        [<$stat_type_prefix Int32DataPageStatsIterator>]::new($iterator)
                            .map(|x| {
                                x.into_iter().map(|x| {
                                    x.and_then(|x| Some(x as u32))
                                })
                            })
                            .flatten()
                ))),
                DataType::UInt64 => Ok(Arc::new(
                    UInt64Array::from_iter(
                        [<$stat_type_prefix Int64DataPageStatsIterator>]::new($iterator)
                            .map(|x| {
                                x.into_iter().map(|x| {
                                    x.and_then(|x| Some(x as u64))
                                })
                            })
                            .flatten()
                ))),
                DataType::Int8 => Ok(Arc::new(
                    Int8Array::from_iter(
                        [<$stat_type_prefix Int32DataPageStatsIterator>]::new($iterator)
                            .map(|x| {
                                x.into_iter().map(|x| {
                                    x.and_then(|x| i8::try_from(x).ok())
                                })
                            })
                            .flatten()
                    )
                )),
                DataType::Int16 => Ok(Arc::new(
                    Int16Array::from_iter(
                        [<$stat_type_prefix Int32DataPageStatsIterator>]::new($iterator)
                            .map(|x| {
                                x.into_iter().map(|x| {
                                    x.and_then(|x| i16::try_from(x).ok())
                                })
                            })
                            .flatten()
                    )
                )),
                DataType::Int32 => Ok(Arc::new(Int32Array::from_iter([<$stat_type_prefix Int32DataPageStatsIterator>]::new($iterator).flatten()))),
                DataType::Int64 => Ok(Arc::new(Int64Array::from_iter([<$stat_type_prefix Int64DataPageStatsIterator>]::new($iterator).flatten()))),
                DataType::Float16 => Ok(Arc::new(
                    Float16Array::from_iter(
                        [<$stat_type_prefix Float16DataPageStatsIterator>]::new($iterator)
                            .map(|x| {
                                x.into_iter().map(|x| {
                                    x.and_then(|x| from_bytes_to_f16(x.data()))
                                })
                            })
                            .flatten()
                    )
                )),
                DataType::Float32 => Ok(Arc::new(Float32Array::from_iter([<$stat_type_prefix Float32DataPageStatsIterator>]::new($iterator).flatten()))),
                DataType::Float64 => Ok(Arc::new(Float64Array::from_iter([<$stat_type_prefix Float64DataPageStatsIterator>]::new($iterator).flatten()))),
                DataType::Binary => Ok(Arc::new(BinaryArray::from_iter([<$stat_type_prefix ByteArrayDataPageStatsIterator>]::new($iterator).flatten()))),
                DataType::LargeBinary => Ok(Arc::new(LargeBinaryArray::from_iter([<$stat_type_prefix ByteArrayDataPageStatsIterator>]::new($iterator).flatten()))),
                DataType::Utf8 => {
                    let mut builder = StringBuilder::new();
                    let iterator = [<$stat_type_prefix ByteArrayDataPageStatsIterator>]::new($iterator);
                    for x in iterator {
                        for x in x.into_iter() {
                            let Some(x) = x else {
                                builder.append_null(); // no statistics value
                                continue;
                            };

                            let Ok(x) = std::str::from_utf8(x.data()) else {
                                builder.append_null();
                                continue;
                            };

                            builder.append_value(x);
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                },
                DataType::LargeUtf8 => {
                    let mut builder = LargeStringBuilder::new();
                    let iterator = [<$stat_type_prefix ByteArrayDataPageStatsIterator>]::new($iterator);
                    for x in iterator {
                        for x in x.into_iter() {
                            let Some(x) = x else {
                                builder.append_null(); // no statistics value
                                continue;
                            };

                            let Ok(x) = std::str::from_utf8(x.data()) else {
                                builder.append_null();
                                continue;
                            };

                            builder.append_value(x);
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                },
                DataType::Dictionary(_, value_type) => {
                    [<$stat_type_prefix:lower _ page_statistics>](value_type, $iterator)
                },
                DataType::Timestamp(unit, timezone) => {
                    let iter = [<$stat_type_prefix Int64DataPageStatsIterator>]::new($iterator).flatten();
                    Ok(match unit {
                        TimeUnit::Second => Arc::new(TimestampSecondArray::from_iter(iter).with_timezone_opt(timezone.clone())),
                        TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from_iter(iter).with_timezone_opt(timezone.clone())),
                        TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from_iter(iter).with_timezone_opt(timezone.clone())),
                        TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from_iter(iter).with_timezone_opt(timezone.clone())),
                    })
                },
                DataType::Date32 => Ok(Arc::new(Date32Array::from_iter([<$stat_type_prefix Int32DataPageStatsIterator>]::new($iterator).flatten()))),
                DataType::Date64 => Ok(
                    Arc::new(
                        Date64Array::from_iter([<$stat_type_prefix Int32DataPageStatsIterator>]::new($iterator)
                            .map(|x| {
                                x.into_iter()
                                .map(|x| {
                                    x.and_then(|x| i64::try_from(x).ok())
                                })
                                .map(|x| x.map(|x| x * 24 * 60 * 60 * 1000))
                            }).flatten()
                        )
                    )
                ),
                DataType::Decimal128(precision, scale) => Ok(Arc::new(
                    Decimal128Array::from_iter([<$stat_type_prefix Decimal128DataPageStatsIterator>]::new($iterator).flatten()).with_precision_and_scale(*precision, *scale)?)),
                DataType::Decimal256(precision, scale) => Ok(Arc::new(
                    Decimal256Array::from_iter([<$stat_type_prefix Decimal256DataPageStatsIterator>]::new($iterator).flatten()).with_precision_and_scale(*precision, *scale)?)),
                DataType::Time32(unit) => {
                    Ok(match unit {
                        TimeUnit::Second =>  Arc::new(Time32SecondArray::from_iter(
                            [<$stat_type_prefix Int32DataPageStatsIterator>]::new($iterator).flatten(),
                        )),
                        TimeUnit::Millisecond => Arc::new(Time32MillisecondArray::from_iter(
                            [<$stat_type_prefix Int32DataPageStatsIterator>]::new($iterator).flatten(),
                        )),
                        _ => {
                            // don't know how to extract statistics, so return an empty array
                            new_empty_array(&DataType::Time32(unit.clone()))
                        }
                    })
                }
                DataType::Time64(unit) => {
                    Ok(match unit {
                        TimeUnit::Microsecond =>  Arc::new(Time64MicrosecondArray::from_iter(
                            [<$stat_type_prefix Int64DataPageStatsIterator>]::new($iterator).flatten(),
                        )),
                        TimeUnit::Nanosecond => Arc::new(Time64NanosecondArray::from_iter(
                            [<$stat_type_prefix Int64DataPageStatsIterator>]::new($iterator).flatten(),
                        )),
                        _ => {
                            // don't know how to extract statistics, so return an empty array
                            new_empty_array(&DataType::Time64(unit.clone()))
                        }
                    })
                },
                DataType::FixedSizeBinary(size) => {
                    let mut builder = FixedSizeBinaryBuilder::new(*size);
                    let iterator = [<$stat_type_prefix FixedLenByteArrayDataPageStatsIterator>]::new($iterator);
                    for x in iterator {
                        for x in x.into_iter() {
                            let Some(x) = x else {
                                builder.append_null(); // no statistics value
                                continue;
                            };

                            if x.len() == *size as usize {
                                let _ = builder.append_value(x.data());
                            } else {
                                builder.append_null();
                            }
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                },
                DataType::Utf8View => {
                    let mut builder = StringViewBuilder::new();
                    let iterator = [<$stat_type_prefix ByteArrayDataPageStatsIterator>]::new($iterator);
                    for x in iterator {
                        for x in x.into_iter() {
                            let Some(x) = x else {
                                builder.append_null(); // no statistics value
                                continue;
                            };

                            let Ok(x) = std::str::from_utf8(x.data()) else {
                                builder.append_null();
                                continue;
                            };

                            builder.append_value(x);
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                },
                DataType::BinaryView => {
                    let mut builder = BinaryViewBuilder::new();
                    let iterator = [<$stat_type_prefix ByteArrayDataPageStatsIterator>]::new($iterator);
                    for x in iterator {
                        for x in x.into_iter() {
                            let Some(x) = x else {
                                builder.append_null(); // no statistics value
                                continue;
                            };

                            builder.append_value(x);
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                },
                DataType::Null |
                DataType::Duration(_) |
                DataType::Interval(_) |
                DataType::List(_) |
                DataType::ListView(_) |
                DataType::FixedSizeList(_, _) |
                DataType::LargeList(_) |
                DataType::LargeListView(_) |
                DataType::Struct(_) |
                DataType::Union(_, _) |
                DataType::Map(_, _) |
                DataType::RunEndEncoded(_, _) => {
                    let len = $iterator.count();
                    // don't know how to extract statistics, so return a null array
                    Ok(new_null_array($data_type, len))
                },
            }
        }
    }
}
/// Extracts the min statistics from an iterator of [`ParquetStatistics`] to an
/// [`ArrayRef`]
///
/// This is an internal helper -- see [`StatisticsConverter`] for public API
fn min_statistics<'a, I: Iterator<Item = Option<&'a ParquetStatistics>>>(
    data_type: &DataType,
    iterator: I,
) -> Result<ArrayRef> {
    get_statistics!(Min, data_type, iterator)
}

/// Extracts the max statistics from an iterator of [`ParquetStatistics`] to an [`ArrayRef`]
///
/// This is an internal helper -- see [`StatisticsConverter`] for public API
fn max_statistics<'a, I: Iterator<Item = Option<&'a ParquetStatistics>>>(
    data_type: &DataType,
    iterator: I,
) -> Result<ArrayRef> {
    get_statistics!(Max, data_type, iterator)
}

/// Extracts the min statistics from an iterator
/// of parquet page [`Index`]'es to an [`ArrayRef`]
pub(crate) fn min_page_statistics<'a, I>(data_type: &DataType, iterator: I) -> Result<ArrayRef>
where
    I: Iterator<Item = (usize, &'a Index)>,
{
    get_data_page_statistics!(Min, data_type, iterator)
}

/// Extracts the max statistics from an iterator
/// of parquet page [`Index`]'es to an [`ArrayRef`]
pub(crate) fn max_page_statistics<'a, I>(data_type: &DataType, iterator: I) -> Result<ArrayRef>
where
    I: Iterator<Item = (usize, &'a Index)>,
{
    get_data_page_statistics!(Max, data_type, iterator)
}

/// Extracts the null count statistics from an iterator
/// of parquet page [`Index`]'es to an [`ArrayRef`]
///
/// The returned Array is an [`UInt64Array`]
pub(crate) fn null_counts_page_statistics<'a, I>(iterator: I) -> Result<UInt64Array>
where
    I: Iterator<Item = (usize, &'a Index)>,
{
    let iter = iterator.flat_map(|(len, index)| match index {
        Index::NONE => vec![None; len],
        Index::BOOLEAN(native_index) => native_index
            .indexes
            .iter()
            .map(|x| x.null_count.map(|x| x as u64))
            .collect::<Vec<_>>(),
        Index::INT32(native_index) => native_index
            .indexes
            .iter()
            .map(|x| x.null_count.map(|x| x as u64))
            .collect::<Vec<_>>(),
        Index::INT64(native_index) => native_index
            .indexes
            .iter()
            .map(|x| x.null_count.map(|x| x as u64))
            .collect::<Vec<_>>(),
        Index::FLOAT(native_index) => native_index
            .indexes
            .iter()
            .map(|x| x.null_count.map(|x| x as u64))
            .collect::<Vec<_>>(),
        Index::DOUBLE(native_index) => native_index
            .indexes
            .iter()
            .map(|x| x.null_count.map(|x| x as u64))
            .collect::<Vec<_>>(),
        Index::FIXED_LEN_BYTE_ARRAY(native_index) => native_index
            .indexes
            .iter()
            .map(|x| x.null_count.map(|x| x as u64))
            .collect::<Vec<_>>(),
        Index::BYTE_ARRAY(native_index) => native_index
            .indexes
            .iter()
            .map(|x| x.null_count.map(|x| x as u64))
            .collect::<Vec<_>>(),
        _ => unimplemented!(),
    });

    Ok(UInt64Array::from_iter(iter))
}

/// Extracts Parquet statistics as Arrow arrays
///
/// This is used to convert Parquet statistics to Arrow [`ArrayRef`], with
/// proper type conversions. This information can be used for pruning Parquet
/// files, row groups, and data pages based on the statistics embedded in
/// Parquet metadata.
///
/// # Schemas
///
/// The converter uses the schema of the Parquet file and the Arrow schema to
/// convert the underlying statistics value (stored as a parquet value) into the
/// corresponding Arrow value. For example, Decimals are stored as binary in
/// parquet files and this structure handles mapping them to the `i128`
/// representation used in Arrow.
///
/// Note: The Parquet schema and Arrow schema do not have to be identical (for
/// example, the columns may be in different orders and one or the other schemas
/// may have additional columns). The function [`parquet_column`] is used to
/// match the column in the Parquet schema to the column in the Arrow schema.
#[derive(Debug)]
pub struct StatisticsConverter<'a> {
    /// the index of the matched column in the Parquet schema
    parquet_column_index: Option<usize>,
    /// The field (with data type) of the column in the Arrow schema
    arrow_field: &'a Field,
    /// treat missing null_counts as 0 nulls
    missing_null_counts_as_zero: bool,
}

impl<'a> StatisticsConverter<'a> {
    /// Return the index of the column in the Parquet schema, if any
    ///
    /// Returns `None` if the column is was present in the Arrow schema, but not
    /// present in the parquet file
    pub fn parquet_column_index(&self) -> Option<usize> {
        self.parquet_column_index
    }

    /// Return the arrow schema's [`Field]` of the column in the Arrow schema
    pub fn arrow_field(&self) -> &'a Field {
        self.arrow_field
    }

    /// Set the statistics converter to treat missing null counts as missing
    ///
    /// By default, the converter will treat missing null counts as though
    /// the null count is known to be `0`.
    ///
    /// Note that parquet files written by parquet-rs currently do not store
    /// null counts even when it is known there are zero nulls, and the reader
    /// will return 0 for the null counts in that instance. This behavior may
    /// change in a future release.
    ///
    /// Both parquet-java and parquet-cpp store null counts as 0 when there are
    /// no nulls, and don't write unknown values to the null count field.
    pub fn with_missing_null_counts_as_zero(mut self, missing_null_counts_as_zero: bool) -> Self {
        self.missing_null_counts_as_zero = missing_null_counts_as_zero;
        self
    }

    /// Returns a [`UInt64Array`] with row counts for each row group
    ///
    /// # Return Value
    ///
    /// The returned array has no nulls, and has one value for each row group.
    /// Each value is the number of rows in the row group.
    ///
    /// # Example
    /// ```no_run
    /// # use arrow::datatypes::Schema;
    /// # use arrow_array::{ArrayRef, UInt64Array};
    /// # use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
    /// # use parquet::file::metadata::ParquetMetaData;
    /// # fn get_parquet_metadata() -> ParquetMetaData { unimplemented!() }
    /// # fn get_arrow_schema() -> Schema { unimplemented!() }
    /// // Given the metadata for a parquet file and the arrow schema
    /// let metadata: ParquetMetaData = get_parquet_metadata();
    /// let arrow_schema: Schema = get_arrow_schema();
    /// let parquet_schema = metadata.file_metadata().schema_descr();
    /// // create a converter
    /// let converter = StatisticsConverter::try_new("foo", &arrow_schema, parquet_schema)
    ///   .unwrap();
    /// // get the row counts for each row group
    /// let row_counts = converter.row_group_row_counts(metadata
    ///   .row_groups()
    ///   .iter()
    /// ).unwrap();
    /// // file had 2 row groups, with 1024 and 23 rows respectively
    /// assert_eq!(row_counts, Some(UInt64Array::from(vec![1024, 23])));
    /// ```
    pub fn row_group_row_counts<I>(&self, metadatas: I) -> Result<Option<UInt64Array>>
    where
        I: IntoIterator<Item = &'a RowGroupMetaData>,
    {
        let Some(_) = self.parquet_column_index else {
            return Ok(None);
        };

        let mut builder = UInt64Array::builder(10);
        for metadata in metadatas.into_iter() {
            let row_count = metadata.num_rows();
            let row_count: u64 = row_count.try_into().map_err(|e| {
                arrow_err!(format!(
                    "Parquet row count {row_count} too large to convert to u64: {e}"
                ))
            })?;
            builder.append_value(row_count);
        }
        Ok(Some(builder.finish()))
    }

    /// Create a new `StatisticsConverter` to extract statistics for a column
    ///
    /// Note if there is no corresponding column in the parquet file, the returned
    /// arrays will be null. This can happen if the column is in the arrow
    /// schema but not in the parquet schema due to schema evolution.
    ///
    /// See example on [`Self::row_group_mins`] for usage
    ///
    /// # Errors
    ///
    /// * If the column is not found in the arrow schema
    pub fn try_new<'b>(
        column_name: &'b str,
        arrow_schema: &'a Schema,
        parquet_schema: &'a SchemaDescriptor,
    ) -> Result<Self> {
        // ensure the requested column is in the arrow schema
        let Some((_idx, arrow_field)) = arrow_schema.column_with_name(column_name) else {
            return Err(arrow_err!(format!(
                "Column '{}' not found in schema for statistics conversion",
                column_name
            )));
        };

        // find the column in the parquet schema, if not, return a null array
        let parquet_index = match parquet_column(parquet_schema, arrow_schema, column_name) {
            Some((parquet_idx, matched_field)) => {
                // sanity check that matching field matches the arrow field
                if matched_field.as_ref() != arrow_field {
                    return Err(arrow_err!(format!(
                        "Matched column '{:?}' does not match original matched column '{:?}'",
                        matched_field, arrow_field
                    )));
                }
                Some(parquet_idx)
            }
            None => None,
        };

        Ok(Self {
            parquet_column_index: parquet_index,
            arrow_field,
            missing_null_counts_as_zero: true,
        })
    }

    /// Extract the minimum values from row group statistics in [`RowGroupMetaData`]
    ///
    /// # Return Value
    ///
    /// The returned array contains 1 value for each row group, in the same order as `metadatas`
    ///
    /// Each value is either
    /// * the minimum value for the column
    /// * a null value, if the statistics can not be extracted
    ///
    /// Note that a null value does NOT mean the min value was actually
    /// `null` it means it the requested statistic is unknown
    ///
    /// # Errors
    ///
    /// Reasons for not being able to extract the statistics include:
    /// * the column is not present in the parquet file
    /// * statistics for the column are not present in the row group
    /// * the stored statistic value can not be converted to the requested type
    ///
    /// # Example
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use arrow::datatypes::Schema;
    /// # use arrow_array::{ArrayRef, Float64Array};
    /// # use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
    /// # use parquet::file::metadata::ParquetMetaData;
    /// # fn get_parquet_metadata() -> ParquetMetaData { unimplemented!() }
    /// # fn get_arrow_schema() -> Schema { unimplemented!() }
    /// // Given the metadata for a parquet file and the arrow schema
    /// let metadata: ParquetMetaData = get_parquet_metadata();
    /// let arrow_schema: Schema = get_arrow_schema();
    /// let parquet_schema = metadata.file_metadata().schema_descr();
    /// // create a converter
    /// let converter = StatisticsConverter::try_new("foo", &arrow_schema, parquet_schema)
    ///   .unwrap();
    /// // get the minimum value for the column "foo" in the parquet file
    /// let min_values: ArrayRef = converter
    ///   .row_group_mins(metadata.row_groups().iter())
    ///  .unwrap();
    /// // if "foo" is a Float64 value, the returned array will contain Float64 values
    /// assert_eq!(min_values, Arc::new(Float64Array::from(vec![Some(1.0), Some(2.0)])) as _);
    /// ```
    pub fn row_group_mins<I>(&self, metadatas: I) -> Result<ArrayRef>
    where
        I: IntoIterator<Item = &'a RowGroupMetaData>,
    {
        let data_type = self.arrow_field.data_type();

        let Some(parquet_index) = self.parquet_column_index else {
            return Ok(self.make_null_array(data_type, metadatas));
        };

        let iter = metadatas
            .into_iter()
            .map(|x| x.column(parquet_index).statistics());
        min_statistics(data_type, iter)
    }

    /// Extract the maximum values from row group statistics in [`RowGroupMetaData`]
    ///
    /// See docs on [`Self::row_group_mins`] for details
    pub fn row_group_maxes<I>(&self, metadatas: I) -> Result<ArrayRef>
    where
        I: IntoIterator<Item = &'a RowGroupMetaData>,
    {
        let data_type = self.arrow_field.data_type();

        let Some(parquet_index) = self.parquet_column_index else {
            return Ok(self.make_null_array(data_type, metadatas));
        };

        let iter = metadatas
            .into_iter()
            .map(|x| x.column(parquet_index).statistics());
        max_statistics(data_type, iter)
    }

    /// Extract the null counts from row group statistics in [`RowGroupMetaData`]
    ///
    /// See docs on [`Self::row_group_mins`] for details
    pub fn row_group_null_counts<I>(&self, metadatas: I) -> Result<UInt64Array>
    where
        I: IntoIterator<Item = &'a RowGroupMetaData>,
    {
        let Some(parquet_index) = self.parquet_column_index else {
            let num_row_groups = metadatas.into_iter().count();
            return Ok(UInt64Array::from_iter(
                std::iter::repeat(None).take(num_row_groups),
            ));
        };

        let null_counts = metadatas
            .into_iter()
            .map(|x| x.column(parquet_index).statistics())
            .map(|s| {
                s.and_then(|s| {
                    if self.missing_null_counts_as_zero {
                        Some(s.null_count_opt().unwrap_or(0))
                    } else {
                        s.null_count_opt()
                    }
                })
            });
        Ok(UInt64Array::from_iter(null_counts))
    }

    /// Extract the minimum values from Data Page statistics.
    ///
    /// In Parquet files, in addition to the Column Chunk level statistics
    /// (stored for each column for each row group) there are also
    /// optional statistics stored for each data page, as part of
    /// the [`ParquetColumnIndex`].
    ///
    /// Since a single Column Chunk is stored as one or more pages,
    /// page level statistics can prune at a finer granularity.
    ///
    /// However since they are stored in a separate metadata
    /// structure ([`Index`]) there is different code to extract them as
    /// compared to arrow statistics.
    ///
    /// # Parameters:
    ///
    /// * `column_page_index`: The parquet column page indices, read from
    ///   `ParquetMetaData` column_index
    ///
    /// * `column_offset_index`: The parquet column offset indices, read from
    ///   `ParquetMetaData` offset_index
    ///
    /// * `row_group_indices`: The indices of the row groups, that are used to
    ///   extract the column page index and offset index on a per row group
    ///   per column basis.
    ///
    /// # Return Value
    ///
    /// The returned array contains 1 value for each `NativeIndex`
    /// in the underlying `Index`es, in the same order as they appear
    /// in `metadatas`.
    ///
    /// For example, if there are two `Index`es in `metadatas`:
    /// 1. the first having `3` `PageIndex` entries
    /// 2. the second having `2` `PageIndex` entries
    ///
    /// The returned array would have 5 rows.
    ///
    /// Each value is either:
    /// * the minimum value for the page
    /// * a null value, if the statistics can not be extracted
    ///
    /// Note that a null value does NOT mean the min value was actually
    /// `null` it means it the requested statistic is unknown
    ///
    /// # Errors
    ///
    /// Reasons for not being able to extract the statistics include:
    /// * the column is not present in the parquet file
    /// * statistics for the pages are not present in the row group
    /// * the stored statistic value can not be converted to the requested type
    pub fn data_page_mins<I>(
        &self,
        column_page_index: &ParquetColumnIndex,
        column_offset_index: &ParquetOffsetIndex,
        row_group_indices: I,
    ) -> Result<ArrayRef>
    where
        I: IntoIterator<Item = &'a usize>,
    {
        let data_type = self.arrow_field.data_type();

        let Some(parquet_index) = self.parquet_column_index else {
            return Ok(self.make_null_array(data_type, row_group_indices));
        };

        let iter = row_group_indices.into_iter().map(|rg_index| {
            let column_page_index_per_row_group_per_column =
                &column_page_index[*rg_index][parquet_index];
            let num_data_pages = &column_offset_index[*rg_index][parquet_index]
                .page_locations()
                .len();

            (*num_data_pages, column_page_index_per_row_group_per_column)
        });

        min_page_statistics(data_type, iter)
    }

    /// Extract the maximum values from Data Page statistics.
    ///
    /// See docs on [`Self::data_page_mins`] for details.
    pub fn data_page_maxes<I>(
        &self,
        column_page_index: &ParquetColumnIndex,
        column_offset_index: &ParquetOffsetIndex,
        row_group_indices: I,
    ) -> Result<ArrayRef>
    where
        I: IntoIterator<Item = &'a usize>,
    {
        let data_type = self.arrow_field.data_type();

        let Some(parquet_index) = self.parquet_column_index else {
            return Ok(self.make_null_array(data_type, row_group_indices));
        };

        let iter = row_group_indices.into_iter().map(|rg_index| {
            let column_page_index_per_row_group_per_column =
                &column_page_index[*rg_index][parquet_index];
            let num_data_pages = &column_offset_index[*rg_index][parquet_index]
                .page_locations()
                .len();

            (*num_data_pages, column_page_index_per_row_group_per_column)
        });

        max_page_statistics(data_type, iter)
    }

    /// Returns a [`UInt64Array`] with null counts for each data page.
    ///
    /// See docs on [`Self::data_page_mins`] for details.
    pub fn data_page_null_counts<I>(
        &self,
        column_page_index: &ParquetColumnIndex,
        column_offset_index: &ParquetOffsetIndex,
        row_group_indices: I,
    ) -> Result<UInt64Array>
    where
        I: IntoIterator<Item = &'a usize>,
    {
        let Some(parquet_index) = self.parquet_column_index else {
            let num_row_groups = row_group_indices.into_iter().count();
            return Ok(UInt64Array::from_iter(
                std::iter::repeat(None).take(num_row_groups),
            ));
        };

        let iter = row_group_indices.into_iter().map(|rg_index| {
            let column_page_index_per_row_group_per_column =
                &column_page_index[*rg_index][parquet_index];
            let num_data_pages = &column_offset_index[*rg_index][parquet_index]
                .page_locations()
                .len();

            (*num_data_pages, column_page_index_per_row_group_per_column)
        });
        null_counts_page_statistics(iter)
    }

    /// Returns a [`UInt64Array`] with row counts for each data page.
    ///
    /// This function iterates over the given row group indexes and computes
    /// the row count for each page in the specified column.
    ///
    /// # Parameters:
    ///
    /// * `column_offset_index`: The parquet column offset indices, read from
    ///   `ParquetMetaData` offset_index
    ///
    /// * `row_group_metadatas`: The metadata slice of the row groups, read
    ///   from `ParquetMetaData` row_groups
    ///
    /// * `row_group_indices`: The indices of the row groups, that are used to
    ///   extract the column offset index on a per row group per column basis.
    ///
    /// See docs on [`Self::data_page_mins`] for details.
    pub fn data_page_row_counts<I>(
        &self,
        column_offset_index: &ParquetOffsetIndex,
        row_group_metadatas: &'a [RowGroupMetaData],
        row_group_indices: I,
    ) -> Result<Option<UInt64Array>>
    where
        I: IntoIterator<Item = &'a usize>,
    {
        let Some(parquet_index) = self.parquet_column_index else {
            // no matching column found in parquet_index;
            // thus we cannot extract page_locations in order to determine
            // the row count on a per DataPage basis.
            return Ok(None);
        };

        let mut row_count_total = Vec::new();
        for rg_idx in row_group_indices {
            let page_locations = &column_offset_index[*rg_idx][parquet_index].page_locations();

            let row_count_per_page = page_locations
                .windows(2)
                .map(|loc| Some(loc[1].first_row_index as u64 - loc[0].first_row_index as u64));

            // append the last page row count
            let num_rows_in_row_group = &row_group_metadatas[*rg_idx].num_rows();
            let row_count_per_page = row_count_per_page
                .chain(std::iter::once(Some(
                    *num_rows_in_row_group as u64
                        - page_locations.last().unwrap().first_row_index as u64,
                )))
                .collect::<Vec<_>>();

            row_count_total.extend(row_count_per_page);
        }

        Ok(Some(UInt64Array::from_iter(row_count_total)))
    }

    /// Returns a null array of data_type with one element per row group
    fn make_null_array<I, A>(&self, data_type: &DataType, metadatas: I) -> ArrayRef
    where
        I: IntoIterator<Item = A>,
    {
        // column was in the arrow schema but not in the parquet schema, so return a null array
        let num_row_groups = metadatas.into_iter().count();
        new_null_array(data_type, num_row_groups)
    }
}

// See tests in parquet/tests/arrow_reader/statistics.rs
