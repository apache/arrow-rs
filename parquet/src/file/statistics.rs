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

//! Contains definitions for working with Parquet statistics.
//!
//! Though some common methods are available on enum, use pattern match to extract
//! actual min and max values from statistics, see below:
//!
//! # Examples
//! ```rust
//! use parquet::file::statistics::Statistics;
//!
//! let stats = Statistics::int32(Some(1), Some(10), None, Some(3), true);
//! assert_eq!(stats.null_count_opt(), Some(3));
//! assert!(stats.is_min_max_deprecated());
//! assert!(stats.min_is_exact());
//! assert!(stats.max_is_exact());
//!
//! match stats {
//!     Statistics::Int32(ref typed) => {
//!         assert_eq!(typed.min_opt(), Some(&1));
//!         assert_eq!(typed.max_opt(), Some(&10));
//!     }
//!     _ => {}
//! }
//! ```

use std::fmt;

use crate::format::Statistics as TStatistics;

use crate::basic::Type;
use crate::data_type::private::ParquetValueType;
use crate::data_type::*;
use crate::errors::{ParquetError, Result};
use crate::util::bit_util::FromBytes;

pub(crate) mod private {
    use super::*;

    pub trait MakeStatistics {
        fn make_statistics(statistics: ValueStatistics<Self>) -> Statistics
        where
            Self: Sized;
    }

    macro_rules! gen_make_statistics {
        ($value_ty:ty, $stat:ident) => {
            impl MakeStatistics for $value_ty {
                fn make_statistics(statistics: ValueStatistics<Self>) -> Statistics
                where
                    Self: Sized,
                {
                    Statistics::$stat(statistics)
                }
            }
        };
    }

    gen_make_statistics!(bool, Boolean);
    gen_make_statistics!(i32, Int32);
    gen_make_statistics!(i64, Int64);
    gen_make_statistics!(Int96, Int96);
    gen_make_statistics!(f32, Float);
    gen_make_statistics!(f64, Double);
    gen_make_statistics!(ByteArray, ByteArray);
    gen_make_statistics!(FixedLenByteArray, FixedLenByteArray);
}

/// Macro to generate methods to create Statistics.
macro_rules! statistics_new_func {
    ($func:ident, $vtype:ty, $stat:ident) => {
        #[doc = concat!("Creates new statistics for `", stringify!($stat), "` column type.")]
        pub fn $func(
            min: $vtype,
            max: $vtype,
            distinct: Option<u64>,
            nulls: Option<u64>,
            is_deprecated: bool,
        ) -> Self {
            Statistics::$stat(ValueStatistics::new(
                min,
                max,
                distinct,
                nulls,
                is_deprecated,
            ))
        }
    };
}

// Macro to generate getter functions for Statistics.
macro_rules! statistics_enum_func {
    ($self:ident, $func:ident) => {{
        match *$self {
            Statistics::Boolean(ref typed) => typed.$func(),
            Statistics::Int32(ref typed) => typed.$func(),
            Statistics::Int64(ref typed) => typed.$func(),
            Statistics::Int96(ref typed) => typed.$func(),
            Statistics::Float(ref typed) => typed.$func(),
            Statistics::Double(ref typed) => typed.$func(),
            Statistics::ByteArray(ref typed) => typed.$func(),
            Statistics::FixedLenByteArray(ref typed) => typed.$func(),
        }
    }};
}

/// Converts Thrift definition into `Statistics`.
pub fn from_thrift(
    physical_type: Type,
    thrift_stats: Option<TStatistics>,
) -> Result<Option<Statistics>> {
    Ok(match thrift_stats {
        Some(stats) => {
            // Number of nulls recorded, when it is not available, we just mark it as 0.
            // TODO this should be `None` if there is no information about NULLS.
            // see https://github.com/apache/arrow-rs/pull/6216/files
            let null_count = stats.null_count.unwrap_or(0);

            if null_count < 0 {
                return Err(ParquetError::General(format!(
                    "Statistics null count is negative {}",
                    null_count
                )));
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

            // Values are encoded using PLAIN encoding definition, except that
            // variable-length byte arrays do not include a length prefix.
            //
            // Instead of using actual decoder, we manually convert values.
            let res = match physical_type {
                Type::BOOLEAN => Statistics::boolean(
                    min.map(|data| data[0] != 0),
                    max.map(|data| data[0] != 0),
                    distinct_count,
                    null_count,
                    old_format,
                ),
                Type::INT32 => Statistics::int32(
                    min.map(|data| i32::from_le_bytes(data[..4].try_into().unwrap())),
                    max.map(|data| i32::from_le_bytes(data[..4].try_into().unwrap())),
                    distinct_count,
                    null_count,
                    old_format,
                ),
                Type::INT64 => Statistics::int64(
                    min.map(|data| i64::from_le_bytes(data[..8].try_into().unwrap())),
                    max.map(|data| i64::from_le_bytes(data[..8].try_into().unwrap())),
                    distinct_count,
                    null_count,
                    old_format,
                ),
                Type::INT96 => {
                    // INT96 statistics may not be correct, because comparison is signed
                    // byte-wise, not actual timestamps. It is recommended to ignore
                    // min/max statistics for INT96 columns.
                    let min = if let Some(data) = min {
                        assert_eq!(data.len(), 12);
                        Some(Int96::try_from_le_slice(&data)?)
                    } else {
                        None
                    };
                    let max = if let Some(data) = max {
                        assert_eq!(data.len(), 12);
                        Some(Int96::try_from_le_slice(&data)?)
                    } else {
                        None
                    };
                    Statistics::int96(min, max, distinct_count, null_count, old_format)
                }
                Type::FLOAT => Statistics::float(
                    min.map(|data| f32::from_le_bytes(data[..4].try_into().unwrap())),
                    max.map(|data| f32::from_le_bytes(data[..4].try_into().unwrap())),
                    distinct_count,
                    null_count,
                    old_format,
                ),
                Type::DOUBLE => Statistics::double(
                    min.map(|data| f64::from_le_bytes(data[..8].try_into().unwrap())),
                    max.map(|data| f64::from_le_bytes(data[..8].try_into().unwrap())),
                    distinct_count,
                    null_count,
                    old_format,
                ),
                Type::BYTE_ARRAY => Statistics::ByteArray(
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
                Type::FIXED_LEN_BYTE_ARRAY => Statistics::FixedLenByteArray(
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

/// Convert Statistics into Thrift definition.
pub fn to_thrift(stats: Option<&Statistics>) -> Option<TStatistics> {
    let stats = stats?;

    // record null count if it can fit in i64
    let null_count = stats
        .null_count_opt()
        .and_then(|value| i64::try_from(value).ok());

    // record distinct count if it can fit in i64
    let distinct_count = stats
        .distinct_count_opt()
        .and_then(|value| i64::try_from(value).ok());

    let mut thrift_stats = TStatistics {
        max: None,
        min: None,
        null_count,
        distinct_count,
        max_value: None,
        min_value: None,
        is_max_value_exact: None,
        is_min_value_exact: None,
    };

    // Get min/max if set.
    let (min, max, min_exact, max_exact) = (
        stats.min_bytes_opt().map(|x| x.to_vec()),
        stats.max_bytes_opt().map(|x| x.to_vec()),
        Some(stats.min_is_exact()),
        Some(stats.max_is_exact()),
    );
    if stats.is_min_max_backwards_compatible() {
        // Copy to deprecated min, max values for compatibility with older readers
        thrift_stats.min.clone_from(&min);
        thrift_stats.max.clone_from(&max);
    }

    if !stats.is_min_max_deprecated() {
        thrift_stats.min_value = min;
        thrift_stats.max_value = max;
    }

    thrift_stats.is_min_value_exact = min_exact;
    thrift_stats.is_max_value_exact = max_exact;

    Some(thrift_stats)
}

/// Strongly typed statistics for a column chunk within a row group.
///
/// This structure is a natively typed, in memory representation of the
/// [`Statistics`] structure in a parquet file footer. The statistics stored in
/// this structure can be used by query engines to skip decoding pages while
/// reading parquet data.
///
/// Page level statistics are stored separately, in [NativeIndex].
///
/// [`Statistics`]: crate::format::Statistics
/// [NativeIndex]: crate::file::page_index::index::NativeIndex
#[derive(Debug, Clone, PartialEq)]
pub enum Statistics {
    /// Statistics for Boolean column
    Boolean(ValueStatistics<bool>),
    /// Statistics for Int32 column
    Int32(ValueStatistics<i32>),
    /// Statistics for Int64 column
    Int64(ValueStatistics<i64>),
    /// Statistics for Int96 column
    Int96(ValueStatistics<Int96>),
    /// Statistics for Float column
    Float(ValueStatistics<f32>),
    /// Statistics for Double column
    Double(ValueStatistics<f64>),
    /// Statistics for ByteArray column
    ByteArray(ValueStatistics<ByteArray>),
    /// Statistics for FixedLenByteArray column
    FixedLenByteArray(ValueStatistics<FixedLenByteArray>),
}

impl<T: ParquetValueType> From<ValueStatistics<T>> for Statistics {
    fn from(t: ValueStatistics<T>) -> Self {
        T::make_statistics(t)
    }
}

impl Statistics {
    /// Creates new statistics for a column type
    pub fn new<T: ParquetValueType>(
        min: Option<T>,
        max: Option<T>,
        distinct_count: Option<u64>,
        null_count: Option<u64>,
        is_deprecated: bool,
    ) -> Self {
        Self::from(ValueStatistics::new(
            min,
            max,
            distinct_count,
            null_count,
            is_deprecated,
        ))
    }

    statistics_new_func![boolean, Option<bool>, Boolean];

    statistics_new_func![int32, Option<i32>, Int32];

    statistics_new_func![int64, Option<i64>, Int64];

    statistics_new_func![int96, Option<Int96>, Int96];

    statistics_new_func![float, Option<f32>, Float];

    statistics_new_func![double, Option<f64>, Double];

    statistics_new_func![byte_array, Option<ByteArray>, ByteArray];

    statistics_new_func![
        fixed_len_byte_array,
        Option<FixedLenByteArray>,
        FixedLenByteArray
    ];

    /// Returns `true` if statistics have old `min` and `max` fields set.
    /// This means that the column order is likely to be undefined, which, for old files
    /// could mean a signed sort order of values.
    ///
    /// Refer to [`ColumnOrder`](crate::basic::ColumnOrder) and
    /// [`SortOrder`](crate::basic::SortOrder) for more information.
    pub fn is_min_max_deprecated(&self) -> bool {
        statistics_enum_func![self, is_min_max_deprecated]
    }

    /// Old versions of parquet stored statistics in `min` and `max` fields, ordered
    /// using signed comparison. This resulted in an undefined ordering for unsigned
    /// quantities, such as booleans and unsigned integers.
    ///
    /// These fields were therefore deprecated in favour of `min_value` and `max_value`,
    /// which have a type-defined sort order.
    ///
    /// However, not all readers have been updated. For backwards compatibility, this method
    /// returns `true` if the statistics within this have a signed sort order, that is
    /// compatible with being stored in the deprecated `min` and `max` fields
    pub fn is_min_max_backwards_compatible(&self) -> bool {
        statistics_enum_func![self, is_min_max_backwards_compatible]
    }

    /// Returns optional value of number of distinct values occurring.
    /// When it is `None`, the value should be ignored.
    #[deprecated(since = "53.0.0", note = "Use `distinct_count_opt` method instead")]
    pub fn distinct_count(&self) -> Option<u64> {
        self.distinct_count_opt()
    }

    /// Returns optional value of number of distinct values occurring.
    /// When it is `None`, the value should be ignored.
    pub fn distinct_count_opt(&self) -> Option<u64> {
        statistics_enum_func![self, distinct_count]
    }

    /// Returns number of null values for the column.
    /// Note that this includes all nulls when column is part of the complex type.
    ///
    /// Note this API returns 0 if the null count is not available.
    #[deprecated(since = "53.0.0", note = "Use `null_count_opt` method instead")]
    pub fn null_count(&self) -> u64 {
        // 0 to remain consistent behavior prior to `null_count_opt`
        self.null_count_opt().unwrap_or(0)
    }

    /// Returns `true` if statistics collected any null values, `false` otherwise.
    #[deprecated(since = "53.0.0", note = "Use `null_count_opt` method instead")]
    #[allow(deprecated)]
    pub fn has_nulls(&self) -> bool {
        self.null_count() > 0
    }

    /// Returns number of null values for the column, if known.
    /// Note that this includes all nulls when column is part of the complex type.
    ///
    /// Note this API returns Some(0) even if the null count was not present
    /// in the statistics.
    /// See <https://github.com/apache/arrow-rs/pull/6216/files>
    pub fn null_count_opt(&self) -> Option<u64> {
        statistics_enum_func![self, null_count_opt]
    }

    /// Whether or not min and max values are set.
    /// Normally both min/max values will be set to `Some(value)` or `None`.
    #[deprecated(
        since = "53.0.0",
        note = "Use `min_bytes_opt` and `max_bytes_opt` methods instead"
    )]
    pub fn has_min_max_set(&self) -> bool {
        statistics_enum_func![self, _internal_has_min_max_set]
    }

    /// Returns `true` if the min value is set, and is an exact min value.
    pub fn min_is_exact(&self) -> bool {
        statistics_enum_func![self, min_is_exact]
    }

    /// Returns `true` if the max value is set, and is an exact max value.
    pub fn max_is_exact(&self) -> bool {
        statistics_enum_func![self, max_is_exact]
    }

    /// Returns slice of bytes that represent min value, if min value is known.
    pub fn min_bytes_opt(&self) -> Option<&[u8]> {
        statistics_enum_func![self, min_bytes_opt]
    }

    /// Returns slice of bytes that represent min value.
    /// Panics if min value is not set.
    #[deprecated(since = "53.0.0", note = "Use `max_bytes_opt` instead")]
    pub fn min_bytes(&self) -> &[u8] {
        self.min_bytes_opt().unwrap()
    }

    /// Returns slice of bytes that represent max value, if max value is known.
    pub fn max_bytes_opt(&self) -> Option<&[u8]> {
        statistics_enum_func![self, max_bytes_opt]
    }

    /// Returns slice of bytes that represent max value.
    /// Panics if max value is not set.
    #[deprecated(since = "53.0.0", note = "Use `max_bytes_opt` instead")]
    pub fn max_bytes(&self) -> &[u8] {
        self.max_bytes_opt().unwrap()
    }

    /// Returns physical type associated with statistics.
    pub fn physical_type(&self) -> Type {
        match self {
            Statistics::Boolean(_) => Type::BOOLEAN,
            Statistics::Int32(_) => Type::INT32,
            Statistics::Int64(_) => Type::INT64,
            Statistics::Int96(_) => Type::INT96,
            Statistics::Float(_) => Type::FLOAT,
            Statistics::Double(_) => Type::DOUBLE,
            Statistics::ByteArray(_) => Type::BYTE_ARRAY,
            Statistics::FixedLenByteArray(_) => Type::FIXED_LEN_BYTE_ARRAY,
        }
    }
}

impl fmt::Display for Statistics {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Statistics::Boolean(typed) => write!(f, "{typed}"),
            Statistics::Int32(typed) => write!(f, "{typed}"),
            Statistics::Int64(typed) => write!(f, "{typed}"),
            Statistics::Int96(typed) => write!(f, "{typed}"),
            Statistics::Float(typed) => write!(f, "{typed}"),
            Statistics::Double(typed) => write!(f, "{typed}"),
            Statistics::ByteArray(typed) => write!(f, "{typed}"),
            Statistics::FixedLenByteArray(typed) => write!(f, "{typed}"),
        }
    }
}

/// Typed implementation for [`Statistics`].
pub type TypedStatistics<T> = ValueStatistics<<T as DataType>::T>;

/// Typed statistics for one column chunk
///
/// See [`Statistics`] for more details
#[derive(Clone, Eq, PartialEq)]
pub struct ValueStatistics<T> {
    min: Option<T>,
    max: Option<T>,
    // Distinct count could be omitted in some cases
    distinct_count: Option<u64>,
    null_count: Option<u64>,

    // Whether or not the min or max values are exact, or truncated.
    is_max_value_exact: bool,
    is_min_value_exact: bool,

    /// If `true` populate the deprecated `min` and `max` fields instead of
    /// `min_value` and `max_value`
    is_min_max_deprecated: bool,

    /// If `true` the statistics are compatible with the deprecated `min` and
    /// `max` fields. See [`ValueStatistics::is_min_max_backwards_compatible`]
    is_min_max_backwards_compatible: bool,
}

impl<T: ParquetValueType> ValueStatistics<T> {
    /// Creates new typed statistics.
    pub fn new(
        min: Option<T>,
        max: Option<T>,
        distinct_count: Option<u64>,
        null_count: Option<u64>,
        is_min_max_deprecated: bool,
    ) -> Self {
        Self {
            is_max_value_exact: max.is_some(),
            is_min_value_exact: min.is_some(),
            min,
            max,
            distinct_count,
            null_count,
            is_min_max_deprecated,
            is_min_max_backwards_compatible: is_min_max_deprecated,
        }
    }

    /// Set whether the stored `min` field represents the exact
    /// minimum, or just a bound on the minimum value.
    ///
    /// see [`Self::min_is_exact`]
    pub fn with_min_is_exact(self, is_min_value_exact: bool) -> Self {
        Self {
            is_min_value_exact,
            ..self
        }
    }

    /// Set whether the stored `max` field represents the exact
    /// maximum, or just a bound on the maximum value.
    ///
    /// see [`Self::max_is_exact`]
    pub fn with_max_is_exact(self, is_max_value_exact: bool) -> Self {
        Self {
            is_max_value_exact,
            ..self
        }
    }

    /// Set whether to write the deprecated `min` and `max` fields
    /// for compatibility with older parquet writers
    ///
    /// This should only be enabled if the field is signed,
    /// see [`Self::is_min_max_backwards_compatible`]
    pub fn with_backwards_compatible_min_max(self, backwards_compatible: bool) -> Self {
        Self {
            is_min_max_backwards_compatible: backwards_compatible,
            ..self
        }
    }

    /// Returns min value of the statistics.
    ///
    /// Panics if min value is not set, e.g. all values are `null`.
    /// Use `has_min_max_set` method to check that.
    #[deprecated(since = "53.0.0", note = "Use `min_opt` instead")]
    pub fn min(&self) -> &T {
        self.min.as_ref().unwrap()
    }

    /// Returns min value of the statistics, if known.
    pub fn min_opt(&self) -> Option<&T> {
        self.min.as_ref()
    }

    /// Returns max value of the statistics.
    ///
    /// Panics if max value is not set, e.g. all values are `null`.
    /// Use `has_min_max_set` method to check that.
    #[deprecated(since = "53.0.0", note = "Use `max_opt` instead")]
    pub fn max(&self) -> &T {
        self.max.as_ref().unwrap()
    }

    /// Returns max value of the statistics, if known.
    pub fn max_opt(&self) -> Option<&T> {
        self.max.as_ref()
    }

    /// Returns min value as bytes of the statistics, if min value is known.
    pub fn min_bytes_opt(&self) -> Option<&[u8]> {
        self.min_opt().map(AsBytes::as_bytes)
    }

    /// Returns min value as bytes of the statistics.
    ///
    /// Panics if min value is not set, use `has_min_max_set` method to check
    /// if values are set.
    #[deprecated(since = "53.0.0", note = "Use `min_bytes_opt` instead")]
    pub fn min_bytes(&self) -> &[u8] {
        self.min_bytes_opt().unwrap()
    }

    /// Returns max value as bytes of the statistics, if max value is known.
    pub fn max_bytes_opt(&self) -> Option<&[u8]> {
        self.max_opt().map(AsBytes::as_bytes)
    }

    /// Returns max value as bytes of the statistics.
    ///
    /// Panics if max value is not set, use `has_min_max_set` method to check
    /// if values are set.
    #[deprecated(since = "53.0.0", note = "Use `max_bytes_opt` instead")]
    pub fn max_bytes(&self) -> &[u8] {
        self.max_bytes_opt().unwrap()
    }

    /// Whether or not min and max values are set.
    /// Normally both min/max values will be set to `Some(value)` or `None`.
    #[deprecated(since = "53.0.0", note = "Use `min_opt` and `max_opt` methods instead")]
    pub fn has_min_max_set(&self) -> bool {
        self._internal_has_min_max_set()
    }

    /// Whether or not min and max values are set.
    /// Normally both min/max values will be set to `Some(value)` or `None`.
    pub(crate) fn _internal_has_min_max_set(&self) -> bool {
        self.min.is_some() && self.max.is_some()
    }

    /// Whether or not max value is set, and is an exact value.
    pub fn max_is_exact(&self) -> bool {
        self.max.is_some() && self.is_max_value_exact
    }

    /// Whether or not min value is set, and is an exact value.
    pub fn min_is_exact(&self) -> bool {
        self.min.is_some() && self.is_min_value_exact
    }

    /// Returns optional value of number of distinct values occurring.
    pub fn distinct_count(&self) -> Option<u64> {
        self.distinct_count
    }

    /// Returns number of null values for the column.
    /// Note that this includes all nulls when column is part of the complex type.
    #[deprecated(since = "53.0.0", note = "Use `null_count_opt` method instead")]
    pub fn null_count(&self) -> u64 {
        // 0 to remain consistent behavior prior to `null_count_opt`
        self.null_count_opt().unwrap_or(0)
    }

    /// Returns null count.
    pub fn null_count_opt(&self) -> Option<u64> {
        self.null_count
    }

    /// Returns `true` if statistics were created using old min/max fields.
    fn is_min_max_deprecated(&self) -> bool {
        self.is_min_max_deprecated
    }

    /// Old versions of parquet stored statistics in `min` and `max` fields, ordered
    /// using signed comparison. This resulted in an undefined ordering for unsigned
    /// quantities, such as booleans and unsigned integers.
    ///
    /// These fields were therefore deprecated in favour of `min_value` and `max_value`,
    /// which have a type-defined sort order.
    ///
    /// However, not all readers have been updated. For backwards compatibility, this method
    /// returns `true` if the statistics within this have a signed sort order, that is
    /// compatible with being stored in the deprecated `min` and `max` fields
    pub fn is_min_max_backwards_compatible(&self) -> bool {
        self.is_min_max_backwards_compatible
    }
}

impl<T: ParquetValueType> fmt::Display for ValueStatistics<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{{")?;
        write!(f, "min: ")?;
        match self.min {
            Some(ref value) => write!(f, "{value}")?,
            None => write!(f, "N/A")?,
        }
        write!(f, ", max: ")?;
        match self.max {
            Some(ref value) => write!(f, "{value}")?,
            None => write!(f, "N/A")?,
        }
        write!(f, ", distinct_count: ")?;
        match self.distinct_count {
            Some(value) => write!(f, "{value}")?,
            None => write!(f, "N/A")?,
        }
        write!(f, ", null_count: ")?;
        match self.null_count {
            Some(value) => write!(f, "{value}")?,
            None => write!(f, "N/A")?,
        }
        write!(f, ", min_max_deprecated: {}", self.is_min_max_deprecated)?;
        write!(f, ", max_value_exact: {}", self.is_max_value_exact)?;
        write!(f, ", min_value_exact: {}", self.is_min_value_exact)?;
        write!(f, "}}")
    }
}

impl<T: ParquetValueType> fmt::Debug for ValueStatistics<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{{min: {:?}, max: {:?}, distinct_count: {:?}, null_count: {:?}, \
             min_max_deprecated: {}, min_max_backwards_compatible: {}, max_value_exact: {}, min_value_exact: {}}}",
            self.min,
            self.max,
            self.distinct_count,
            self.null_count,
            self.is_min_max_deprecated,
            self.is_min_max_backwards_compatible,
            self.is_max_value_exact,
            self.is_min_value_exact
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_statistics_min_max_bytes() {
        let stats = Statistics::int32(Some(-123), Some(234), None, Some(1), false);
        assert_eq!(stats.min_bytes_opt(), Some((-123).as_bytes()));
        assert_eq!(stats.max_bytes_opt(), Some(234.as_bytes()));

        let stats = Statistics::byte_array(
            Some(ByteArray::from(vec![1, 2, 3])),
            Some(ByteArray::from(vec![3, 4, 5])),
            None,
            Some(1),
            true,
        );
        assert_eq!(stats.min_bytes_opt().unwrap(), &[1, 2, 3]);
        assert_eq!(stats.max_bytes_opt().unwrap(), &[3, 4, 5]);
    }

    #[test]
    #[should_panic(expected = "General(\"Statistics null count is negative -10\")")]
    fn test_statistics_negative_null_count() {
        let thrift_stats = TStatistics {
            max: None,
            min: None,
            null_count: Some(-10),
            distinct_count: None,
            max_value: None,
            min_value: None,
            is_max_value_exact: None,
            is_min_value_exact: None,
        };

        from_thrift(Type::INT32, Some(thrift_stats)).unwrap();
    }

    #[test]
    fn test_statistics_thrift_none() {
        assert_eq!(from_thrift(Type::INT32, None).unwrap(), None);
        assert_eq!(from_thrift(Type::BYTE_ARRAY, None).unwrap(), None);
    }

    #[test]
    fn test_statistics_debug() {
        let stats = Statistics::int32(Some(1), Some(12), None, Some(12), true);
        assert_eq!(
            format!("{stats:?}"),
            "Int32({min: Some(1), max: Some(12), distinct_count: None, null_count: Some(12), \
             min_max_deprecated: true, min_max_backwards_compatible: true, max_value_exact: true, min_value_exact: true})"
        );

        let stats = Statistics::int32(None, None, None, Some(7), false);
        assert_eq!(
            format!("{stats:?}"),
            "Int32({min: None, max: None, distinct_count: None, null_count: Some(7), \
             min_max_deprecated: false, min_max_backwards_compatible: false, max_value_exact: false, min_value_exact: false})"
        )
    }

    #[test]
    fn test_statistics_display() {
        let stats = Statistics::int32(Some(1), Some(12), None, Some(12), true);
        assert_eq!(
            format!("{stats}"),
            "{min: 1, max: 12, distinct_count: N/A, null_count: 12, min_max_deprecated: true, max_value_exact: true, min_value_exact: true}"
        );

        let stats = Statistics::int64(None, None, None, Some(7), false);
        assert_eq!(
            format!("{stats}"),
            "{min: N/A, max: N/A, distinct_count: N/A, null_count: 7, min_max_deprecated: \
             false, max_value_exact: false, min_value_exact: false}"
        );

        let stats = Statistics::int96(
            Some(Int96::from(vec![1, 0, 0])),
            Some(Int96::from(vec![2, 3, 4])),
            None,
            Some(3),
            true,
        );
        assert_eq!(
            format!("{stats}"),
            "{min: [1, 0, 0], max: [2, 3, 4], distinct_count: N/A, null_count: 3, \
             min_max_deprecated: true, max_value_exact: true, min_value_exact: true}"
        );

        let stats = Statistics::ByteArray(
            ValueStatistics::new(
                Some(ByteArray::from(vec![1u8])),
                Some(ByteArray::from(vec![2u8])),
                Some(5),
                Some(7),
                false,
            )
            .with_max_is_exact(false)
            .with_min_is_exact(false),
        );
        assert_eq!(
            format!("{stats}"),
            "{min: [1], max: [2], distinct_count: 5, null_count: 7, min_max_deprecated: false, max_value_exact: false, min_value_exact: false}"
        );
    }

    #[test]
    fn test_statistics_partial_eq() {
        let expected = Statistics::int32(Some(12), Some(45), None, Some(11), true);

        assert!(Statistics::int32(Some(12), Some(45), None, Some(11), true) == expected);
        assert!(Statistics::int32(Some(11), Some(45), None, Some(11), true) != expected);
        assert!(Statistics::int32(Some(12), Some(44), None, Some(11), true) != expected);
        assert!(Statistics::int32(Some(12), Some(45), None, Some(23), true) != expected);
        assert!(Statistics::int32(Some(12), Some(45), None, Some(11), false) != expected);

        assert!(
            Statistics::int32(Some(12), Some(45), None, Some(11), false)
                != Statistics::int64(Some(12), Some(45), None, Some(11), false)
        );

        assert!(
            Statistics::boolean(Some(false), Some(true), None, None, true)
                != Statistics::double(Some(1.2), Some(4.5), None, None, true)
        );

        assert!(
            Statistics::byte_array(
                Some(ByteArray::from(vec![1, 2, 3])),
                Some(ByteArray::from(vec![1, 2, 3])),
                None,
                None,
                true
            ) != Statistics::fixed_len_byte_array(
                Some(ByteArray::from(vec![1, 2, 3]).into()),
                Some(ByteArray::from(vec![1, 2, 3]).into()),
                None,
                None,
                true,
            )
        );

        assert!(
            Statistics::byte_array(
                Some(ByteArray::from(vec![1, 2, 3])),
                Some(ByteArray::from(vec![1, 2, 3])),
                None,
                None,
                true,
            ) != Statistics::ByteArray(
                ValueStatistics::new(
                    Some(ByteArray::from(vec![1, 2, 3])),
                    Some(ByteArray::from(vec![1, 2, 3])),
                    None,
                    None,
                    true,
                )
                .with_max_is_exact(false)
            )
        );

        assert!(
            Statistics::fixed_len_byte_array(
                Some(FixedLenByteArray::from(vec![1, 2, 3])),
                Some(FixedLenByteArray::from(vec![1, 2, 3])),
                None,
                None,
                true,
            ) != Statistics::FixedLenByteArray(
                ValueStatistics::new(
                    Some(FixedLenByteArray::from(vec![1, 2, 3])),
                    Some(FixedLenByteArray::from(vec![1, 2, 3])),
                    None,
                    None,
                    true,
                )
                .with_min_is_exact(false)
            )
        );
    }

    #[test]
    fn test_statistics_from_thrift() {
        // Helper method to check statistics conversion.
        fn check_stats(stats: Statistics) {
            let tpe = stats.physical_type();
            let thrift_stats = to_thrift(Some(&stats));
            assert_eq!(from_thrift(tpe, thrift_stats).unwrap(), Some(stats));
        }

        check_stats(Statistics::boolean(
            Some(false),
            Some(true),
            None,
            Some(7),
            true,
        ));
        check_stats(Statistics::boolean(
            Some(false),
            Some(true),
            None,
            Some(7),
            true,
        ));
        check_stats(Statistics::boolean(
            Some(false),
            Some(true),
            None,
            Some(0),
            false,
        ));
        check_stats(Statistics::boolean(
            Some(true),
            Some(true),
            None,
            Some(7),
            true,
        ));
        check_stats(Statistics::boolean(
            Some(false),
            Some(false),
            None,
            Some(7),
            true,
        ));
        check_stats(Statistics::boolean(None, None, None, Some(7), true));

        check_stats(Statistics::int32(
            Some(-100),
            Some(500),
            None,
            Some(7),
            true,
        ));
        check_stats(Statistics::int32(
            Some(-100),
            Some(500),
            None,
            Some(0),
            false,
        ));
        check_stats(Statistics::int32(None, None, None, Some(7), true));

        check_stats(Statistics::int64(
            Some(-100),
            Some(200),
            None,
            Some(7),
            true,
        ));
        check_stats(Statistics::int64(
            Some(-100),
            Some(200),
            None,
            Some(0),
            false,
        ));
        check_stats(Statistics::int64(None, None, None, Some(7), true));

        check_stats(Statistics::float(Some(1.2), Some(3.4), None, Some(7), true));
        check_stats(Statistics::float(
            Some(1.2),
            Some(3.4),
            None,
            Some(0),
            false,
        ));
        check_stats(Statistics::float(None, None, None, Some(7), true));

        check_stats(Statistics::double(
            Some(1.2),
            Some(3.4),
            None,
            Some(7),
            true,
        ));
        check_stats(Statistics::double(
            Some(1.2),
            Some(3.4),
            None,
            Some(0),
            false,
        ));
        check_stats(Statistics::double(None, None, None, Some(7), true));

        check_stats(Statistics::byte_array(
            Some(ByteArray::from(vec![1, 2, 3])),
            Some(ByteArray::from(vec![3, 4, 5])),
            None,
            Some(7),
            true,
        ));
        check_stats(Statistics::byte_array(None, None, None, Some(7), true));

        check_stats(Statistics::fixed_len_byte_array(
            Some(ByteArray::from(vec![1, 2, 3]).into()),
            Some(ByteArray::from(vec![3, 4, 5]).into()),
            None,
            Some(7),
            true,
        ));
        check_stats(Statistics::fixed_len_byte_array(
            None,
            None,
            None,
            Some(7),
            true,
        ));
    }

    #[test]
    fn test_count_encoding() {
        statistics_count_test(None, None);
        statistics_count_test(Some(0), Some(0));
        statistics_count_test(Some(100), Some(2000));
        statistics_count_test(Some(1), None);
        statistics_count_test(None, Some(1));
    }

    #[test]
    fn test_count_encoding_distinct_too_large() {
        // statistics are stored using i64, so test trying to store larger values
        let statistics = make_bool_stats(Some(u64::MAX), Some(100));
        let thrift_stats = to_thrift(Some(&statistics)).unwrap();
        assert_eq!(thrift_stats.distinct_count, None); // can't store u64 max --> null
        assert_eq!(thrift_stats.null_count, Some(100));
    }

    #[test]
    fn test_count_encoding_null_too_large() {
        // statistics are stored using i64, so test trying to store larger values
        let statistics = make_bool_stats(Some(100), Some(u64::MAX));
        let thrift_stats = to_thrift(Some(&statistics)).unwrap();
        assert_eq!(thrift_stats.distinct_count, Some(100));
        assert_eq!(thrift_stats.null_count, None); // can' store u64 max --> null
    }

    #[test]
    fn test_count_decoding_null_invalid() {
        let tstatistics = TStatistics {
            null_count: Some(-42),
            ..Default::default()
        };
        let err = from_thrift(Type::BOOLEAN, Some(tstatistics)).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Parquet error: Statistics null count is negative -42"
        );
    }

    /// Writes statistics to thrift and reads them back and ensures:
    /// - The statistics are the same
    /// - The statistics written to thrift are the same as the original statistics
    fn statistics_count_test(distinct_count: Option<u64>, null_count: Option<u64>) {
        let statistics = make_bool_stats(distinct_count, null_count);

        let thrift_stats = to_thrift(Some(&statistics)).unwrap();
        assert_eq!(thrift_stats.null_count.map(|c| c as u64), null_count);
        assert_eq!(
            thrift_stats.distinct_count.map(|c| c as u64),
            distinct_count
        );

        let round_tripped = from_thrift(Type::BOOLEAN, Some(thrift_stats))
            .unwrap()
            .unwrap();
        // TODO: remove branch when we no longer support assuming null_count==None in the thrift
        // means null_count = Some(0)
        if null_count.is_none() {
            assert_ne!(round_tripped, statistics);
            assert!(round_tripped.null_count_opt().is_some());
            assert_eq!(round_tripped.null_count_opt(), Some(0));
            assert_eq!(round_tripped.min_bytes_opt(), statistics.min_bytes_opt());
            assert_eq!(round_tripped.max_bytes_opt(), statistics.max_bytes_opt());
            assert_eq!(
                round_tripped.distinct_count_opt(),
                statistics.distinct_count_opt()
            );
        } else {
            assert_eq!(round_tripped, statistics);
        }
    }

    fn make_bool_stats(distinct_count: Option<u64>, null_count: Option<u64>) -> Statistics {
        let min = Some(true);
        let max = Some(false);
        let is_min_max_deprecated = false;

        // test is about the counts, so we aren't really testing the min/max values
        Statistics::Boolean(ValueStatistics::new(
            min,
            max,
            distinct_count,
            null_count,
            is_min_max_deprecated,
        ))
    }
}
