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

//! Options used to control metadata parsing

use std::collections::HashSet;
use std::sync::Arc;

use crate::schema::types::SchemaDescPtr;

/// Enum to control decoding of some Parquet statistics fields.
///
/// # Example
/// ```rust
/// use parquet::file::metadata::ParquetStatisticsPolicy;
/// use parquet::file::serialized_reader::ReadOptionsBuilder;
/// use parquet::arrow::arrow_reader::ArrowReaderOptions;
///
/// // Set arrow options to skip encoding statistics for all columns.
/// let options =
///     ArrowReaderOptions::new().with_encoding_stats_policy(ParquetStatisticsPolicy::SkipAll);
///
/// // Set serialized reader options to decode encoding statistics for all columns.
/// let options =
///     ReadOptionsBuilder::new().with_encoding_stats_policy(ParquetStatisticsPolicy::KeepAll)
///     .build();
///
/// // Set arrow options to skip encoding statistics for all columns, but to decode statistics
/// // for columns 0 and 1.
/// let options = ArrowReaderOptions::new()
///     .with_encoding_stats_policy(ParquetStatisticsPolicy::skip_except(&[0, 1]));
/// ```
#[derive(Default, Debug, Clone)]
pub enum ParquetStatisticsPolicy {
    /// Decode the relevant statistics for all columns.
    #[default]
    KeepAll,
    /// Skip decoding the relevant statistics for all columns.
    SkipAll,
    /// Skip decoding the relevant statistics for all columns not in the provided set
    /// of column indices.
    SkipExcept(Arc<HashSet<usize>>),
}

impl ParquetStatisticsPolicy {
    /// Create a `ParquetStatisticsPolicy` to skip all columns except those in `keep`.
    ///
    /// If `keep` is empty, then this returns [`Self::SkipAll`]
    pub fn skip_except(keep: &[usize]) -> Self {
        if keep.is_empty() {
            Self::SkipAll
        } else {
            let mut keep_set = HashSet::<usize>::with_capacity(keep.len());
            keep_set.extend(keep.iter());
            Self::SkipExcept(Arc::new(keep_set))
        }
    }

    /// Returns whether the policy for the given column index is to skip the statistics.
    pub(crate) fn is_skip(&self, col_index: usize) -> bool {
        match self {
            Self::KeepAll => false,
            Self::SkipAll => true,
            Self::SkipExcept(keep) => !keep.contains(&col_index),
        }
    }
}

/// Options that can be set to control what parts of the Parquet file footer
/// metadata will be decoded and made present in the [`ParquetMetaData`] returned
/// by [`ParquetMetaDataReader`] and [`ParquetMetaDataPushDecoder`].
///
/// [`ParquetMetaData`]: crate::file::metadata::ParquetMetaData
/// [`ParquetMetaDataReader`]: crate::file::metadata::ParquetMetaDataReader
/// [`ParquetMetaDataPushDecoder`]: crate::file::metadata::ParquetMetaDataPushDecoder
#[derive(Default, Debug, Clone)]
pub struct ParquetMetaDataOptions {
    schema_descr: Option<SchemaDescPtr>,
    encoding_stats_as_mask: bool,
    encoding_stats_policy: ParquetStatisticsPolicy,
}

impl ParquetMetaDataOptions {
    /// Return a new default [`ParquetMetaDataOptions`].
    pub fn new() -> Self {
        Default::default()
    }

    /// Returns an optional [`SchemaDescPtr`] to use when decoding. If this is not `None` then
    /// the schema in the footer will be skipped.
    pub fn schema(&self) -> Option<&SchemaDescPtr> {
        self.schema_descr.as_ref()
    }

    /// Provide a schema to use when decoding the metadata.
    pub fn set_schema(&mut self, val: SchemaDescPtr) {
        self.schema_descr = Some(val);
    }

    /// Call [`Self::set_schema`] and return `Self` for chaining.
    pub fn with_schema(mut self, val: SchemaDescPtr) -> Self {
        self.set_schema(val);
        self
    }

    /// Returns whether to present the [`encoding_stats`] field of the Parquet `ColumnMetaData`
    /// as a bitmask (defaults to `false`).
    ///
    /// See [`ColumnChunkMetaData::page_encoding_stats_mask`] for an explanation of why this
    /// might be desirable.
    ///
    /// [`ColumnChunkMetaData::page_encoding_stats_mask`]:
    /// crate::file::metadata::ColumnChunkMetaData::page_encoding_stats_mask
    /// [`encoding_stats`]:
    /// https://github.com/apache/parquet-format/blob/786142e26740487930ddc3ec5e39d780bd930907/src/main/thrift/parquet.thrift#L917
    pub fn encoding_stats_as_mask(&self) -> bool {
        self.encoding_stats_as_mask
    }

    /// Convert [`encoding_stats`] from a vector of [`PageEncodingStats`] to a bitmask. This can
    /// speed up metadata decoding while still enabling some use cases served by the full stats.
    ///
    /// Note that if for a given column both this option and `skip_encoding_stats` are `true`, the
    /// stats will be skipped and not be returned as a mask.
    ///
    /// See [`ColumnChunkMetaData::page_encoding_stats_mask`] for more information.
    ///
    /// [`PageEncodingStats`]: crate::file::metadata::PageEncodingStats
    /// [`ColumnChunkMetaData::page_encoding_stats_mask`]:
    /// crate::file::metadata::ColumnChunkMetaData::page_encoding_stats_mask
    /// [`encoding_stats`]:
    /// https://github.com/apache/parquet-format/blob/786142e26740487930ddc3ec5e39d780bd930907/src/main/thrift/parquet.thrift#L917
    pub fn set_encoding_stats_as_mask(&mut self, val: bool) {
        self.encoding_stats_as_mask = val;
    }

    /// Call [`Self::set_encoding_stats_as_mask`] and return `Self` for chaining.
    pub fn with_encoding_stats_as_mask(mut self, val: bool) -> Self {
        self.set_encoding_stats_as_mask(val);
        self
    }

    /// Returns whether to skip decoding the [`encoding_stats`] in the Parquet `ColumnMetaData`
    /// for the column indexed by `col_index`.
    ///
    /// [`encoding_stats`]:
    /// https://github.com/apache/parquet-format/blob/786142e26740487930ddc3ec5e39d780bd930907/src/main/thrift/parquet.thrift#L917
    pub fn skip_encoding_stats(&self, col_index: usize) -> bool {
        self.encoding_stats_policy.is_skip(col_index)
    }

    /// Sets the decoding policy for [`encoding_stats`] in the Parquet `ColumnMetaData`.
    ///
    /// The default policy is to decode all `encoding_stats`.
    ///
    /// This option takes precedence over [`Self::encoding_stats_as_mask`].
    ///
    /// [`encoding_stats`]:
    /// https://github.com/apache/parquet-format/blob/786142e26740487930ddc3ec5e39d780bd930907/src/main/thrift/parquet.thrift#L917
    pub fn set_encoding_stats_policy(&mut self, policy: ParquetStatisticsPolicy) {
        self.encoding_stats_policy = policy;
    }

    /// Call [`Self::set_encoding_stats_policy`] and return `Self` for chaining.
    pub fn with_encoding_stats_policy(mut self, policy: ParquetStatisticsPolicy) -> Self {
        self.set_encoding_stats_policy(policy);
        self
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::{
        DecodeResult,
        file::metadata::{ParquetMetaDataOptions, ParquetMetaDataPushDecoder},
        util::test_common::file_util::get_test_file,
    };
    use std::{io::Read, sync::Arc};

    #[test]
    fn test_provide_schema() {
        let mut buf: Vec<u8> = Vec::new();
        get_test_file("alltypes_plain.parquet")
            .read_to_end(&mut buf)
            .unwrap();

        let data = Bytes::from(buf);
        let mut decoder = ParquetMetaDataPushDecoder::try_new(data.len() as u64).unwrap();
        decoder
            .push_range(0..data.len() as u64, data.clone())
            .unwrap();

        let expected = match decoder.try_decode().unwrap() {
            DecodeResult::Data(m) => m,
            _ => panic!("could not parse metadata"),
        };
        let expected_schema = expected.file_metadata().schema_descr_ptr();

        let mut options = ParquetMetaDataOptions::new();
        options.set_schema(expected_schema);
        let options = Arc::new(options);

        let mut decoder = ParquetMetaDataPushDecoder::try_new(data.len() as u64)
            .unwrap()
            .with_metadata_options(Some(options));
        decoder.push_range(0..data.len() as u64, data).unwrap();
        let metadata = match decoder.try_decode().unwrap() {
            DecodeResult::Data(m) => m,
            _ => panic!("could not parse metadata"),
        };

        assert_eq!(expected, metadata);
        // the schema pointers should be the same
        assert!(Arc::ptr_eq(
            &expected.file_metadata().schema_descr_ptr(),
            &metadata.file_metadata().schema_descr_ptr()
        ));
    }
}
