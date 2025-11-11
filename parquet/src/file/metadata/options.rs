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

use paste::paste;
use std::collections::HashSet;
use std::sync::Arc;

use crate::schema::types::SchemaDescPtr;

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
    // The outer option acts as a global boolean, so if `skip_encoding_stats.is_some()`
    // is `true` then we're at least skipping some stats. The inner `Option` is a keep
    // list of column indicies to decode.
    skip_encoding_stats: Option<Option<Arc<HashSet<usize>>>>,
}

// wraps `set_X` with a `with_X` function that returns `Self`
macro_rules! add_mutator {
    ($name:expr, $type:ty) => {
        paste! {
            #[doc = concat!("Call [`Self::set_", stringify!($name), "`] and return `Self` for chaining.")]
            pub fn [<with_ $name>](mut self, val: $type) -> Self {
                self.[<set_ $name>](val);
                self
            }
        }
    }
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

    // with_schema
    add_mutator!(schema, SchemaDescPtr);

    /// Returns whether to present the `encoding_stats` field of the `ColumnMetaData` as a
    /// bitmask.
    ///
    /// See [`ColumnChunkMetaData::page_encoding_stats_mask`] for an explanation of why this
    /// might be desirable.
    ///
    /// [`ColumnChunkMetaData::page_encoding_stats_mask`]:
    /// crate::file::metadata::ColumnChunkMetaData::page_encoding_stats_mask
    pub fn encoding_stats_as_mask(&self) -> bool {
        self.encoding_stats_as_mask
    }

    /// Convert `encoding_stats` from a vector of [`PageEncodingStats`] to a bitmask. This can
    /// speed up metadata decoding while still enabling some use cases served by the full stats.
    ///
    /// See [`ColumnChunkMetaData::page_encoding_stats_mask`] for more information.
    ///
    /// [`PageEncodingStats`]: crate::file::metadata::PageEncodingStats
    /// [`ColumnChunkMetaData::page_encoding_stats_mask`]:
    /// crate::file::metadata::ColumnChunkMetaData::page_encoding_stats_mask
    pub fn set_encoding_stats_as_mask(&mut self, val: bool) {
        self.encoding_stats_as_mask = val;
    }

    // with_encoding_stats_as_mask
    add_mutator!(encoding_stats_as_mask, bool);

    /// Returns whether to skip decoding the `encoding_stats` in the `ColumnMetaData`
    /// for the column indexed by `col_index`.
    pub fn skip_encoding_stats(&self, col_index: usize) -> bool {
        self.skip_encoding_stats
            .as_ref()
            .is_some_and(|oset| oset.as_ref().is_none_or(|keep| !keep.contains(&col_index)))
    }

    /// Skip decoding of all `encoding_stats`. Takes precedence over
    /// [`Self::encoding_stats_as_mask`].
    pub fn set_skip_encoding_stats(&mut self, val: bool) {
        self.skip_encoding_stats = if val { Some(None) } else { None };
    }

    // with_skip_encoding_stats
    add_mutator!(skip_encoding_stats, bool);

    /// Skip decoding of `encoding_stats`, but decode the stats for those columns in
    /// the provided list of column indices.
    ///
    /// This allows for optimizations such as only decoding the page encoding statistics
    /// for columns present in a predicate.
    pub fn set_keep_encoding_stats(&mut self, keep: &[usize]) {
        if keep.is_empty() {
            self.set_skip_encoding_stats(true);
        } else {
            let mut keep_set = HashSet::<usize>::with_capacity(keep.len());
            keep_set.extend(keep.iter());
            self.skip_encoding_stats = Some(Some(Arc::new(keep_set)))
        }
    }

    // with_keep_encoding_stats
    add_mutator!(keep_encoding_stats, &[usize]);
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
