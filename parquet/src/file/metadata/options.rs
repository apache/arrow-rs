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

use crate::schema::types::SchemaDescPtr;

/// Options that can be set to control what parts of the Parquet file footer
/// metadata will be decoded and made present in the [`ParquetMetaData`] returned
/// by [`ParquetMetaDataReader`] and [`ParquetMetaDataPushDecoder`].
///
/// [`ParquetMetaData`]: crate::file::metadata::ParquetMetaData
/// [`ParquetMetaDataReader`]: crate::file::metadata::ParquetMetaDataReader
/// [`ParquetMetaDataPushDecoder`]: crate::file::metadata::ParquetMetaDataPushDecoder
#[derive(Default, Debug, Clone)]
pub struct MetadataOptions {
    schema_descr: Option<SchemaDescPtr>,
}

impl MetadataOptions {
    /// Return a new default [`MetadataOptions`].
    pub fn new() -> Self {
        Default::default()
    }

    /// Returns an optional [`SchemaDescPtr`] to use when decoding. If this is not `None` then
    /// the schema in the footer will be skipped
    pub fn schema(&self) -> Option<&SchemaDescPtr> {
        self.schema_descr.as_ref()
    }

    /// Provide a schema to use when decoding the metadata.
    pub fn with_schema(self, val: SchemaDescPtr) -> Self {
        Self {
            schema_descr: Some(val),
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::{
        DecodeResult,
        file::metadata::{MetadataOptions, ParquetMetaDataPushDecoder},
        util::test_common::file_util::get_test_file,
    };
    use std::{io::Read, sync::Arc};

    #[test]
    fn test_provide_schema() {
        let mut buf: Vec<u8> = Vec::new();
        get_test_file("alltypes_plain.parquet")
            .read_to_end(&mut buf)
            .unwrap();

        let footer = Bytes::from(buf);
        let mut decoder = ParquetMetaDataPushDecoder::try_new(footer.len() as u64).unwrap();
        decoder
            .push_range(0..footer.len() as u64, footer.clone())
            .unwrap();

        let expected = match decoder.try_decode().unwrap() {
            DecodeResult::Data(m) => m,
            _ => panic!("could not parse metadata"),
        };

        let options = Arc::new(
            MetadataOptions::new().with_schema(expected.file_metadata().schema_descr_ptr()),
        );
        let mut decoder = ParquetMetaDataPushDecoder::try_new(footer.len() as u64)
            .unwrap()
            .with_metadata_options(Some(options));
        decoder.push_range(0..footer.len() as u64, footer).unwrap();
        let metadata = match decoder.try_decode().unwrap() {
            DecodeResult::Data(m) => m,
            _ => panic!("could not parse metadata"),
        };

        assert_eq!(expected, metadata);
    }
}
