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

use crate::errors::{ParquetError, Result};
use crate::file::footer::{decode_footer, decode_metadata};
use crate::file::metadata::ParquetMetaData;
use bytes::{BufMut, Bytes, BytesMut};
use std::future::Future;
use std::ops::Range;

/// Fetches parquet metadata
///
/// Parameters:
/// * fetch: an async function that can fetch byte ranges
/// * file_size: the total size of the parquet file
/// * footer_size_hint: footer prefetch size (see comments below)
///
/// The length of the parquet footer, which contains file metadata, is not
/// known up front. Therefore this function will first issue a request to read
/// the last 8 bytes to determine the footer's precise length, before
/// issuing a second request to fetch the metadata bytes
///
/// If a hint is set, this method will read the specified number of bytes
/// in the first request, instead of 8, and only issue a second request
/// if additional bytes are needed. This can therefore eliminate a
/// potentially costly additional fetch operation
pub async fn fetch_parquet_metadata<F, Fut>(
    mut fetch: F,
    file_size: usize,
    footer_size_hint: Option<usize>,
) -> Result<ParquetMetaData>
where
    F: FnMut(Range<usize>) -> Fut,
    Fut: Future<Output = Result<Bytes>>,
{
    if file_size < 8 {
        return Err(ParquetError::EOF(format!(
            "file size of {file_size} is less than footer"
        )));
    }

    // If a size hint is provided, read more than the minimum size
    // to try and avoid a second fetch.
    let footer_start = if let Some(size_hint) = footer_size_hint {
        file_size.saturating_sub(size_hint)
    } else {
        file_size - 8
    };

    let suffix = fetch(footer_start..file_size).await?;
    let suffix_len = suffix.len();

    let mut footer = [0; 8];
    footer.copy_from_slice(&suffix[suffix_len - 8..suffix_len]);

    let length = decode_footer(&footer)?;

    if file_size < length + 8 {
        return Err(ParquetError::EOF(format!(
            "file size of {} is less than footer + metadata {}",
            file_size,
            length + 8
        )));
    }

    // Did not fetch the entire file metadata in the initial read, need to make a second request
    if length > suffix_len - 8 {
        let metadata_start = file_size - length - 8;
        let remaining_metadata = fetch(metadata_start..footer_start).await?;

        let mut metadata = BytesMut::with_capacity(length);

        metadata.put(remaining_metadata.as_ref());
        metadata.put(&suffix[..suffix_len - 8]);

        Ok(decode_metadata(metadata.as_ref())?)
    } else {
        let metadata_start = file_size - length - 8;

        Ok(decode_metadata(
            &suffix[metadata_start - footer_start..suffix_len - 8],
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file::reader::{FileReader, Length, SerializedFileReader};
    use crate::util::test_common::file_util::get_test_file;
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom};

    fn read_range(file: &mut File, range: Range<usize>) -> Result<Bytes> {
        file.seek(SeekFrom::Start(range.start as _))?;
        let len = range.end - range.start;
        let mut buf = Vec::with_capacity(len);
        file.take(len as _).read_to_end(&mut buf)?;
        Ok(buf.into())
    }

    #[tokio::test]
    async fn test_simple() {
        let mut file = get_test_file("nulls.snappy.parquet");
        let len = file.len() as usize;

        let reader = SerializedFileReader::new(file.try_clone().unwrap()).unwrap();
        let expected = reader.metadata().file_metadata().schema();

        let mut fetch = |range| futures::future::ready(read_range(&mut file, range));
        let actual = fetch_parquet_metadata(&mut fetch, len, None).await.unwrap();
        assert_eq!(actual.file_metadata().schema(), expected);

        // Metadata hint too small
        let actual = fetch_parquet_metadata(&mut fetch, len, Some(10))
            .await
            .unwrap();
        assert_eq!(actual.file_metadata().schema(), expected);

        // Metadata hint too large
        let actual = fetch_parquet_metadata(&mut fetch, len, Some(500))
            .await
            .unwrap();
        assert_eq!(actual.file_metadata().schema(), expected);

        // Metadata hint exactly correct
        let actual = fetch_parquet_metadata(&mut fetch, len, Some(428))
            .await
            .unwrap();
        assert_eq!(actual.file_metadata().schema(), expected);

        let err = fetch_parquet_metadata(&mut fetch, 4, None)
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(err, "EOF: file size of 4 is less than footer");

        let err = fetch_parquet_metadata(&mut fetch, 20, None)
            .await
            .unwrap_err()
            .to_string();
        assert_eq!(err, "Parquet error: Invalid Parquet file. Corrupt footer");
    }
}
