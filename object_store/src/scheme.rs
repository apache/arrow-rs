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

use crate::{Error, Result};
use url::Url;

/// Recognises various URL formats, identifying the relevant [`ObjectStore`](crate::ObjectStore)
///
/// This can be combined with the [with_url](crate::aws::AmazonS3Builder::with_url) methods
/// on the corresponding builder to construct the relevant type of store
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ObjectStoreScheme {
    /// Url corresponding to [`LocalFileSystem`](crate::local::LocalFileSystem)
    Local,
    /// Url corresponding to [`InMemory`](crate::memory::InMemory)
    Memory,
    /// Url corresponding to [`AmazonS3`](crate::aws::AmazonS3)
    AmazonS3,
    /// Url corresponding to [`GoogleCloudStorage`](crate::gcp::GoogleCloudStorage)
    GoogleCloudStorage,
    /// Url corresponding to [`MicrosoftAzure`](crate::azure::MicrosoftAzure)
    MicrosoftAzure,
    /// Url corresponding to [`HttpStore`](crate::http::HttpStore)
    Http,
}

impl ObjectStoreScheme {
    /// Create an [`ObjectStoreScheme`] from the provided [`Url`]
    pub fn try_new(url: &Url) -> Result<Self> {
        match (url.scheme(), url.host_str()) {
            ("file", None) => Ok(Self::Local),
            ("memory", None) => Ok(Self::Memory),
            ("s3" | "s3a", Some(_)) => Ok(Self::AmazonS3),
            ("gs", Some(_)) => Ok(Self::GoogleCloudStorage),
            ("az" | "adl" | "azure" | "abfs" | "abfss", Some(_)) => {
                Ok(Self::MicrosoftAzure)
            }
            ("http", Some(_)) => Ok(Self::Http),
            ("https", Some(host)) => {
                if host.ends_with("dfs.core.windows.net") {
                    Ok(Self::MicrosoftAzure)
                } else if host.ends_with("blob.core.windows.net") {
                    Ok(Self::MicrosoftAzure)
                } else if host.ends_with("amazonaws.com") {
                    Ok(Self::AmazonS3)
                } else {
                    Ok(Self::Http)
                }
            }
            _ => Err(Error::Generic {
                store: "ObjectStoreScheme",
                source: format!("Unrecognized URL: {url}").into(),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scheme() {
        let cases = [
            ("file:/path", ObjectStoreScheme::Local),
            ("file:///path", ObjectStoreScheme::Local),
            ("memory:/foo", ObjectStoreScheme::Memory),
            ("memory:///", ObjectStoreScheme::Memory),
            ("s3://bucket/path", ObjectStoreScheme::AmazonS3),
            ("s3a://bucket/path", ObjectStoreScheme::AmazonS3),
            (
                "https://s3.bucket.amazonaws.com",
                ObjectStoreScheme::AmazonS3,
            ),
            (
                "https://bucket.s3.region.amazonaws.com",
                ObjectStoreScheme::AmazonS3,
            ),
            ("abfs://container/path", ObjectStoreScheme::MicrosoftAzure),
            (
                "abfs://file_system@account_name.dfs.core.windows.net/path",
                ObjectStoreScheme::MicrosoftAzure,
            ),
            (
                "abfss://file_system@account_name.dfs.core.windows.net/path",
                ObjectStoreScheme::MicrosoftAzure,
            ),
            (
                "https://account.dfs.core.windows.net",
                ObjectStoreScheme::MicrosoftAzure,
            ),
            (
                "https://account.blob.core.windows.net",
                ObjectStoreScheme::MicrosoftAzure,
            ),
            ("gs://bucket/path", ObjectStoreScheme::GoogleCloudStorage),
            ("http://mydomain/path", ObjectStoreScheme::Http),
            ("https://mydomain/path", ObjectStoreScheme::Http),
        ];

        for (s, expected) in cases {
            let url = Url::parse(s).unwrap();
            assert_eq!(ObjectStoreScheme::try_new(&url).unwrap(), expected);
        }

        let neg_cases = [
            "unix:/run/foo.socket",
            "file://remote/path",
            "memory://remote/",
        ];
        for s in neg_cases {
            let url = Url::parse(s).unwrap();
            assert!(ObjectStoreScheme::try_new(&url).is_err());
        }
    }
}
