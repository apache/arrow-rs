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

use crate::local::LocalFileSystem;
use crate::memory::InMemory;
use crate::path::Path;
use crate::ObjectStore;
use snafu::Snafu;
use url::Url;

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Unable to convert URL \"{}\" to filesystem path", url))]
    InvalidUrl { url: Url },

    #[snafu(display("Unable to recognise URL \"{}\"", url))]
    Unrecognised { url: Url },

    #[snafu(display("Feature {scheme:?} not enabled"))]
    NotEnabled { scheme: ObjectStoreScheme },
}

impl From<Error> for super::Error {
    fn from(e: Error) -> Self {
        Self::Generic {
            store: "URL",
            source: Box::new(e),
        }
    }
}

/// Recognises various URL formats, identifying the relevant [`ObjectStore`](crate::ObjectStore)
#[derive(Debug, Eq, PartialEq)]
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
    fn parse(url: &Url) -> Result<Self, Error> {
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
                if host.ends_with("dfs.core.windows.net")
                    || host.ends_with("blob.core.windows.net")
                {
                    Ok(Self::MicrosoftAzure)
                } else if host.ends_with("amazonaws.com")
                    || host.ends_with("r2.cloudflarestorage.com")
                {
                    Ok(Self::AmazonS3)
                } else {
                    Ok(Self::Http)
                }
            }
            _ => Err(Error::Unrecognised { url: url.clone() }),
        }
    }
}

#[cfg(any(feature = "aws", feature = "gcp", feature = "azure", feature = "http"))]
macro_rules! builder_opts {
    ($builder:ty, $url:expr, $options:expr) => {{
        let builder = $options.into_iter().fold(
            <$builder>::from_env().with_url($url.as_str()),
            |builder, (key, value)| match key.as_ref().parse() {
                Ok(k) => builder.with_config(k, value),
                Err(_) => builder,
            },
        );
        Ok(Box::new(builder.build()?))
    }};
}

/// Returns a [`ObjectStore`] for the provided `url`
pub fn parse_url(url: &Url) -> Result<Box<dyn ObjectStore>, super::Error> {
    parse_url_opts(url, std::iter::empty::<(&str, &str)>())
}

/// Returns a [`ObjectStore`] for the provided `url` and options
pub fn parse_url_opts<I, K, V>(
    url: &Url,
    options: I,
) -> Result<Box<dyn ObjectStore>, super::Error>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let _options = options;
    let scheme = ObjectStoreScheme::parse(url)?;

    match scheme {
        ObjectStoreScheme::Local => match url.path_segments().is_some() {
            true => {
                let path = url
                    .to_file_path()
                    .map_err(|_| Error::InvalidUrl { url: url.clone() })?;
                Ok(Box::new(LocalFileSystem::new_with_prefix(path)?))
            }
            false => Ok(Box::new(LocalFileSystem::new())),
        },
        ObjectStoreScheme::Memory => Ok(Box::new(InMemory::new())),
        #[cfg(feature = "aws")]
        ObjectStoreScheme::AmazonS3 => {
            builder_opts!(crate::gcp::GoogleCloudStorageBuilder, url, _options)
        }
        #[cfg(feature = "gcp")]
        ObjectStoreScheme::GoogleCloudStorage => {
            builder_opts!(crate::gcp::GoogleCloudStorageBuilder, url, _options)
        }
        #[cfg(feature = "azure")]
        ObjectStoreScheme::MicrosoftAzure => {
            builder_opts!(crate::azure::MicrosoftAzureBuilder, url, _options)
        }
        #[cfg(feature = "http")]
        ObjectStoreScheme::Http => Ok(Box::new(
            crate::http::HttpBuilder::new().with_url(url).build()?,
        )),
        #[cfg(not(all(
            feature = "aws",
            feature = "azure",
            feature = "gcp",
            feature = "http"
        )))]
        s => Err(super::Error::Generic {
            store: "parse_url",
            source: format!("feature for {s:?} not enabled").into(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_local_prefix() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("test.txt"), "test").unwrap();
        let url = Url::from_file_path(dir.path()).unwrap();
        let store = parse_url(&url).unwrap();
        let result = store.list_with_delimiter(None).await.unwrap();

        assert_eq!(result.objects.len(), 1);
        assert_eq!(result.objects[0].location, Path::from("test.txt"));
    }

    #[test]
    fn test_parse() {
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
            (
                "https://ACCOUNT_ID.r2.cloudflarestorage.com/bucket",
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
            assert_eq!(ObjectStoreScheme::parse(&url).unwrap(), expected);
        }

        let neg_cases = [
            "unix:/run/foo.socket",
            "file://remote/path",
            "memory://remote/",
        ];
        for s in neg_cases {
            let url = Url::parse(s).unwrap();
            assert!(ObjectStoreScheme::parse(&url).is_err());
        }
    }
}
