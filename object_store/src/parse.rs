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

#[cfg(not(target_arch = "wasm32"))]
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

    #[snafu(context(false))]
    Path { source: crate::path::Error },
}

impl From<Error> for super::Error {
    fn from(e: Error) -> Self {
        Self::Generic {
            store: "URL",
            source: Box::new(e),
        }
    }
}

/// Recognises various URL formats, identifying the relevant [`ObjectStore`]
#[derive(Debug, Eq, PartialEq)]
enum ObjectStoreScheme {
    /// Url corresponding to [`LocalFileSystem`]
    Local,
    /// Url corresponding to [`InMemory`]
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
    ///
    /// Returns the [`ObjectStoreScheme`] and the remaining [`Path`]
    fn parse(url: &Url) -> Result<(Self, Path), Error> {
        let strip_bucket = || Some(url.path().strip_prefix('/')?.split_once('/')?.1);

        let (scheme, path) = match (url.scheme(), url.host_str()) {
            ("file", None) => (Self::Local, url.path()),
            ("memory", None) => (Self::Memory, url.path()),
            ("s3" | "s3a", Some(_)) => (Self::AmazonS3, url.path()),
            ("gs", Some(_)) => (Self::GoogleCloudStorage, url.path()),
            ("az" | "adl" | "azure" | "abfs" | "abfss", Some(_)) => {
                (Self::MicrosoftAzure, url.path())
            }
            ("http", Some(_)) => (Self::Http, url.path()),
            ("https", Some(host)) => {
                if host.ends_with("dfs.core.windows.net")
                    || host.ends_with("blob.core.windows.net")
                {
                    (Self::MicrosoftAzure, url.path())
                } else if host.ends_with("amazonaws.com") {
                    match host.starts_with("s3") {
                        true => (Self::AmazonS3, strip_bucket().unwrap_or_default()),
                        false => (Self::AmazonS3, url.path()),
                    }
                } else if host.ends_with("r2.cloudflarestorage.com") {
                    (Self::AmazonS3, strip_bucket().unwrap_or_default())
                } else {
                    (Self::Http, url.path())
                }
            }
            _ => return Err(Error::Unrecognised { url: url.clone() }),
        };

        let path = Path::parse(path)?;
        Ok((scheme, path))
    }
}

#[cfg(any(feature = "aws", feature = "gcp", feature = "azure"))]
macro_rules! builder_opts {
    ($builder:ty, $url:expr, $options:expr) => {{
        let builder = $options.into_iter().fold(
            <$builder>::new().with_url($url.as_str()),
            |builder, (key, value)| match key.as_ref().parse() {
                Ok(k) => builder.with_config(k, value),
                Err(_) => builder,
            },
        );
        Box::new(builder.build()?) as _
    }};
}

/// Create an [`ObjectStore`] based on the provided `url`
///
/// Returns
/// - An [`ObjectStore`] of the corresponding type
/// - The [`Path`] into the [`ObjectStore`] of the addressed resource
pub fn parse_url(url: &Url) -> Result<(Box<dyn ObjectStore>, Path), super::Error> {
    parse_url_opts(url, std::iter::empty::<(&str, &str)>())
}

/// Create an [`ObjectStore`] based on the provided `url` and options
///
/// Returns
/// - An [`ObjectStore`] of the corresponding type
/// - The [`Path`] into the [`ObjectStore`] of the addressed resource
pub fn parse_url_opts<I, K, V>(
    url: &Url,
    options: I,
) -> Result<(Box<dyn ObjectStore>, Path), super::Error>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<str>,
    V: Into<String>,
{
    let _options = options;
    let (scheme, path) = ObjectStoreScheme::parse(url)?;
    let path = Path::parse(path)?;

    let store = match scheme {
        #[cfg(not(target_arch = "wasm32"))]
        ObjectStoreScheme::Local => Box::new(LocalFileSystem::new()) as _,
        ObjectStoreScheme::Memory => Box::new(InMemory::new()) as _,
        #[cfg(feature = "aws")]
        ObjectStoreScheme::AmazonS3 => {
            builder_opts!(crate::aws::AmazonS3Builder, url, _options)
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
        ObjectStoreScheme::Http => {
            let url = &url[..url::Position::BeforePath];
            Box::new(crate::http::HttpBuilder::new().with_url(url).build()?) as _
        }
        #[cfg(not(all(
            feature = "aws",
            feature = "azure",
            feature = "gcp",
            feature = "http"
        )))]
        s => {
            return Err(super::Error::Generic {
                store: "parse_url",
                source: format!("feature for {s:?} not enabled").into(),
            })
        }
    };

    Ok((store, path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse() {
        let cases = [
            ("file:/path", (ObjectStoreScheme::Local, "path")),
            ("file:///path", (ObjectStoreScheme::Local, "path")),
            ("memory:/path", (ObjectStoreScheme::Memory, "path")),
            ("memory:///", (ObjectStoreScheme::Memory, "")),
            ("s3://bucket/path", (ObjectStoreScheme::AmazonS3, "path")),
            ("s3a://bucket/path", (ObjectStoreScheme::AmazonS3, "path")),
            (
                "https://s3.region.amazonaws.com/bucket",
                (ObjectStoreScheme::AmazonS3, ""),
            ),
            (
                "https://s3.region.amazonaws.com/bucket/path",
                (ObjectStoreScheme::AmazonS3, "path"),
            ),
            (
                "https://bucket.s3.region.amazonaws.com",
                (ObjectStoreScheme::AmazonS3, ""),
            ),
            (
                "https://ACCOUNT_ID.r2.cloudflarestorage.com/bucket",
                (ObjectStoreScheme::AmazonS3, ""),
            ),
            (
                "https://ACCOUNT_ID.r2.cloudflarestorage.com/bucket/path",
                (ObjectStoreScheme::AmazonS3, "path"),
            ),
            (
                "abfs://container/path",
                (ObjectStoreScheme::MicrosoftAzure, "path"),
            ),
            (
                "abfs://file_system@account_name.dfs.core.windows.net/path",
                (ObjectStoreScheme::MicrosoftAzure, "path"),
            ),
            (
                "abfss://file_system@account_name.dfs.core.windows.net/path",
                (ObjectStoreScheme::MicrosoftAzure, "path"),
            ),
            (
                "https://account.dfs.core.windows.net",
                (ObjectStoreScheme::MicrosoftAzure, ""),
            ),
            (
                "https://account.blob.core.windows.net",
                (ObjectStoreScheme::MicrosoftAzure, ""),
            ),
            (
                "gs://bucket/path",
                (ObjectStoreScheme::GoogleCloudStorage, "path"),
            ),
            ("http://mydomain/path", (ObjectStoreScheme::Http, "path")),
            ("https://mydomain/path", (ObjectStoreScheme::Http, "path")),
        ];

        for (s, (expected_scheme, expected_path)) in cases {
            let url = Url::parse(s).unwrap();
            let (scheme, path) = ObjectStoreScheme::parse(&url).unwrap();

            assert_eq!(scheme, expected_scheme, "{s}");
            assert_eq!(path, Path::parse(expected_path).unwrap(), "{s}");
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
