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

//! Implementation of `ObjectStoreBuilder` which allows the creation of object store
//! from provided url and options.
//!
use std::collections::HashMap;
pub use std::str::FromStr;
use url::Url;

#[cfg(feature = "aws")]
use crate::aws;
#[cfg(feature = "azure")]
use crate::azure;
#[cfg(any(feature = "gcp", feature = "aws", feature = "azure", feature = "http"))]
use crate::client::ClientOptions;
#[cfg(feature = "gcp")]
use crate::gcp;
#[cfg(feature = "http")]
use crate::http;
#[cfg(not(target_arch = "wasm32"))]
use crate::local;

use crate::{memory, DynObjectStore, Error, Result};

#[allow(dead_code)]
#[derive(Debug, Clone)]
/// Creates object store from provided url and options
///
/// The scheme of the provided url is used to instantiate the store. If the url
/// scheme is cannot be mapped to a store, [`NotImplemented`] is raised. For invalid
/// input, e.g. url with no scheme the default behaviour is to return
/// [`local::LocalFileSystem`].
///
/// # Examples
/// ```
/// # let BUCKET_NAME = "foo";
/// # let ACCESS_KEY_ID = "foo";
/// # let SECRET_KEY = "foo";
/// # use object_store::builder::ObjectStoreBuilder;///
///
/// // Instantiate Local FS
/// # let LOCAL_URL = "file:///";
/// let local = ObjectStoreBuilder::new(LOCAL_URL).build();
///
/// // Instantiate S3
/// # let S3_URL = "s3://foo/";
/// let s3_opts =
///    vec![
///       ("AWS_ACCESS_KEY_ID", "abc"),
///       ("AWS_SECRET_ACCESS_KEY", "xyz")
///    ];
/// let s3 = ObjectStoreBuilder::new(S3_URL).with_store_options(s3_opts).build();
///
/// // Instantiate Azure
/// # let AZURE_URL = "az://foo/";
/// let azure = ObjectStoreBuilder::new(AZURE_URL).with_env_variables(true).build();
///
/// // Instantiate GCS
/// # let GCS_URL = "gs://foo/";
/// let gs_opts = vec![("GOOGLE_SERVICE_ACCOUNT", "/tmp/foo.json")];
/// let gs = ObjectStoreBuilder::new(AZURE_URL)
///    .with_store_options(gs_opts)
///    .with_env_variables(true)
///    .build();
/// ```
pub struct ObjectStoreBuilder {
    /// URL of the store
    url: String,

    /// Store specific options like key, secret, region etc.
    store_options: HashMap<String, String>,

    /// Options for the internal client
    #[cfg(any(feature = "gcp", feature = "aws", feature = "azure", feature = "http"))]
    client_options: ClientOptions,

    /// Include store specific options like key, secret, region etc. from env
    from_env: bool,
}

#[allow(dead_code)]
impl ObjectStoreBuilder {
    /// Create an instance of `ObjectStoreBuilder` with provided url
    #[cfg(any(feature = "gcp", feature = "aws", feature = "azure", feature = "http"))]
    pub fn new(url: impl AsRef<str>) -> Self {
        Self {
            url: url.as_ref().to_string(),
            store_options: HashMap::new(),
            client_options: ClientOptions::default(),
            from_env: false,
        }
    }
    /// Create an instance of `ObjectStoreBuilder` with provided url
    #[cfg(not(any(
        feature = "gcp",
        feature = "aws",
        feature = "azure",
        feature = "http"
    )))]
    pub fn new(url: impl AsRef<str>) -> Self {
        Self {
            url: url.as_ref().to_string(),
            store_options: HashMap::new(),
            from_env: false,
        }
    }

    /// Allows this `ObjectStoreBuilder` to take provided store options into account
    pub fn with_store_options<
        I: IntoIterator<Item = (impl AsRef<str>, impl Into<String>)>,
    >(
        mut self,
        store_options: I,
    ) -> Self {
        self.store_options.extend(Self::iter_2_opts(store_options));
        self
    }

    /// Allows this `ObjectStoreBuilder` to take provided internal client options into account
    #[cfg(any(feature = "gcp", feature = "aws", feature = "azure", feature = "http"))]
    pub fn with_client_options(mut self, client_options: ClientOptions) -> Self {
        self.client_options = client_options;
        self
    }

    /// Allows this `ObjectStoreBuilder` to take environment variables into account
    pub fn with_env_variables(mut self, env: bool) -> Self {
        self.from_env = env;
        self
    }

    /// Gets an instance of ClientOptions
    #[cfg(any(feature = "gcp", feature = "aws", feature = "azure", feature = "http"))]
    fn get_client_options(&self) -> ClientOptions {
        self.client_options.clone()
    }

    /// Ensures that provided options are compatible with Azure
    #[cfg(feature = "azure")]
    fn get_azure_options(&self) -> Result<HashMap<azure::AzureConfigKey, String>> {
        let mut opts = HashMap::new();
        for (key, value) in &self.store_options {
            let conf_key = azure::AzureConfigKey::from_str(&key.to_ascii_lowercase())?;
            opts.insert(conf_key, value.clone());
        }

        Ok(opts)
    }

    /// Ensures that provided options are compatible with S3
    #[cfg(feature = "aws")]
    fn get_s3_options(&self) -> Result<HashMap<aws::AmazonS3ConfigKey, String>> {
        let mut opts = HashMap::new();
        for (key, value) in &self.store_options {
            let conf_key = aws::AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase())?;
            opts.insert(conf_key, value.clone());
        }

        Ok(opts)
    }

    /// Ensures that provided options are compatible with GCS
    #[cfg(feature = "gcp")]
    fn get_gcs_options(&self) -> Result<HashMap<gcp::GoogleConfigKey, String>> {
        let mut opts = HashMap::new();
        for (key, value) in &self.store_options {
            let conf_key = gcp::GoogleConfigKey::from_str(&key.to_ascii_lowercase())?;
            opts.insert(conf_key, value.clone());
        }

        Ok(opts)
    }

    fn iter_2_opts<I: IntoIterator<Item = (impl AsRef<str>, impl Into<String>)>>(
        iterable: I,
    ) -> HashMap<String, String> {
        iterable
            .into_iter()
            .map(|(key, value)| (key.as_ref().to_ascii_lowercase(), value.into()))
            .collect()
    }

    /// Creates a boxed `DynObjectStore` from provided url and options (if any)
    pub fn build(self) -> Result<Box<DynObjectStore>> {
        let storage_url = self.url.as_ref();

        match Url::parse(storage_url) {
            Ok(url) => match url.scheme() {
                #[cfg(any(feature = "aws", feature = "aws_profile"))]
                "s3" | "s3a" => {
                    let store = if self.from_env {
                        aws::AmazonS3Builder::from_env()
                    } else {
                        aws::AmazonS3Builder::default()
                    }
                    .with_url(storage_url)
                    .with_client_options(self.get_client_options())
                    .try_with_options(self.get_s3_options()?)?
                    .build()?;

                    Ok(Box::from(store))
                }

                #[cfg(not(any(feature = "aws", feature = "aws_profile")))]
                "s3" | "s3a" => Err(Error::MissingFeature {
                    feature: "aws",
                    url: storage_url.into(),
                }),

                #[cfg(feature = "gcp")]
                "gs" => {
                    let store = if self.from_env {
                        gcp::GoogleCloudStorageBuilder::from_env()
                    } else {
                        gcp::GoogleCloudStorageBuilder::default()
                    }
                    .with_url(storage_url)
                    .with_client_options(self.get_client_options())
                    .try_with_options(self.get_gcs_options()?)?
                    .build()?;

                    Ok(Box::from(store))
                }

                #[cfg(not(feature = "gcp"))]
                "gs" => Err(Error::MissingFeature {
                    feature: "gcp",
                    url: storage_url.into(),
                }),

                #[cfg(feature = "azure")]
                "az" | "adl" | "azure" | "abfs" | "abfss" => {
                    let store = if self.from_env {
                        azure::MicrosoftAzureBuilder::from_env()
                    } else {
                        azure::MicrosoftAzureBuilder::default()
                    }
                    .with_url(storage_url)
                    .with_client_options(self.get_client_options())
                    .try_with_options(self.get_azure_options()?)?
                    .build()?;

                    Ok(Box::from(store))
                }

                #[cfg(not(feature = "azure"))]
                "az" | "adl" | "azure" | "abfs" | "abfss" => Err(Error::MissingFeature {
                    feature: "azure",
                    url: storage_url.into(),
                }),

                #[cfg(feature = "http")]
                "http" | "https" => {
                    let store = http::HttpBuilder::default()
                        .with_url(url.as_ref())
                        .with_client_options(self.client_options)
                        .build()?;

                    Ok(Box::from(store))
                }

                #[cfg(not(feature = "http"))]
                "http" | "https" => Err(Error::MissingFeature {
                    feature: "http",
                    url: storage_url.into(),
                }),

                "memory" => Ok(Box::from(memory::InMemory::default())),

                #[cfg(not(target_arch = "wasm32"))]
                "file" => {
                    let path = url.path();
                    let store = local::LocalFileSystem::new_with_prefix(path)?;

                    Ok(Box::from(store))
                }

                _ => Err(Error::NotImplemented),
            },

            Err(e) => Err(Error::NotSupported {
                source: Box::new(e),
            }),
        }
    }
}
