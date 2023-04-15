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

use std::collections::HashMap;
pub use std::str::FromStr;

#[cfg(any(feature = "gcp", feature = "aws", feature = "azure", feature = "http"))]
use crate::client::ClientOptions;

#[cfg(any(feature = "aws"))]
use crate::aws::AmazonS3ConfigKey;
#[cfg(feature = "azure")]
use crate::azure::AzureConfigKey;
#[cfg(feature = "gcp")]
use crate::gcp::GoogleConfigKey;

/// Options used for configuring backend store
#[derive(Clone, Debug, Default)]
pub struct StoreOptions {
    /// Store specific options like key, secret, region etc.
    _store_options: HashMap<String, String>,

    /// Options specific for the internal client
    #[cfg(any(feature = "gcp", feature = "aws", feature = "azure", feature = "http"))]
    client_options: ClientOptions,
}

impl StoreOptions {
    /// Create a new instance of [`StorageOptions`]
    #[cfg(any(feature = "gcp", feature = "aws", feature = "azure", feature = "http"))]
    pub fn new(
        store_options: HashMap<String, String>,
        client_options: ClientOptions,
    ) -> Self {
        Self {
            _store_options: store_options,
            client_options,
        }
    }

    #[cfg(not(any(
        feature = "gcp",
        feature = "aws",
        feature = "azure",
        feature = "http"
    )))]
    pub fn new(store_options: HashMap<String, String>) -> Self {
        Self { _store_options: store_options }
    }

    /// Gets an instance of ClientOptions
    #[cfg(any(feature = "gcp", feature = "aws", feature = "azure", feature = "http"))]
    pub fn get_client_options(&self) -> ClientOptions {
        self.client_options.clone()
    }

    /// Ensures that provided options are compatible with Azure
    #[cfg(feature = "azure")]
    pub fn get_azure_options(&self) -> HashMap<AzureConfigKey, String> {
        self._store_options
            .iter()
            .map(|(key, value)| {
                let conf_key =
                    AzureConfigKey::from_str(&key.to_ascii_lowercase()).unwrap();

                (conf_key, value.clone())
            })
            .collect()
    }

    /// Ensures that provided options are compatible with S3
    #[cfg(feature = "aws")]
    pub fn get_s3_options(&self) -> HashMap<AmazonS3ConfigKey, String> {
        self._store_options
            .iter()
            .map(|(key, value)| {
                let conf_key =
                    AmazonS3ConfigKey::from_str(&key.to_ascii_lowercase()).unwrap();

                (conf_key, value.clone())
            })
            .collect()
    }

    /// Ensures that provided options are compatible with GCS
    #[cfg(feature = "gcp")]
    pub fn get_gcs_options(&self) -> HashMap<GoogleConfigKey, String> {
        self._store_options
            .iter()
            .map(|(key, value)| {
                let conf_key =
                    GoogleConfigKey::from_str(&key.to_ascii_lowercase()).unwrap();

                (conf_key, value.clone())
            })
            .collect()
    }
}

impl From<HashMap<String, String>> for StoreOptions {
    #[cfg(any(feature = "gcp", feature = "aws", feature = "azure", feature = "http"))]
    fn from(value: HashMap<String, String>) -> Self {
        Self::new(value, ClientOptions::default())
    }

    #[cfg(not(any(
        feature = "gcp",
        feature = "aws",
        feature = "azure",
        feature = "http"
    )))]
    fn from(value: HashMap<String, String>) -> Self {
        Self::new(value)
    }
}
