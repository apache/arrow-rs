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

//! An object store implementation for Google Cloud Storage
use std::collections::BTreeSet;
use std::fs::File;
use std::io::BufReader;
use std::ops::Range;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{stream::BoxStream, StreamExt, TryStreamExt};
use percent_encoding::{percent_encode, NON_ALPHANUMERIC};
use reqwest::header::RANGE;
use reqwest::{header, Client, Method, Response, StatusCode};
use snafu::{ResultExt, Snafu};

use crate::util::format_http_range;
use crate::{
    oauth::OAuthProvider,
    path::{Path, DELIMITER},
    token::TokenCache,
    util::format_prefix,
    GetResult, ListResult, ObjectMeta, ObjectStore, Result,
};

#[derive(Debug, Snafu)]
enum Error {
    #[snafu(display("Unable to open service account file: {}", source))]
    OpenCredentials { source: std::io::Error },

    #[snafu(display("Unable to decode service account file: {}", source))]
    DecodeCredentials { source: serde_json::Error },

    #[snafu(display("Error performing list request: {}", source))]
    ListRequest { source: reqwest::Error },

    #[snafu(display("Error performing get request {}: {}", path, source))]
    GetRequest {
        source: reqwest::Error,
        path: String,
    },

    #[snafu(display("Error performing delete request {}: {}", path, source))]
    DeleteRequest {
        source: reqwest::Error,
        path: String,
    },

    #[snafu(display("Error performing copy request {}: {}", path, source))]
    CopyRequest {
        source: reqwest::Error,
        path: String,
    },

    #[snafu(display("Error performing put request: {}", source))]
    PutRequest { source: reqwest::Error },

    #[snafu(display("Error decoding object size: {}", source))]
    InvalidSize { source: std::num::ParseIntError },
}

impl From<Error> for super::Error {
    fn from(err: Error) -> Self {
        match err {
            Error::GetRequest { source, path }
            | Error::DeleteRequest { source, path }
            | Error::CopyRequest { source, path }
                if matches!(source.status(), Some(StatusCode::NOT_FOUND)) =>
            {
                Self::NotFound {
                    path,
                    source: Box::new(source),
                }
            }
            _ => Self::Generic {
                store: "GCS",
                source: Box::new(err),
            },
        }
    }
}

/// A deserialized `service-account-********.json`-file.
#[derive(serde::Deserialize, Debug)]
struct ServiceAccountCredentials {
    /// The private key in RSA format.
    pub private_key: String,

    /// The email address associated with the service account.
    pub client_email: String,

    /// Base URL for GCS
    #[serde(default = "default_gcs_base_url")]
    pub gcs_base_url: String,

    /// Disable oauth and use empty tokens.
    #[serde(default = "default_disable_oauth")]
    pub disable_oauth: bool,
}

fn default_gcs_base_url() -> String {
    "https://storage.googleapis.com".to_owned()
}

fn default_disable_oauth() -> bool {
    false
}

#[derive(serde::Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct ListResponse {
    next_page_token: Option<String>,
    #[serde(default)]
    prefixes: Vec<String>,
    #[serde(default)]
    items: Vec<Object>,
}

#[derive(serde::Deserialize, Debug)]
struct Object {
    name: String,
    size: String,
    updated: DateTime<Utc>,
}

/// Configuration for connecting to [Google Cloud Storage](https://cloud.google.com/storage/).
#[derive(Debug)]
pub struct GoogleCloudStorage {
    client: Client,
    base_url: String,

    oauth_provider: Option<OAuthProvider>,
    token_cache: TokenCache<String>,

    bucket_name: String,
    bucket_name_encoded: String,

    // TODO: Hook this up in tests
    max_list_results: Option<String>,
}

impl std::fmt::Display for GoogleCloudStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GoogleCloudStorage({})", self.bucket_name)
    }
}

impl GoogleCloudStorage {
    async fn get_token(&self) -> Result<String> {
        if let Some(oauth_provider) = &self.oauth_provider {
            Ok(self
                .token_cache
                .get_or_insert_with(|| oauth_provider.fetch_token(&self.client))
                .await?)
        } else {
            Ok("".to_owned())
        }
    }

    fn object_url(&self, path: &Path) -> String {
        let encoded =
            percent_encoding::utf8_percent_encode(path.as_ref(), NON_ALPHANUMERIC);
        format!(
            "{}/storage/v1/b/{}/o/{}",
            self.base_url, self.bucket_name_encoded, encoded
        )
    }

    /// Perform a get request <https://cloud.google.com/storage/docs/json_api/v1/objects/get>
    async fn get_request(
        &self,
        path: &Path,
        range: Option<Range<usize>>,
        head: bool,
    ) -> Result<Response> {
        let token = self.get_token().await?;
        let url = self.object_url(path);

        let mut builder = self.client.request(Method::GET, url);

        if let Some(range) = range {
            builder = builder.header(RANGE, format_http_range(range));
        }

        let alt = match head {
            true => "json",
            false => "media",
        };

        let response = builder
            .bearer_auth(token)
            .query(&[("alt", alt)])
            .send()
            .await
            .context(GetRequestSnafu {
                path: path.as_ref(),
            })?
            .error_for_status()
            .context(GetRequestSnafu {
                path: path.as_ref(),
            })?;

        Ok(response)
    }

    /// Perform a put request <https://cloud.google.com/storage/docs/json_api/v1/objects/insert>
    async fn put_request(&self, path: &Path, payload: Bytes) -> Result<()> {
        let token = self.get_token().await?;
        let url = format!(
            "{}/upload/storage/v1/b/{}/o",
            self.base_url, self.bucket_name_encoded
        );

        self.client
            .request(Method::POST, url)
            .bearer_auth(token)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::CONTENT_LENGTH, payload.len())
            .query(&[("uploadType", "media"), ("name", path.as_ref())])
            .body(payload)
            .send()
            .await
            .context(PutRequestSnafu)?
            .error_for_status()
            .context(PutRequestSnafu)?;

        Ok(())
    }

    /// Perform a delete request <https://cloud.google.com/storage/docs/json_api/v1/objects/delete>
    async fn delete_request(&self, path: &Path) -> Result<()> {
        let token = self.get_token().await?;
        let url = self.object_url(path);

        let builder = self.client.request(Method::DELETE, url);
        builder
            .bearer_auth(token)
            .send()
            .await
            .context(DeleteRequestSnafu {
                path: path.as_ref(),
            })?
            .error_for_status()
            .context(DeleteRequestSnafu {
                path: path.as_ref(),
            })?;

        Ok(())
    }

    /// Perform a copy request <https://cloud.google.com/storage/docs/json_api/v1/objects/copy>
    async fn copy_request(
        &self,
        from: &Path,
        to: &Path,
        if_not_exists: bool,
    ) -> Result<()> {
        let token = self.get_token().await?;

        let source =
            percent_encoding::utf8_percent_encode(from.as_ref(), NON_ALPHANUMERIC);
        let destination =
            percent_encoding::utf8_percent_encode(to.as_ref(), NON_ALPHANUMERIC);
        let url = format!(
            "{}/storage/v1/b/{}/o/{}/copyTo/b/{}/o/{}",
            self.base_url,
            self.bucket_name_encoded,
            source,
            self.bucket_name_encoded,
            destination
        );

        let mut builder = self.client.request(Method::POST, url);

        if if_not_exists {
            builder = builder.query(&[("ifGenerationMatch", "0")]);
        }

        builder
            .bearer_auth(token)
            .send()
            .await
            .context(CopyRequestSnafu {
                path: from.as_ref(),
            })?
            .error_for_status()
            .context(CopyRequestSnafu {
                path: from.as_ref(),
            })?;

        Ok(())
    }

    /// Perform a list request <https://cloud.google.com/storage/docs/json_api/v1/objects/list>
    async fn list_request(
        &self,
        prefix: Option<&str>,
        delimiter: bool,
        page_token: Option<&str>,
    ) -> Result<ListResponse> {
        let token = self.get_token().await?;

        let url = format!(
            "{}/storage/v1/b/{}/o",
            self.base_url, self.bucket_name_encoded
        );

        let mut query = Vec::with_capacity(4);
        if delimiter {
            query.push(("delimiter", DELIMITER))
        }

        if let Some(prefix) = &prefix {
            query.push(("prefix", prefix))
        }

        if let Some(page_token) = page_token {
            query.push(("pageToken", page_token))
        }

        if let Some(max_results) = &self.max_list_results {
            query.push(("maxResults", max_results))
        }

        let response: ListResponse = self
            .client
            .request(Method::GET, url)
            .query(&query)
            .bearer_auth(token)
            .send()
            .await
            .context(ListRequestSnafu)?
            .error_for_status()
            .context(ListRequestSnafu)?
            .json()
            .await
            .context(ListRequestSnafu)?;

        Ok(response)
    }

    /// Perform a list operation automatically handling pagination
    fn list_paginated(
        &self,
        prefix: Option<&Path>,
        delimiter: bool,
    ) -> Result<BoxStream<'_, Result<ListResponse>>> {
        let prefix = format_prefix(prefix);

        enum ListState {
            Start,
            HasMore(String),
            Done,
        }

        Ok(futures::stream::unfold(ListState::Start, move |state| {
            let prefix = prefix.clone();

            async move {
                let page_token = match &state {
                    ListState::Start => None,
                    ListState::HasMore(page_token) => Some(page_token.as_str()),
                    ListState::Done => {
                        return None;
                    }
                };

                let resp = match self
                    .list_request(prefix.as_deref(), delimiter, page_token)
                    .await
                {
                    Ok(resp) => resp,
                    Err(e) => return Some((Err(e), state)),
                };

                let next_state = match &resp.next_page_token {
                    Some(token) => ListState::HasMore(token.clone()),
                    None => ListState::Done,
                };

                Some((Ok(resp), next_state))
            }
        })
        .boxed())
    }
}

#[async_trait]
impl ObjectStore for GoogleCloudStorage {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        self.put_request(location, bytes).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let response = self.get_request(location, None, false).await?;
        let stream = response
            .bytes_stream()
            .map_err(|source| crate::Error::Generic {
                store: "GCS",
                source: Box::new(source),
            })
            .boxed();

        Ok(GetResult::Stream(stream))
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let response = self.get_request(location, Some(range), false).await?;
        Ok(response.bytes().await.context(GetRequestSnafu {
            path: location.as_ref(),
        })?)
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let response = self.get_request(location, None, true).await?;
        let object = response.json().await.context(GetRequestSnafu {
            path: location.as_ref(),
        })?;
        convert_object_meta(&object)
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        self.delete_request(location).await
    }

    async fn list(
        &self,
        prefix: Option<&Path>,
    ) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        let stream = self
            .list_paginated(prefix, false)?
            .map_ok(|r| {
                futures::stream::iter(
                    r.items.into_iter().map(|x| convert_object_meta(&x)),
                )
            })
            .try_flatten()
            .boxed();

        Ok(stream)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let mut stream = self.list_paginated(prefix, true)?;

        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();

        while let Some(result) = stream.next().await {
            let response = result?;

            for p in response.prefixes {
                common_prefixes.insert(Path::parse(p)?);
            }

            objects.reserve(response.items.len());
            for object in &response.items {
                objects.push(convert_object_meta(object)?);
            }
        }

        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        self.copy_request(from, to, false).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.copy_request(from, to, true).await
    }
}

fn reader_credentials_file(
    service_account_path: impl AsRef<std::path::Path>,
) -> Result<ServiceAccountCredentials> {
    let file = File::open(service_account_path).context(OpenCredentialsSnafu)?;
    let reader = BufReader::new(file);
    Ok(serde_json::from_reader(reader).context(DecodeCredentialsSnafu)?)
}

/// Configure a connection to Google Cloud Storage.
pub fn new_gcs(
    service_account_path: impl AsRef<std::path::Path>,
    bucket_name: impl Into<String>,
) -> Result<GoogleCloudStorage> {
    new_gcs_with_client(service_account_path, bucket_name, Client::new())
}

/// Configure a connection to Google Cloud Storage with the specified HTTP client.
pub fn new_gcs_with_client(
    service_account_path: impl AsRef<std::path::Path>,
    bucket_name: impl Into<String>,
    client: Client,
) -> Result<GoogleCloudStorage> {
    let credentials = reader_credentials_file(service_account_path)?;

    // TODO: https://cloud.google.com/storage/docs/authentication#oauth-scopes
    let scope = "https://www.googleapis.com/auth/devstorage.full_control";
    let audience = "https://www.googleapis.com/oauth2/v4/token".to_string();

    let oauth_provider = (!credentials.disable_oauth)
        .then(|| {
            OAuthProvider::new(
                credentials.client_email,
                credentials.private_key,
                scope.to_string(),
                audience,
            )
        })
        .transpose()?;

    let bucket_name = bucket_name.into();
    let encoded_bucket_name =
        percent_encode(bucket_name.as_bytes(), NON_ALPHANUMERIC).to_string();

    // The cloud storage crate currently only supports authentication via
    // environment variables. Set the environment variable explicitly so
    // that we can optionally accept command line arguments instead.
    Ok(GoogleCloudStorage {
        client,
        base_url: credentials.gcs_base_url,
        oauth_provider,
        token_cache: Default::default(),
        bucket_name,
        bucket_name_encoded: encoded_bucket_name,
        max_list_results: None,
    })
}

fn convert_object_meta(object: &Object) -> Result<ObjectMeta> {
    let location = Path::parse(&object.name)?;
    let last_modified = object.updated;
    let size = object.size.parse().context(InvalidSizeSnafu)?;

    Ok(ObjectMeta {
        location,
        last_modified,
        size,
    })
}

#[cfg(test)]
mod test {
    use std::env;

    use bytes::Bytes;

    use crate::{
        tests::{
            get_nonexistent_object, list_uses_directories_correctly, list_with_delimiter,
            put_get_delete_list, rename_and_copy,
        },
        Error as ObjectStoreError, ObjectStore,
    };

    use super::*;

    const NON_EXISTENT_NAME: &str = "nonexistentname";

    #[derive(Debug)]
    struct GoogleCloudConfig {
        bucket: String,
        service_account: String,
    }

    impl GoogleCloudConfig {
        fn build_test(self) -> Result<GoogleCloudStorage> {
            // ignore HTTPS errors in tests so we can use fake-gcs server
            let client = Client::builder()
                .danger_accept_invalid_certs(true)
                .build()
                .expect("Error creating http client for testing");

            new_gcs_with_client(self.service_account, self.bucket, client)
        }
    }

    // Helper macro to skip tests if TEST_INTEGRATION and the GCP environment variables are not set.
    macro_rules! maybe_skip_integration {
        () => {{
            dotenv::dotenv().ok();

            let required_vars = ["OBJECT_STORE_BUCKET", "GOOGLE_SERVICE_ACCOUNT"];
            let unset_vars: Vec<_> = required_vars
                .iter()
                .filter_map(|&name| match env::var(name) {
                    Ok(_) => None,
                    Err(_) => Some(name),
                })
                .collect();
            let unset_var_names = unset_vars.join(", ");

            let force = std::env::var("TEST_INTEGRATION");

            if force.is_ok() && !unset_var_names.is_empty() {
                panic!(
                    "TEST_INTEGRATION is set, \
                            but variable(s) {} need to be set",
                    unset_var_names
                )
            } else if force.is_err() {
                eprintln!(
                    "skipping Google Cloud integration test - set {}TEST_INTEGRATION to run",
                    if unset_var_names.is_empty() {
                        String::new()
                    } else {
                        format!("{} and ", unset_var_names)
                    }
                );
                return;
            } else {
                GoogleCloudConfig {
                    bucket: env::var("OBJECT_STORE_BUCKET")
                        .expect("already checked OBJECT_STORE_BUCKET"),
                    service_account: env::var("GOOGLE_SERVICE_ACCOUNT")
                        .expect("already checked GOOGLE_SERVICE_ACCOUNT"),
                }
            }
        }};
    }

    #[tokio::test]
    async fn gcs_test() {
        let config = maybe_skip_integration!();
        let integration = config.build().unwrap();

        put_get_delete_list(&integration).await.unwrap();
        list_uses_directories_correctly(&integration).await.unwrap();
        list_with_delimiter(&integration).await.unwrap();
        rename_and_copy(&integration).await.unwrap();
    }

    #[tokio::test]
    async fn gcs_test_get_nonexistent_location() {
        let config = maybe_skip_integration!();
        let integration = config.build().unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = integration.get(&location).await.unwrap_err();

        assert!(
            matches!(err, ObjectStoreError::NotFound { .. }),
            "unexpected error type: {}",
            err
        );
    }

    #[tokio::test]
    async fn gcs_test_get_nonexistent_bucket() {
        let mut config = maybe_skip_integration!();
        config.bucket = NON_EXISTENT_NAME.into();
        let integration = config.build().unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = get_nonexistent_object(&integration, Some(location))
            .await
            .unwrap_err();

        assert!(
            matches!(err, ObjectStoreError::NotFound { .. }),
            "unexpected error type: {}",
            err
        );
    }

    #[tokio::test]
    async fn gcs_test_delete_nonexistent_location() {
        let config = maybe_skip_integration!();
        let integration = config.build().unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = integration.delete(&location).await.unwrap_err();
        assert!(
            matches!(err, ObjectStoreError::NotFound { .. }),
            "unexpected error type: {}",
            err
        );
    }

    #[tokio::test]
    async fn gcs_test_delete_nonexistent_bucket() {
        let mut config = maybe_skip_integration!();
        config.bucket = NON_EXISTENT_NAME.into();
        let integration = config.build().unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = integration.delete(&location).await.unwrap_err();
        assert!(
            matches!(err, ObjectStoreError::NotFound { .. }),
            "unexpected error type: {}",
            err
        );
    }

    #[tokio::test]
    async fn gcs_test_put_nonexistent_bucket() {
        let mut config = maybe_skip_integration!();
        config.bucket = NON_EXISTENT_NAME.into();
        let integration = config.build().unwrap();

        let location = Path::from_iter([NON_EXISTENT_NAME]);
        let data = Bytes::from("arbitrary data");

        let err = integration
            .put(&location, data)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(
                "Error performing put request: HTTP status client error (404 Not Found)"
            ),
            "{}",
            err
        )
    }
}
