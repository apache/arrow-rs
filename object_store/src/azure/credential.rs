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

use crate::client::retry::RetryExt;
use crate::client::token::{TemporaryToken, TokenCache};
use crate::util::hmac_sha256;
use crate::RetryConfig;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use chrono::Utc;
use reqwest::header::ACCEPT;
use reqwest::{
    header::{
        HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_ENCODING,
        CONTENT_LANGUAGE, CONTENT_LENGTH, CONTENT_TYPE, DATE, IF_MATCH,
        IF_MODIFIED_SINCE, IF_NONE_MATCH, IF_UNMODIFIED_SINCE, RANGE,
    },
    Client, Method, RequestBuilder,
};
use serde::Deserialize;
use snafu::{ResultExt, Snafu};
use std::borrow::Cow;
use std::str;
use std::time::{Duration, Instant};
use url::Url;

static AZURE_VERSION: HeaderValue = HeaderValue::from_static("2021-08-06");
static VERSION: HeaderName = HeaderName::from_static("x-ms-version");
pub(crate) static BLOB_TYPE: HeaderName = HeaderName::from_static("x-ms-blob-type");
pub(crate) static DELETE_SNAPSHOTS: HeaderName =
    HeaderName::from_static("x-ms-delete-snapshots");
pub(crate) static COPY_SOURCE: HeaderName = HeaderName::from_static("x-ms-copy-source");
static CONTENT_MD5: HeaderName = HeaderName::from_static("content-md5");
pub(crate) const RFC1123_FMT: &str = "%a, %d %h %Y %T GMT";
const CONTENT_TYPE_JSON: &str = "application/json";
const MSI_SECRET_ENV_KEY: &str = "IDENTITY_HEADER";
const MSI_API_VERSION: &str = "2019-08-01";
const AZURE_STORAGE_SCOPE: &str = "https://storage.azure.com/.default";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error performing token request: {}", source))]
    TokenRequest { source: crate::client::retry::Error },

    #[snafu(display("Error getting token response body: {}", source))]
    TokenResponseBody { source: reqwest::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Provides credentials for use when signing requests
#[derive(Debug)]
pub enum CredentialProvider {
    AccessKey(String),
    SASToken(Vec<(String, String)>),
    TokenCredential(Box<dyn TokenCredential>),
}

pub(crate) enum AzureCredential {
    AccessKey(String),
    SASToken(Vec<(String, String)>),
    AuthorizationToken(HeaderValue),
}

/// A list of known Azure authority hosts
pub mod authority_hosts {
    /// China-based Azure Authority Host
    pub const AZURE_CHINA: &str = "https://login.chinacloudapi.cn";
    /// Germany-based Azure Authority Host
    pub const AZURE_GERMANY: &str = "https://login.microsoftonline.de";
    /// US Government Azure Authority Host
    pub const AZURE_GOVERNMENT: &str = "https://login.microsoftonline.us";
    /// Public Cloud Azure Authority Host
    pub const AZURE_PUBLIC_CLOUD: &str = "https://login.microsoftonline.com";
}

pub(crate) trait CredentialExt {
    /// Apply authorization to requests against azure storage accounts
    /// <https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-requests-to-azure-storage>
    fn with_azure_authorization(
        self,
        credential: &AzureCredential,
        account: &str,
    ) -> Self;
}

impl CredentialExt for RequestBuilder {
    fn with_azure_authorization(
        mut self,
        credential: &AzureCredential,
        account: &str,
    ) -> Self {
        // rfc2822 string should never contain illegal characters
        let date = Utc::now();
        let date_str = date.format(RFC1123_FMT).to_string();
        // we formatted the data string ourselves, so unwrapping should be fine
        let date_val = HeaderValue::from_str(&date_str).unwrap();
        self = self
            .header(DATE, &date_val)
            .header(&VERSION, &AZURE_VERSION);

        // Hack around lack of access to underlying request
        // https://github.com/seanmonstar/reqwest/issues/1212
        let request = self
            .try_clone()
            .expect("not stream")
            .build()
            .expect("request valid");

        match credential {
            AzureCredential::AccessKey(key) => {
                let signature = generate_authorization(
                    request.headers(),
                    request.url(),
                    request.method(),
                    account,
                    key.as_str(),
                );
                self = self
                    // "signature" is a base 64 encoded string so it should never contain illegal characters.
                    .header(
                        AUTHORIZATION,
                        HeaderValue::from_str(signature.as_str()).unwrap(),
                    );
            }
            AzureCredential::AuthorizationToken(token) => {
                self = self.header(AUTHORIZATION, token);
            }
            AzureCredential::SASToken(query_pairs) => {
                self = self.query(&query_pairs);
            }
        };

        self
    }
}

/// Generate signed key for authorization via access keys
/// <https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key>
fn generate_authorization(
    h: &HeaderMap,
    u: &Url,
    method: &Method,
    account: &str,
    key: &str,
) -> String {
    let str_to_sign = string_to_sign(h, u, method, account);
    let auth = hmac_sha256(BASE64_STANDARD.decode(key).unwrap(), str_to_sign);
    format!("SharedKey {}:{}", account, BASE64_STANDARD.encode(auth))
}

fn add_if_exists<'a>(h: &'a HeaderMap, key: &HeaderName) -> &'a str {
    h.get(key)
        .map(|s| s.to_str())
        .transpose()
        .ok()
        .flatten()
        .unwrap_or_default()
}

/// <https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key#constructing-the-signature-string>
fn string_to_sign(h: &HeaderMap, u: &Url, method: &Method, account: &str) -> String {
    // content length must only be specified if != 0
    // this is valid from 2015-02-21
    let content_length = h
        .get(&CONTENT_LENGTH)
        .map(|s| s.to_str())
        .transpose()
        .ok()
        .flatten()
        .filter(|&v| v != "0")
        .unwrap_or_default();
    format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}{}",
        method.as_ref(),
        add_if_exists(h, &CONTENT_ENCODING),
        add_if_exists(h, &CONTENT_LANGUAGE),
        content_length,
        add_if_exists(h, &CONTENT_MD5),
        add_if_exists(h, &CONTENT_TYPE),
        add_if_exists(h, &DATE),
        add_if_exists(h, &IF_MODIFIED_SINCE),
        add_if_exists(h, &IF_MATCH),
        add_if_exists(h, &IF_NONE_MATCH),
        add_if_exists(h, &IF_UNMODIFIED_SINCE),
        add_if_exists(h, &RANGE),
        canonicalize_header(h),
        canonicalized_resource(account, u)
    )
}

/// <https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key#constructing-the-canonicalized-headers-string>
fn canonicalize_header(headers: &HeaderMap) -> String {
    let mut names = headers
        .iter()
        .filter_map(|(k, _)| {
            (k.as_str().starts_with("x-ms"))
                // TODO remove unwraps
                .then(|| (k.as_str(), headers.get(k).unwrap().to_str().unwrap()))
        })
        .collect::<Vec<_>>();
    names.sort_unstable();

    let mut result = String::new();
    for (name, value) in names {
        result.push_str(name);
        result.push(':');
        result.push_str(value);
        result.push('\n');
    }
    result
}

/// <https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key#constructing-the-canonicalized-resource-string>
fn canonicalized_resource(account: &str, uri: &Url) -> String {
    let mut can_res: String = String::new();
    can_res.push('/');
    can_res.push_str(account);
    can_res.push_str(uri.path().to_string().as_str());
    can_res.push('\n');

    // query parameters
    let query_pairs = uri.query_pairs();
    {
        let mut qps: Vec<String> = Vec::new();
        for (q, _) in query_pairs {
            if !(qps.iter().any(|x| x == &*q)) {
                qps.push(q.into_owned());
            }
        }

        qps.sort();

        for qparam in qps {
            // find correct parameter
            let ret = lexy_sort(query_pairs, &qparam);

            can_res = can_res + &qparam.to_lowercase() + ":";

            for (i, item) in ret.iter().enumerate() {
                if i > 0 {
                    can_res.push(',');
                }
                can_res.push_str(item);
            }

            can_res.push('\n');
        }
    };

    can_res[0..can_res.len() - 1].to_owned()
}

fn lexy_sort<'a>(
    vec: impl Iterator<Item = (Cow<'a, str>, Cow<'a, str>)> + 'a,
    query_param: &str,
) -> Vec<Cow<'a, str>> {
    let mut values = vec
        .filter(|(k, _)| *k == query_param)
        .map(|(_, v)| v)
        .collect::<Vec<_>>();
    values.sort_unstable();
    values
}

#[async_trait::async_trait]
pub trait TokenCredential: std::fmt::Debug + Send + Sync + 'static {
    async fn fetch_token(&self, client: &Client, retry: &RetryConfig) -> Result<String>;
}

#[derive(Deserialize, Debug)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
}

/// Encapsulates the logic to perform an OAuth token challenge
#[derive(Debug)]
pub struct ClientSecretOAuthProvider {
    token_url: String,
    client_id: String,
    client_secret: String,
    cache: TokenCache<String>,
}

impl ClientSecretOAuthProvider {
    /// Create a new [`ClientSecretOAuthProvider`] for an azure backed store
    pub fn new(
        client_id: String,
        client_secret: String,
        tenant_id: String,
        authority_host: Option<String>,
    ) -> Self {
        let authority_host = authority_host
            .unwrap_or_else(|| authority_hosts::AZURE_PUBLIC_CLOUD.to_owned());

        Self {
            token_url: format!("{}/{}/oauth2/v2.0/token", authority_host, tenant_id),
            client_id,
            client_secret,
            cache: TokenCache::default(),
        }
    }

    /// Fetch a fresh token
    async fn fetch_token_inner(
        &self,
        client: &Client,
        retry: &RetryConfig,
    ) -> Result<TemporaryToken<String>> {
        let response: TokenResponse = client
            .request(Method::POST, &self.token_url)
            .header(ACCEPT, HeaderValue::from_static(CONTENT_TYPE_JSON))
            .form(&[
                ("client_id", self.client_id.as_str()),
                ("client_secret", self.client_secret.as_str()),
                ("scope", AZURE_STORAGE_SCOPE),
                ("grant_type", "client_credentials"),
            ])
            .send_retry(retry)
            .await
            .context(TokenRequestSnafu)?
            .json()
            .await
            .context(TokenResponseBodySnafu)?;

        let token = TemporaryToken {
            token: response.access_token,
            expiry: Instant::now() + Duration::from_secs(response.expires_in),
        };

        Ok(token)
    }
}

#[async_trait::async_trait]
impl TokenCredential for ClientSecretOAuthProvider {
    /// Fetch a token
    async fn fetch_token(&self, client: &Client, retry: &RetryConfig) -> Result<String> {
        self.cache
            .get_or_insert_with(|| self.fetch_token_inner(client, retry))
            .await
    }
}

fn expires_in_string<'de, D>(deserializer: D) -> std::result::Result<u64, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    let v = String::deserialize(deserializer)?;
    v.parse::<u64>().map_err(serde::de::Error::custom)
}

// NOTE: expires_on is a String version of unix epoch time, not an integer.
// <https://learn.microsoft.com/en-gb/azure/active-directory/managed-identities-azure-resources/how-to-use-vm-token#get-a-token-using-http>
#[derive(Debug, Clone, Deserialize)]
struct MsiTokenResponse {
    pub access_token: String,
    #[serde(deserialize_with = "expires_in_string")]
    pub expires_in: u64,
}

/// Attempts authentication using a managed identity that has been assigned to the deployment environment.
///
/// This authentication type works in Azure VMs, App Service and Azure Functions applications, as well as the Azure Cloud Shell
/// <https://learn.microsoft.com/en-gb/azure/active-directory/managed-identities-azure-resources/how-to-use-vm-token#get-a-token-using-http>
#[derive(Debug)]
pub struct ImdsManagedIdentityOAuthProvider {
    msi_endpoint: String,
    client_id: Option<String>,
    object_id: Option<String>,
    msi_res_id: Option<String>,
    cache: TokenCache<String>,
}

impl ImdsManagedIdentityOAuthProvider {
    /// Create a new [`ImdsManagedIdentityOAuthProvider`] for an azure backed store
    pub fn new(
        client_id: Option<String>,
        object_id: Option<String>,
        msi_res_id: Option<String>,
        msi_endpoint: Option<String>,
    ) -> Self {
        let msi_endpoint = msi_endpoint.unwrap_or_else(|| {
            "http://169.254.169.254/metadata/identity/oauth2/token".to_owned()
        });

        Self {
            msi_endpoint,
            client_id,
            object_id,
            msi_res_id,
            cache: TokenCache::default(),
        }
    }

    /// Fetch a fresh token
    async fn fetch_token_inner(
        &self,
        client: &Client,
        retry: &RetryConfig,
    ) -> Result<TemporaryToken<String>> {
        let mut query_items = vec![
            ("api-version", MSI_API_VERSION),
            ("resource", AZURE_STORAGE_SCOPE),
        ];

        match (
            self.object_id.as_ref(),
            self.client_id.as_ref(),
            self.msi_res_id.as_ref(),
        ) {
            (Some(object_id), None, None) => query_items.push(("object_id", object_id)),
            (None, Some(client_id), None) => query_items.push(("client_id", client_id)),
            (None, None, Some(msi_res_id)) => {
                query_items.push(("msi_res_id", msi_res_id))
            }
            _ => (),
        }

        let mut builder = client
            .request(Method::GET, &self.msi_endpoint)
            .header("metadata", "true")
            .query(&query_items);

        if let Ok(val) = std::env::var(MSI_SECRET_ENV_KEY) {
            builder = builder.header("x-identity-header", val);
        };

        let response: MsiTokenResponse = builder
            .send_retry(retry)
            .await
            .context(TokenRequestSnafu)?
            .json()
            .await
            .context(TokenResponseBodySnafu)?;

        let token = TemporaryToken {
            token: response.access_token,
            expiry: Instant::now() + Duration::from_secs(response.expires_in),
        };

        Ok(token)
    }
}

#[async_trait::async_trait]
impl TokenCredential for ImdsManagedIdentityOAuthProvider {
    /// Fetch a token
    async fn fetch_token(&self, client: &Client, retry: &RetryConfig) -> Result<String> {
        self.cache
            .get_or_insert_with(|| self.fetch_token_inner(client, retry))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::mock_server::MockServer;
    use hyper::{Body, Response};
    use reqwest::{Client, Method};

    #[tokio::test]
    async fn test_managed_identity() {
        let server = MockServer::new();

        std::env::set_var(MSI_SECRET_ENV_KEY, "env-secret");

        let endpoint = server.url();
        let client = Client::new();
        let retry_config = RetryConfig::default();

        // Test IMDS
        server.push_fn(|req| {
            assert_eq!(req.uri().path(), "/metadata/identity/oauth2/token");
            assert!(req.uri().query().unwrap().contains("client_id=client_id"));
            assert_eq!(req.method(), &Method::GET);
            let t = req
                .headers()
                .get("x-identity-header")
                .unwrap()
                .to_str()
                .unwrap();
            assert_eq!(t, "env-secret");
            let t = req.headers().get("metadata").unwrap().to_str().unwrap();
            assert_eq!(t, "true");
            Response::new(Body::from(
                r#"
            {
                "access_token": "TOKEN",
                "refresh_token": "",
                "expires_in": "3599",
                "expires_on": "1506484173",
                "not_before": "1506480273",
                "resource": "https://management.azure.com/",
                "token_type": "Bearer"
              }
            "#,
            ))
        });

        let credential = ImdsManagedIdentityOAuthProvider::new(
            Some("client_id".into()),
            None,
            None,
            Some(format!("{}/metadata/identity/oauth2/token", endpoint)),
        );

        let token = credential
            .fetch_token(&client, &retry_config)
            .await
            .unwrap();

        assert_eq!(&token, "TOKEN");
    }
}
