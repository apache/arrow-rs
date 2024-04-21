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

use crate::azure::STORE;
use crate::client::retry::RetryExt;
use crate::client::token::{TemporaryToken, TokenCache};
use crate::client::{CredentialProvider, TokenProvider};
use crate::util::hmac_sha256;
use crate::RetryConfig;
use async_trait::async_trait;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use chrono::{DateTime, SecondsFormat, Utc};
use reqwest::header::{
    HeaderMap, HeaderName, HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_ENCODING, CONTENT_LANGUAGE,
    CONTENT_LENGTH, CONTENT_TYPE, DATE, IF_MATCH, IF_MODIFIED_SINCE, IF_NONE_MATCH,
    IF_UNMODIFIED_SINCE, RANGE,
};
use reqwest::{Client, Method, Request, RequestBuilder};
use serde::Deserialize;
use snafu::{ResultExt, Snafu};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Debug;
use std::ops::Deref;
use std::process::Command;
use std::str;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use url::Url;

use super::client::UserDelegationKey;

static AZURE_VERSION: HeaderValue = HeaderValue::from_static("2023-11-03");
static VERSION: HeaderName = HeaderName::from_static("x-ms-version");
pub(crate) static BLOB_TYPE: HeaderName = HeaderName::from_static("x-ms-blob-type");
pub(crate) static DELETE_SNAPSHOTS: HeaderName = HeaderName::from_static("x-ms-delete-snapshots");
pub(crate) static COPY_SOURCE: HeaderName = HeaderName::from_static("x-ms-copy-source");
static CONTENT_MD5: HeaderName = HeaderName::from_static("content-md5");
pub(crate) const RFC1123_FMT: &str = "%a, %d %h %Y %T GMT";
const CONTENT_TYPE_JSON: &str = "application/json";
const MSI_SECRET_ENV_KEY: &str = "IDENTITY_HEADER";
const MSI_API_VERSION: &str = "2019-08-01";

/// OIDC scope used when interacting with OAuth2 APIs
///
/// <https://learn.microsoft.com/en-us/azure/active-directory/develop/scopes-oidc#the-default-scope>
const AZURE_STORAGE_SCOPE: &str = "https://storage.azure.com/.default";

/// Resource ID used when obtaining an access token from the metadata endpoint
///
/// <https://learn.microsoft.com/en-us/azure/storage/blobs/authorize-access-azure-active-directory#microsoft-authentication-library-msal>
const AZURE_STORAGE_RESOURCE: &str = "https://storage.azure.com";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error performing token request: {}", source))]
    TokenRequest { source: crate::client::retry::Error },

    #[snafu(display("Error getting token response body: {}", source))]
    TokenResponseBody { source: reqwest::Error },

    #[snafu(display("Error reading federated token file "))]
    FederatedTokenFile,

    #[snafu(display("Invalid Access Key: {}", source))]
    InvalidAccessKey { source: base64::DecodeError },

    #[snafu(display("'az account get-access-token' command failed: {message}"))]
    AzureCli { message: String },

    #[snafu(display("Failed to parse azure cli response: {source}"))]
    AzureCliResponse { source: serde_json::Error },

    #[snafu(display("Generating SAS keys with SAS tokens auth is not supported"))]
    SASforSASNotSupported,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

impl From<Error> for crate::Error {
    fn from(value: Error) -> Self {
        Self::Generic {
            store: STORE,
            source: Box::new(value),
        }
    }
}

/// A shared Azure Storage Account Key
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct AzureAccessKey(Vec<u8>);

impl AzureAccessKey {
    /// Create a new [`AzureAccessKey`], checking it for validity
    pub fn try_new(key: &str) -> Result<Self> {
        let key = BASE64_STANDARD.decode(key).context(InvalidAccessKeySnafu)?;
        Ok(Self(key))
    }
}

/// An Azure storage credential
#[derive(Debug, Eq, PartialEq)]
pub enum AzureCredential {
    /// A shared access key
    ///
    /// <https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key>
    AccessKey(AzureAccessKey),
    /// A shared access signature
    ///
    /// <https://learn.microsoft.com/en-us/rest/api/storageservices/delegate-access-with-shared-access-signature>
    SASToken(Vec<(String, String)>),
    /// An authorization token
    ///
    /// <https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-azure-active-directory>
    BearerToken(String),
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

pub(crate) struct AzureSigner {
    signing_key: AzureAccessKey,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    account: String,
    delegation_key: Option<UserDelegationKey>,
}

impl AzureSigner {
    pub fn new(
        signing_key: AzureAccessKey,
        account: String,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        delegation_key: Option<UserDelegationKey>,
    ) -> Self {
        Self {
            signing_key,
            account,
            start,
            end,
            delegation_key,
        }
    }

    pub fn sign(&self, method: &Method, url: &mut Url) -> Result<()> {
        let (str_to_sign, query_pairs) = match &self.delegation_key {
            Some(delegation_key) => string_to_sign_user_delegation_sas(
                url,
                method,
                &self.account,
                &self.start,
                &self.end,
                delegation_key,
            ),
            None => string_to_sign_service_sas(url, method, &self.account, &self.start, &self.end),
        };
        let auth = hmac_sha256(&self.signing_key.0, str_to_sign);
        url.query_pairs_mut().extend_pairs(query_pairs);
        url.query_pairs_mut()
            .append_pair("sig", BASE64_STANDARD.encode(auth).as_str());
        Ok(())
    }
}

fn add_date_and_version_headers(request: &mut Request) {
    // rfc2822 string should never contain illegal characters
    let date = Utc::now();
    let date_str = date.format(RFC1123_FMT).to_string();
    // we formatted the data string ourselves, so unwrapping should be fine
    let date_val = HeaderValue::from_str(&date_str).unwrap();
    request.headers_mut().insert(DATE, date_val);
    request
        .headers_mut()
        .insert(&VERSION, AZURE_VERSION.clone());
}

/// Authorize a [`Request`] with an [`AzureAuthorizer`]
#[derive(Debug)]
pub struct AzureAuthorizer<'a> {
    credential: &'a AzureCredential,
    account: &'a str,
}

impl<'a> AzureAuthorizer<'a> {
    /// Create a new [`AzureAuthorizer`]
    pub fn new(credential: &'a AzureCredential, account: &'a str) -> Self {
        AzureAuthorizer {
            credential,
            account,
        }
    }

    /// Authorize `request`
    pub fn authorize(&self, request: &mut Request) {
        add_date_and_version_headers(request);

        match self.credential {
            AzureCredential::AccessKey(key) => {
                let signature = generate_authorization(
                    request.headers(),
                    request.url(),
                    request.method(),
                    self.account,
                    key,
                );

                // "signature" is a base 64 encoded string so it should never
                // contain illegal characters
                request.headers_mut().append(
                    AUTHORIZATION,
                    HeaderValue::from_str(signature.as_str()).unwrap(),
                );
            }
            AzureCredential::BearerToken(token) => {
                request.headers_mut().append(
                    AUTHORIZATION,
                    HeaderValue::from_str(format!("Bearer {}", token).as_str()).unwrap(),
                );
            }
            AzureCredential::SASToken(query_pairs) => {
                request
                    .url_mut()
                    .query_pairs_mut()
                    .extend_pairs(query_pairs);
            }
        }
    }
}

pub(crate) trait CredentialExt {
    /// Apply authorization to requests against azure storage accounts
    /// <https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-requests-to-azure-storage>
    fn with_azure_authorization(
        self,
        credential: &Option<impl Deref<Target = AzureCredential>>,
        account: &str,
    ) -> Self;
}

impl CredentialExt for RequestBuilder {
    fn with_azure_authorization(
        self,
        credential: &Option<impl Deref<Target = AzureCredential>>,
        account: &str,
    ) -> Self {
        let (client, request) = self.build_split();
        let mut request = request.expect("request valid");

        match credential.as_deref() {
            Some(credential) => {
                AzureAuthorizer::new(credential, account).authorize(&mut request);
            }
            None => {
                add_date_and_version_headers(&mut request);
            }
        }

        Self::from_parts(client, request)
    }
}

/// Generate signed key for authorization via access keys
/// <https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key>
fn generate_authorization(
    h: &HeaderMap,
    u: &Url,
    method: &Method,
    account: &str,
    key: &AzureAccessKey,
) -> String {
    let str_to_sign = string_to_sign(h, u, method, account);
    let auth = hmac_sha256(&key.0, str_to_sign);
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

fn string_to_sign_sas(
    u: &Url,
    method: &Method,
    account: &str,
    start: &DateTime<Utc>,
    end: &DateTime<Utc>,
) -> (String, String, String, String, String) {
    // NOTE: for now only blob signing is supported.
    let signed_resource = "b".to_string();

    // https://learn.microsoft.com/en-us/rest/api/storageservices/create-service-sas#permissions-for-a-directory-container-or-blob
    let signed_permissions = match *method {
        // read and list permissions
        Method::GET => match signed_resource.as_str() {
            "c" => "rl",
            "b" => "r",
            _ => unreachable!(),
        },
        // write permissions (also allows crating a new blob in a sub-key)
        Method::PUT => "w",
        // delete permissions
        Method::DELETE => "d",
        // other methods are not used in any of the current operations
        _ => "",
    }
    .to_string();
    let signed_start = start.to_rfc3339_opts(SecondsFormat::Secs, true);
    let signed_expiry = end.to_rfc3339_opts(SecondsFormat::Secs, true);
    let canonicalized_resource = if u.host_str().unwrap_or_default().contains(account) {
        format!("/blob/{}{}", account, u.path())
    } else {
        // NOTE: in case of the emulator, the account name is not part of the host
        //      but the path starts with the account name
        format!("/blob{}", u.path())
    };

    (
        signed_resource,
        signed_permissions,
        signed_start,
        signed_expiry,
        canonicalized_resource,
    )
}

/// Create a string to be signed for authorization via [service sas].
///
/// [service sas]: https://learn.microsoft.com/en-us/rest/api/storageservices/create-service-sas#version-2020-12-06-and-later
fn string_to_sign_service_sas(
    u: &Url,
    method: &Method,
    account: &str,
    start: &DateTime<Utc>,
    end: &DateTime<Utc>,
) -> (String, HashMap<&'static str, String>) {
    let (signed_resource, signed_permissions, signed_start, signed_expiry, canonicalized_resource) =
        string_to_sign_sas(u, method, account, start, end);

    let string_to_sign = format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
        signed_permissions,
        signed_start,
        signed_expiry,
        canonicalized_resource,
        "",                               // signed identifier
        "",                               // signed ip
        "",                               // signed protocol
        &AZURE_VERSION.to_str().unwrap(), // signed version
        signed_resource,                  // signed resource
        "",                               // signed snapshot time
        "",                               // signed encryption scope
        "",                               // rscc - response header: Cache-Control
        "",                               // rscd - response header: Content-Disposition
        "",                               // rsce - response header: Content-Encoding
        "",                               // rscl - response header: Content-Language
        "",                               // rsct - response header: Content-Type
    );

    let mut pairs = HashMap::new();
    pairs.insert("sv", AZURE_VERSION.to_str().unwrap().to_string());
    pairs.insert("sp", signed_permissions);
    pairs.insert("st", signed_start);
    pairs.insert("se", signed_expiry);
    pairs.insert("sr", signed_resource);

    (string_to_sign, pairs)
}

/// Create a string to be signed for authorization via [user delegation sas].
///
/// [user delegation sas]: https://learn.microsoft.com/en-us/rest/api/storageservices/create-user-delegation-sas#version-2020-12-06-and-later
fn string_to_sign_user_delegation_sas(
    u: &Url,
    method: &Method,
    account: &str,
    start: &DateTime<Utc>,
    end: &DateTime<Utc>,
    delegation_key: &UserDelegationKey,
) -> (String, HashMap<&'static str, String>) {
    let (signed_resource, signed_permissions, signed_start, signed_expiry, canonicalized_resource) =
        string_to_sign_sas(u, method, account, start, end);

    let string_to_sign = format!(
        "{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
        signed_permissions,
        signed_start,
        signed_expiry,
        canonicalized_resource,
        delegation_key.signed_oid,        // signed key object id
        delegation_key.signed_tid,        // signed key tenant id
        delegation_key.signed_start,      // signed key start
        delegation_key.signed_expiry,     // signed key expiry
        delegation_key.signed_service,    // signed key service
        delegation_key.signed_version,    // signed key version
        "",                               // signed authorized user object id
        "",                               // signed unauthorized user object id
        "",                               // signed correlation id
        "",                               // signed ip
        "",                               // signed protocol
        &AZURE_VERSION.to_str().unwrap(), // signed version
        signed_resource,                  // signed resource
        "",                               // signed snapshot time
        "",                               // signed encryption scope
        "",                               // rscc - response header: Cache-Control
        "",                               // rscd - response header: Content-Disposition
        "",                               // rsce - response header: Content-Encoding
        "",                               // rscl - response header: Content-Language
        "",                               // rsct - response header: Content-Type
    );

    let mut pairs = HashMap::new();
    pairs.insert("sv", AZURE_VERSION.to_str().unwrap().to_string());
    pairs.insert("sp", signed_permissions);
    pairs.insert("st", signed_start);
    pairs.insert("se", signed_expiry);
    pairs.insert("sr", signed_resource);
    pairs.insert("skoid", delegation_key.signed_oid.clone());
    pairs.insert("sktid", delegation_key.signed_tid.clone());
    pairs.insert("skt", delegation_key.signed_start.clone());
    pairs.insert("ske", delegation_key.signed_expiry.clone());
    pairs.insert("sks", delegation_key.signed_service.clone());
    pairs.insert("skv", delegation_key.signed_version.clone());

    (string_to_sign, pairs)
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
        canonicalize_resource(account, u)
    )
}

/// <https://docs.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key#constructing-the-canonicalized-headers-string>
fn canonicalize_header(headers: &HeaderMap) -> String {
    let mut names = headers
        .iter()
        .filter(|&(k, _)| (k.as_str().starts_with("x-ms")))
        // TODO remove unwraps
        .map(|(k, _)| (k.as_str(), headers.get(k).unwrap().to_str().unwrap()))
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
fn canonicalize_resource(account: &str, uri: &Url) -> String {
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

/// <https://learn.microsoft.com/en-us/azure/active-directory/develop/v2-oauth2-client-creds-grant-flow#successful-response-1>
#[derive(Deserialize, Debug)]
struct OAuthTokenResponse {
    access_token: String,
    expires_in: u64,
}

/// Encapsulates the logic to perform an OAuth token challenge
///
/// <https://learn.microsoft.com/en-us/azure/active-directory/develop/v2-oauth2-client-creds-grant-flow#first-case-access-token-request-with-a-shared-secret>
#[derive(Debug)]
pub struct ClientSecretOAuthProvider {
    token_url: String,
    client_id: String,
    client_secret: String,
}

impl ClientSecretOAuthProvider {
    /// Create a new [`ClientSecretOAuthProvider`] for an azure backed store
    pub fn new(
        client_id: String,
        client_secret: String,
        tenant_id: impl AsRef<str>,
        authority_host: Option<String>,
    ) -> Self {
        let authority_host =
            authority_host.unwrap_or_else(|| authority_hosts::AZURE_PUBLIC_CLOUD.to_owned());

        Self {
            token_url: format!(
                "{}/{}/oauth2/v2.0/token",
                authority_host,
                tenant_id.as_ref()
            ),
            client_id,
            client_secret,
        }
    }
}

#[async_trait::async_trait]
impl TokenProvider for ClientSecretOAuthProvider {
    type Credential = AzureCredential;

    /// Fetch a token
    async fn fetch_token(
        &self,
        client: &Client,
        retry: &RetryConfig,
    ) -> crate::Result<TemporaryToken<Arc<AzureCredential>>> {
        let response: OAuthTokenResponse = client
            .request(Method::POST, &self.token_url)
            .header(ACCEPT, HeaderValue::from_static(CONTENT_TYPE_JSON))
            .form(&[
                ("client_id", self.client_id.as_str()),
                ("client_secret", self.client_secret.as_str()),
                ("scope", AZURE_STORAGE_SCOPE),
                ("grant_type", "client_credentials"),
            ])
            .retryable(retry)
            .idempotent(true)
            .send()
            .await
            .context(TokenRequestSnafu)?
            .json()
            .await
            .context(TokenResponseBodySnafu)?;

        Ok(TemporaryToken {
            token: Arc::new(AzureCredential::BearerToken(response.access_token)),
            expiry: Some(Instant::now() + Duration::from_secs(response.expires_in)),
        })
    }
}

fn expires_on_string<'de, D>(deserializer: D) -> std::result::Result<Instant, D::Error>
where
    D: serde::de::Deserializer<'de>,
{
    let v = String::deserialize(deserializer)?;
    let v = v.parse::<u64>().map_err(serde::de::Error::custom)?;
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_err(serde::de::Error::custom)?;

    Ok(Instant::now() + Duration::from_secs(v.saturating_sub(now.as_secs())))
}

/// NOTE: expires_on is a String version of unix epoch time, not an integer.
/// <https://learn.microsoft.com/en-gb/azure/active-directory/managed-identities-azure-resources/how-to-use-vm-token#get-a-token-using-http>
/// <https://learn.microsoft.com/en-us/azure/app-service/overview-managed-identity?tabs=portal%2Chttp#connect-to-azure-services-in-app-code>
#[derive(Debug, Clone, Deserialize)]
struct ImdsTokenResponse {
    pub access_token: String,
    #[serde(deserialize_with = "expires_on_string")]
    pub expires_on: Instant,
}

/// Attempts authentication using a managed identity that has been assigned to the deployment environment.
///
/// This authentication type works in Azure VMs, App Service and Azure Functions applications, as well as the Azure Cloud Shell
/// <https://learn.microsoft.com/en-gb/azure/active-directory/managed-identities-azure-resources/how-to-use-vm-token#get-a-token-using-http>
#[derive(Debug)]
pub struct ImdsManagedIdentityProvider {
    msi_endpoint: String,
    client_id: Option<String>,
    object_id: Option<String>,
    msi_res_id: Option<String>,
}

impl ImdsManagedIdentityProvider {
    /// Create a new [`ImdsManagedIdentityProvider`] for an azure backed store
    pub fn new(
        client_id: Option<String>,
        object_id: Option<String>,
        msi_res_id: Option<String>,
        msi_endpoint: Option<String>,
    ) -> Self {
        let msi_endpoint = msi_endpoint
            .unwrap_or_else(|| "http://169.254.169.254/metadata/identity/oauth2/token".to_owned());

        Self {
            msi_endpoint,
            client_id,
            object_id,
            msi_res_id,
        }
    }
}

#[async_trait::async_trait]
impl TokenProvider for ImdsManagedIdentityProvider {
    type Credential = AzureCredential;

    /// Fetch a token
    async fn fetch_token(
        &self,
        client: &Client,
        retry: &RetryConfig,
    ) -> crate::Result<TemporaryToken<Arc<AzureCredential>>> {
        let mut query_items = vec![
            ("api-version", MSI_API_VERSION),
            ("resource", AZURE_STORAGE_RESOURCE),
        ];

        let mut identity = None;
        if let Some(client_id) = &self.client_id {
            identity = Some(("client_id", client_id));
        }
        if let Some(object_id) = &self.object_id {
            identity = Some(("object_id", object_id));
        }
        if let Some(msi_res_id) = &self.msi_res_id {
            identity = Some(("msi_res_id", msi_res_id));
        }
        if let Some((key, value)) = identity {
            query_items.push((key, value));
        }

        let mut builder = client
            .request(Method::GET, &self.msi_endpoint)
            .header("metadata", "true")
            .query(&query_items);

        if let Ok(val) = std::env::var(MSI_SECRET_ENV_KEY) {
            builder = builder.header("x-identity-header", val);
        };

        let response: ImdsTokenResponse = builder
            .send_retry(retry)
            .await
            .context(TokenRequestSnafu)?
            .json()
            .await
            .context(TokenResponseBodySnafu)?;

        Ok(TemporaryToken {
            token: Arc::new(AzureCredential::BearerToken(response.access_token)),
            expiry: Some(response.expires_on),
        })
    }
}

/// Credential for using workload identity federation
///
/// <https://learn.microsoft.com/en-us/azure/active-directory/develop/workload-identity-federation>
#[derive(Debug)]
pub struct WorkloadIdentityOAuthProvider {
    token_url: String,
    client_id: String,
    federated_token_file: String,
}

impl WorkloadIdentityOAuthProvider {
    /// Create a new [`WorkloadIdentityOAuthProvider`] for an azure backed store
    pub fn new(
        client_id: impl Into<String>,
        federated_token_file: impl Into<String>,
        tenant_id: impl AsRef<str>,
        authority_host: Option<String>,
    ) -> Self {
        let authority_host =
            authority_host.unwrap_or_else(|| authority_hosts::AZURE_PUBLIC_CLOUD.to_owned());

        Self {
            token_url: format!(
                "{}/{}/oauth2/v2.0/token",
                authority_host,
                tenant_id.as_ref()
            ),
            client_id: client_id.into(),
            federated_token_file: federated_token_file.into(),
        }
    }
}

#[async_trait::async_trait]
impl TokenProvider for WorkloadIdentityOAuthProvider {
    type Credential = AzureCredential;

    /// Fetch a token
    async fn fetch_token(
        &self,
        client: &Client,
        retry: &RetryConfig,
    ) -> crate::Result<TemporaryToken<Arc<AzureCredential>>> {
        let token_str = std::fs::read_to_string(&self.federated_token_file)
            .map_err(|_| Error::FederatedTokenFile)?;

        // https://learn.microsoft.com/en-us/azure/active-directory/develop/v2-oauth2-client-creds-grant-flow#third-case-access-token-request-with-a-federated-credential
        let response: OAuthTokenResponse = client
            .request(Method::POST, &self.token_url)
            .header(ACCEPT, HeaderValue::from_static(CONTENT_TYPE_JSON))
            .form(&[
                ("client_id", self.client_id.as_str()),
                (
                    "client_assertion_type",
                    "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                ),
                ("client_assertion", token_str.as_str()),
                ("scope", AZURE_STORAGE_SCOPE),
                ("grant_type", "client_credentials"),
            ])
            .retryable(retry)
            .idempotent(true)
            .send()
            .await
            .context(TokenRequestSnafu)?
            .json()
            .await
            .context(TokenResponseBodySnafu)?;

        Ok(TemporaryToken {
            token: Arc::new(AzureCredential::BearerToken(response.access_token)),
            expiry: Some(Instant::now() + Duration::from_secs(response.expires_in)),
        })
    }
}

mod az_cli_date_format {
    use chrono::{DateTime, TimeZone};
    use serde::{self, Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<chrono::Local>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        // expiresOn from azure cli uses the local timezone
        let date = chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S.%6f")
            .map_err(serde::de::Error::custom)?;
        chrono::Local
            .from_local_datetime(&date)
            .single()
            .ok_or(serde::de::Error::custom(
                "azure cli returned ambiguous expiry date",
            ))
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AzureCliTokenResponse {
    pub access_token: String,
    #[serde(with = "az_cli_date_format")]
    pub expires_on: DateTime<chrono::Local>,
    pub token_type: String,
}

#[derive(Default, Debug)]
pub struct AzureCliCredential {
    cache: TokenCache<Arc<AzureCredential>>,
}

impl AzureCliCredential {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fetch a token
    async fn fetch_token(&self) -> Result<TemporaryToken<Arc<AzureCredential>>> {
        // on window az is a cmd and it should be called like this
        // see https://doc.rust-lang.org/nightly/std/process/struct.Command.html
        let program = if cfg!(target_os = "windows") {
            "cmd"
        } else {
            "az"
        };
        let mut args = Vec::new();
        if cfg!(target_os = "windows") {
            args.push("/C");
            args.push("az");
        }
        args.push("account");
        args.push("get-access-token");
        args.push("--output");
        args.push("json");
        args.push("--scope");
        args.push(AZURE_STORAGE_SCOPE);

        match Command::new(program).args(args).output() {
            Ok(az_output) if az_output.status.success() => {
                let output = str::from_utf8(&az_output.stdout).map_err(|_| Error::AzureCli {
                    message: "az response is not a valid utf-8 string".to_string(),
                })?;

                let token_response = serde_json::from_str::<AzureCliTokenResponse>(output)
                    .context(AzureCliResponseSnafu)?;
                if !token_response.token_type.eq_ignore_ascii_case("bearer") {
                    return Err(Error::AzureCli {
                        message: format!(
                            "got unexpected token type from azure cli: {0}",
                            token_response.token_type
                        ),
                    });
                }
                let duration =
                    token_response.expires_on.naive_local() - chrono::Local::now().naive_local();
                Ok(TemporaryToken {
                    token: Arc::new(AzureCredential::BearerToken(token_response.access_token)),
                    expiry: Some(
                        Instant::now()
                            + duration.to_std().map_err(|_| Error::AzureCli {
                                message: "az returned invalid lifetime".to_string(),
                            })?,
                    ),
                })
            }
            Ok(az_output) => {
                let message = String::from_utf8_lossy(&az_output.stderr);
                Err(Error::AzureCli {
                    message: message.into(),
                })
            }
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => Err(Error::AzureCli {
                    message: "Azure Cli not installed".into(),
                }),
                error_kind => Err(Error::AzureCli {
                    message: format!("io error: {error_kind:?}"),
                }),
            },
        }
    }
}

#[async_trait]
impl CredentialProvider for AzureCliCredential {
    type Credential = AzureCredential;

    async fn get_credential(&self) -> crate::Result<Arc<Self::Credential>> {
        Ok(self.cache.get_or_insert_with(|| self.fetch_token()).await?)
    }
}

#[cfg(test)]
mod tests {
    use futures::executor::block_on;
    use http_body_util::BodyExt;
    use hyper::{Response, StatusCode};
    use reqwest::{Client, Method};
    use tempfile::NamedTempFile;

    use super::*;
    use crate::azure::MicrosoftAzureBuilder;
    use crate::client::mock_server::MockServer;
    use crate::{ObjectStore, Path};

    #[tokio::test]
    async fn test_managed_identity() {
        let server = MockServer::new().await;

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
            Response::new(
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
            "#
                .to_string(),
            )
        });

        let credential = ImdsManagedIdentityProvider::new(
            Some("client_id".into()),
            None,
            None,
            Some(format!("{endpoint}/metadata/identity/oauth2/token")),
        );

        let token = credential
            .fetch_token(&client, &retry_config)
            .await
            .unwrap();

        assert_eq!(
            token.token.as_ref(),
            &AzureCredential::BearerToken("TOKEN".into())
        );
    }

    #[tokio::test]
    async fn test_workload_identity() {
        let server = MockServer::new().await;
        let tokenfile = NamedTempFile::new().unwrap();
        let tenant = "tenant";
        std::fs::write(tokenfile.path(), "federated-token").unwrap();

        let endpoint = server.url();
        let client = Client::new();
        let retry_config = RetryConfig::default();

        // Test IMDS
        server.push_fn(move |req| {
            assert_eq!(req.uri().path(), format!("/{tenant}/oauth2/v2.0/token"));
            assert_eq!(req.method(), &Method::POST);
            let body = block_on(async move { req.into_body().collect().await.unwrap().to_bytes() });
            let body = String::from_utf8(body.to_vec()).unwrap();
            assert!(body.contains("federated-token"));
            Response::new(
                r#"
            {
                "access_token": "TOKEN",
                "refresh_token": "",
                "expires_in": 3599,
                "expires_on": "1506484173",
                "not_before": "1506480273",
                "resource": "https://management.azure.com/",
                "token_type": "Bearer"
              }
            "#
                .to_string(),
            )
        });

        let credential = WorkloadIdentityOAuthProvider::new(
            "client_id",
            tokenfile.path().to_str().unwrap(),
            tenant,
            Some(endpoint.to_string()),
        );

        let token = credential
            .fetch_token(&client, &retry_config)
            .await
            .unwrap();

        assert_eq!(
            token.token.as_ref(),
            &AzureCredential::BearerToken("TOKEN".into())
        );
    }

    #[tokio::test]
    async fn test_no_credentials() {
        let server = MockServer::new().await;

        let endpoint = server.url();
        let store = MicrosoftAzureBuilder::new()
            .with_account("test")
            .with_container_name("test")
            .with_allow_http(true)
            .with_bearer_token_authorization("token")
            .with_endpoint(endpoint.to_string())
            .with_skip_signature(true)
            .build()
            .unwrap();

        server.push_fn(|req| {
            assert_eq!(req.method(), &Method::GET);
            assert!(req.headers().get("Authorization").is_none());
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body("not found".to_string())
                .unwrap()
        });

        let path = Path::from("file.txt");
        match store.get(&path).await {
            Err(crate::Error::NotFound { .. }) => {}
            _ => {
                panic!("unexpected response");
            }
        }
    }
}
