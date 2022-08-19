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
use crate::client::token::TemporaryToken;
use crate::util::hmac_sha256;
use chrono::Utc;
use oauth2::{
    basic::BasicClient, reqwest::async_http_client, AuthType, AuthUrl, Scope, TokenUrl,
};
use reqwest::{
    header::{
        HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_ENCODING,
        CONTENT_LANGUAGE, CONTENT_LENGTH, CONTENT_TYPE, DATE, IF_MATCH,
        IF_MODIFIED_SINCE, IF_NONE_MATCH, IF_UNMODIFIED_SINCE, RANGE,
    },
    Method, RequestBuilder,
};
use snafu::{ResultExt, Snafu};
use std::borrow::Cow;
use std::str;
use std::time::Instant;
use url::Url;

pub(crate) static STORAGE_TOKEN_SCOPE: &str = "https://storage.azure.com/";
pub(crate) static AZURE_VERSION: HeaderValue = HeaderValue::from_static("2021-08-06");
pub(crate) static VERSION: HeaderName = HeaderName::from_static("x-ms-version");
pub(crate) static RANGE_GET_CONTENT_CRC64: HeaderName =
    HeaderName::from_static("x-ms-range-get-content-crc64");
pub(crate) static MS_RANGE: HeaderName = HeaderName::from_static("x-ms-range");
pub(crate) static BLOB_TYPE: HeaderName = HeaderName::from_static("x-ms-blob-type");
pub(crate) static DELETE_SNAPSHOTS: HeaderName =
    HeaderName::from_static("x-ms-delete-snapshots");
pub(crate) static COPY_SOURCE: HeaderName = HeaderName::from_static("x-ms-copy-source");
pub(crate) static RFC1123_FMT: &str = "%a, %d %h %Y %T GMT";

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("No RSA key found in pem file"))]
    MissingKey,

    #[snafu(display("Invalid RSA key: {}", source), context(false))]
    InvalidKey { source: ring::error::KeyRejected },

    #[snafu(display("Error signing jwt: {}", source))]
    Sign { source: ring::error::Unspecified },

    #[snafu(display("Error encoding jwt payload: {}", source))]
    Encode { source: serde_json::Error },

    #[snafu(display("Error parsing token endpoint: {}", source))]
    Parse { source: url::ParseError },

    #[snafu(display("Unsupported key encoding: {}", encoding))]
    UnsupportedKey { encoding: String },

    #[snafu(display("Error performing token request: {}", source))]
    TokenRequest {
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Provides credentials for use when signing requests
#[derive(Debug)]
pub enum CredentialProvider {
    AccessKey(String),
    SASToken(String),
    ClientSecret(ClientSecretCredential),
}

pub enum AzureCredential {
    AccessKey(String),
    SASToken(String),
    BearerToken(String),
}

/// Provides options to configure how the Identity library makes authentication
/// requests to Azure Active Directory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TokenCredentialOptions {
    authority_host: String,
}

impl Default for TokenCredentialOptions {
    fn default() -> Self {
        Self {
            authority_host: authority_hosts::AZURE_PUBLIC_CLOUD.to_owned(),
        }
    }
}

impl TokenCredentialOptions {
    /// Create a new TokenCredentialsOptions. `default()` may also be used.
    pub fn new(authority_host: String) -> Self {
        Self { authority_host }
    }
    /// Set the authority host for authentication requests.
    pub fn set_authority_host(&mut self, authority_host: String) {
        self.authority_host = authority_host
    }

    /// The authority host to use for authentication requests.  The default is
    /// `https://login.microsoftonline.com`.
    pub fn authority_host(&self) -> &str {
        &self.authority_host
    }
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

/// Enables authentication to Azure Active Directory using a client secret that was generated for an App Registration.
///
/// More information on how to configure a client secret can be found here:
/// <https://docs.microsoft.com/azure/active-directory/develop/quickstart-configure-app-access-web-apis#add-credentials-to-your-web-application>
pub struct ClientSecretCredential {
    tenant_id: String,
    client_id: oauth2::ClientId,
    client_secret: Option<oauth2::ClientSecret>,
    options: TokenCredentialOptions,
}

impl std::fmt::Debug for ClientSecretCredential {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ClientSecretCredential{{ client_id: {}, tenant_id: {} }}",
            self.client_id.as_str(),
            self.tenant_id
        )
    }
}

impl ClientSecretCredential {
    /// Create a new ClientSecretCredential
    pub fn new(
        tenant_id: String,
        client_id: String,
        client_secret: String,
        options: TokenCredentialOptions,
    ) -> Self {
        Self {
            tenant_id,
            client_id: oauth2::ClientId::new(client_id),
            client_secret: Some(oauth2::ClientSecret::new(client_secret)),
            options,
        }
    }

    fn options(&self) -> &TokenCredentialOptions {
        &self.options
    }

    pub async fn get_token(&self) -> Result<TemporaryToken<String>> {
        let options = self.options();
        let authority_host = options.authority_host();

        let token_url = TokenUrl::from_url(
            Url::parse(&format!(
                "{}/{}/oauth2/v2.0/token",
                authority_host, self.tenant_id
            ))
            .context(ParseSnafu)?,
        );

        let auth_url = AuthUrl::from_url(
            Url::parse(&format!(
                "{}/{}/oauth2/v2.0/authorize",
                authority_host, self.tenant_id
            ))
            .context(ParseSnafu)?,
        );

        let client = BasicClient::new(
            self.client_id.clone(),
            self.client_secret.clone(),
            auth_url,
            Some(token_url),
        )
        .set_auth_type(AuthType::RequestBody);

        let token_result = client
            .exchange_client_credentials()
            .add_scope(Scope::new(format!("{}/.default", STORAGE_TOKEN_SCOPE)))
            .request_async(async_http_client)
            .await
            .map(|r| {
                use oauth2::TokenResponse as _;
                TemporaryToken {
                    token: r.access_token().secret().to_owned(),
                    expiry: Instant::now() + r.expires_in().unwrap_or_default(),
                }
            })
            .map_err(|err| Error::TokenRequest {
                source: Box::new(err),
            })?;

        Ok(token_result)
    }
}

pub trait CredentialExt {
    /// Sign a request <https://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html>
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
        let date = Utc::now();
        let date_str = date.format(RFC1123_FMT).to_string();
        let date_val = HeaderValue::from_str(&date_str).unwrap();

        // Hack around lack of access to underlying request
        // https://github.com/seanmonstar/reqwest/issues/1212
        let request = self
            .try_clone()
            .expect("not stream")
            .header("x-ms-date", &date_val)
            .header(&VERSION, &AZURE_VERSION)
            .build()
            .expect("request valid");

        self = self
            .header("x-ms-date", &date_val)
            .header(&VERSION, &AZURE_VERSION);

        match credential {
            AzureCredential::AccessKey(key) => {
                let auth = generate_authorization(
                    request.headers(),
                    request.url(),
                    request.method(),
                    account,
                    key.as_str(),
                );
                self = self
                    // TODO how to handle unwrap? It's hex so we are fine?
                    .header(AUTHORIZATION, HeaderValue::from_str(auth.as_str()).unwrap());
            }
            AzureCredential::BearerToken(token) => {
                self = self
                    // TODO how to handle unwrap? We generated the token, so we are fine?
                    .header(
                        AUTHORIZATION,
                        HeaderValue::from_str(&format!("Bearer {}", token)).unwrap(),
                    );
            }
            AzureCredential::SASToken(_sas) => todo!(),
        };

        self
    }
}

pub(super) fn generate_authorization(
    h: &HeaderMap,
    u: &Url,
    method: &Method,
    account: &str,
    key: &str,
) -> String {
    let str_to_sign = string_to_sign(h, u, method, account);
    let auth = hmac_sha256(base64::decode(key).unwrap(), &str_to_sign);
    format!("SharedKey {}:{}", account, base64::encode(auth))
}

fn add_if_exists<'a>(h: &'a HeaderMap, key: &HeaderName) -> &'a str {
    // h.get_optional_str(key).unwrap_or_default()
    h.get(key)
        .map(|s| s.to_str())
        .transpose()
        .ok()
        .unwrap_or(Some(""))
        .unwrap_or_default()
}

static CONTENT_MD5: HeaderName = HeaderName::from_static("content-md5");

fn string_to_sign(h: &HeaderMap, u: &Url, method: &Method, account: &str) -> String {
    // content length must only be specified if != 0
    // this is valid from 2015-02-21
    let content_length = h
        .get(&CONTENT_LENGTH)
        .map(|s| s.to_str())
        .transpose()
        .ok()
        .unwrap_or(Some(""))
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
        result = format!("{result}{name}:{value}\n");
    }
    result
}

fn canonicalized_resource(account: &str, uri: &Url) -> String {
    let mut can_res: String = String::new();
    can_res += "/";
    can_res += account;

    for p in uri.path_segments().into_iter().flatten() {
        can_res.push('/');
        can_res.push_str(p);
    }
    can_res += "\n";

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
                    can_res += ","
                }
                can_res += item;
            }

            can_res += "\n";
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
