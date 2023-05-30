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
use crate::client::token::TemporaryToken;
use crate::client::{TokenCredentialProvider, TokenProvider};
use crate::gcp::credential::Error::UnsupportedCredentialsType;
use crate::gcp::{GcpCredentialProvider, STORE};
use crate::ClientOptions;
use crate::RetryConfig;
use async_trait::async_trait;
use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use base64::Engine;
use futures::TryFutureExt;
use reqwest::{Client, Method};
use ring::signature::RsaKeyPair;
use snafu::{ResultExt, Snafu};
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to open service account file from {}: {}", path.display(), source))]
    OpenCredentials {
        source: std::io::Error,
        path: PathBuf,
    },

    #[snafu(display("Unable to decode service account file: {}", source))]
    DecodeCredentials { source: serde_json::Error },

    #[snafu(display("No RSA key found in pem file"))]
    MissingKey,

    #[snafu(display("Invalid RSA key: {}", source), context(false))]
    InvalidKey { source: ring::error::KeyRejected },

    #[snafu(display("Error signing jwt: {}", source))]
    Sign { source: ring::error::Unspecified },

    #[snafu(display("Error encoding jwt payload: {}", source))]
    Encode { source: serde_json::Error },

    #[snafu(display("Unsupported key encoding: {}", encoding))]
    UnsupportedKey { encoding: String },

    #[snafu(display("Error performing token request: {}", source))]
    TokenRequest { source: crate::client::retry::Error },

    #[snafu(display("Error getting token response body: {}", source))]
    TokenResponseBody { source: reqwest::Error },

    #[snafu(display("Unsupported ApplicationCredentials type: {}", type_))]
    UnsupportedCredentialsType { type_: String },
}

impl From<Error> for crate::Error {
    fn from(value: Error) -> Self {
        Self::Generic {
            store: STORE,
            source: Box::new(value),
        }
    }
}

/// A Google Cloud Storage Credential
#[derive(Debug, Eq, PartialEq)]
pub struct GcpCredential {
    /// An HTTP bearer token
    pub bearer: String,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Default, serde::Serialize)]
pub struct JwtHeader {
    /// The type of JWS: it can only be "JWT" here
    ///
    /// Defined in [RFC7515#4.1.9](https://tools.ietf.org/html/rfc7515#section-4.1.9).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub typ: Option<String>,
    /// The algorithm used
    ///
    /// Defined in [RFC7515#4.1.1](https://tools.ietf.org/html/rfc7515#section-4.1.1).
    pub alg: String,
    /// Content type
    ///
    /// Defined in [RFC7519#5.2](https://tools.ietf.org/html/rfc7519#section-5.2).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cty: Option<String>,
    /// JSON Key URL
    ///
    /// Defined in [RFC7515#4.1.2](https://tools.ietf.org/html/rfc7515#section-4.1.2).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jku: Option<String>,
    /// Key ID
    ///
    /// Defined in [RFC7515#4.1.4](https://tools.ietf.org/html/rfc7515#section-4.1.4).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kid: Option<String>,
    /// X.509 URL
    ///
    /// Defined in [RFC7515#4.1.5](https://tools.ietf.org/html/rfc7515#section-4.1.5).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub x5u: Option<String>,
    /// X.509 certificate thumbprint
    ///
    /// Defined in [RFC7515#4.1.7](https://tools.ietf.org/html/rfc7515#section-4.1.7).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub x5t: Option<String>,
}

#[derive(serde::Serialize)]
struct TokenClaims<'a> {
    iss: &'a str,
    scope: &'a str,
    aud: &'a str,
    exp: u64,
    iat: u64,
}

#[derive(serde::Deserialize, Debug)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
}

/// Encapsulates the logic to perform an OAuth token challenge
#[derive(Debug)]
pub struct OAuthProvider {
    issuer: String,
    scope: String,
    audience: String,
    key_pair: RsaKeyPair,
    jwt_header: String,
    random: ring::rand::SystemRandom,
}

impl OAuthProvider {
    /// Create a new [`OAuthProvider`]
    pub fn new(
        issuer: String,
        private_key_pem: String,
        scope: String,
        audience: String,
    ) -> Result<Self> {
        let key_pair = decode_first_rsa_key(private_key_pem)?;
        let jwt_header = b64_encode_obj(&JwtHeader {
            alg: "RS256".to_string(),
            ..Default::default()
        })?;

        Ok(Self {
            issuer,
            key_pair,
            scope,
            audience,
            jwt_header,
            random: ring::rand::SystemRandom::new(),
        })
    }
}

#[async_trait]
impl TokenProvider for OAuthProvider {
    type Credential = GcpCredential;

    /// Fetch a fresh token
    async fn fetch_token(
        &self,
        client: &Client,
        retry: &RetryConfig,
    ) -> crate::Result<TemporaryToken<Arc<GcpCredential>>> {
        let now = seconds_since_epoch();
        let exp = now + 3600;

        let claims = TokenClaims {
            iss: &self.issuer,
            scope: &self.scope,
            aud: &self.audience,
            exp,
            iat: now,
        };

        let claim_str = b64_encode_obj(&claims)?;
        let message = [self.jwt_header.as_ref(), claim_str.as_ref()].join(".");
        let mut sig_bytes = vec![0; self.key_pair.public_modulus_len()];
        self.key_pair
            .sign(
                &ring::signature::RSA_PKCS1_SHA256,
                &self.random,
                message.as_bytes(),
                &mut sig_bytes,
            )
            .context(SignSnafu)?;

        let signature = BASE64_URL_SAFE_NO_PAD.encode(sig_bytes);
        let jwt = [message, signature].join(".");

        let body = [
            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
            ("assertion", &jwt),
        ];

        let response: TokenResponse = client
            .request(Method::POST, &self.audience)
            .form(&body)
            .send_retry(retry)
            .await
            .context(TokenRequestSnafu)?
            .json()
            .await
            .context(TokenResponseBodySnafu)?;

        Ok(TemporaryToken {
            token: Arc::new(GcpCredential {
                bearer: response.access_token,
            }),
            expiry: Some(Instant::now() + Duration::from_secs(response.expires_in)),
        })
    }
}

fn read_credentials_file<T>(
    service_account_path: impl AsRef<std::path::Path>,
) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let file = File::open(&service_account_path).context(OpenCredentialsSnafu {
        path: service_account_path.as_ref().to_owned(),
    })?;
    let reader = BufReader::new(file);
    serde_json::from_reader(reader).context(DecodeCredentialsSnafu)
}

/// A deserialized `service-account-********.json`-file.
#[derive(serde::Deserialize, Debug)]
pub struct ServiceAccountCredentials {
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

pub fn default_gcs_base_url() -> String {
    "https://storage.googleapis.com".to_owned()
}

pub fn default_disable_oauth() -> bool {
    false
}

impl ServiceAccountCredentials {
    /// Create a new [`ServiceAccountCredentials`] from a file.
    pub fn from_file<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        read_credentials_file(path)
    }

    /// Create a new [`ServiceAccountCredentials`] from a string.
    pub fn from_key(key: &str) -> Result<Self> {
        serde_json::from_str(key).context(DecodeCredentialsSnafu)
    }

    /// Create an [`OAuthProvider`] from this credentials struct.
    pub fn oauth_provider(
        self,
        scope: &str,
        audience: &str,
    ) -> crate::Result<OAuthProvider> {
        Ok(OAuthProvider::new(
            self.client_email,
            self.private_key,
            scope.to_string(),
            audience.to_string(),
        )?)
    }
}

/// Returns the number of seconds since unix epoch
fn seconds_since_epoch() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn decode_first_rsa_key(private_key_pem: String) -> Result<RsaKeyPair> {
    use rustls_pemfile::Item;
    use std::io::Cursor;

    let mut cursor = Cursor::new(private_key_pem);
    let mut reader = BufReader::new(&mut cursor);

    // Reading from string is infallible
    match rustls_pemfile::read_one(&mut reader).unwrap() {
        Some(Item::PKCS8Key(key)) => Ok(RsaKeyPair::from_pkcs8(&key)?),
        Some(Item::RSAKey(key)) => Ok(RsaKeyPair::from_der(&key)?),
        _ => Err(Error::MissingKey),
    }
}

fn b64_encode_obj<T: serde::Serialize>(obj: &T) -> Result<String> {
    let string = serde_json::to_string(obj).context(EncodeSnafu)?;
    Ok(BASE64_URL_SAFE_NO_PAD.encode(string))
}

/// A provider that uses the Google Cloud Platform metadata server to fetch a token.
///
/// <https://cloud.google.com/docs/authentication/get-id-token#metadata-server>
#[derive(Debug, Default)]
pub struct InstanceCredentialProvider {
    audience: String,
}

impl InstanceCredentialProvider {
    /// Create a new [`InstanceCredentialProvider`], we need to control the client in order to enable http access so save the options.
    pub fn new<T: Into<String>>(audience: T) -> Self {
        Self {
            audience: audience.into(),
        }
    }
}

/// Make a request to the metadata server to fetch a token, using a a given hostname.
async fn make_metadata_request(
    client: &Client,
    hostname: &str,
    retry: &RetryConfig,
    audience: &str,
) -> crate::Result<TokenResponse> {
    let url = format!(
        "http://{hostname}/computeMetadata/v1/instance/service-accounts/default/token"
    );
    let response: TokenResponse = client
        .request(Method::GET, url)
        .header("Metadata-Flavor", "Google")
        .query(&[("audience", audience)])
        .send_retry(retry)
        .await
        .context(TokenRequestSnafu)?
        .json()
        .await
        .context(TokenResponseBodySnafu)?;
    Ok(response)
}

#[async_trait]
impl TokenProvider for InstanceCredentialProvider {
    type Credential = GcpCredential;

    /// Fetch a token from the metadata server.
    /// Since the connection is local we need to enable http access and don't actually use the client object passed in.
    async fn fetch_token(
        &self,
        client: &Client,
        retry: &RetryConfig,
    ) -> crate::Result<TemporaryToken<Arc<GcpCredential>>> {
        const METADATA_IP: &str = "169.254.169.254";
        const METADATA_HOST: &str = "metadata";

        info!("fetching token from metadata server");
        let response =
            make_metadata_request(client, METADATA_HOST, retry, &self.audience)
                .or_else(|_| {
                    make_metadata_request(client, METADATA_IP, retry, &self.audience)
                })
                .await?;
        let token = TemporaryToken {
            token: Arc::new(GcpCredential {
                bearer: response.access_token,
            }),
            expiry: Some(Instant::now() + Duration::from_secs(response.expires_in)),
        };
        Ok(token)
    }
}

/// ApplicationDefaultCredentials
/// <https://google.aip.dev/auth/4110>
pub fn application_default_credentials(
    path: Option<&str>,
    client: &ClientOptions,
    retry: &RetryConfig,
) -> crate::Result<Option<GcpCredentialProvider>> {
    let file = match ApplicationDefaultCredentialsFile::read(path)? {
        Some(x) => x,
        None => return Ok(None),
    };

    match file.type_.as_str() {
        // <https://google.aip.dev/auth/4113>
        "authorized_user" => {
            let token = AuthorizedUserCredentials {
                client_id: file.client_id,
                client_secret: file.client_secret,
                refresh_token: file.refresh_token,
            };

            Ok(Some(Arc::new(TokenCredentialProvider::new(
                token,
                client.client()?,
                retry.clone(),
            ))))
        }
        type_ => Err(UnsupportedCredentialsType {
            type_: type_.to_string(),
        }
        .into()),
    }
}

/// A deserialized `application_default_credentials.json`-file.
/// <https://cloud.google.com/docs/authentication/application-default-credentials#personal>
#[derive(serde::Deserialize)]
struct ApplicationDefaultCredentialsFile {
    #[serde(default)]
    client_id: String,
    #[serde(default)]
    client_secret: String,
    #[serde(default)]
    refresh_token: String,
    #[serde(rename = "type")]
    type_: String,
}

impl ApplicationDefaultCredentialsFile {
    const CREDENTIALS_PATH: &'static str =
        ".config/gcloud/application_default_credentials.json";

    // Create a new application default credential in the following situations:
    //  1. a file is passed in and the type matches.
    //  2. without argument if the well-known configuration file is present.
    fn read(path: Option<&str>) -> Result<Option<Self>, Error> {
        if let Some(path) = path {
            return read_credentials_file::<Self>(path).map(Some);
        }
        if let Some(home) = env::var_os("HOME") {
            let path = Path::new(&home).join(Self::CREDENTIALS_PATH);

            // It's expected for this file to not exist unless it has been explicitly configured by the user.
            if path.try_exists().unwrap_or(false) {
                return read_credentials_file::<Self>(path).map(Some);
            }
        }
        Ok(None)
    }
}

const DEFAULT_TOKEN_GCP_URI: &str = "https://accounts.google.com/o/oauth2/token";

/// <https://google.aip.dev/auth/4113>
#[derive(Debug)]
struct AuthorizedUserCredentials {
    client_id: String,
    client_secret: String,
    refresh_token: String,
}

#[async_trait]
impl TokenProvider for AuthorizedUserCredentials {
    type Credential = GcpCredential;

    async fn fetch_token(
        &self,
        client: &Client,
        retry: &RetryConfig,
    ) -> crate::Result<TemporaryToken<Arc<GcpCredential>>> {
        let response = client
            .request(Method::POST, DEFAULT_TOKEN_GCP_URI)
            .form(&[
                ("grant_type", "refresh_token"),
                ("client_id", &self.client_id),
                ("client_secret", &self.client_secret),
                ("refresh_token", &self.refresh_token),
            ])
            .send_retry(retry)
            .await
            .context(TokenRequestSnafu)?
            .json::<TokenResponse>()
            .await
            .context(TokenResponseBodySnafu)?;

        Ok(TemporaryToken {
            token: Arc::new(GcpCredential {
                bearer: response.access_token,
            }),
            expiry: Some(Instant::now() + Duration::from_secs(response.expires_in)),
        })
    }
}
