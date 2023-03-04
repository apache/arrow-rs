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
use crate::ClientOptions;
use crate::RetryConfig;
use async_trait::async_trait;
use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use base64::Engine;
use futures::TryFutureExt;
use reqwest::{Client, Method};
use ring::signature::RsaKeyPair;
use serde_json::json;
use snafu::{ResultExt, Snafu};
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::time::{Duration, Instant};
use tracing::info;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Unable to open service account file: {}", source))]
    OpenCredentials { source: std::io::Error },

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

    #[snafu(display("Missing AWS Credentials for Workload Identity Federation"))]
    ExternalAccountMissingAwsCredentials,

    #[snafu(display("Error encoding STS Caller Identity Token: {}", source))]
    ExternalAccountEncodeCallerIdentityToken { source: serde_json::Error },

    #[snafu(display("Error creating client: {}", source))]
    Client { source: crate::Error },
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

#[async_trait]
pub trait TokenProvider: std::fmt::Debug + Send + Sync {
    async fn fetch_token(
        &self,
        client: &Client,
        retry: &RetryConfig,
    ) -> Result<TemporaryToken<String>>;
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
    /// Fetch a fresh token
    async fn fetch_token(
        &self,
        client: &Client,
        retry: &RetryConfig,
    ) -> Result<TemporaryToken<String>> {
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

        let token = TemporaryToken {
            token: response.access_token,
            expiry: Some(Instant::now() + Duration::from_secs(response.expires_in)),
        };

        Ok(token)
    }
}

fn read_credentials_file<T>(
    service_account_path: impl AsRef<std::path::Path>,
) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    let file = File::open(service_account_path).context(OpenCredentialsSnafu)?;
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
    pub fn token_provider(
        self,
        scope: &str,
        audience: &str,
    ) -> Result<Box<dyn TokenProvider>> {
        Ok(Box::new(OAuthProvider::new(
            self.client_email,
            self.private_key,
            scope.to_string(),
            audience.to_string(),
        )?) as Box<dyn TokenProvider>)
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
    client: Client,
}

impl InstanceCredentialProvider {
    /// Create a new [`InstanceCredentialProvider`], we need to control the client in order to enable http access so save the options.
    pub fn new<T: Into<String>>(
        audience: T,
        client_options: ClientOptions,
    ) -> Result<Self> {
        client_options
            .with_allow_http(true)
            .client()
            .map(|client| Self {
                audience: audience.into(),
                client,
            })
            .context(ClientSnafu)
    }
}

/// Make a request to the metadata server to fetch a token, using a a given hostname.
async fn make_metadata_request(
    client: &Client,
    hostname: &str,
    retry: &RetryConfig,
    audience: &str,
) -> Result<TokenResponse> {
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
    /// Fetch a token from the metadata server.
    /// Since the connection is local we need to enable http access and don't actually use the client object passed in.
    async fn fetch_token(
        &self,
        _client: &Client,
        retry: &RetryConfig,
    ) -> Result<TemporaryToken<String>> {
        const METADATA_IP: &str = "169.254.169.254";
        const METADATA_HOST: &str = "metadata";

        info!("fetching token from metadata server");
        let response =
            make_metadata_request(&self.client, METADATA_HOST, retry, &self.audience)
                .or_else(|_| {
                    make_metadata_request(
                        &self.client,
                        METADATA_IP,
                        retry,
                        &self.audience,
                    )
                })
                .await?;
        let token = TemporaryToken {
            token: response.access_token,
            expiry: Some(Instant::now() + Duration::from_secs(response.expires_in)),
        };
        Ok(token)
    }
}

#[derive(serde::Deserialize, Debug)]
pub struct AuthorizedUserCredentials {
    client_id: String,
    client_secret: String,
    refresh_token: String,
}

#[derive(serde::Deserialize, Debug)]
pub struct AwsExternalAccountCredentials {
    audience: String,
    credential_source: AwsCredentialSource,
    service_account_impersonation: Option<ServiceAccountImpersonation>,
    service_account_impersonation_url: String,
    subject_token_type: String,
    token_url: String,
}
#[derive(serde::Deserialize, Debug)]
struct AwsCredentialSource {
    #[allow(dead_code)]
    environment_id: String,
    regional_cred_verification_url: String,
}
#[derive(serde::Deserialize, Debug)]
struct ServiceAccountImpersonation {
    token_lifetime_seconds: u64,
}

#[derive(serde::Deserialize, Debug)]
#[serde(untagged)]
pub enum ExternalAccountCredentials {
    Aws(AwsExternalAccountCredentials),
}

/// Application Default Credential files include several types including User Credentials (containing actual credentials)
/// and External Account credential files describing how to exchange credentials from AWS or Azure.
/// <https://google.aip.dev/auth/4110>
#[derive(serde::Deserialize, Debug)]
#[serde(tag = "type")]
pub enum ApplicationDefaultCredentials {
    #[serde(rename = "authorized_user")]
    AuthorizedUser(AuthorizedUserCredentials),
    #[serde(rename = "external_account")]
    ExternalAccount(ExternalAccountCredentials),
}

impl ApplicationDefaultCredentials {
    const CREDENTIALS_PATH: &'static str =
        ".config/gcloud/application_default_credentials.json";

    // Create a new application default credential in the following situations:
    //  1. a file is passed in and the type matches.
    //  2. without argument if the well-known configuration file is present.
    pub fn new(path: Option<&str>) -> Result<Option<Self>, Error> {
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

#[async_trait]
impl TokenProvider for ApplicationDefaultCredentials {
    async fn fetch_token(
        &self,
        client: &Client,
        retry: &RetryConfig,
    ) -> Result<TemporaryToken<String>, Error> {
        match self {
            Self::AuthorizedUser(credentials) => {
                credentials.fetch_token(client, retry).await
            }
            Self::ExternalAccount(credentials) => match credentials {
                ExternalAccountCredentials::Aws(credentials) => {
                    credentials.fetch_token(client, retry).await
                }
            },
        }
    }
}

#[derive(serde::Deserialize, Debug)]
struct GcpStsTokenResponse {
    access_token: String,
}

#[derive(serde::Serialize, Debug)]
struct GetCallerIdentityToken {
    url: String,
    method: String,
    headers: Vec<GetCallerIdentityTokenHeader>,
}
#[derive(serde::Deserialize, serde::Serialize, Debug)]
struct GetCallerIdentityTokenHeader {
    key: String,
    value: String,
}

#[derive(serde::Deserialize, Debug)]
struct GcpGenerateAccessTokenResponse {
    #[serde(rename = "accessToken")]
    access_token: String,
    #[serde(rename = "expireTime")]
    expire_time: String,
}

const EXTERNAL_ACCOUNT_AWS_HEADERS: [&str; 5] = [
    "authorization",
    "host",
    "x-amz-date",
    "x-goog-cloud-target-resource",
    "x-amz-security-token",
];

#[async_trait]
impl TokenProvider for AwsExternalAccountCredentials {
    async fn fetch_token(
        &self,
        client: &Client,
        retry: &RetryConfig,
    ) -> Result<TemporaryToken<String>, Error> {
        // Steps documented in https://cloud.google.com/iam/docs/workload-identity-federation-with-other-clouds#aws
        let region_opt = std::env::var("AWS_REGION").ok();
        let cred_url = if self
            .credential_source
            .regional_cred_verification_url
            .contains("{region}")
        {
            match &region_opt {
                Some(region) => self.credential_source.regional_cred_verification_url.replace("{region}", region),
                // Use global endpoint if region is not available
                None => "https://sts.amazonaws.com?Action=GetCallerIdentity&Version=2011-06-15".to_string(),
            }
        } else {
            self.credential_source
                .regional_cred_verification_url
                .clone()
        };
        let region = region_opt.as_deref().unwrap_or("us-east-1");

        let req = client
            .post(&cred_url)
            .header("x-goog-cloud-target-resource", &self.audience);

        // TODO: other credential sources (share?)?
        let key_id = std::env::var("AWS_ACCESS_KEY_ID")
            .map_err(|_| Error::ExternalAccountMissingAwsCredentials)?;
        let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
            .map_err(|_| Error::ExternalAccountMissingAwsCredentials)?;
        let credential = crate::aws::credential::AwsCredential {
            key_id,
            secret_key,
            token: std::env::var("AWS_SESSION_TOKEN").ok(),
        };

        let signer = crate::aws::credential::RequestSigner {
            credential: &credential,
            date: chrono::Utc::now(),
            region,
            service: "sts",
            sign_payload: false,
        };
        let mut req = req.build().context(TokenResponseBodySnafu)?;
        signer.sign(&mut req, false);

        let headers = req
            .headers()
            .iter()
            // must include only these headers
            .filter(|(key, _)| {
                EXTERNAL_ACCOUNT_AWS_HEADERS
                    .iter()
                    .any(|&x| x == key.as_str().to_lowercase())
            })
            .map(|(key, value)| GetCallerIdentityTokenHeader {
                key: key.as_str().to_string(),
                value: value.to_str().unwrap().to_string(),
            })
            .collect::<Vec<_>>();

        let get_caller_identity_token = GetCallerIdentityToken {
            url: cred_url.to_string(),
            method: "POST".to_string(),
            headers,
        };

        let get_caller_identity_token = serde_json::to_string(&get_caller_identity_token)
            .context(ExternalAccountEncodeCallerIdentityTokenSnafu)?;
        let get_caller_identity_token = urlencoding::encode(&get_caller_identity_token);

        let form_data = [
            ("audience", self.audience.as_str()),
            (
                "grant_type",
                "urn:ietf:params:oauth:grant-type:token-exchange",
            ),
            (
                "requested_token_type",
                "urn:ietf:params:oauth:token-type:access_token",
            ),
            ("scope", "https://www.googleapis.com/auth/cloud-platform"),
            ("subject_token_type", self.subject_token_type.as_str()),
            ("subject_token", &get_caller_identity_token),
        ];

        let sts_token = client
            .post(&self.token_url)
            .form(&form_data)
            .send_retry(retry)
            .await
            .context(TokenRequestSnafu)?
            .json::<GcpStsTokenResponse>()
            .await
            .context(TokenResponseBodySnafu)?
            .access_token;

        // https://cloud.google.com/iam/docs/reference/credentials/rest/v1/projects.serviceAccounts/generateAccessToken
        let mut generate_access_token_body = json!({
            "scope": [ "https://www.googleapis.com/auth/cloud-platform" ]
        });
        if let Some(impersonation) = &self.service_account_impersonation {
            let exp = json!(format!("{}s", impersonation.token_lifetime_seconds));
            generate_access_token_body["lifetime"] = exp;
        }

        let resp = client
            .post(&self.service_account_impersonation_url)
            .header("Content-Type", "text/json")
            .bearer_auth(sts_token)
            .json(&generate_access_token_body)
            .send_retry(retry)
            .await
            .context(TokenRequestSnafu)?
            .json::<GcpGenerateAccessTokenResponse>()
            .await
            .context(TokenResponseBodySnafu)?;

        let expiry = chrono::DateTime::parse_from_rfc3339(&resp.expire_time)
            .unwrap()
            .with_timezone(&chrono::Utc);
        let now_instant = Instant::now();
        let now = chrono::Utc::now();
        let duration = (expiry - now).to_std().unwrap();

        let token = TemporaryToken {
            token: resp.access_token,
            expiry: Some(now_instant + duration),
        };

        Ok(token)
    }
}

const DEFAULT_TOKEN_GCP_URI: &str = "https://accounts.google.com/o/oauth2/token";
#[async_trait]
impl TokenProvider for AuthorizedUserCredentials {
    async fn fetch_token(
        &self,
        client: &Client,
        retry: &RetryConfig,
    ) -> Result<TemporaryToken<String>, Error> {
        let body = [
            ("grant_type", "refresh_token"),
            ("client_id", &self.client_id),
            ("client_secret", &self.client_secret),
            ("refresh_token", &self.refresh_token),
        ];

        let response = client
            .request(Method::POST, DEFAULT_TOKEN_GCP_URI)
            .form(&body)
            .send_retry(retry)
            .await
            .context(TokenRequestSnafu)?
            .json::<TokenResponse>()
            .await
            .context(TokenResponseBodySnafu)?;
        let token = TemporaryToken {
            token: response.access_token,
            expiry: Some(Instant::now() + Duration::from_secs(response.expires_in)),
        };
        Ok(token)
    }
}
