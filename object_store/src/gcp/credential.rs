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
use crate::client::TokenProvider;
use crate::gcp::{DEFAULT_GCS_PLAYLOAD_STRING, STORE, STRICT_ENCODE_SET};
use crate::RetryConfig;
use async_trait::async_trait;
use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{DateTime, Utc};
use futures::TryFutureExt;
use hyper::HeaderMap;
use itertools::Itertools;
use percent_encoding::utf8_percent_encode;
use reqwest::{Client, Method};
use ring::signature::RsaKeyPair;
use serde::Deserialize;
use snafu::{ResultExt, Snafu};
use std::collections::BTreeMap;
use std::env;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::info;
use url::Url;

use super::client::GoogleCloudStorageClient;

pub const DEFAULT_SCOPE: &str = "https://www.googleapis.com/auth/devstorage.full_control";

pub const DEFAULT_GCS_BASE_URL: &str = "https://storage.googleapis.com";

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
}

impl From<Error> for crate::Error {
    fn from(value: Error) -> Self {
        Self::Generic {
            store: STORE,
            source: Box::new(value),
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A Google Cloud Storage Credential
#[derive(Debug, Eq, PartialEq)]
pub struct GcpCredential {
    /// An HTTP bearer token
    pub bearer: String,

    /// client email address
    pub client_email: Option<String>,
}
impl GcpCredential {}

#[derive(Debug, Default, serde::Serialize)]
pub struct JwtHeader<'a> {
    /// The type of JWS: it can only be "JWT" here
    ///
    /// Defined in [RFC7515#4.1.9](https://tools.ietf.org/html/rfc7515#section-4.1.9).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub typ: Option<&'a str>,
    /// The algorithm used
    ///
    /// Defined in [RFC7515#4.1.1](https://tools.ietf.org/html/rfc7515#section-4.1.1).
    pub alg: &'a str,
    /// Content type
    ///
    /// Defined in [RFC7519#5.2](https://tools.ietf.org/html/rfc7519#section-5.2).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cty: Option<&'a str>,
    /// JSON Key URL
    ///
    /// Defined in [RFC7515#4.1.2](https://tools.ietf.org/html/rfc7515#section-4.1.2).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub jku: Option<&'a str>,
    /// Key ID
    ///
    /// Defined in [RFC7515#4.1.4](https://tools.ietf.org/html/rfc7515#section-4.1.4).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kid: Option<&'a str>,
    /// X.509 URL
    ///
    /// Defined in [RFC7515#4.1.5](https://tools.ietf.org/html/rfc7515#section-4.1.5).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub x5u: Option<&'a str>,
    /// X.509 certificate thumbprint
    ///
    /// Defined in [RFC7515#4.1.7](https://tools.ietf.org/html/rfc7515#section-4.1.7).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub x5t: Option<&'a str>,
}

#[derive(serde::Serialize)]
struct TokenClaims<'a> {
    iss: &'a str,
    sub: &'a str,
    scope: &'a str,
    exp: u64,
    iat: u64,
}

#[derive(serde::Deserialize, Debug)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
}

/// Self-signed JWT (JSON Web Token).
///
/// # References
/// - <https://google.aip.dev/auth/4111>
#[derive(Debug)]
pub struct SelfSignedJwt {
    issuer: String,
    scope: String,
    key_pair: RsaKeyPair,
    jwt_header: String,
    random: ring::rand::SystemRandom,
}

impl SelfSignedJwt {
    /// Create a new [`SelfSignedJwt`]
    pub fn new(
        key_id: String,
        issuer: String,
        private_key_pem: String,
        scope: String,
    ) -> Result<Self> {
        let key_pair = decode_first_rsa_key(private_key_pem)?;
        let jwt_header = b64_encode_obj(&JwtHeader {
            alg: "RS256",
            typ: Some("JWT"),
            kid: Some(&key_id),
            ..Default::default()
        })?;

        Ok(Self {
            issuer,
            key_pair,
            scope,
            jwt_header,
            random: ring::rand::SystemRandom::new(),
        })
    }
}

#[async_trait]
impl TokenProvider for SelfSignedJwt {
    type Credential = GcpCredential;

    /// Fetch a fresh token
    async fn fetch_token(
        &self,
        _client: &Client,
        _retry: &RetryConfig,
    ) -> crate::Result<TemporaryToken<Arc<GcpCredential>>> {
        let now = seconds_since_epoch();
        let exp = now + 3600;

        let claims = TokenClaims {
            iss: &self.issuer,
            sub: &self.issuer,
            scope: &self.scope,
            iat: now,
            exp,
        };

        let claim_str = b64_encode_obj(&claims)?;
        let message = [self.jwt_header.as_ref(), claim_str.as_ref()].join(".");
        let mut sig_bytes = vec![0; self.key_pair.public().modulus_len()];
        self.key_pair
            .sign(
                &ring::signature::RSA_PKCS1_SHA256,
                &self.random,
                message.as_bytes(),
                &mut sig_bytes,
            )
            .context(SignSnafu)?;

        let signature = BASE64_URL_SAFE_NO_PAD.encode(sig_bytes);
        let bearer = [message, signature].join(".");

        Ok(TemporaryToken {
            token: Arc::new(GcpCredential {
                bearer,
                client_email: Some(self.issuer.clone()),
            }),
            expiry: Some(Instant::now() + Duration::from_secs(3600)),
        })
    }
}

fn read_credentials_file<T>(service_account_path: impl AsRef<std::path::Path>) -> Result<T>
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

    /// The private key ID
    pub private_key_id: String,

    /// The email address associated with the service account.
    pub client_email: String,

    /// Base URL for GCS
    #[serde(default)]
    pub gcs_base_url: Option<String>,

    /// Disable oauth and use empty tokens.
    #[serde(default)]
    pub disable_oauth: bool,
}

impl ServiceAccountCredentials {
    /// Create a new [`ServiceAccountCredentials`] from a file.
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        read_credentials_file(path)
    }

    /// Create a new [`ServiceAccountCredentials`] from a string.
    pub fn from_key(key: &str) -> Result<Self> {
        serde_json::from_str(key).context(DecodeCredentialsSnafu)
    }

    /// Create a [`SelfSignedJwt`] from this credentials struct.
    ///
    /// We use a scope of [`DEFAULT_SCOPE`] as opposed to an audience
    /// as GCS appears to not support audience
    ///
    /// # References
    /// - <https://stackoverflow.com/questions/63222450/service-account-authorization-without-oauth-can-we-get-file-from-google-cloud/71834557#71834557>
    /// - <https://www.codejam.info/2022/05/google-cloud-service-account-authorization-without-oauth.html>
    pub fn token_provider(self) -> crate::Result<SelfSignedJwt> {
        Ok(SelfSignedJwt::new(
            self.private_key_id,
            self.client_email,
            self.private_key,
            DEFAULT_SCOPE.to_string(),
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
        Some(Item::Pkcs8Key(key)) => Ok(RsaKeyPair::from_pkcs8(key.secret_pkcs8_der())?),
        Some(Item::Pkcs1Key(key)) => Ok(RsaKeyPair::from_der(key.secret_pkcs1_der())?),
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
pub struct InstanceCredentialProvider {}

/// Make a request to the metadata server to fetch a token, using a a given hostname.
async fn make_metadata_request(
    client: &Client,
    hostname: &str,
    retry: &RetryConfig,
) -> crate::Result<TokenResponse> {
    let url =
        format!("http://{hostname}/computeMetadata/v1/instance/service-accounts/default/token");
    let response: TokenResponse = client
        .request(Method::GET, url)
        .header("Metadata-Flavor", "Google")
        .query(&[("audience", "https://www.googleapis.com/oauth2/v4/token")])
        .send_retry(retry)
        .await
        .context(TokenRequestSnafu)?
        .json()
        .await
        .context(TokenResponseBodySnafu)?;
    Ok(response)
}

/// Make a request to the metadata server to fetch the client email, using a a given hostname.
async fn make_metadata_request_for_email(
    client: &Client,
    hostname: &str,
    retry: &RetryConfig,
) -> crate::Result<String> {
    let url =
        format!("http://{hostname}/computeMetadata/v1/instance/service-accounts/default/email",);
    let response = client
        .request(Method::GET, url)
        .header("Metadata-Flavor", "Google")
        .send_retry(retry)
        .await
        .context(TokenRequestSnafu)?
        .text()
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
        let response = make_metadata_request(client, METADATA_HOST, retry)
            .or_else(|_| make_metadata_request(client, METADATA_IP, retry))
            .await?;

        let client_email = make_metadata_request_for_email(client, METADATA_HOST, retry)
            .or_else(|_| make_metadata_request_for_email(client, METADATA_IP, retry))
            .await?;
        let token = TemporaryToken {
            token: Arc::new(GcpCredential {
                bearer: response.access_token,
                client_email: Some(client_email),
            }),
            expiry: Some(Instant::now() + Duration::from_secs(response.expires_in)),
        };
        Ok(token)
    }
}

/// A deserialized `application_default_credentials.json`-file.
///
/// # References
/// - <https://cloud.google.com/docs/authentication/application-default-credentials#personal>
/// - <https://google.aip.dev/auth/4110>
#[derive(serde::Deserialize)]
#[serde(tag = "type")]
pub enum ApplicationDefaultCredentials {
    /// Service Account.
    ///
    /// # References
    /// - <https://google.aip.dev/auth/4112>
    #[serde(rename = "service_account")]
    ServiceAccount(ServiceAccountCredentials),
    /// Authorized user via "gcloud CLI Integration".
    ///
    /// # References
    /// - <https://google.aip.dev/auth/4113>
    #[serde(rename = "authorized_user")]
    AuthorizedUser(AuthorizedUserCredentials),
}

impl ApplicationDefaultCredentials {
    const CREDENTIALS_PATH: &'static str = ".config/gcloud/application_default_credentials.json";

    // Create a new application default credential in the following situations:
    //  1. a file is passed in and the type matches.
    //  2. without argument if the well-known configuration file is present.
    pub fn read(path: Option<&str>) -> Result<Option<Self>, Error> {
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
#[derive(Debug, Deserialize)]
pub struct AuthorizedUserCredentials {
    client_id: String,
    client_secret: String,
    refresh_token: String,
}

impl AuthorizedUserCredentials {
    async fn client_email(&self) -> Result<String> {
        todo!()
    }
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

        let client_email = self.client_email().await?;

        Ok(TemporaryToken {
            token: Arc::new(GcpCredential {
                bearer: response.access_token,
                client_email: Some(client_email),
            }),
            expiry: Some(Instant::now() + Duration::from_secs(response.expires_in)),
        })
    }
}

/// Computes the SHA256 digest of `body` returned as a hex encoded string
fn hex_digest(bytes: &[u8]) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, bytes);
    hex_encode(digest.as_ref())
}

/// Returns `bytes` as a lower-case hex encoded string
fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        // String writing is infallible
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Trim whitespace from header values
fn trim_header_value(value: &str) -> String {
    let mut ret = value.to_string();
    ret.retain(|c| !c.is_whitespace());
    ret
}

/// Authorize a [`Request`] with an [`AwsCredential`] using [AWS SigV4]
///
/// [AWS SigV4]: https://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html
#[derive(Debug)]
pub struct GCSAuthorizer {
    date: Option<DateTime<Utc>>,
}

impl GCSAuthorizer {
    /// Create a new [`GCSAuthorizer`]
    pub fn new() -> Self {
        Self { date: None }
    }

    pub(crate) async fn sign(
        &self,
        method: Method,
        url: &mut Url,
        expires_in: Duration,
        host: &str,
        client_email: &str,
        client: &GoogleCloudStorageClient,
    ) -> crate::Result<()> {
        let date = self.date.unwrap_or_else(Utc::now);
        let scope = self.scope(date);
        let credential_with_scope = format!("{}/{}", client_email, scope);

        let mut headers = HeaderMap::new();
        headers.insert("host", host.parse().unwrap());

        let (_, signed_headers) = self.canonicalize_headers(&headers);

        url.query_pairs_mut()
            .append_pair("X-Goog-Algorithm", "GOOG4-RSA-SHA256")
            .append_pair("X-Goog-Credential", &credential_with_scope)
            .append_pair("X-Goog-Date", &date.format("%Y%m%dT%H%M%SZ").to_string())
            .append_pair("X-Goog-Expires", &expires_in.as_secs().to_string())
            .append_pair("X-Goog-SignedHeaders", &signed_headers);

        let string_to_sign = self.string_to_sign(date, &method, url, &headers);
        let signature = client.sign_blob(&string_to_sign, client_email).await?;

        url.query_pairs_mut()
            .append_pair("X-Goog-Signature", &signature);
        Ok(())
    }

    /// Get scope for the request
    fn scope(&self, date: DateTime<Utc>) -> String {
        format!(
            "{}/us-central1/storage/goog4_request",
            date.format("%Y%m%d"),
        )
    }

    /// Canonicalizes query parameters into the GCP canonical form
    /// form like:
    /// ```
    ///HTTP_VERB  
    ///PATH_TO_RESOURCE  
    ///CANONICAL_QUERY_STRING  
    ///CANONICAL_HEADERS  
    ///
    ///SIGNED_HEADERS  
    ///PAYLOAD
    ///```
    ///
    /// <https://cloud.google.com/storage/docs/authentication/canonical-requests>
    fn canonicalize_request(&self, url: &Url, methond: &Method, headers: &HeaderMap) -> String {
        let verb = methond.as_str();
        let path = url.path();
        let query = self.canonicalize_query(url);
        let (canaonical_headers, signed_headers) = self.canonicalize_headers(headers);

        format!(
            "{}\n{}\n{}\n{}\n\n{}\n{}",
            verb, path, query, canaonical_headers, signed_headers, DEFAULT_GCS_PLAYLOAD_STRING
        )
    }

    fn canonicalize_query(&self, url: &Url) -> String {
        url.query_pairs()
            .sorted_unstable_by(|a, b| a.0.cmp(&b.0))
            .map(|(k, v)| {
                format!(
                    "{}={}",
                    utf8_percent_encode(k.as_ref(), &STRICT_ENCODE_SET),
                    utf8_percent_encode(v.as_ref(), &STRICT_ENCODE_SET)
                )
            })
            .join("&")
    }

    /// Canonicalizes query parameters into the GCP canonical form
    ///
    /// <https://cloud.google.com/storage/docs/authentication/canonical-requests#about-headers>
    fn canonicalize_headers(&self, header_map: &HeaderMap) -> (String, String) {
        //FIXME add error handling for invalid header values
        let mut headers = BTreeMap::<String, Vec<&str>>::new();
        for (k, v) in header_map {
            headers
                .entry(k.as_str().to_lowercase())
                .or_default()
                .push(std::str::from_utf8(v.as_bytes()).unwrap());
        }

        let canonicalize_headers = headers
            .iter()
            .map(|(k, v)| {
                format!(
                    "{}:{}",
                    k.trim(),
                    v.iter().map(|v| trim_header_value(v)).join(",")
                )
            })
            .join("\n");

        let signed_headers = headers.keys().join(";");

        (canonicalize_headers, signed_headers)
    }

    ///construct the string to sign
    ///form like:
    ///```
    ///SIGNING_ALGORITHM  
    ///ACTIVE_DATETIME  
    ///CREDENTIAL_SCOPE  
    ///HASHED_CANONICAL_REQUEST
    ///````
    ///`ACTIVE_DATETIME` format:`YYYYMMDD'T'HHMMSS'Z'`
    /// <https://cloud.google.com/storage/docs/authentication/signatures#string-to-sign>
    pub fn string_to_sign(
        &self,
        date: DateTime<Utc>,
        request_method: &Method,
        url: &Url,
        headers: &HeaderMap,
    ) -> String {
        let caninical_request = self.canonicalize_request(url, request_method, headers);
        let hashed_canonical_req = hex_digest(caninical_request.as_bytes());
        let scope = self.scope(date);

        format!(
            "{}\n{}\n{}\n{}",
            "GOOG4-RSA-SHA256",
            date.format("%Y%m%dT%H%M%SZ"),
            scope,
            hashed_canonical_req
        )
    }
}

//
// #[cfg(test)]
// mod tests {
//     use super::*;
//
//     #[test]
//     fn test_canonicalize_headers() {
//         let mut input_header = HeaderMap::new();
//         input_header.insert("content-type", "text/plain".parse().unwrap());
//         input_header.insert("host", "storage.googleapis.com".parse().unwrap());
//         input_header.insert("x-goog-meta-reviewer", "jane".parse().unwrap());
//         input_header.append("x-goog-meta-reviewer", "john".parse().unwrap());
//         assert_eq!(
//             canonicalize_headers(&input_header),
//             (
//                 "content-type:text/plain
// host:storage.googleapis.com
// x-goog-meta-reviewer:jane,john"
//                     .to_string(),
//                 "content-type;host;x-goog-meta-reviewer".to_string()
//             )
//         );
//     }
//
//     #[test]
//     fn test_canonicalize_query() {
//         let mut url = Url::parse("https://storage.googleapis.com/bucket/object").unwrap();
//         url.query_pairs_mut()
//             .append_pair("max-keys", "2")
//             .append_pair("prefix", "object");
//         assert_eq!(
//             canonicalize_query(&url),
//             "max-keys=2&prefix=object".to_string()
//         );
//     }
// }
