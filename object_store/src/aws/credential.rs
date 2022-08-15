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
use crate::{Result, RetryConfig};
use bytes::Buf;
use chrono::{DateTime, Utc};
use futures::TryFutureExt;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::{Client, Method, Request, RequestBuilder};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

type StdError = Box<dyn std::error::Error + Send + Sync>;

/// SHA256 hash of empty string
static EMPTY_SHA256_HASH: &str =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

#[derive(Debug)]
pub struct AwsCredential {
    pub key_id: String,
    pub secret_key: String,
    pub token: Option<String>,
}

impl AwsCredential {
    /// Signs a string
    ///
    /// <https://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html>
    fn sign(
        &self,
        to_sign: &str,
        date: DateTime<Utc>,
        region: &str,
        service: &str,
    ) -> String {
        let date_string = date.format("%Y%m%d").to_string();
        let date_hmac = hmac_sha256(format!("AWS4{}", self.secret_key), date_string);
        let region_hmac = hmac_sha256(date_hmac, region);
        let service_hmac = hmac_sha256(region_hmac, service);
        let signing_hmac = hmac_sha256(service_hmac, b"aws4_request");
        hex_encode(hmac_sha256(signing_hmac, to_sign).as_ref())
    }
}

struct RequestSigner<'a> {
    date: DateTime<Utc>,
    credential: &'a AwsCredential,
    service: &'a str,
    region: &'a str,
}

const DATE_HEADER: &str = "x-amz-date";
const HASH_HEADER: &str = "x-amz-content-sha256";
const TOKEN_HEADER: &str = "x-amz-security-token";
const AUTH_HEADER: &str = "authorization";

const ALL_HEADERS: &[&str; 4] = &[DATE_HEADER, HASH_HEADER, TOKEN_HEADER, AUTH_HEADER];

impl<'a> RequestSigner<'a> {
    fn sign(&self, request: &mut Request) {
        if let Some(ref token) = self.credential.token {
            let token_val = HeaderValue::from_str(token).unwrap();
            request.headers_mut().insert(TOKEN_HEADER, token_val);
        }

        let host_val = HeaderValue::from_str(
            &request.url()[url::Position::BeforeHost..url::Position::AfterPort],
        )
        .unwrap();
        request.headers_mut().insert("host", host_val);

        let date_str = self.date.format("%Y%m%dT%H%M%SZ").to_string();
        let date_val = HeaderValue::from_str(&date_str).unwrap();
        request.headers_mut().insert(DATE_HEADER, date_val);

        let digest = match request.body() {
            None => EMPTY_SHA256_HASH.to_string(),
            Some(body) => hex_digest(body.as_bytes().unwrap()),
        };

        let header_digest = HeaderValue::from_str(&digest).unwrap();
        request.headers_mut().insert(HASH_HEADER, header_digest);

        let (signed_headers, canonical_headers) = canonicalize_headers(request.headers());

        // https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            request.method().as_str(),
            request.url().path(), // S3 doesn't percent encode this like other services
            request.url().query().unwrap_or(""), // This assumes the query pairs are in order
            canonical_headers,
            signed_headers,
            digest
        );

        let hashed_canonical_request = hex_digest(canonical_request.as_bytes());
        let scope = format!(
            "{}/{}/{}/aws4_request",
            self.date.format("%Y%m%d"),
            self.region,
            self.service
        );

        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            self.date.format("%Y%m%dT%H%M%SZ"),
            scope,
            hashed_canonical_request
        );

        // sign the string
        let signature =
            self.credential
                .sign(&string_to_sign, self.date, self.region, self.service);

        // build the actual auth header
        let authorisation = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.credential.key_id, scope, signed_headers, signature
        );

        let authorization_val = HeaderValue::from_str(&authorisation).unwrap();
        request.headers_mut().insert(AUTH_HEADER, authorization_val);
    }
}

pub trait CredentialExt {
    /// Sign a request <https://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html>
    fn with_aws_sigv4(
        self,
        credential: &AwsCredential,
        region: &str,
        service: &str,
    ) -> Self;
}

impl CredentialExt for RequestBuilder {
    fn with_aws_sigv4(
        mut self,
        credential: &AwsCredential,
        region: &str,
        service: &str,
    ) -> Self {
        // Hack around lack of access to underlying request
        // https://github.com/seanmonstar/reqwest/issues/1212
        let mut request = self
            .try_clone()
            .expect("not stream")
            .build()
            .expect("request valid");

        let date = Utc::now();
        let signer = RequestSigner {
            date,
            credential,
            service,
            region,
        };

        signer.sign(&mut request);

        for header in ALL_HEADERS {
            if let Some(val) = request.headers_mut().remove(*header) {
                self = self.header(*header, val)
            }
        }
        self
    }
}

fn hmac_sha256(secret: impl AsRef<[u8]>, bytes: impl AsRef<[u8]>) -> ring::hmac::Tag {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_ref());
    ring::hmac::sign(&key, bytes.as_ref())
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
        let _ = write!(out, "{:02x}", byte);
    }
    out
}

/// Canonicalizes headers into the AWS Canonical Form.
///
/// <https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html>
fn canonicalize_headers(header_map: &HeaderMap) -> (String, String) {
    let mut headers = BTreeMap::<&str, Vec<&str>>::new();
    let mut value_count = 0;
    let mut value_bytes = 0;
    let mut key_bytes = 0;

    for (key, value) in header_map {
        let key = key.as_str();
        if ["authorization", "content-length", "user-agent"].contains(&key) {
            continue;
        }

        let value = std::str::from_utf8(value.as_bytes()).unwrap();
        key_bytes += key.len();
        value_bytes += value.len();
        value_count += 1;
        headers.entry(key).or_default().push(value);
    }

    let mut signed_headers = String::with_capacity(key_bytes + headers.len());
    let mut canonical_headers =
        String::with_capacity(key_bytes + value_bytes + headers.len() + value_count);

    for (header_idx, (name, values)) in headers.into_iter().enumerate() {
        if header_idx != 0 {
            signed_headers.push(';');
        }

        signed_headers.push_str(name);
        canonical_headers.push_str(name);
        canonical_headers.push(':');
        for (value_idx, value) in values.into_iter().enumerate() {
            if value_idx != 0 {
                canonical_headers.push(',');
            }
            canonical_headers.push_str(value.trim());
        }
        canonical_headers.push('\n');
    }

    (signed_headers, canonical_headers)
}

/// Provides credentials for use when signing requests
#[derive(Debug)]
pub enum CredentialProvider {
    Static(StaticCredentialProvider),
    Instance(InstanceCredentialProvider),
    WebIdentity(WebIdentityProvider),
}

impl CredentialProvider {
    pub async fn get_credential(&self) -> Result<Arc<AwsCredential>> {
        match self {
            Self::Static(s) => Ok(Arc::clone(&s.credential)),
            Self::Instance(c) => c.get_credential().await,
            Self::WebIdentity(c) => c.get_credential().await,
        }
    }
}

/// A static set of credentials
#[derive(Debug)]
pub struct StaticCredentialProvider {
    pub credential: Arc<AwsCredential>,
}

/// Credentials sourced from the instance metadata service
///
/// <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-service.html>
#[derive(Debug)]
pub struct InstanceCredentialProvider {
    pub cache: TokenCache<Arc<AwsCredential>>,
    pub client: Client,
    pub retry_config: RetryConfig,
}

impl InstanceCredentialProvider {
    async fn get_credential(&self) -> Result<Arc<AwsCredential>> {
        self.cache
            .get_or_insert_with(|| {
                const METADATA_ENDPOINT: &str = "http://169.254.169.254";
                instance_creds(&self.client, &self.retry_config, METADATA_ENDPOINT)
                    .map_err(|source| crate::Error::Generic {
                        store: "S3",
                        source,
                    })
            })
            .await
    }
}

/// Credentials sourced using AssumeRoleWithWebIdentity
///
/// <https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts-technical-overview.html>
#[derive(Debug)]
pub struct WebIdentityProvider {
    pub cache: TokenCache<Arc<AwsCredential>>,
    pub token: String,
    pub role_arn: String,
    pub session_name: String,
    pub endpoint: String,
    pub client: Client,
    pub retry_config: RetryConfig,
}

impl WebIdentityProvider {
    async fn get_credential(&self) -> Result<Arc<AwsCredential>> {
        self.cache
            .get_or_insert_with(|| {
                web_identity(
                    &self.client,
                    &self.retry_config,
                    &self.token,
                    &self.role_arn,
                    &self.session_name,
                    &self.endpoint,
                )
                .map_err(|source| crate::Error::Generic {
                    store: "S3",
                    source,
                })
            })
            .await
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct InstanceCredentials {
    access_key_id: String,
    secret_access_key: String,
    token: String,
    expiration: DateTime<Utc>,
}

impl From<InstanceCredentials> for AwsCredential {
    fn from(s: InstanceCredentials) -> Self {
        Self {
            key_id: s.access_key_id,
            secret_key: s.secret_access_key,
            token: Some(s.token),
        }
    }
}

/// <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html#instance-metadata-security-credentials>
async fn instance_creds(
    client: &Client,
    retry_config: &RetryConfig,
    endpoint: &str,
) -> Result<TemporaryToken<Arc<AwsCredential>>, StdError> {
    const CREDENTIALS_PATH: &str = "latest/meta-data/iam/security-credentials";
    const AWS_EC2_METADATA_TOKEN_HEADER: &str = "X-aws-ec2-metadata-token";

    let token_url = format!("{}/latest/api/token", endpoint);
    let token = client
        .request(Method::PUT, token_url)
        .header("X-aws-ec2-metadata-token-ttl-seconds", "600") // 10 minute TTL
        .send_retry(retry_config)
        .await?
        .text()
        .await?;

    let role_url = format!("{}/{}/", endpoint, CREDENTIALS_PATH);
    let role = client
        .request(Method::GET, role_url)
        .header(AWS_EC2_METADATA_TOKEN_HEADER, &token)
        .send_retry(retry_config)
        .await?
        .text()
        .await?;

    let creds_url = format!("{}/{}/{}", endpoint, CREDENTIALS_PATH, role);
    let creds: InstanceCredentials = client
        .request(Method::GET, creds_url)
        .header(AWS_EC2_METADATA_TOKEN_HEADER, &token)
        .send_retry(retry_config)
        .await?
        .json()
        .await?;

    let now = Utc::now();
    let ttl = (creds.expiration - now).to_std().unwrap_or_default();
    Ok(TemporaryToken {
        token: Arc::new(creds.into()),
        expiry: Instant::now() + ttl,
    })
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AssumeRoleResponse {
    assume_role_with_web_identity_result: AssumeRoleResult,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AssumeRoleResult {
    credentials: AssumeRoleCredentials,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct AssumeRoleCredentials {
    session_token: String,
    secret_access_key: String,
    access_key_id: String,
    expiration: DateTime<Utc>,
}

impl From<AssumeRoleCredentials> for AwsCredential {
    fn from(s: AssumeRoleCredentials) -> Self {
        Self {
            key_id: s.access_key_id,
            secret_key: s.secret_access_key,
            token: Some(s.session_token),
        }
    }
}

/// <https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts-technical-overview.html>
async fn web_identity(
    client: &Client,
    retry_config: &RetryConfig,
    token: &str,
    role_arn: &str,
    session_name: &str,
    endpoint: &str,
) -> Result<TemporaryToken<Arc<AwsCredential>>, StdError> {
    let bytes = client
        .request(Method::POST, endpoint)
        .query(&[
            ("Action", "AssumeRoleWithWebIdentity"),
            ("DurationSeconds", "3600"),
            ("RoleArn", role_arn),
            ("RoleSessionName", session_name),
            ("Version", "2011-06-15"),
            ("WebIdentityToken", token),
        ])
        .send_retry(retry_config)
        .await?
        .bytes()
        .await?;

    let resp: AssumeRoleResponse = quick_xml::de::from_reader(bytes.reader())
        .map_err(|e| format!("Invalid AssumeRoleWithWebIdentity response: {}", e))?;

    let creds = resp.assume_role_with_web_identity_result.credentials;
    let now = Utc::now();
    let ttl = (creds.expiration - now).to_std().unwrap_or_default();

    Ok(TemporaryToken {
        token: Arc::new(creds.into()),
        expiry: Instant::now() + ttl,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::{Client, Method};
    use std::env;

    // Test generated using https://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html
    #[test]
    fn test_sign() {
        let client = Client::new();

        // Test credentials from https://docs.aws.amazon.com/AmazonS3/latest/userguide/RESTAuthentication.html
        let credential = AwsCredential {
            key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            token: None,
        };

        // method = 'GET'
        // service = 'ec2'
        // host = 'ec2.amazonaws.com'
        // region = 'us-east-1'
        // endpoint = 'https://ec2.amazonaws.com'
        // request_parameters = ''
        let date = DateTime::parse_from_rfc3339("2022-08-06T18:01:34Z")
            .unwrap()
            .with_timezone(&Utc);

        let mut request = client
            .request(Method::GET, "https://ec2.amazon.com/")
            .build()
            .unwrap();

        let signer = RequestSigner {
            date,
            credential: &credential,
            service: "ec2",
            region: "us-east-1",
        };

        signer.sign(&mut request);
        assert_eq!(request.headers().get(AUTH_HEADER).unwrap(), "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20220806/us-east-1/ec2/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=a3c787a7ed37f7fdfbfd2d7056a3d7c9d85e6d52a2bfbec73793c0be6e7862d4")
    }

    #[test]
    fn test_sign_port() {
        let client = Client::new();

        let credential = AwsCredential {
            key_id: "H20ABqCkLZID4rLe".to_string(),
            secret_key: "jMqRDgxSsBqqznfmddGdu1TmmZOJQxdM".to_string(),
            token: None,
        };

        let date = DateTime::parse_from_rfc3339("2022-08-09T13:05:25Z")
            .unwrap()
            .with_timezone(&Utc);

        let mut request = client
            .request(Method::GET, "http://localhost:9000/tsm-schemas")
            .query(&[
                ("delimiter", "/"),
                ("encoding-type", "url"),
                ("list-type", "2"),
                ("prefix", ""),
            ])
            .build()
            .unwrap();

        let signer = RequestSigner {
            date,
            credential: &credential,
            service: "s3",
            region: "us-east-1",
        };

        signer.sign(&mut request);
        assert_eq!(request.headers().get(AUTH_HEADER).unwrap(), "AWS4-HMAC-SHA256 Credential=H20ABqCkLZID4rLe/20220809/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=9ebf2f92872066c99ac94e573b4e1b80f4dbb8a32b1e8e23178318746e7d1b4d")
    }

    #[tokio::test]
    async fn test_instance_metadata() {
        if env::var("TEST_INTEGRATION").is_err() {
            eprintln!("skipping AWS integration test");
        }

        // For example https://github.com/aws/amazon-ec2-metadata-mock
        let endpoint = env::var("EC2_METADATA_ENDPOINT").unwrap();
        let client = Client::new();
        let retry_config = RetryConfig::default();

        // Verify only allows IMDSv2
        let resp = client
            .request(Method::GET, format!("{}/latest/meta-data/ami-id", endpoint))
            .send()
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            reqwest::StatusCode::UNAUTHORIZED,
            "Ensure metadata endpoint is set to only allow IMDSv2"
        );

        let creds = instance_creds(&client, &retry_config, &endpoint)
            .await
            .unwrap();

        let id = &creds.token.key_id;
        let secret = &creds.token.secret_key;
        let token = creds.token.token.as_ref().unwrap();

        assert!(!id.is_empty());
        assert!(!secret.is_empty());
        assert!(!token.is_empty())
    }
}
