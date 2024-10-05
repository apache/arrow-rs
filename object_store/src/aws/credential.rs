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

use crate::aws::{AwsCredentialProvider, STORE, STRICT_ENCODE_SET, STRICT_PATH_ENCODE_SET};
use crate::client::retry::RetryExt;
use crate::client::token::{TemporaryToken, TokenCache};
use crate::client::TokenProvider;
use crate::util::{hex_digest, hex_encode, hmac_sha256};
use crate::{CredentialProvider, Result, RetryConfig};
use async_trait::async_trait;
use bytes::Buf;
use chrono::{DateTime, Utc};
use hyper::header::HeaderName;
use percent_encoding::utf8_percent_encode;
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION};
use reqwest::{Client, Method, Request, RequestBuilder, StatusCode};
use serde::Deserialize;
use snafu::{ResultExt, Snafu};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::warn;
use url::Url;

#[derive(Debug, Snafu)]
#[allow(clippy::enum_variant_names)]
enum Error {
    #[snafu(display("Error performing CreateSession request: {source}"))]
    CreateSessionRequest { source: crate::client::retry::Error },

    #[snafu(display("Error getting CreateSession response: {source}"))]
    CreateSessionResponse { source: reqwest::Error },

    #[snafu(display("Invalid CreateSessionOutput response: {source}"))]
    CreateSessionOutput { source: quick_xml::DeError },
}

impl From<Error> for crate::Error {
    fn from(value: Error) -> Self {
        Self::Generic {
            store: STORE,
            source: Box::new(value),
        }
    }
}

type StdError = Box<dyn std::error::Error + Send + Sync>;

/// SHA256 hash of empty string
static EMPTY_SHA256_HASH: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
static UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
static STREAMING_PAYLOAD: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

/// A set of AWS security credentials
#[derive(Debug, Eq, PartialEq)]
pub struct AwsCredential {
    /// AWS_ACCESS_KEY_ID
    pub key_id: String,
    /// AWS_SECRET_ACCESS_KEY
    pub secret_key: String,
    /// AWS_SESSION_TOKEN
    pub token: Option<String>,
}

impl AwsCredential {
    /// Signs a string
    ///
    /// <https://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html>
    fn sign(&self, to_sign: &str, date: DateTime<Utc>, region: &str, service: &str) -> String {
        let date_string = date.format("%Y%m%d").to_string();
        let date_hmac = hmac_sha256(format!("AWS4{}", self.secret_key), date_string);
        let region_hmac = hmac_sha256(date_hmac, region);
        let service_hmac = hmac_sha256(region_hmac, service);
        let signing_hmac = hmac_sha256(service_hmac, b"aws4_request");
        hex_encode(hmac_sha256(signing_hmac, to_sign).as_ref())
    }
}

/// Authorize a [`Request`] with an [`AwsCredential`] using [AWS SigV4]
///
/// [AWS SigV4]: https://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html
#[derive(Debug)]
pub struct AwsAuthorizer<'a> {
    date: Option<DateTime<Utc>>,
    credential: &'a AwsCredential,
    service: &'a str,
    region: &'a str,
    token_header: Option<HeaderName>,
    sign_payload: bool,
}

static DATE_HEADER: HeaderName = HeaderName::from_static("x-amz-date");
static HASH_HEADER: HeaderName = HeaderName::from_static("x-amz-content-sha256");
static TOKEN_HEADER: HeaderName = HeaderName::from_static("x-amz-security-token");
const ALGORITHM: &str = "AWS4-HMAC-SHA256";

impl<'a> AwsAuthorizer<'a> {
    /// Create a new [`AwsAuthorizer`]
    pub fn new(credential: &'a AwsCredential, service: &'a str, region: &'a str) -> Self {
        Self {
            credential,
            service,
            region,
            date: None,
            sign_payload: true,
            token_header: None,
        }
    }

    /// Controls whether this [`AwsAuthorizer`] will attempt to sign the request payload,
    /// the default is `true`
    pub fn with_sign_payload(mut self, signed: bool) -> Self {
        self.sign_payload = signed;
        self
    }

    /// Overrides the header name for security tokens, defaults to `x-amz-security-token`
    pub(crate) fn with_token_header(mut self, header: HeaderName) -> Self {
        self.token_header = Some(header);
        self
    }

    /// Authorize `request` with an optional pre-calculated SHA256 digest by attaching
    /// the relevant [AWS SigV4] headers
    ///
    /// # Payload Signature
    ///
    /// AWS SigV4 requests must contain the `x-amz-content-sha256` header, it is set as follows:
    ///
    /// * If not configured to sign payloads, it is set to `UNSIGNED-PAYLOAD`
    /// * If a `pre_calculated_digest` is provided, it is set to the hex encoding of it
    /// * If it is a streaming request, it is set to `STREAMING-AWS4-HMAC-SHA256-PAYLOAD`
    /// * Otherwise it is set to the hex encoded SHA256 of the request body
    ///
    /// [AWS SigV4]: https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html
    pub fn authorize(&self, request: &mut Request, pre_calculated_digest: Option<&[u8]>) {
        if let Some(ref token) = self.credential.token {
            let token_val = HeaderValue::from_str(token).unwrap();
            let header = self.token_header.as_ref().unwrap_or(&TOKEN_HEADER);
            request.headers_mut().insert(header, token_val);
        }

        let host = &request.url()[url::Position::BeforeHost..url::Position::AfterPort];
        let host_val = HeaderValue::from_str(host).unwrap();
        request.headers_mut().insert("host", host_val);

        let date = self.date.unwrap_or_else(Utc::now);
        let date_str = date.format("%Y%m%dT%H%M%SZ").to_string();
        let date_val = HeaderValue::from_str(&date_str).unwrap();
        request.headers_mut().insert(&DATE_HEADER, date_val);

        let digest = match self.sign_payload {
            false => UNSIGNED_PAYLOAD.to_string(),
            true => match pre_calculated_digest {
                Some(digest) => hex_encode(digest),
                None => match request.body() {
                    None => EMPTY_SHA256_HASH.to_string(),
                    Some(body) => match body.as_bytes() {
                        Some(bytes) => hex_digest(bytes),
                        None => STREAMING_PAYLOAD.to_string(),
                    },
                },
            },
        };

        let header_digest = HeaderValue::from_str(&digest).unwrap();
        request.headers_mut().insert(&HASH_HEADER, header_digest);

        let (signed_headers, canonical_headers) = canonicalize_headers(request.headers());

        let scope = self.scope(date);

        let string_to_sign = self.string_to_sign(
            date,
            &scope,
            request.method(),
            request.url(),
            &canonical_headers,
            &signed_headers,
            &digest,
        );

        // sign the string
        let signature = self
            .credential
            .sign(&string_to_sign, date, self.region, self.service);

        // build the actual auth header
        let authorisation = format!(
            "{} Credential={}/{}, SignedHeaders={}, Signature={}",
            ALGORITHM, self.credential.key_id, scope, signed_headers, signature
        );

        let authorization_val = HeaderValue::from_str(&authorisation).unwrap();
        request
            .headers_mut()
            .insert(&AUTHORIZATION, authorization_val);
    }

    pub(crate) fn sign(&self, method: Method, url: &mut Url, expires_in: Duration) {
        let date = self.date.unwrap_or_else(Utc::now);
        let scope = self.scope(date);

        // https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
        url.query_pairs_mut()
            .append_pair("X-Amz-Algorithm", ALGORITHM)
            .append_pair(
                "X-Amz-Credential",
                &format!("{}/{}", self.credential.key_id, scope),
            )
            .append_pair("X-Amz-Date", &date.format("%Y%m%dT%H%M%SZ").to_string())
            .append_pair("X-Amz-Expires", &expires_in.as_secs().to_string())
            .append_pair("X-Amz-SignedHeaders", "host");

        // For S3, you must include the X-Amz-Security-Token query parameter in the URL if
        // using credentials sourced from the STS service.
        if let Some(ref token) = self.credential.token {
            url.query_pairs_mut()
                .append_pair("X-Amz-Security-Token", token);
        }

        // We don't have a payload; the user is going to send the payload directly themselves.
        let digest = UNSIGNED_PAYLOAD;

        let host = &url[url::Position::BeforeHost..url::Position::AfterPort].to_string();
        let mut headers = HeaderMap::new();
        let host_val = HeaderValue::from_str(host).unwrap();
        headers.insert("host", host_val);

        let (signed_headers, canonical_headers) = canonicalize_headers(&headers);

        let string_to_sign = self.string_to_sign(
            date,
            &scope,
            &method,
            url,
            &canonical_headers,
            &signed_headers,
            digest,
        );

        let signature = self
            .credential
            .sign(&string_to_sign, date, self.region, self.service);

        url.query_pairs_mut()
            .append_pair("X-Amz-Signature", &signature);
    }

    #[allow(clippy::too_many_arguments)]
    fn string_to_sign(
        &self,
        date: DateTime<Utc>,
        scope: &str,
        request_method: &Method,
        url: &Url,
        canonical_headers: &str,
        signed_headers: &str,
        digest: &str,
    ) -> String {
        // Each path segment must be URI-encoded twice (except for Amazon S3 which only gets
        // URI-encoded once).
        // see https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
        let canonical_uri = match self.service {
            "s3" => url.path().to_string(),
            _ => utf8_percent_encode(url.path(), &STRICT_PATH_ENCODE_SET).to_string(),
        };

        let canonical_query = canonicalize_query(url);

        // https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            request_method.as_str(),
            canonical_uri,
            canonical_query,
            canonical_headers,
            signed_headers,
            digest
        );

        let hashed_canonical_request = hex_digest(canonical_request.as_bytes());

        format!(
            "{}\n{}\n{}\n{}",
            ALGORITHM,
            date.format("%Y%m%dT%H%M%SZ"),
            scope,
            hashed_canonical_request
        )
    }

    fn scope(&self, date: DateTime<Utc>) -> String {
        format!(
            "{}/{}/{}/aws4_request",
            date.format("%Y%m%d"),
            self.region,
            self.service
        )
    }
}

pub(crate) trait CredentialExt {
    /// Sign a request <https://docs.aws.amazon.com/general/latest/gr/sigv4_signing.html>
    fn with_aws_sigv4(
        self,
        authorizer: Option<AwsAuthorizer<'_>>,
        payload_sha256: Option<&[u8]>,
    ) -> Self;
}

impl CredentialExt for RequestBuilder {
    fn with_aws_sigv4(
        self,
        authorizer: Option<AwsAuthorizer<'_>>,
        payload_sha256: Option<&[u8]>,
    ) -> Self {
        match authorizer {
            Some(authorizer) => {
                let (client, request) = self.build_split();
                let mut request = request.expect("request valid");
                authorizer.authorize(&mut request, payload_sha256);

                Self::from_parts(client, request)
            }
            None => self,
        }
    }
}

/// Canonicalizes query parameters into the AWS canonical form
///
/// <https://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html>
fn canonicalize_query(url: &Url) -> String {
    use std::fmt::Write;

    let capacity = match url.query() {
        Some(q) if !q.is_empty() => q.len(),
        _ => return String::new(),
    };
    let mut encoded = String::with_capacity(capacity + 1);

    let mut headers = url.query_pairs().collect::<Vec<_>>();
    headers.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));

    let mut first = true;
    for (k, v) in headers {
        if !first {
            encoded.push('&');
        }
        first = false;
        let _ = write!(
            encoded,
            "{}={}",
            utf8_percent_encode(k.as_ref(), &STRICT_ENCODE_SET),
            utf8_percent_encode(v.as_ref(), &STRICT_ENCODE_SET)
        );
    }
    encoded
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

/// Credentials sourced from the instance metadata service
///
/// <https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-service.html>
#[derive(Debug)]
pub(crate) struct InstanceCredentialProvider {
    pub imdsv1_fallback: bool,
    pub metadata_endpoint: String,
}

#[async_trait]
impl TokenProvider for InstanceCredentialProvider {
    type Credential = AwsCredential;

    async fn fetch_token(
        &self,
        client: &Client,
        retry: &RetryConfig,
    ) -> Result<TemporaryToken<Arc<AwsCredential>>> {
        instance_creds(client, retry, &self.metadata_endpoint, self.imdsv1_fallback)
            .await
            .map_err(|source| crate::Error::Generic {
                store: STORE,
                source,
            })
    }
}

/// Credentials sourced using AssumeRoleWithWebIdentity
///
/// <https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts-technical-overview.html>
#[derive(Debug)]
pub(crate) struct WebIdentityProvider {
    pub token_path: String,
    pub role_arn: String,
    pub session_name: String,
    pub endpoint: String,
}

#[async_trait]
impl TokenProvider for WebIdentityProvider {
    type Credential = AwsCredential;

    async fn fetch_token(
        &self,
        client: &Client,
        retry: &RetryConfig,
    ) -> Result<TemporaryToken<Arc<AwsCredential>>> {
        web_identity(
            client,
            retry,
            &self.token_path,
            &self.role_arn,
            &self.session_name,
            &self.endpoint,
        )
        .await
        .map_err(|source| crate::Error::Generic {
            store: STORE,
            source,
        })
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
    imdsv1_fallback: bool,
) -> Result<TemporaryToken<Arc<AwsCredential>>, StdError> {
    const CREDENTIALS_PATH: &str = "latest/meta-data/iam/security-credentials";
    const AWS_EC2_METADATA_TOKEN_HEADER: &str = "X-aws-ec2-metadata-token";

    let token_url = format!("{endpoint}/latest/api/token");

    let token_result = client
        .request(Method::PUT, token_url)
        .header("X-aws-ec2-metadata-token-ttl-seconds", "600") // 10 minute TTL
        .retryable(retry_config)
        .idempotent(true)
        .send()
        .await;

    let token = match token_result {
        Ok(t) => Some(t.text().await?),
        Err(e) if imdsv1_fallback && matches!(e.status(), Some(StatusCode::FORBIDDEN)) => {
            warn!("received 403 from metadata endpoint, falling back to IMDSv1");
            None
        }
        Err(e) => return Err(e.into()),
    };

    let role_url = format!("{endpoint}/{CREDENTIALS_PATH}/");
    let mut role_request = client.request(Method::GET, role_url);

    if let Some(token) = &token {
        role_request = role_request.header(AWS_EC2_METADATA_TOKEN_HEADER, token);
    }

    let role = role_request.send_retry(retry_config).await?.text().await?;

    let creds_url = format!("{endpoint}/{CREDENTIALS_PATH}/{role}");
    let mut creds_request = client.request(Method::GET, creds_url);
    if let Some(token) = &token {
        creds_request = creds_request.header(AWS_EC2_METADATA_TOKEN_HEADER, token);
    }

    let creds: InstanceCredentials = creds_request.send_retry(retry_config).await?.json().await?;

    let now = Utc::now();
    let ttl = (creds.expiration - now).to_std().unwrap_or_default();
    Ok(TemporaryToken {
        token: Arc::new(creds.into()),
        expiry: Some(Instant::now() + ttl),
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
    credentials: SessionCredentials,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct SessionCredentials {
    session_token: String,
    secret_access_key: String,
    access_key_id: String,
    expiration: DateTime<Utc>,
}

impl From<SessionCredentials> for AwsCredential {
    fn from(s: SessionCredentials) -> Self {
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
    token_path: &str,
    role_arn: &str,
    session_name: &str,
    endpoint: &str,
) -> Result<TemporaryToken<Arc<AwsCredential>>, StdError> {
    let token = std::fs::read_to_string(token_path)
        .map_err(|e| format!("Failed to read token file '{token_path}': {e}"))?;

    let bytes = client
        .request(Method::POST, endpoint)
        .query(&[
            ("Action", "AssumeRoleWithWebIdentity"),
            ("DurationSeconds", "3600"),
            ("RoleArn", role_arn),
            ("RoleSessionName", session_name),
            ("Version", "2011-06-15"),
            ("WebIdentityToken", &token),
        ])
        .retryable(retry_config)
        .idempotent(true)
        .sensitive(true)
        .send()
        .await?
        .bytes()
        .await?;

    let resp: AssumeRoleResponse = quick_xml::de::from_reader(bytes.reader())
        .map_err(|e| format!("Invalid AssumeRoleWithWebIdentity response: {e}"))?;

    let creds = resp.assume_role_with_web_identity_result.credentials;
    let now = Utc::now();
    let ttl = (creds.expiration - now).to_std().unwrap_or_default();

    Ok(TemporaryToken {
        token: Arc::new(creds.into()),
        expiry: Some(Instant::now() + ttl),
    })
}

/// Credentials sourced from a task IAM role
///
/// <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html>
#[derive(Debug)]
pub(crate) struct TaskCredentialProvider {
    pub url: String,
    pub retry: RetryConfig,
    pub client: Client,
    pub cache: TokenCache<Arc<AwsCredential>>,
}

#[async_trait]
impl CredentialProvider for TaskCredentialProvider {
    type Credential = AwsCredential;

    async fn get_credential(&self) -> Result<Arc<AwsCredential>> {
        self.cache
            .get_or_insert_with(|| task_credential(&self.client, &self.retry, &self.url))
            .await
            .map_err(|source| crate::Error::Generic {
                store: STORE,
                source,
            })
    }
}

/// <https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html>
async fn task_credential(
    client: &Client,
    retry: &RetryConfig,
    url: &str,
) -> Result<TemporaryToken<Arc<AwsCredential>>, StdError> {
    let creds: InstanceCredentials = client.get(url).send_retry(retry).await?.json().await?;

    let now = Utc::now();
    let ttl = (creds.expiration - now).to_std().unwrap_or_default();
    Ok(TemporaryToken {
        token: Arc::new(creds.into()),
        expiry: Some(Instant::now() + ttl),
    })
}

/// A session provider as used by S3 Express One Zone
///
/// <https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateSession.html>
#[derive(Debug)]
pub(crate) struct SessionProvider {
    pub endpoint: String,
    pub region: String,
    pub credentials: AwsCredentialProvider,
}

#[async_trait]
impl TokenProvider for SessionProvider {
    type Credential = AwsCredential;

    async fn fetch_token(
        &self,
        client: &Client,
        retry: &RetryConfig,
    ) -> Result<TemporaryToken<Arc<Self::Credential>>> {
        let creds = self.credentials.get_credential().await?;
        let authorizer = AwsAuthorizer::new(&creds, "s3", &self.region);

        let bytes = client
            .get(format!("{}?session", self.endpoint))
            .with_aws_sigv4(Some(authorizer), None)
            .send_retry(retry)
            .await
            .context(CreateSessionRequestSnafu)?
            .bytes()
            .await
            .context(CreateSessionResponseSnafu)?;

        let resp: CreateSessionOutput =
            quick_xml::de::from_reader(bytes.reader()).context(CreateSessionOutputSnafu)?;

        let creds = resp.credentials;
        Ok(TemporaryToken {
            token: Arc::new(creds.into()),
            // Credentials last 5 minutes - https://docs.aws.amazon.com/AmazonS3/latest/API/API_CreateSession.html
            expiry: Some(Instant::now() + Duration::from_secs(5 * 60)),
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CreateSessionOutput {
    credentials: SessionCredentials,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::mock_server::MockServer;
    use hyper::Response;
    use reqwest::{Client, Method};
    use std::env;

    // Test generated using https://docs.aws.amazon.com/general/latest/gr/sigv4-signed-request-examples.html
    #[test]
    fn test_sign_with_signed_payload() {
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

        let signer = AwsAuthorizer {
            date: Some(date),
            credential: &credential,
            service: "ec2",
            region: "us-east-1",
            sign_payload: true,
            token_header: None,
        };

        signer.authorize(&mut request, None);
        assert_eq!(request.headers().get(&AUTHORIZATION).unwrap(), "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20220806/us-east-1/ec2/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=a3c787a7ed37f7fdfbfd2d7056a3d7c9d85e6d52a2bfbec73793c0be6e7862d4")
    }

    #[test]
    fn test_sign_with_unsigned_payload() {
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

        let authorizer = AwsAuthorizer {
            date: Some(date),
            credential: &credential,
            service: "ec2",
            region: "us-east-1",
            token_header: None,
            sign_payload: false,
        };

        authorizer.authorize(&mut request, None);
        assert_eq!(request.headers().get(&AUTHORIZATION).unwrap(), "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20220806/us-east-1/ec2/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=653c3d8ea261fd826207df58bc2bb69fbb5003e9eb3c0ef06e4a51f2a81d8699");
    }

    #[test]
    fn signed_get_url() {
        // Values from https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html
        let credential = AwsCredential {
            key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            token: None,
        };

        let date = DateTime::parse_from_rfc3339("2013-05-24T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        let authorizer = AwsAuthorizer {
            date: Some(date),
            credential: &credential,
            service: "s3",
            region: "us-east-1",
            token_header: None,
            sign_payload: false,
        };

        let mut url = Url::parse("https://examplebucket.s3.amazonaws.com/test.txt").unwrap();
        authorizer.sign(Method::GET, &mut url, Duration::from_secs(86400));

        assert_eq!(
            url,
            Url::parse(
                "https://examplebucket.s3.amazonaws.com/test.txt?\
                X-Amz-Algorithm=AWS4-HMAC-SHA256&\
                X-Amz-Credential=AKIAIOSFODNN7EXAMPLE%2F20130524%2Fus-east-1%2Fs3%2Faws4_request&\
                X-Amz-Date=20130524T000000Z&\
                X-Amz-Expires=86400&\
                X-Amz-SignedHeaders=host&\
                X-Amz-Signature=aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
            )
            .unwrap()
        );
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

        let authorizer = AwsAuthorizer {
            date: Some(date),
            credential: &credential,
            service: "s3",
            region: "us-east-1",
            token_header: None,
            sign_payload: true,
        };

        authorizer.authorize(&mut request, None);
        assert_eq!(request.headers().get(&AUTHORIZATION).unwrap(), "AWS4-HMAC-SHA256 Credential=H20ABqCkLZID4rLe/20220809/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=9ebf2f92872066c99ac94e573b4e1b80f4dbb8a32b1e8e23178318746e7d1b4d")
    }

    #[tokio::test]
    async fn test_instance_metadata() {
        if env::var("TEST_INTEGRATION").is_err() {
            eprintln!("skipping AWS integration test");
            return;
        }

        // For example https://github.com/aws/amazon-ec2-metadata-mock
        let endpoint = env::var("EC2_METADATA_ENDPOINT").unwrap();
        let client = Client::new();
        let retry_config = RetryConfig::default();

        // Verify only allows IMDSv2
        let resp = client
            .request(Method::GET, format!("{endpoint}/latest/meta-data/ami-id"))
            .send()
            .await
            .unwrap();

        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "Ensure metadata endpoint is set to only allow IMDSv2"
        );

        let creds = instance_creds(&client, &retry_config, &endpoint, false)
            .await
            .unwrap();

        let id = &creds.token.key_id;
        let secret = &creds.token.secret_key;
        let token = creds.token.token.as_ref().unwrap();

        assert!(!id.is_empty());
        assert!(!secret.is_empty());
        assert!(!token.is_empty())
    }

    #[tokio::test]
    async fn test_mock() {
        let server = MockServer::new().await;

        const IMDSV2_HEADER: &str = "X-aws-ec2-metadata-token";

        let secret_access_key = "SECRET";
        let access_key_id = "KEYID";
        let token = "TOKEN";

        let endpoint = server.url();
        let client = Client::new();
        let retry_config = RetryConfig::default();

        // Test IMDSv2
        server.push_fn(|req| {
            assert_eq!(req.uri().path(), "/latest/api/token");
            assert_eq!(req.method(), &Method::PUT);
            Response::new("cupcakes".to_string())
        });
        server.push_fn(|req| {
            assert_eq!(
                req.uri().path(),
                "/latest/meta-data/iam/security-credentials/"
            );
            assert_eq!(req.method(), &Method::GET);
            let t = req.headers().get(IMDSV2_HEADER).unwrap().to_str().unwrap();
            assert_eq!(t, "cupcakes");
            Response::new("myrole".to_string())
        });
        server.push_fn(|req| {
            assert_eq!(req.uri().path(), "/latest/meta-data/iam/security-credentials/myrole");
            assert_eq!(req.method(), &Method::GET);
            let t = req.headers().get(IMDSV2_HEADER).unwrap().to_str().unwrap();
            assert_eq!(t, "cupcakes");
            Response::new(r#"{"AccessKeyId":"KEYID","Code":"Success","Expiration":"2022-08-30T10:51:04Z","LastUpdated":"2022-08-30T10:21:04Z","SecretAccessKey":"SECRET","Token":"TOKEN","Type":"AWS-HMAC"}"#.to_string())
        });

        let creds = instance_creds(&client, &retry_config, endpoint, true)
            .await
            .unwrap();

        assert_eq!(creds.token.token.as_deref().unwrap(), token);
        assert_eq!(&creds.token.key_id, access_key_id);
        assert_eq!(&creds.token.secret_key, secret_access_key);

        // Test IMDSv1 fallback
        server.push_fn(|req| {
            assert_eq!(req.uri().path(), "/latest/api/token");
            assert_eq!(req.method(), &Method::PUT);
            Response::builder()
                .status(StatusCode::FORBIDDEN)
                .body(String::new())
                .unwrap()
        });
        server.push_fn(|req| {
            assert_eq!(
                req.uri().path(),
                "/latest/meta-data/iam/security-credentials/"
            );
            assert_eq!(req.method(), &Method::GET);
            assert!(req.headers().get(IMDSV2_HEADER).is_none());
            Response::new("myrole".to_string())
        });
        server.push_fn(|req| {
            assert_eq!(req.uri().path(), "/latest/meta-data/iam/security-credentials/myrole");
            assert_eq!(req.method(), &Method::GET);
            assert!(req.headers().get(IMDSV2_HEADER).is_none());
            Response::new(r#"{"AccessKeyId":"KEYID","Code":"Success","Expiration":"2022-08-30T10:51:04Z","LastUpdated":"2022-08-30T10:21:04Z","SecretAccessKey":"SECRET","Token":"TOKEN","Type":"AWS-HMAC"}"#.to_string())
        });

        let creds = instance_creds(&client, &retry_config, endpoint, true)
            .await
            .unwrap();

        assert_eq!(creds.token.token.as_deref().unwrap(), token);
        assert_eq!(&creds.token.key_id, access_key_id);
        assert_eq!(&creds.token.secret_key, secret_access_key);

        // Test IMDSv1 fallback disabled
        server.push(
            Response::builder()
                .status(StatusCode::FORBIDDEN)
                .body(String::new())
                .unwrap(),
        );

        // Should fail
        instance_creds(&client, &retry_config, endpoint, false)
            .await
            .unwrap_err();
    }
}
