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
use crate::RetryConfig;
use reqwest::{Client, Method};
use ring::signature::RsaKeyPair;
use snafu::{ResultExt, Snafu};
use std::time::{Duration, Instant};

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

    #[snafu(display("Unsupported key encoding: {}", encoding))]
    UnsupportedKey { encoding: String },

    #[snafu(display("Error performing token request: {}", source))]
    TokenRequest { source: crate::client::retry::Error },

    #[snafu(display("Error getting token response body: {}", source))]
    TokenResponseBody { source: reqwest::Error },
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

    /// Fetch a fresh token
    pub async fn fetch_token(
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

        let signature = base64::encode_config(&sig_bytes, base64::URL_SAFE_NO_PAD);
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
            expiry: Instant::now() + Duration::from_secs(response.expires_in),
        };

        Ok(token)
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
    use std::io::{BufReader, Cursor};

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
    Ok(base64::encode_config(string, base64::URL_SAFE_NO_PAD))
}
