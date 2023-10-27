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

//! A DynamoDB based lock system

use crate::aws::client::S3Client;
use crate::aws::credential::CredentialExt;
use crate::client::get::GetClientExt;
use crate::client::retry::Error as RetryError;
use crate::client::retry::RetryExt;
use crate::path::Path;
use crate::{Error, GetOptions, Result};
use chrono::Utc;
use reqwest::StatusCode;
use serde::ser::SerializeMap;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// The exception returned by DynamoDB on conflict
const CONFLICT: &str = "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException";

/// A DynamoDB-based commit protocol, used to provide conditional write support for S3
///
/// ## Limitations
///
/// Only conditional operations, e.g. `copy_if_not_exists` will be synchronized, and can
/// therefore race with non-conditional operations, e.g. `put`, `copy`, or conditional
/// operations performed by writers not configured to synchronize with DynamoDB.
///
/// Workloads making use of this mechanism **must** ensure:
///
/// * Conditional and non-conditional operations are not performed on the same paths
/// * Conditional operations are only performed via similarly configured clients
///
/// Additionally as the locking mechanism relies on timeouts to detect stale locks,
/// performance will be poor for systems that frequently delete and then create
/// objects at the same path, instead being optimised for systems that primarily create
/// files with paths never used before, or perform conditional updates to existing files
///
/// ## Commit Protocol
///
/// The DynamoDB schema is as follows:
///
/// * A string hash key named `"key"`
/// * A numeric [TTL] attribute named `"ttl"`
/// * A numeric attribute named `"generation"`
/// * A numeric attribute named `"timeout"`
///
/// To perform a conditional operation on an object with a given `path` and `etag` (if exists),
/// the commit protocol is as follows:
///
/// 1. Perform HEAD request on `path` and error on precondition mismatch
/// 2. Create record in DynamoDB with key `{path}#{etag}` with the configured timeout
///     1. On Success: Perform operation with the configured timeout
///     2. On Conflict:
///         1. Periodically re-perform HEAD request on `path` and error on precondition mismatch
///         2. If `timeout * max_skew_rate` passed, replace the record incrementing the `"generation"`
///             1. On Success: GOTO 2.1
///             2. On Conflict: GOTO 2.2
///
/// Provided no writer modifies an object with a given `path` and `etag` without first adding a
/// corresponding record to DynamoDB, we are guaranteed that only one writer will ever commit.
///
/// This is inspired by the [DynamoDB Lock Client] but simplified for the more limited
/// requirements of synchronizing object storage. The major changes are:
///
/// * Uses a monotonic generation count instead of a UUID rvn, as this is:
///     * Cheaper to generate, serialize and compare
///     * Cannot collide
///     * More human readable / interpretable
/// * Relies on [TTL] to eventually clean up old locks
///
/// It also draws inspiration from the DeltaLake [S3 Multi-Cluster] commit protocol, but
/// generalised to not make assumptions about the workload and not rely on first writing
/// to a temporary path.
///
/// [TTL]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/howitworks-ttl.html
/// [DynamoDB Lock Client]: https://aws.amazon.com/blogs/database/building-distributed-locks-with-the-dynamodb-lock-client/
/// [S3 Multi-Cluster]: https://docs.google.com/document/d/1Gs4ZsTH19lMxth4BSdwlWjUNR-XhKHicDvBjd2RqNd8/edit#heading=h.mjjuxw9mcz9h
#[derive(Debug, Clone)]
pub struct DynamoCommit {
    table_name: String,
    /// The number of seconds a lease is valid for
    timeout: usize,
    /// The maximum clock skew rate tolerated by the system
    max_clock_skew_rate: u32,
    /// The length of time a record will be retained in DynamoDB before being cleaned up
    ///
    /// This is purely an optimisation to avoid indefinite growth of the DynamoDB table
    /// and does not impact how long clients may wait to acquire a lock
    ttl: Duration,
    /// The backoff duration before retesting a condition
    test_interval: Duration,
}

impl DynamoCommit {
    /// Create a new [`DynamoCommit`] with a given table name
    pub fn new(table_name: String) -> Self {
        Self {
            table_name,
            timeout: 20,
            max_clock_skew_rate: 3,
            ttl: Duration::from_secs(60 * 60),
            test_interval: Duration::from_millis(100),
        }
    }

    /// Returns the name of the DynamoDB table
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub(crate) async fn copy_if_not_exists(
        &self,
        client: &S3Client,
        from: &Path,
        to: &Path,
    ) -> Result<()> {
        check_not_exists(client, to).await?;

        let mut previous_lease = None;

        loop {
            let existing = previous_lease.as_ref();
            match self.try_lock(client, to.as_ref(), existing).await? {
                TryLockResult::Ok(lease) => {
                    let fut = client.copy_request(from, to).send();
                    let expiry = lease.acquire + lease.timeout;
                    return match tokio::time::timeout_at(expiry.into(), fut).await {
                        Ok(Ok(_)) => Ok(()),
                        Ok(Err(e)) => Err(e),
                        Err(_) => Err(Error::Generic {
                            store: "DynamoDB",
                            source: format!(
                                "Failed to perform copy operation in {} seconds",
                                self.timeout
                            )
                            .into(),
                        }),
                    };
                }
                TryLockResult::Conflict(conflict) => {
                    let mut interval = tokio::time::interval(self.test_interval);
                    let expiry = conflict.timeout * self.max_clock_skew_rate;
                    loop {
                        interval.tick().await;
                        check_not_exists(client, to).await?;
                        if conflict.acquire.elapsed() > expiry {
                            previous_lease = Some(conflict);
                            break;
                        }
                    }
                }
            }
        }
    }

    async fn try_lock(
        &self,
        s3: &S3Client,
        key: &str,
        existing: Option<&Lease>,
    ) -> Result<TryLockResult> {
        let attributes;
        let (next_gen, condition_expression, expression_attribute_values) = match existing {
            None => (0_usize, "attribute_not_exists(#pk)", Map(&[])),
            Some(existing) => {
                attributes = [(":g", AttributeValue::Number(existing.generation))];
                (
                    existing.generation.checked_add(1).unwrap(),
                    "attribute_exists(#pk) AND generation == :g",
                    Map(attributes.as_slice()),
                )
            }
        };

        let ttl = (Utc::now() + self.ttl).timestamp();
        let items = [
            ("key", AttributeValue::String(key)),
            ("generation", AttributeValue::Number(next_gen)),
            ("timeout", AttributeValue::Number(self.timeout)),
            ("ttl", AttributeValue::Number(ttl as _)),
        ];
        let names = [("#pk", "key")];

        let req = PutItem {
            table_name: &self.table_name,
            condition_expression,
            expression_attribute_values,
            expression_attribute_names: Map(&names),
            item: Map(&items),
            return_values: None,
            return_values_on_condition_check_failure: Some(ReturnValues::AllOld),
        };

        let credential = s3.config.get_credential().await?;

        let acquire = Instant::now();
        let region = &s3.config.region;

        let builder = match &s3.config.endpoint {
            Some(e) => s3.client.post(e),
            None => {
                let url = format!("https://dynamodb.{region}.amazonaws.com",);
                s3.client.post(url)
            }
        };

        let response = builder
            .json(&req)
            .header("X-Amz-Target", "DynamoDB_20120810.PutItem")
            .with_aws_sigv4(credential.as_deref(), region, "dynamodb", true, None)
            .send_retry(&s3.config.retry_config)
            .await;

        match response {
            Ok(_) => Ok(TryLockResult::Ok(Lease {
                acquire,
                generation: next_gen,
                timeout: Duration::from_secs(self.timeout as _),
            })),
            Err(e) => match try_extract_lease(&e) {
                Some(lease) => Ok(TryLockResult::Conflict(lease)),
                None => Err(Error::Generic {
                    store: "DynamoDB",
                    source: Box::new(e),
                }),
            },
        }
    }
}

#[derive(Debug)]
enum TryLockResult {
    /// Successfully acquired a lease
    Ok(Lease),
    /// An existing lease was found
    Conflict(Lease),
}

/// Returns an [`Error::AlreadyExists`] if `path` exists
async fn check_not_exists(client: &S3Client, path: &Path) -> Result<()> {
    let options = GetOptions {
        head: true,
        ..Default::default()
    };
    match client.get_opts(path, options).await {
        Ok(_) => Err(Error::AlreadyExists {
            path: path.to_string(),
            source: "Already Exists".to_string().into(),
        }),
        Err(Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(e),
    }
}

/// If [`RetryError`] corresponds to [`CONFLICT`] extracts the pre-existing [`Lease`]
fn try_extract_lease(e: &RetryError) -> Option<Lease> {
    match e {
        RetryError::Client {
            status: StatusCode::BAD_REQUEST,
            body: Some(b),
        } => {
            let resp: ErrorResponse<'_> = serde_json::from_str(b).ok()?;
            if resp.error != CONFLICT {
                return None;
            }

            let generation = match resp.item.get("generation") {
                Some(AttributeValue::Number(generation)) => generation,
                _ => return None,
            };

            let timeout = match resp.item.get("timeout") {
                Some(AttributeValue::Number(timeout)) => *timeout,
                _ => return None,
            };

            Some(Lease {
                acquire: Instant::now(),
                generation: *generation,
                timeout: Duration::from_secs(timeout as _),
            })
        }
        _ => None,
    }
}

/// A lock lease
#[derive(Debug, Clone)]
struct Lease {
    acquire: Instant,
    generation: usize,
    timeout: Duration,
}

/// A DynamoDB [PutItem] payload
///
/// [PutItem]: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_PutItem.html
#[derive(Serialize)]
#[serde(rename_all = "PascalCase")]
struct PutItem<'a> {
    /// The table name
    table_name: &'a str,

    /// A condition that must be satisfied in order for a conditional PutItem operation to succeed.
    condition_expression: &'a str,

    /// One or more substitution tokens for attribute names in an expression
    expression_attribute_names: Map<'a, &'a str, &'a str>,

    /// One or more values that can be substituted in an expression
    expression_attribute_values: Map<'a, &'a str, AttributeValue<'a>>,

    /// A map of attribute name/value pairs, one for each attribute
    item: Map<'a, &'a str, AttributeValue<'a>>,

    /// Use ReturnValues if you want to get the item attributes as they appeared
    /// before they were updated with the PutItem request.
    #[serde(skip_serializing_if = "Option::is_none")]
    return_values: Option<ReturnValues>,

    /// An optional parameter that returns the item attributes for a PutItem operation
    /// that failed a condition check.
    #[serde(skip_serializing_if = "Option::is_none")]
    return_values_on_condition_check_failure: Option<ReturnValues>,
}

#[derive(Deserialize)]
struct ErrorResponse<'a> {
    #[serde(rename = "__type")]
    error: &'a str,

    #[serde(borrow, default, rename = "Item")]
    item: HashMap<&'a str, AttributeValue<'a>>,
}

#[derive(Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
enum ReturnValues {
    AllOld,
}

/// A collection of key value pairs
///
/// This provides cheap, ordered serialization of maps
struct Map<'a, K, V>(&'a [(K, V)]);

impl<'a, K: Serialize, V: Serialize> Serialize for Map<'a, K, V> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if self.0.is_empty() {
            return serializer.serialize_none();
        }
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (k, v) in self.0 {
            map.serialize_entry(k, v)?
        }
        map.end()
    }
}

/// A DynamoDB [AttributeValue]
///
/// [AttributeValue]: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_AttributeValue.html
#[derive(Debug, Serialize, Deserialize)]
enum AttributeValue<'a> {
    #[serde(rename = "S")]
    String(&'a str),
    #[serde(rename = "N", with = "number")]
    Number(usize),
}

/// Numbers are serialized as strings
mod number {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &usize, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&v.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<usize, D::Error> {
        let v: &str = Deserialize::deserialize(d)?;
        v.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_attribute_serde() {
        let serde = serde_json::to_string(&AttributeValue::Number(23)).unwrap();
        assert_eq!(serde, "{\"N\":\"23\"}");
        let back: AttributeValue<'_> = serde_json::from_str(&serde).unwrap();
        assert!(matches!(back, AttributeValue::Number(23)));
    }
}
