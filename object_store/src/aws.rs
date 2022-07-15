//! An object store implementation for S3
use crate::util::format_http_range;
use crate::{
    collect_bytes,
    path::{Path, DELIMITER},
    util::format_prefix,
    GetResult, ListResult, ObjectMeta, ObjectStore, Result,
};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{
    stream::{self, BoxStream},
    Future, Stream, StreamExt, TryStreamExt,
};
use hyper::client::Builder as HyperBuilder;
use rusoto_core::ByteStream;
use rusoto_credential::{InstanceMetadataProvider, StaticProvider};
use rusoto_s3::S3;
use rusoto_sts::WebIdentityProvider;
use snafu::{OptionExt, ResultExt, Snafu};
use std::ops::Range;
use std::{convert::TryFrom, fmt, num::NonZeroUsize, ops::Deref, sync::Arc, time::Duration};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::{debug, warn};

/// The maximum number of times a request will be retried in the case of an AWS server error
pub const MAX_NUM_RETRIES: u32 = 3;

/// A specialized `Error` for object store-related errors
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
enum Error {
    #[snafu(display("Expected streamed data to have length {}, got {}", expected, actual))]
    DataDoesNotMatchLength { expected: usize, actual: usize },

    #[snafu(display("Did not receive any data. Bucket: {}, Location: {}", bucket, path))]
    NoData { bucket: String, path: String },

    #[snafu(display(
        "Unable to DELETE data. Bucket: {}, Location: {}, Error: {} ({:?})",
        bucket,
        path,
        source,
        source,
    ))]
    UnableToDeleteData {
        source: rusoto_core::RusotoError<rusoto_s3::DeleteObjectError>,
        bucket: String,
        path: String,
    },

    #[snafu(display(
        "Unable to GET data. Bucket: {}, Location: {}, Error: {} ({:?})",
        bucket,
        path,
        source,
        source,
    ))]
    UnableToGetData {
        source: rusoto_core::RusotoError<rusoto_s3::GetObjectError>,
        bucket: String,
        path: String,
    },

    #[snafu(display(
        "Unable to HEAD data. Bucket: {}, Location: {}, Error: {} ({:?})",
        bucket,
        path,
        source,
        source,
    ))]
    UnableToHeadData {
        source: rusoto_core::RusotoError<rusoto_s3::HeadObjectError>,
        bucket: String,
        path: String,
    },

    #[snafu(display(
        "Unable to GET part of the data. Bucket: {}, Location: {}, Error: {} ({:?})",
        bucket,
        path,
        source,
        source,
    ))]
    UnableToGetPieceOfData {
        source: std::io::Error,
        bucket: String,
        path: String,
    },

    #[snafu(display(
        "Unable to PUT data. Bucket: {}, Location: {}, Error: {} ({:?})",
        bucket,
        path,
        source,
        source,
    ))]
    UnableToPutData {
        source: rusoto_core::RusotoError<rusoto_s3::PutObjectError>,
        bucket: String,
        path: String,
    },

    #[snafu(display(
        "Unable to list data. Bucket: {}, Error: {} ({:?})",
        bucket,
        source,
        source,
    ))]
    UnableToListData {
        source: rusoto_core::RusotoError<rusoto_s3::ListObjectsV2Error>,
        bucket: String,
    },

    #[snafu(display(
        "Unable to copy object. Bucket: {}, From: {}, To: {}, Error: {}",
        bucket,
        from,
        to,
        source,
    ))]
    UnableToCopyObject {
        source: rusoto_core::RusotoError<rusoto_s3::CopyObjectError>,
        bucket: String,
        from: String,
        to: String,
    },

    #[snafu(display(
        "Unable to parse last modified date. Bucket: {}, Error: {} ({:?})",
        bucket,
        source,
        source,
    ))]
    UnableToParseLastModified {
        source: chrono::ParseError,
        bucket: String,
    },

    #[snafu(display(
        "Unable to buffer data into temporary file, Error: {} ({:?})",
        source,
        source,
    ))]
    UnableToBufferStream { source: std::io::Error },

    #[snafu(display(
        "Could not parse `{}` as an AWS region. Regions should look like `us-east-2`. {} ({:?})",
        region,
        source,
        source,
    ))]
    InvalidRegion {
        region: String,
        source: rusoto_core::region::ParseRegionError,
    },

    #[snafu(display("Missing aws-access-key"))]
    MissingAccessKey,

    #[snafu(display("Missing aws-secret-access-key"))]
    MissingSecretAccessKey,

    NotFound {
        path: String,
        source: Box<dyn std::error::Error + Send + Sync + 'static>,
    },
}

impl From<Error> for super::Error {
    fn from(source: Error) -> Self {
        match source {
            Error::NotFound { path, source } => Self::NotFound { path, source },
            _ => Self::Generic {
                store: "S3",
                source: Box::new(source),
            },
        }
    }
}

/// Configuration for connecting to [Amazon S3](https://aws.amazon.com/s3/).
pub struct AmazonS3 {
    /// S3 client w/o any connection limit.
    ///
    /// You should normally use [`Self::client`] instead.
    client_unrestricted: rusoto_s3::S3Client,

    /// Semaphore that limits the usage of [`client_unrestricted`](Self::client_unrestricted).
    connection_semaphore: Arc<Semaphore>,

    /// Bucket name used by this object store client.
    bucket_name: String,
}

impl fmt::Debug for AmazonS3 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AmazonS3")
            .field("client", &"rusoto_s3::S3Client")
            .field("bucket_name", &self.bucket_name)
            .finish()
    }
}

impl fmt::Display for AmazonS3 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "AmazonS3({})", self.bucket_name)
    }
}

#[async_trait]
impl ObjectStore for AmazonS3 {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let bucket_name = self.bucket_name.clone();
        let request_factory = move || {
            let bytes = bytes.clone();

            let length = bytes.len();
            let stream_data = Ok(bytes);
            let stream = futures::stream::once(async move { stream_data });
            let byte_stream = ByteStream::new_with_size(stream, length);

            rusoto_s3::PutObjectRequest {
                bucket: bucket_name.clone(),
                key: location.to_string(),
                body: Some(byte_stream),
                ..Default::default()
            }
        };

        let s3 = self.client().await;

        s3_request(move || {
            let (s3, request_factory) = (s3.clone(), request_factory.clone());

            async move { s3.put_object(request_factory()).await }
        })
        .await
        .context(UnableToPutDataSnafu {
            bucket: &self.bucket_name,
            path: location.as_ref(),
        })?;

        Ok(())
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        Ok(GetResult::Stream(
            self.get_object(location, None).await?.boxed(),
        ))
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let size_hint = range.end - range.start;
        let stream = self.get_object(location, Some(range)).await?;
        collect_bytes(stream, Some(size_hint)).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let key = location.to_string();
        let head_request = rusoto_s3::HeadObjectRequest {
            bucket: self.bucket_name.clone(),
            key: key.clone(),
            ..Default::default()
        };
        let s = self
            .client()
            .await
            .head_object(head_request)
            .await
            .map_err(|e| match e {
                rusoto_core::RusotoError::Service(rusoto_s3::HeadObjectError::NoSuchKey(_)) => {
                    Error::NotFound {
                        path: key.clone(),
                        source: e.into(),
                    }
                }
                rusoto_core::RusotoError::Unknown(h) if h.status.as_u16() == 404 => {
                    Error::NotFound {
                        path: key.clone(),
                        source: "resource not found".into(),
                    }
                }
                _ => Error::UnableToHeadData {
                    bucket: self.bucket_name.to_owned(),
                    path: key.clone(),
                    source: e,
                },
            })?;

        // Note: GetObject and HeadObject return a different date format from ListObjects
        //
        // S3 List returns timestamps in the form
        //     <LastModified>2013-09-17T18:07:53.000Z</LastModified>
        // S3 GetObject returns timestamps in the form
        //            Last-Modified: Sun, 1 Jan 2006 12:00:00 GMT
        let last_modified = match s.last_modified {
            Some(lm) => DateTime::parse_from_rfc2822(&lm)
                .context(UnableToParseLastModifiedSnafu {
                    bucket: &self.bucket_name,
                })?
                .with_timezone(&Utc),
            None => Utc::now(),
        };

        Ok(ObjectMeta {
            last_modified,
            location: location.clone(),
            size: usize::try_from(s.content_length.unwrap_or(0))
                .expect("unsupported size on this platform"),
        })
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let bucket_name = self.bucket_name.clone();

        let request_factory = move || rusoto_s3::DeleteObjectRequest {
            bucket: bucket_name.clone(),
            key: location.to_string(),
            ..Default::default()
        };

        let s3 = self.client().await;

        s3_request(move || {
            let (s3, request_factory) = (s3.clone(), request_factory.clone());

            async move { s3.delete_object(request_factory()).await }
        })
        .await
        .context(UnableToDeleteDataSnafu {
            bucket: &self.bucket_name,
            path: location.as_ref(),
        })?;

        Ok(())
    }

    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        Ok(self
            .list_objects_v2(prefix, None)
            .await?
            .map_ok(move |list_objects_v2_result| {
                let contents = list_objects_v2_result.contents.unwrap_or_default();
                let iter = contents
                    .into_iter()
                    .map(|object| convert_object_meta(object, &self.bucket_name));

                futures::stream::iter(iter)
            })
            .try_flatten()
            .boxed())
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        Ok(self
            .list_objects_v2(prefix, Some(DELIMITER.to_string()))
            .await?
            .try_fold(
                ListResult {
                    common_prefixes: vec![],
                    objects: vec![],
                },
                |acc, list_objects_v2_result| async move {
                    let mut res = acc;
                    let contents = list_objects_v2_result.contents.unwrap_or_default();
                    let mut objects = contents
                        .into_iter()
                        .map(|object| convert_object_meta(object, &self.bucket_name))
                        .collect::<Result<Vec<_>>>()?;

                    res.objects.append(&mut objects);

                    let prefixes = list_objects_v2_result.common_prefixes.unwrap_or_default();
                    res.common_prefixes.reserve(prefixes.len());

                    for p in prefixes {
                        let prefix = p.prefix.expect("can't have a prefix without a value");
                        res.common_prefixes.push(Path::parse(prefix)?);
                    }

                    Ok(res)
                },
            )
            .await?)
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let from = from.as_ref();
        let to = to.as_ref();
        let bucket_name = self.bucket_name.clone();

        let request_factory = move || rusoto_s3::CopyObjectRequest {
            bucket: bucket_name.clone(),
            copy_source: format!("{}/{}", &bucket_name, from),
            key: to.to_string(),
            ..Default::default()
        };

        let s3 = self.client().await;

        s3_request(move || {
            let (s3, request_factory) = (s3.clone(), request_factory.clone());

            async move { s3.copy_object(request_factory()).await }
        })
        .await
        .context(UnableToCopyObjectSnafu {
            bucket: &self.bucket_name,
            from,
            to,
        })?;

        Ok(())
    }

    async fn copy_if_not_exists(&self, _source: &Path, _dest: &Path) -> Result<()> {
        // Will need dynamodb_lock
        Err(crate::Error::NotImplemented)
    }
}

fn convert_object_meta(object: rusoto_s3::Object, bucket: &str) -> Result<ObjectMeta> {
    let key = object.key.expect("object doesn't exist without a key");
    let location = Path::parse(key)?;
    let last_modified = match object.last_modified {
        Some(lm) => DateTime::parse_from_rfc3339(&lm)
            .context(UnableToParseLastModifiedSnafu { bucket })?
            .with_timezone(&Utc),
        None => Utc::now(),
    };
    let size =
        usize::try_from(object.size.unwrap_or(0)).expect("unsupported size on this platform");

    Ok(ObjectMeta {
        location,
        last_modified,
        size,
    })
}

/// Configure a connection to Amazon S3 using the specified credentials in
/// the specified Amazon region and bucket.
#[allow(clippy::too_many_arguments)]
pub fn new_s3(
    access_key_id: Option<impl Into<String>>,
    secret_access_key: Option<impl Into<String>>,
    region: impl Into<String>,
    bucket_name: impl Into<String>,
    endpoint: Option<impl Into<String>>,
    session_token: Option<impl Into<String>>,
    max_connections: NonZeroUsize,
    allow_http: bool,
) -> Result<AmazonS3> {
    let region = region.into();
    let region: rusoto_core::Region = match endpoint {
        None => region.parse().context(InvalidRegionSnafu { region })?,
        Some(endpoint) => rusoto_core::Region::Custom {
            name: region,
            endpoint: endpoint.into(),
        },
    };

    let mut builder = HyperBuilder::default();
    builder.pool_max_idle_per_host(max_connections.get());

    let connector = if allow_http {
        hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .build()
    } else {
        hyper_rustls::HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_only()
            .enable_http1()
            .enable_http2()
            .build()
    };

    let http_client = rusoto_core::request::HttpClient::from_builder(builder, connector);

    let client = match (access_key_id, secret_access_key, session_token) {
        (Some(access_key_id), Some(secret_access_key), Some(session_token)) => {
            let credentials_provider = StaticProvider::new(
                access_key_id.into(),
                secret_access_key.into(),
                Some(session_token.into()),
                None,
            );
            rusoto_s3::S3Client::new_with(http_client, credentials_provider, region)
        }
        (Some(access_key_id), Some(secret_access_key), None) => {
            let credentials_provider =
                StaticProvider::new_minimal(access_key_id.into(), secret_access_key.into());
            rusoto_s3::S3Client::new_with(http_client, credentials_provider, region)
        }
        (None, Some(_), _) => return Err(Error::MissingAccessKey.into()),
        (Some(_), None, _) => return Err(Error::MissingSecretAccessKey.into()),
        _ if std::env::var_os("AWS_WEB_IDENTITY_TOKEN_FILE").is_some() => {
            rusoto_s3::S3Client::new_with(http_client, WebIdentityProvider::from_k8s_env(), region)
        }
        _ => rusoto_s3::S3Client::new_with(http_client, InstanceMetadataProvider::new(), region),
    };

    Ok(AmazonS3 {
        client_unrestricted: client,
        connection_semaphore: Arc::new(Semaphore::new(max_connections.get())),
        bucket_name: bucket_name.into(),
    })
}

/// Create a new [`AmazonS3`] that always errors
pub fn new_failing_s3() -> Result<AmazonS3> {
    new_s3(
        Some("foo"),
        Some("bar"),
        "us-east-1",
        "bucket",
        None as Option<&str>,
        None as Option<&str>,
        NonZeroUsize::new(16).unwrap(),
        true,
    )
}

/// S3 client bundled w/ a semaphore permit.
#[derive(Clone)]
struct SemaphoreClient {
    /// Permit for this specific use of the client.
    ///
    /// Note that this field is never read and therefore considered "dead code" by rustc.
    #[allow(dead_code)]
    permit: Arc<OwnedSemaphorePermit>,

    inner: rusoto_s3::S3Client,
}

impl Deref for SemaphoreClient {
    type Target = rusoto_s3::S3Client;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl AmazonS3 {
    /// Get a client according to the current connection limit.
    async fn client(&self) -> SemaphoreClient {
        let permit = Arc::clone(&self.connection_semaphore)
            .acquire_owned()
            .await
            .expect("semaphore shouldn't be closed yet");
        SemaphoreClient {
            permit: Arc::new(permit),
            inner: self.client_unrestricted.clone(),
        }
    }

    async fn get_object(
        &self,
        location: &Path,
        range: Option<Range<usize>>,
    ) -> Result<impl Stream<Item = Result<Bytes>>> {
        let key = location.to_string();
        let get_request = rusoto_s3::GetObjectRequest {
            bucket: self.bucket_name.clone(),
            key: key.clone(),
            range: range.map(format_http_range),
            ..Default::default()
        };
        let bucket_name = self.bucket_name.clone();
        let stream = self
            .client()
            .await
            .get_object(get_request)
            .await
            .map_err(|e| match e {
                rusoto_core::RusotoError::Service(rusoto_s3::GetObjectError::NoSuchKey(_)) => {
                    Error::NotFound {
                        path: key.clone(),
                        source: e.into(),
                    }
                }
                _ => Error::UnableToGetData {
                    bucket: self.bucket_name.to_owned(),
                    path: key.clone(),
                    source: e,
                },
            })?
            .body
            .context(NoDataSnafu {
                bucket: self.bucket_name.to_owned(),
                path: key.clone(),
            })?
            .map_err(move |source| Error::UnableToGetPieceOfData {
                source,
                bucket: bucket_name.clone(),
                path: key.clone(),
            })
            .err_into();

        Ok(stream)
    }

    async fn list_objects_v2(
        &self,
        prefix: Option<&Path>,
        delimiter: Option<String>,
    ) -> Result<BoxStream<'_, Result<rusoto_s3::ListObjectsV2Output>>> {
        enum ListState {
            Start,
            HasMore(String),
            Done,
        }

        let prefix = format_prefix(prefix);
        let bucket = self.bucket_name.clone();

        let request_factory = move || rusoto_s3::ListObjectsV2Request {
            bucket,
            prefix,
            delimiter,
            ..Default::default()
        };
        let s3 = self.client().await;

        Ok(stream::unfold(ListState::Start, move |state| {
            let request_factory = request_factory.clone();
            let s3 = s3.clone();

            async move {
                let continuation_token = match &state {
                    ListState::HasMore(continuation_token) => Some(continuation_token),
                    ListState::Done => {
                        return None;
                    }
                    // If this is the first request we've made, we don't need to make any
                    // modifications to the request
                    ListState::Start => None,
                };

                let resp = s3_request(move || {
                    let (s3, request_factory, continuation_token) = (
                        s3.clone(),
                        request_factory.clone(),
                        continuation_token.cloned(),
                    );

                    async move {
                        s3.list_objects_v2(rusoto_s3::ListObjectsV2Request {
                            continuation_token,
                            ..request_factory()
                        })
                        .await
                    }
                })
                .await;

                let resp = match resp {
                    Ok(resp) => resp,
                    Err(e) => return Some((Err(e), state)),
                };

                // The AWS response contains a field named `is_truncated` as well as
                // `next_continuation_token`, and we're assuming that `next_continuation_token`
                // is only set when `is_truncated` is true (and therefore not
                // checking `is_truncated`).
                let next_state =
                    if let Some(next_continuation_token) = &resp.next_continuation_token {
                        ListState::HasMore(next_continuation_token.to_string())
                    } else {
                        ListState::Done
                    };

                Some((Ok(resp), next_state))
            }
        })
        .map_err(move |e| {
            Error::UnableToListData {
                source: e,
                bucket: self.bucket_name.clone(),
            }
            .into()
        })
        .boxed())
    }
}

/// Handles retrying a request to S3 up to `MAX_NUM_RETRIES` times if S3 returns 5xx server errors.
///
/// The `future_factory` argument is a function `F` that takes no arguments and, when called, will
/// return a `Future` (type `G`) that, when `await`ed, will perform a request to S3 through
/// `rusoto` and return a `Result` that returns some type `R` on success and some
/// `rusoto_core::RusotoError<E>` on error.
///
/// If the executed `Future` returns success, this function will return that success.
/// If the executed `Future` returns a 5xx server error, this function will wait an amount of
/// time that increases exponentially with the number of times it has retried, get a new `Future` by
/// calling `future_factory` again, and retry the request by `await`ing the `Future` again.
/// The retries will continue until the maximum number of retries has been attempted. In that case,
/// this function will return the last encountered error.
///
/// Client errors (4xx) will never be retried by this function.
async fn s3_request<E, F, G, R>(future_factory: F) -> Result<R, rusoto_core::RusotoError<E>>
where
    E: std::error::Error + Send,
    F: Fn() -> G + Send,
    G: Future<Output = Result<R, rusoto_core::RusotoError<E>>> + Send,
    R: Send,
{
    let mut attempts = 0;

    loop {
        let request = future_factory();

        let result = request.await;

        match result {
            Ok(r) => return Ok(r),
            Err(error) => {
                attempts += 1;

                let should_retry = matches!(
                    error,
                    rusoto_core::RusotoError::Unknown(ref response)
                        if response.status.is_server_error()
                );

                if attempts > MAX_NUM_RETRIES {
                    warn!(
                        ?error,
                        attempts, "maximum number of retries exceeded for AWS S3 request"
                    );
                    return Err(error);
                } else if !should_retry {
                    return Err(error);
                } else {
                    debug!(?error, attempts, "retrying AWS S3 request");
                    let wait_time = Duration::from_millis(2u64.pow(attempts) * 50);
                    tokio::time::sleep(wait_time).await;
                }
            }
        }
    }
}

impl Error {
    #[cfg(test)]
    fn s3_error_due_to_credentials(&self) -> bool {
        use rusoto_core::RusotoError;
        use Error::*;

        matches!(
            self,
            UnableToPutData {
                source: RusotoError::Credentials(_),
                bucket: _,
                path: _,
            } | UnableToGetData {
                source: RusotoError::Credentials(_),
                bucket: _,
                path: _,
            } | UnableToDeleteData {
                source: RusotoError::Credentials(_),
                bucket: _,
                path: _,
            } | UnableToListData {
                source: RusotoError::Credentials(_),
                bucket: _,
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        tests::{
            get_nonexistent_object, list_uses_directories_correctly, list_with_delimiter,
            put_get_delete_list, rename_and_copy,
        },
        Error as ObjectStoreError, ObjectStore,
    };
    use bytes::Bytes;
    use std::env;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T, E = TestError> = std::result::Result<T, E>;

    const NON_EXISTENT_NAME: &str = "nonexistentname";

    #[derive(Debug)]
    struct AwsConfig {
        access_key_id: String,
        secret_access_key: String,
        region: String,
        bucket: String,
        endpoint: Option<String>,
        token: Option<String>,
    }

    // Helper macro to skip tests if TEST_INTEGRATION and the AWS environment variables are not set.
    macro_rules! maybe_skip_integration {
        () => {{
            dotenv::dotenv().ok();

            let required_vars = [
                "AWS_DEFAULT_REGION",
                "OBJECT_STORE_BUCKET",
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
            ];
            let unset_vars: Vec<_> = required_vars
                .iter()
                .filter_map(|&name| match env::var(name) {
                    Ok(_) => None,
                    Err(_) => Some(name),
                })
                .collect();
            let unset_var_names = unset_vars.join(", ");

            let force = env::var("TEST_INTEGRATION");

            if force.is_ok() && !unset_var_names.is_empty() {
                panic!(
                    "TEST_INTEGRATION is set, \
                            but variable(s) {} need to be set",
                    unset_var_names
                );
            } else if force.is_err() {
                eprintln!(
                    "skipping AWS integration test - set {}TEST_INTEGRATION to run",
                    if unset_var_names.is_empty() {
                        String::new()
                    } else {
                        format!("{} and ", unset_var_names)
                    }
                );
                return;
            } else {
                AwsConfig {
                    access_key_id: env::var("AWS_ACCESS_KEY_ID")
                        .expect("already checked AWS_ACCESS_KEY_ID"),
                    secret_access_key: env::var("AWS_SECRET_ACCESS_KEY")
                        .expect("already checked AWS_SECRET_ACCESS_KEY"),
                    region: env::var("AWS_DEFAULT_REGION")
                        .expect("already checked AWS_DEFAULT_REGION"),
                    bucket: env::var("OBJECT_STORE_BUCKET")
                        .expect("already checked OBJECT_STORE_BUCKET"),
                    endpoint: env::var("AWS_ENDPOINT").ok(),
                    token: env::var("AWS_SESSION_TOKEN").ok(),
                }
            }
        }};
    }

    fn check_credentials<T>(r: Result<T>) -> Result<T> {
        if let Err(e) = &r {
            let e = &**e;
            if let Some(e) = e.downcast_ref::<Error>() {
                if e.s3_error_due_to_credentials() {
                    eprintln!(
                        "Try setting the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY \
                               environment variables"
                    );
                }
            }
        }

        r
    }

    fn make_integration(config: AwsConfig) -> AmazonS3 {
        new_s3(
            Some(config.access_key_id),
            Some(config.secret_access_key),
            config.region,
            config.bucket,
            config.endpoint,
            config.token,
            NonZeroUsize::new(16).unwrap(),
            true,
        )
        .expect("Valid S3 config")
    }

    #[tokio::test]
    async fn s3_test() {
        let config = maybe_skip_integration!();
        let integration = make_integration(config);

        check_credentials(put_get_delete_list(&integration).await).unwrap();
        check_credentials(list_uses_directories_correctly(&integration).await).unwrap();
        check_credentials(list_with_delimiter(&integration).await).unwrap();
        check_credentials(rename_and_copy(&integration).await).unwrap();
    }

    #[tokio::test]
    async fn s3_test_get_nonexistent_location() {
        let config = maybe_skip_integration!();
        let integration = make_integration(config);

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = get_nonexistent_object(&integration, Some(location))
            .await
            .unwrap_err();
        if let ObjectStoreError::NotFound { path, source } = err {
            let source_variant = source.downcast_ref::<rusoto_core::RusotoError<_>>();
            assert!(
                matches!(
                    source_variant,
                    Some(rusoto_core::RusotoError::Service(
                        rusoto_s3::GetObjectError::NoSuchKey(_)
                    )),
                ),
                "got: {:?}",
                source_variant
            );
            assert_eq!(path, NON_EXISTENT_NAME);
        } else {
            panic!("unexpected error type: {:?}", err);
        }
    }

    #[tokio::test]
    async fn s3_test_get_nonexistent_bucket() {
        let mut config = maybe_skip_integration!();
        config.bucket = NON_EXISTENT_NAME.into();
        let integration = make_integration(config);

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = integration.get(&location).await.unwrap_err().to_string();
        assert!(
            err.contains("The specified bucket does not exist"),
            "{}",
            err
        )
    }

    #[tokio::test]
    async fn s3_test_put_nonexistent_bucket() {
        let mut config = maybe_skip_integration!();
        config.bucket = NON_EXISTENT_NAME.into();
        let integration = make_integration(config);

        let location = Path::from_iter([NON_EXISTENT_NAME]);
        let data = Bytes::from("arbitrary data");

        let err = integration
            .put(&location, data)
            .await
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("The specified bucket does not exist")
                && err.contains("Unable to PUT data"),
            "{}",
            err
        )
    }

    #[tokio::test]
    async fn s3_test_delete_nonexistent_location() {
        let config = maybe_skip_integration!();
        let integration = make_integration(config);

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        integration.delete(&location).await.unwrap();
    }

    #[tokio::test]
    async fn s3_test_delete_nonexistent_bucket() {
        let mut config = maybe_skip_integration!();
        config.bucket = NON_EXISTENT_NAME.into();
        let integration = make_integration(config);

        let location = Path::from_iter([NON_EXISTENT_NAME]);

        let err = integration.delete(&location).await.unwrap_err().to_string();
        assert!(
            err.contains("The specified bucket does not exist")
                && err.contains("Unable to DELETE data"),
            "{}",
            err
        )
    }
}
