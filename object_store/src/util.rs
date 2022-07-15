//! Common logic for interacting with remote object stores
use super::Result;
use bytes::Bytes;
use futures::{stream::StreamExt, Stream};

/// Returns the prefix to be passed to an object store
#[cfg(any(feature = "aws", feature = "gcp", feature = "azure"))]
pub fn format_prefix(prefix: Option<&crate::path::Path>) -> Option<String> {
    prefix
        .filter(|x| !x.as_ref().is_empty())
        .map(|p| format!("{}{}", p.as_ref(), crate::path::DELIMITER))
}

/// Returns a formatted HTTP range header as per
/// <https://httpwg.org/specs/rfc7233.html#header.range>
#[cfg(any(feature = "aws", feature = "gcp"))]
pub fn format_http_range(range: std::ops::Range<usize>) -> String {
    format!("bytes={}-{}", range.start, range.end.saturating_sub(1))
}

/// Collect a stream into [`Bytes`] avoiding copying in the event of a single chunk
pub async fn collect_bytes<S>(mut stream: S, size_hint: Option<usize>) -> Result<Bytes>
where
    S: Stream<Item = Result<Bytes>> + Send + Unpin,
{
    let first = stream.next().await.transpose()?.unwrap_or_default();

    // Avoid copying if single response
    match stream.next().await.transpose()? {
        None => Ok(first),
        Some(second) => {
            let size_hint = size_hint.unwrap_or_else(|| first.len() + second.len());

            let mut buf = Vec::with_capacity(size_hint);
            buf.extend_from_slice(&first);
            buf.extend_from_slice(&second);
            while let Some(maybe_bytes) = stream.next().await {
                buf.extend_from_slice(&maybe_bytes?);
            }

            Ok(buf.into())
        }
    }
}

/// Takes a function and spawns it to a tokio blocking pool if available
pub async fn maybe_spawn_blocking<F, T>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    match tokio::runtime::Handle::try_current() {
        Ok(runtime) => runtime.spawn_blocking(f).await?,
        Err(_) => f(),
    }
}
