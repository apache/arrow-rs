use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures::{FutureExt, Stream, StreamExt, ready};
use tonic::{Status, Streaming, metadata::MetadataMap};

/// Extract [`LazyTrailers`] from [`Streaming`] [tonic] response.
///
/// Note that [`LazyTrailers`] has inner mutability and will only hold actual data after [`ExtractTrailersStream`] is
/// fully consumed (dropping it is not required though).
pub fn extract_lazy_trailers<T>(s: Streaming<T>) -> (ExtractTrailersStream<T>, LazyTrailers) {
    let trailers: SharedTrailers = Default::default();
    let stream = ExtractTrailersStream {
        inner: s,
        trailers: Arc::clone(&trailers),
    };
    let lazy_trailers = LazyTrailers { trailers };
    (stream, lazy_trailers)
}

type SharedTrailers = Arc<Mutex<Option<MetadataMap>>>;

/// [Stream] that stores the gRPC trailers into [`LazyTrailers`].
///
/// See [`extract_lazy_trailers`] for construction.
#[derive(Debug)]
pub struct ExtractTrailersStream<T> {
    inner: Streaming<T>,
    trailers: SharedTrailers,
}

impl<T> Stream for ExtractTrailersStream<T> {
    type Item = Result<T, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let res = ready!(self.inner.poll_next_unpin(cx));

        if res.is_none() {
            // stream exhausted => trailers should available
            if let Some(trailers) = self
                .inner
                .trailers()
                .now_or_never()
                .and_then(|res| res.ok())
                .flatten()
            {
                *self.trailers.lock().expect("poisoned") = Some(trailers);
            }
        }

        Poll::Ready(res)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// gRPC trailers that are extracted by [`ExtractTrailersStream`].
///
/// See [`extract_lazy_trailers`] for construction.
#[derive(Debug)]
pub struct LazyTrailers {
    trailers: SharedTrailers,
}

impl LazyTrailers {
    /// gRPC trailers that are known at the end of a stream.
    pub fn get(&self) -> Option<MetadataMap> {
        self.trailers.lock().expect("poisoned").clone()
    }
}
