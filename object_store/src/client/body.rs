use crate::client::connection::HttpError;
use crate::{collect_bytes, PutPayload};
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use http_body_util::combinators::BoxBody;
use http_body_util::BodyExt;
use hyper::body::{Body, Frame, SizeHint};
use std::pin::Pin;
use std::task::{Context, Poll};

/// An HTTP Request
pub type HttpRequest = http::Request<HttpRequestBody>;

/// The [`Body`] of an [`HttpRequest`]
#[derive(Debug, Clone)]
pub struct HttpRequestBody(Inner);

impl HttpRequestBody {
    pub(crate) fn into_reqwest(self) -> reqwest::Body {
        match self.0 {
            Inner::Bytes(b) => b.into(),
            Inner::PutPayload(_, payload) => reqwest::Body::wrap_stream(futures::stream::iter(
                payload.into_iter().map(Ok::<_, HttpError>),
            )),
        }
    }
}

impl From<Bytes> for HttpRequestBody {
    fn from(value: Bytes) -> Self {
        Self(Inner::Bytes(value))
    }
}

impl From<PutPayload> for HttpRequestBody {
    fn from(value: PutPayload) -> Self {
        Self(Inner::PutPayload(0, value))
    }
}

#[derive(Debug, Clone)]
enum Inner {
    Bytes(Bytes),
    PutPayload(usize, PutPayload),
}

impl Body for HttpRequestBody {
    type Data = Bytes;
    type Error = HttpError;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        Poll::Ready(match &mut self.0 {
            Inner::Bytes(bytes) => {
                let out = bytes.split_off(0);
                if out.is_empty() {
                    None
                } else {
                    Some(Ok(Frame::data(out)))
                }
            }
            Inner::PutPayload(offset, payload) => {
                let slice = payload.as_ref();
                if *offset == slice.len() {
                    None
                } else {
                    Some(Ok(Frame::data(
                        slice[std::mem::replace(offset, *offset + 1)].clone(),
                    )))
                }
            }
        })
    }

    fn is_end_stream(&self) -> bool {
        match self.0 {
            Inner::Bytes(ref bytes) => bytes.is_empty(),
            Inner::PutPayload(offset, ref body) => offset == body.as_ref().len(),
        }
    }

    fn size_hint(&self) -> SizeHint {
        match self.0 {
            Inner::Bytes(ref bytes) => SizeHint::with_exact(bytes.len() as u64),
            Inner::PutPayload(offset, ref payload) => {
                let iter = payload.as_ref().iter().skip(offset);
                SizeHint::with_exact(iter.map(|x| x.len() as u64).sum())
            }
        }
    }
}

/// An HTTP response
pub type HttpResponse = http::Response<HttpResponseBody>;

/// The body of an [`HttpResponse`]
#[derive(Debug)]
pub struct HttpResponseBody(BoxBody<Bytes, HttpError>);

impl HttpResponseBody {
    /// Create an [`HttpResponseBody`] from the provided [`Body`]
    ///
    /// Note: [`BodyExt::map_err`] can be used to alter error variants
    pub fn new<B>(body: B) -> Self
    where
        B: Body<Data = Bytes, Error = HttpError> + Send + Sync + 'static,
    {
        Self(BoxBody::new(body))
    }

    /// Collects this response into a [`Bytes`]
    pub async fn bytes(self) -> Result<Bytes, HttpError> {
        let size_hint = self.0.size_hint().lower();
        let s = self.0.into_data_stream();
        collect_bytes(s, Some(size_hint)).await
    }

    /// Returns a stream of this response data
    pub async fn bytes_stream(self) -> BoxStream<'static, Result<Bytes, HttpError>> {
        self.0.into_data_stream().boxed()
    }
}
