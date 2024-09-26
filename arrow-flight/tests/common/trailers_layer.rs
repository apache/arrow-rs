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

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::ready;
use http::{HeaderValue, Request, Response};
use http_body::{Frame, SizeHint};
use pin_project_lite::pin_project;
use tower::{Layer, Service};

#[derive(Debug, Copy, Clone, Default)]
pub struct TrailersLayer;

impl<S> Layer<S> for TrailersLayer {
    type Service = TrailersService<S>;

    fn layer(&self, service: S) -> Self::Service {
        TrailersService { service }
    }
}

#[derive(Debug, Clone)]
pub struct TrailersService<S> {
    service: S,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for TrailersService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
    ResBody: http_body::Body,
{
    type Response = Response<WrappedBody<ResBody>>;
    type Error = S::Error;
    type Future = WrappedFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        WrappedFuture {
            inner: self.service.call(request),
        }
    }
}

pin_project! {
    #[derive(Debug)]
    pub struct WrappedFuture<F> {
        #[pin]
        inner: F,
    }
}

impl<F, ResBody, Error> Future for WrappedFuture<F>
where
    F: Future<Output = Result<Response<ResBody>, Error>>,
    ResBody: http_body::Body,
{
    type Output = Result<Response<WrappedBody<ResBody>>, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let result: Result<Response<ResBody>, Error> =
            ready!(self.as_mut().project().inner.poll(cx));

        match result {
            Ok(response) => Poll::Ready(Ok(response.map(|body| WrappedBody { inner: body }))),
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

pin_project! {
    #[derive(Debug)]
    pub struct WrappedBody<B> {
        #[pin]
        inner: B,
    }
}

impl<B: http_body::Body> http_body::Body for WrappedBody<B> {
    type Data = B::Data;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut result = ready!(self.project().inner.poll_frame(cx));

        if let Some(Ok(frame)) = &mut result {
            if let Some(trailers) = frame.trailers_mut() {
                trailers.insert("test-trailer", HeaderValue::from_static("trailer_val"));
            }
        }

        Poll::Ready(result)
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}
