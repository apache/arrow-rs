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

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Request, Response, Server};
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

pub type ResponseFn = Box<dyn FnOnce(Request<Body>) -> Response<Body> + Send>;

/// A mock server
pub struct MockServer {
    responses: Arc<Mutex<VecDeque<ResponseFn>>>,
    shutdown: oneshot::Sender<()>,
    handle: JoinHandle<()>,
    url: String,
}

impl MockServer {
    pub fn new() -> Self {
        let responses: Arc<Mutex<VecDeque<ResponseFn>>> =
            Arc::new(Mutex::new(VecDeque::with_capacity(10)));

        let r = Arc::clone(&responses);
        let make_service = make_service_fn(move |_conn| {
            let r = Arc::clone(&r);
            async move {
                Ok::<_, Infallible>(service_fn(move |req| {
                    let r = Arc::clone(&r);
                    async move {
                        Ok::<_, Infallible>(match r.lock().pop_front() {
                            Some(r) => r(req),
                            None => Response::new(Body::from("Hello World")),
                        })
                    }
                }))
            }
        });

        let (shutdown, rx) = oneshot::channel::<()>();
        let server =
            Server::bind(&SocketAddr::from(([127, 0, 0, 1], 0))).serve(make_service);

        let url = format!("http://{}", server.local_addr());

        let handle = tokio::spawn(async move {
            server
                .with_graceful_shutdown(async {
                    rx.await.ok();
                })
                .await
                .unwrap()
        });

        Self {
            responses,
            shutdown,
            handle,
            url,
        }
    }

    /// The url of the mock server
    pub fn url(&self) -> &str {
        &self.url
    }

    /// Add a response
    pub fn push(&self, response: Response<Body>) {
        self.push_fn(|_| response)
    }

    /// Add a response function
    pub fn push_fn<F>(&self, f: F)
    where
        F: FnOnce(Request<Body>) -> Response<Body> + Send + 'static,
    {
        self.responses.lock().push_back(Box::new(f))
    }

    /// Shutdown the mock server
    pub async fn shutdown(self) {
        let _ = self.shutdown.send(());
        self.handle.await.unwrap()
    }
}
