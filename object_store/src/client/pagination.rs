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

use crate::Result;
use futures::Stream;
use std::future::Future;

/// Performs a paginated operation
pub fn paginated<F, Fut, S, T>(state: S, list: F) -> impl Stream<Item = Result<T>>
where
    F: Fn(S, Option<String>) -> Fut + Copy,
    Fut: Future<Output = Result<(T, S, Option<String>)>>,
{
    enum ListState<T> {
        Start(T),
        HasMore(T, String),
        Done,
    }

    futures::stream::unfold(ListState::Start(state), move |state| async move {
        let (s, page_token) = match state {
            ListState::Start(s) => (s, None),
            ListState::HasMore(s, page_token) => (s, Some(page_token)),
            ListState::Done => {
                return None;
            }
        };

        let (resp, s, continuation) = match list(s, page_token).await {
            Ok(resp) => resp,
            Err(e) => return Some((Err(e), ListState::Done)),
        };

        let next_state = match continuation {
            Some(token) => ListState::HasMore(s, token),
            None => ListState::Done,
        };

        Some((Ok(resp), next_state))
    })
}
