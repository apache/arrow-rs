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

use std::str::FromStr;

use hyper_util::client::legacy::connect::dns::{
    GaiResolver as HyperGaiResolver, Name as HyperName,
};
use rand::prelude::SliceRandom;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use tower_service::Service;

type DynErr = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug)]
pub(crate) struct ShuffleResolver(HyperGaiResolver);

impl Default for ShuffleResolver {
    fn default() -> Self {
        Self(HyperGaiResolver::new())
    }
}

impl Resolve for ShuffleResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let inner = self.0.clone();

        Box::pin(async move {
            let mut inner = inner;

            // convert name reqwest -> hyper
            let name = HyperName::from_str(name.as_str()).map_err(|err| Box::new(err) as DynErr)?;

            let mut addr = inner
                .call(name)
                .await
                .map_err(|err| Box::new(err) as DynErr)?
                .collect::<Vec<_>>();

            addr.shuffle(&mut rand::rng());

            Ok(Box::new(addr.into_iter()) as Addrs)
        })
    }
}
