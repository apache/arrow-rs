// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! The server side of the Arrow Flight integration tests.

use arrow_integration_testing::flight_server_scenarios;
use clap::Parser;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = Error> = std::result::Result<T, E>;

#[derive(clap::ValueEnum, Debug, Clone)]
enum Scenario {
    Middleware,
    #[clap(name = "auth:basic_proto")]
    AuthBasicProto,
}

#[derive(Debug, Parser)]
#[clap(author, version, about("rust flight-test-integration-server"), long_about = None)]
struct Args {
    #[clap(long)]
    port: u16,
    #[clap(long, value_enum)]
    scenario: Option<Scenario>,
}

#[tokio::main]
async fn main() -> Result {
    #[cfg(feature = "logging")]
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let port = args.port;

    match args.scenario {
        Some(Scenario::Middleware) => {
            flight_server_scenarios::middleware::scenario_setup(port).await?
        }
        Some(Scenario::AuthBasicProto) => {
            flight_server_scenarios::auth_basic_proto::scenario_setup(port).await?
        }
        None => {
            flight_server_scenarios::integration_test::scenario_setup(port).await?;
        }
    }
    Ok(())
}
