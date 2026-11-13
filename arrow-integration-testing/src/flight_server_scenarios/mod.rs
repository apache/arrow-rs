// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Collection of utilities for testing the Flight server.
use std::net::SocketAddr;

use arrow_flight::{FlightEndpoint, Location, Ticket};
use tokio::net::TcpListener;

pub mod auth_basic_proto;
pub mod integration_test;
pub mod middleware;

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = Error> = std::result::Result<T, E>;

/// Listen on a port and return the address
pub async fn listen_on(port: u16) -> Result<SocketAddr> {
    let addr: SocketAddr = format!("0.0.0.0:{port}").parse()?;

    let listener = TcpListener::bind(addr).await?;
    let addr = listener.local_addr()?;

    Ok(addr)
}

/// Create a FlightEndpoint with a ticket and location
pub fn endpoint(ticket: &str, location_uri: impl Into<String>) -> FlightEndpoint {
    FlightEndpoint {
        ticket: Some(Ticket {
            ticket: ticket.as_bytes().to_vec().into(),
        }),
        location: vec![Location {
            uri: location_uri.into(),
        }],
        expiration_time: None,
        app_metadata: vec![].into(),
    }
}
