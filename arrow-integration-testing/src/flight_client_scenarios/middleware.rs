// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Scenario for testing middleware.

use arrow_flight::{
    FlightDescriptor, flight_descriptor::DescriptorType, flight_service_client::FlightServiceClient,
};
use prost::bytes::Bytes;
use tonic::{Request, Status};

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = Error> = std::result::Result<T, E>;

/// Run a scenario that tests middleware.
pub async fn run_scenario(host: &str, port: u16) -> Result {
    let url = format!("http://{host}:{port}");
    let conn = tonic::transport::Endpoint::new(url)?.connect().await?;
    let mut client = FlightServiceClient::with_interceptor(conn, middleware_interceptor);

    let mut descriptor = FlightDescriptor::default();
    descriptor.set_type(DescriptorType::Cmd);
    descriptor.cmd = Bytes::from_static(b"");

    // This call is expected to fail.
    match client
        .get_flight_info(Request::new(descriptor.clone()))
        .await
    {
        Ok(_) => return Err(Box::new(Status::internal("Expected call to fail"))),
        Err(e) => {
            let headers = e.metadata();
            let middleware_header = headers.get("x-middleware");
            let value = middleware_header.map(|v| v.to_str().unwrap()).unwrap_or("");

            if value != "expected value" {
                let msg = format!(
                    "On failing call: Expected to receive header 'x-middleware: expected value', \
                     but instead got: '{value}'"
                );
                return Err(Box::new(Status::internal(msg)));
            }
        }
    }

    // This call should succeed
    descriptor.cmd = Bytes::from_static(b"success");
    let resp = client.get_flight_info(Request::new(descriptor)).await?;

    let headers = resp.metadata();
    let middleware_header = headers.get("x-middleware");
    let value = middleware_header.map(|v| v.to_str().unwrap()).unwrap_or("");

    if value != "expected value" {
        let msg = format!(
            "On success call: Expected to receive header 'x-middleware: expected value', \
            but instead got: '{value}'"
        );
        return Err(Box::new(Status::internal(msg)));
    }

    Ok(())
}

fn middleware_interceptor(mut req: Request<()>) -> Result<Request<()>, Status> {
    let metadata = req.metadata_mut();
    metadata.insert("x-middleware", "expected value".parse().unwrap());
    Ok(req)
}
