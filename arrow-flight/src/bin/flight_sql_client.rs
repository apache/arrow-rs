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

use std::{error::Error, sync::Arc, time::Duration};

use anyhow::{Context, Result};
use arrow_array::{ArrayRef, Datum, RecordBatch, StringArray};
use arrow_cast::{cast_with_options, pretty::pretty_format_batches, CastOptions};
use arrow_flight::{
    sql::client::FlightSqlServiceClient, utils::flight_data_to_batches, FlightData,
    FlightInfo,
};
use arrow_schema::Schema;
use clap::{Parser, Subcommand};
use futures::TryStreamExt;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tracing_log::log::info;

/// A ':' separated key value pair
#[derive(Debug, Clone)]
struct KeyValue<K, V> {
    pub key: K,
    pub value: V,
}

impl<K, V> std::str::FromStr for KeyValue<K, V>
where
    K: std::str::FromStr,
    V: std::str::FromStr,
    K::Err: std::fmt::Display,
    V::Err: std::fmt::Display,
{
    type Err = String;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let parts = s.splitn(2, ':').collect::<Vec<_>>();
        match parts.as_slice() {
            [key, value] => {
                let key = K::from_str(key).map_err(|e| e.to_string())?;
                let value = V::from_str(value.trim()).map_err(|e| e.to_string())?;
                Ok(Self { key, value })
            }
            _ => Err(format!(
                "Invalid key value pair - expected 'KEY:VALUE' got '{s}'"
            )),
        }
    }
}

#[derive(Debug, Parser)]
struct ClientArgs {
    /// Additional headers.
    ///
    /// Values should be key value pairs separated by ':'
    #[clap(long, value_delimiter = ',')]
    headers: Vec<KeyValue<String, String>>,

    /// Username
    #[clap(long)]
    username: Option<String>,

    /// Password
    #[clap(long)]
    password: Option<String>,

    /// Auth token.
    #[clap(long)]
    token: Option<String>,

    /// Use TLS.
    #[clap(long)]
    tls: bool,

    /// Server host.
    #[clap(long)]
    host: String,

    /// Server port.
    #[clap(long)]
    port: Option<u16>,
}

#[derive(Debug, Parser)]
struct Args {
    /// Client args.
    #[clap(flatten)]
    client_args: ClientArgs,

    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    StatementQuery {
        query: String,
    },
    PreparedStatementQuery {
        query: String,
        #[clap(short, value_parser = parse_key_val)]
        params: Vec<(String, String)>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    setup_logging()?;
    let mut client = setup_client(args.client_args)
        .await
        .context("setup client")?;

    let flight_info = match args.cmd {
        Command::StatementQuery { query } => client
            .execute(query, None)
            .await
            .context("execute statement")?,
        Command::PreparedStatementQuery { query, params } => {
            let mut prepared_stmt = client
                .prepare(query, None)
                .await
                .context("prepare statement")?;

            if !params.is_empty() {
                prepared_stmt
                    .set_parameters(
                        construct_record_batch_from_params(
                            &params,
                            prepared_stmt
                                .parameter_schema()
                                .context("get parameter schema")?,
                        )
                        .context("construct parameters")?,
                    )
                    .context("bind parameters")?;
            }

            prepared_stmt
                .execute()
                .await
                .context("execute prepared statement")?
        }
    };

    let batches = execute_flight(&mut client, flight_info)
        .await
        .context("read flight data")?;

    let res = pretty_format_batches(batches.as_slice()).context("format results")?;
    println!("{res}");

    Ok(())
}

async fn execute_flight(
    client: &mut FlightSqlServiceClient<Channel>,
    info: FlightInfo,
) -> Result<Vec<RecordBatch>> {
    let schema = Arc::new(Schema::try_from(info.clone()).context("valid schema")?);
    let mut batches = Vec::with_capacity(info.endpoint.len() + 1);
    batches.push(RecordBatch::new_empty(schema));
    info!("decoded schema");

    for endpoint in info.endpoint {
        let Some(ticket) = &endpoint.ticket else {
            panic!("did not get ticket");
        };
        let flight_data = client.do_get(ticket.clone()).await.context("do get")?;
        let flight_data: Vec<FlightData> = flight_data
            .try_collect()
            .await
            .context("collect data stream")?;
        let mut endpoint_batches = flight_data_to_batches(&flight_data)
            .context("convert flight data to record batches")?;
        batches.append(&mut endpoint_batches);
    }
    info!("received data");

    Ok(batches)
}

fn construct_record_batch_from_params(
    params: &[(String, String)],
    parameter_schema: &Schema,
) -> Result<RecordBatch> {
    let mut items = Vec::<(&String, ArrayRef)>::new();

    for (name, value) in params {
        let field = parameter_schema.field_with_name(name)?;
        let value_as_array = StringArray::new_scalar(value);
        let casted = cast_with_options(
            value_as_array.get().0,
            field.data_type(),
            &CastOptions::default(),
        )?;
        items.push((name, casted))
    }

    Ok(RecordBatch::try_from_iter(items)?)
}

fn setup_logging() -> Result<()> {
    tracing_log::LogTracer::init().context("tracing log init")?;
    tracing_subscriber::fmt::init();
    Ok(())
}

async fn setup_client(args: ClientArgs) -> Result<FlightSqlServiceClient<Channel>> {
    let port = args.port.unwrap_or(if args.tls { 443 } else { 80 });

    let protocol = if args.tls { "https" } else { "http" };

    let mut endpoint = Endpoint::new(format!("{}://{}:{}", protocol, args.host, port))
        .context("create endpoint")?
        .connect_timeout(Duration::from_secs(20))
        .timeout(Duration::from_secs(20))
        .tcp_nodelay(true) // Disable Nagle's Algorithm since we don't want packets to wait
        .tcp_keepalive(Option::Some(Duration::from_secs(3600)))
        .http2_keep_alive_interval(Duration::from_secs(300))
        .keep_alive_timeout(Duration::from_secs(20))
        .keep_alive_while_idle(true);

    if args.tls {
        let tls_config = ClientTlsConfig::new();
        endpoint = endpoint
            .tls_config(tls_config)
            .context("create TLS endpoint")?;
    }

    let channel = endpoint.connect().await.context("connect to endpoint")?;

    let mut client = FlightSqlServiceClient::new(channel);
    info!("connected");

    for kv in args.headers {
        client.set_header(kv.key, kv.value);
    }

    if let Some(token) = args.token {
        client.set_token(token);
        info!("token set");
    }

    match (args.username, args.password) {
        (None, None) => {}
        (Some(username), Some(password)) => {
            client
                .handshake(&username, &password)
                .await
                .context("handshake")?;
            info!("performed handshake");
        }
        (Some(_), None) => {
            panic!("when username is set, you also need to set a password")
        }
        (None, Some(_)) => {
            panic!("when password is set, you also need to set a username")
        }
    }

    Ok(client)
}

/// Parse a single key-value pair
fn parse_key_val(
    s: &str,
) -> Result<(String, String), Box<dyn Error + Send + Sync + 'static>> {
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
    Ok((s[..pos].parse()?, s[pos + 1..].parse()?))
}
