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

use std::{sync::Arc, time::Duration};

use anyhow::{bail, Context, Result};
use arrow_array::{ArrayRef, Datum, RecordBatch, StringArray};
use arrow_cast::{cast_with_options, pretty::pretty_format_batches, CastOptions};
use arrow_flight::{
    sql::{client::FlightSqlServiceClient, CommandGetDbSchemas, CommandGetTables},
    FlightInfo,
};
use arrow_schema::Schema;
use clap::{Parser, Subcommand};
use core::str;
use futures::TryStreamExt;
use tonic::{
    metadata::MetadataMap,
    transport::{Channel, ClientTlsConfig, Endpoint},
};
use tracing_log::log::info;

/// Logging CLI config.
#[derive(Debug, Parser)]
pub struct LoggingArgs {
    /// Log verbosity.
    ///
    /// Defaults to "warn".
    ///
    /// Use `-v` for "info", `-vv` for "debug", `-vvv` for "trace".
    ///
    /// Note you can also set logging level using `RUST_LOG` environment variable:
    /// `RUST_LOG=debug`.
    #[clap(
        short = 'v',
        long = "verbose",
        action = clap::ArgAction::Count,
    )]
    log_verbose_count: u8,
}

#[derive(Debug, Parser)]
struct ClientArgs {
    /// Additional headers.
    ///
    /// Can be given multiple times. Headers and values are separated by '='.
    ///
    /// Example: `-H foo=bar -H baz=42`
    #[clap(long = "header", short = 'H', value_parser = parse_key_val)]
    headers: Vec<(String, String)>,

    /// Username.
    ///
    /// Optional. If given, `password` must also be set.
    #[clap(long, requires = "password")]
    username: Option<String>,

    /// Password.
    ///
    /// Optional. If given, `username` must also be set.
    #[clap(long, requires = "username")]
    password: Option<String>,

    /// Auth token.
    #[clap(long)]
    token: Option<String>,

    /// Use TLS.
    ///
    /// If not provided, use cleartext connection.
    #[clap(long)]
    tls: bool,

    /// Server host.
    ///
    /// Required.
    #[clap(long)]
    host: String,

    /// Server port.
    ///
    /// Defaults to `443` if `tls` is set, otherwise defaults to `80`.
    #[clap(long)]
    port: Option<u16>,
}

#[derive(Debug, Parser)]
struct Args {
    /// Logging args.
    #[clap(flatten)]
    logging_args: LoggingArgs,

    /// Client args.
    #[clap(flatten)]
    client_args: ClientArgs,

    #[clap(subcommand)]
    cmd: Command,
}

/// Different available commands.
#[derive(Debug, Subcommand)]
enum Command {
    /// Get catalogs.
    Catalogs,
    /// Get db schemas for a catalog.
    DbSchemas {
        /// Name of a catalog.
        ///
        /// Required.
        catalog: String,
        /// Specifies a filter pattern for schemas to search for.
        /// When no schema_filter is provided, the pattern will not be used to narrow the search.
        /// In the pattern string, two special characters can be used to denote matching rules:
        ///     - "%" means to match any substring with 0 or more characters.
        ///     - "_" means to match any one character.
        #[clap(short, long)]
        db_schema_filter: Option<String>,
    },
    /// Get tables for a catalog.
    Tables {
        /// Name of a catalog.
        ///
        /// Required.
        catalog: String,
        /// Specifies a filter pattern for schemas to search for.
        /// When no schema_filter is provided, the pattern will not be used to narrow the search.
        /// In the pattern string, two special characters can be used to denote matching rules:
        ///     - "%" means to match any substring with 0 or more characters.
        ///     - "_" means to match any one character.
        #[clap(short, long)]
        db_schema_filter: Option<String>,
        /// Specifies a filter pattern for tables to search for.
        /// When no table_filter is provided, all tables matching other filters are searched.
        /// In the pattern string, two special characters can be used to denote matching rules:
        ///     - "%" means to match any substring with 0 or more characters.
        ///     - "_" means to match any one character.
        #[clap(short, long)]
        table_filter: Option<String>,
        /// Specifies a filter of table types which must match.
        /// The table types depend on vendor/implementation. It is usually used to separate tables from views or system tables.
        /// TABLE, VIEW, and SYSTEM TABLE are commonly supported.
        #[clap(long)]
        table_types: Vec<String>,
    },
    /// Get table types.
    TableTypes,

    /// Execute given statement.
    StatementQuery {
        /// SQL query.
        ///
        /// Required.
        query: String,
    },

    /// Prepare given statement and then execute it.
    PreparedStatementQuery {
        /// SQL query.
        ///
        /// Required.
        ///
        /// Can contains placeholders like `$1`.
        ///
        /// Example: `SELECT * FROM t WHERE x = $1`
        query: String,

        /// Additional parameters.
        ///
        /// Can be given multiple times. Names and values are separated by '='. Values will be
        /// converted to the type that the server reported for the prepared statement.
        ///
        /// Example: `-p $1=42`
        #[clap(short, value_parser = parse_key_val)]
        params: Vec<(String, String)>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    setup_logging(args.logging_args)?;
    let mut client = setup_client(args.client_args)
        .await
        .context("setup client")?;

    let flight_info = match args.cmd {
        Command::Catalogs => client.get_catalogs().await.context("get catalogs")?,
        Command::DbSchemas {
            catalog,
            db_schema_filter,
        } => client
            .get_db_schemas(CommandGetDbSchemas {
                catalog: Some(catalog),
                db_schema_filter_pattern: db_schema_filter,
            })
            .await
            .context("get db schemas")?,
        Command::Tables {
            catalog,
            db_schema_filter,
            table_filter,
            table_types,
        } => client
            .get_tables(CommandGetTables {
                catalog: Some(catalog),
                db_schema_filter_pattern: db_schema_filter,
                table_name_filter_pattern: table_filter,
                table_types,
                // Schema is returned as ipc encoded bytes.
                // We do not support returning the schema as there is no trivial mechanism
                // to display the information to the user.
                include_schema: false,
            })
            .await
            .context("get tables")?,
        Command::TableTypes => client.get_table_types().await.context("get table types")?,
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
            bail!("did not get ticket");
        };

        let mut flight_data = client.do_get(ticket.clone()).await.context("do get")?;
        log_metadata(flight_data.headers(), "header");

        let mut endpoint_batches: Vec<_> = (&mut flight_data)
            .try_collect()
            .await
            .context("collect data stream")?;
        batches.append(&mut endpoint_batches);

        if let Some(trailers) = flight_data.trailers() {
            log_metadata(&trailers, "trailer");
        }
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

fn setup_logging(args: LoggingArgs) -> Result<()> {
    use tracing_subscriber::{util::SubscriberInitExt, EnvFilter, FmtSubscriber};

    tracing_log::LogTracer::init().context("tracing log init")?;

    let filter = match args.log_verbose_count {
        0 => "warn",
        1 => "info",
        2 => "debug",
        _ => "trace",
    };
    let filter = EnvFilter::try_new(filter).context("set up log env filter")?;

    let subscriber = FmtSubscriber::builder().with_env_filter(filter).finish();
    subscriber.try_init().context("init logging subscriber")?;

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
        let tls_config = ClientTlsConfig::new().with_enabled_roots();
        endpoint = endpoint
            .tls_config(tls_config)
            .context("create TLS endpoint")?;
    }

    let channel = endpoint.connect().await.context("connect to endpoint")?;

    let mut client = FlightSqlServiceClient::new(channel);
    info!("connected");

    for (k, v) in args.headers {
        client.set_header(k, v);
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
            bail!("when username is set, you also need to set a password")
        }
        (None, Some(_)) => {
            bail!("when password is set, you also need to set a username")
        }
    }

    Ok(client)
}

/// Parse a single key-value pair
fn parse_key_val(s: &str) -> Result<(String, String), String> {
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{s}`"))?;
    Ok((s[..pos].to_owned(), s[pos + 1..].to_owned()))
}

/// Log headers/trailers.
fn log_metadata(map: &MetadataMap, what: &'static str) {
    for k_v in map.iter() {
        match k_v {
            tonic::metadata::KeyAndValueRef::Ascii(k, v) => {
                info!(
                    "{}: {}={}",
                    what,
                    k.as_str(),
                    v.to_str().unwrap_or("<invalid>"),
                );
            }
            tonic::metadata::KeyAndValueRef::Binary(k, v) => {
                info!(
                    "{}: {}={}",
                    what,
                    k.as_str(),
                    String::from_utf8_lossy(v.as_ref()),
                );
            }
        }
    }
}
