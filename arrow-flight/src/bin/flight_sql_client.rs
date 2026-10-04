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

//! A command line client for Arrow Flight SQL.

use std::{
    collections::{HashMap, hash_map::Entry},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result, bail};
use arrow_array::{ArrayRef, Datum, RecordBatch, StringArray};
use arrow_cast::{CastOptions, cast_with_options, pretty::pretty_format_batches};
use arrow_flight::{
    FlightInfo,
    flight_service_client::FlightServiceClient,
    sql::{CommandGetDbSchemas, CommandGetTables, client::FlightSqlServiceClient},
};
use arrow_schema::Schema;
use clap::{Parser, Subcommand, ValueEnum};
use core::str;
use futures::TryStreamExt;
use tonic::{
    metadata::MetadataMap,
    transport::{Channel, ClientTlsConfig, Endpoint, Uri},
};
use tracing_log::log::info;

/// Reserved location URI meaning "redeem this ticket on the connection that returned the
/// `FlightInfo`", rather than on a separate server. An empty string means the same.
const REUSE_CONNECTION_URI: &str = "arrow-flight-reuse-connection://?";

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

/// gRPC/HTTP compression algorithms.
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum CompressionEncoding {
    Gzip,
    Deflate,
    Zstd,
}

impl From<CompressionEncoding> for tonic::codec::CompressionEncoding {
    fn from(encoding: CompressionEncoding) -> Self {
        match encoding {
            CompressionEncoding::Gzip => Self::Gzip,
            CompressionEncoding::Deflate => Self::Deflate,
            CompressionEncoding::Zstd => Self::Zstd,
        }
    }
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
    /// Endpoint locations must also use TLS, unless reusing this connection.
    ///
    /// If not provided, use cleartext connection.
    #[clap(long)]
    tls: bool,

    /// Dump TLS key log.
    ///
    /// The target file is specified by the `SSLKEYLOGFILE` environment variable.
    ///
    /// Requires `--tls`.
    #[clap(long, requires = "tls")]
    key_log: bool,

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

    /// Compression accepted by the client for responses sent by the server.
    ///
    /// The client will send this information to the server as part of the request. The server is free to pick an
    /// algorithm from that list or use no compression (called "identity" encoding).
    ///
    /// You may define multiple algorithms by using a comma-separated list.
    #[clap(long, value_delimiter = ',')]
    accept_compression: Vec<CompressionEncoding>,

    /// Compression of requests sent by the client to the server.
    ///
    /// Since the client needs to decide on the compression before sending the request, there is no client<->server
    /// negotiation. If the server does NOT support the chosen compression, it will respond with an error a la:
    ///
    /// ```text
    /// Ipc error: Status {
    ///     code: Unimplemented,
    ///     message: "Content is compressed with `zstd` which isn't supported",
    ///     metadata: MetadataMap { headers: {"grpc-accept-encoding": "identity", ...} },
    ///     ...
    /// }
    /// ```
    ///
    /// Based on the algorithms listed in the `grpc-accept-encoding` header, you may make a more educated guess for
    /// your next request. Note that `identity` is a synonym for "no compression".
    #[clap(long)]
    send_compression: Option<CompressionEncoding>,
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
    let mut client = setup_client(&args.client_args)
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

    let batches = execute_flight(&mut client, &args.client_args, flight_info)
        .await
        .context("read flight data")?;

    let res = pretty_format_batches(batches.as_slice()).context("format results")?;
    println!("{res}");

    Ok(())
}

async fn execute_flight(
    client: &mut FlightSqlServiceClient<Channel>,
    client_args: &ClientArgs,
    info: FlightInfo,
) -> Result<Vec<RecordBatch>> {
    let schema = Arc::new(Schema::try_from(info.clone()).context("valid schema")?);
    let mut batches = Vec::with_capacity(info.endpoint.len() + 1);
    batches.push(RecordBatch::new_empty(schema));
    info!("decoded schema");

    let mut location_clients = HashMap::new();

    for endpoint in info.endpoint {
        let location = select_endpoint_location(
            endpoint
                .location
                .iter()
                .map(|location| location.uri.as_str()),
            client_args.tls,
        )?;
        let Some(ticket) = &endpoint.ticket else {
            bail!("did not get ticket");
        };

        let client = match location {
            None => &mut *client,
            Some(uri) => match location_clients.entry(uri.to_owned()) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let mut endpoint_client = setup_client_for_uri(client_args, uri)
                        .await
                        .context("setup client for endpoint location")?;
                    if let Some(token) = client.token() {
                        endpoint_client.set_token(token.to_owned());
                    }
                    entry.insert(endpoint_client)
                }
            },
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
    use tracing_subscriber::{EnvFilter, FmtSubscriber, util::SubscriberInitExt};

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

/// Prefer a separate location, falling back to the original connection when permitted.
/// HTTP downloads and Unix domain sockets are deliberately unsupported by this CLI.
fn select_endpoint_location<'a>(
    locations: impl IntoIterator<Item = &'a str>,
    tls: bool,
) -> Result<Option<&'a str>> {
    let mut reuse_connection = false;
    let mut has_locations = false;
    let mut has_insecure_location = false;
    for uri in locations {
        has_locations = true;
        if uri.is_empty() || uri == REUSE_CONNECTION_URI {
            reuse_connection = true;
        } else if let Ok(location) = uri.parse::<Uri>() {
            let Some(scheme) = location.scheme() else {
                continue;
            };
            if scheme != "grpc" && scheme != "grpc+tcp" && scheme != "grpc+tls" {
                continue;
            }
            let Ok(location) = transport_uri(location) else {
                continue;
            };
            if location.scheme_str() == Some("https") || !tls {
                return Ok(Some(uri));
            }
            has_insecure_location = true;
        }
    }
    if has_locations && !reuse_connection {
        if tls && has_insecure_location {
            bail!(
                "--tls requires a secure endpoint location, but no secure or reusable location was provided"
            );
        }
        bail!(
            "unsupported endpoint location: expected grpc, grpc+tcp or grpc+tls, or connection reuse"
        );
    }
    Ok(None)
}

/// Map Flight gRPC schemes at the transport boundary. HTTP(S) is also accepted here
/// for the initial connection constructed by setup_client, not advertised HTTP downloads.
fn transport_uri(uri: Uri) -> Result<Uri> {
    let scheme = uri.scheme().context("transport URI has no scheme")?;
    let transport_scheme = if scheme == "grpc" || scheme == "grpc+tcp" {
        "http"
    } else if scheme == "grpc+tls" {
        "https"
    } else if scheme == "http" || scheme == "https" {
        return Ok(uri);
    } else {
        bail!("unsupported transport URI scheme");
    };
    // gRPC locations name a server address, not credentials or a download resource.
    let authority = uri.authority().context("gRPC location has no authority")?;
    let host = authority.host();
    if host.is_empty()
        || authority.as_str().contains('@')
        || uri.query().is_some()
        || !matches!(uri.path(), "" | "/")
    {
        bail!("invalid gRPC location address");
    }
    let suffix = authority
        .as_str()
        .strip_prefix(host)
        .context("invalid gRPC location authority")?;
    if !suffix.is_empty() {
        let port = suffix
            .strip_prefix(':')
            .context("invalid gRPC location port")?;
        if port.is_empty()
            || !port.bytes().all(|byte| byte.is_ascii_digit())
            || port.parse::<u16>().is_err()
        {
            bail!("invalid gRPC location port");
        }
    }
    let mut parts = uri.into_parts();
    parts.scheme = Some(
        transport_scheme
            .parse()
            .context("invalid transport scheme")?,
    );
    Uri::from_parts(parts).context("invalid transport URI")
}

async fn setup_client(args: &ClientArgs) -> Result<FlightSqlServiceClient<Channel>> {
    let port = args.port.unwrap_or(if args.tls { 443 } else { 80 });

    let protocol = if args.tls { "https" } else { "http" };

    let mut client =
        setup_client_for_uri(args, &format!("{}://{}:{}", protocol, args.host, port)).await?;

    if let Some(token) = &args.token {
        client.set_token(token.clone());
        info!("token set");
    }

    match (&args.username, &args.password) {
        (None, None) => {}
        (Some(username), Some(password)) => {
            client
                .handshake(username, password)
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

/// Connect a client to `uri`, applying the headers and compression settings from `args`.
/// TLS is used for `grpc+tls` or internal `https` URIs. Authentication is handled by the caller,
/// so separate endpoint clients can reuse the original client's token without a handshake.
async fn setup_client_for_uri(
    args: &ClientArgs,
    uri: &str,
) -> Result<FlightSqlServiceClient<Channel>> {
    let uri = transport_uri(uri.parse().context("invalid transport URI")?)?;
    let tls = uri.scheme_str() == Some("https");

    let mut endpoint = Endpoint::new(uri)
        .context("create endpoint")?
        .connect_timeout(Duration::from_secs(20))
        .timeout(Duration::from_secs(20))
        .tcp_nodelay(true) // Disable Nagle's Algorithm since we don't want packets to wait
        .tcp_keepalive(Option::Some(Duration::from_secs(3600)))
        .http2_keep_alive_interval(Duration::from_secs(300))
        .keep_alive_timeout(Duration::from_secs(20))
        .keep_alive_while_idle(true);

    if tls {
        let mut tls_config = ClientTlsConfig::new().with_enabled_roots();
        if args.key_log {
            tls_config = tls_config.use_key_log();
        }

        endpoint = endpoint
            .tls_config(tls_config)
            .context("create TLS endpoint")?;
    }

    let channel = endpoint.connect().await.context("connect to endpoint")?;

    let mut client = FlightServiceClient::new(channel);
    for encoding in &args.accept_compression {
        client = client.accept_compressed((*encoding).into());
    }
    if let Some(encoding) = args.send_compression {
        client = client.send_compressed(encoding.into());
    }
    let mut client = FlightSqlServiceClient::new_from_inner(client);
    info!("connected");

    for (k, v) in &args.headers {
        client.set_header(k, v);
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

#[cfg(test)]
mod tests {
    use super::{
        ClientArgs, REUSE_CONNECTION_URI, execute_flight, select_endpoint_location, transport_uri,
    };
    use arrow_flight::{FlightEndpoint, FlightInfo, sql::client::FlightSqlServiceClient};
    use arrow_schema::Schema;
    use clap::Parser;
    use tonic::transport::Channel;

    #[test]
    fn flight_schemes_map_to_transport_uris() {
        for (scheme, transport) in [
            ("grpc", "http"),
            ("grpc+tcp", "http"),
            ("grpc+tls", "https"),
            ("GRPC+TLS", "https"),
        ] {
            let location = format!("{scheme}://[::1]:1234");
            assert_eq!(
                transport_uri(location.parse().unwrap())
                    .unwrap()
                    .to_string(),
                format!("{transport}://[::1]:1234/")
            );
            assert_eq!(
                select_endpoint_location([location.as_str()], false).unwrap(),
                Some(location.as_str())
            );
        }
    }

    #[test]
    fn tls_selects_later_secure_location() {
        let locations = [
            REUSE_CONNECTION_URI,
            "grpc://insecure:80",
            "grpc+tcp://other:80",
            "grpc+tls://secure:443",
            "grpc+tls://other:443",
        ];
        assert_eq!(
            select_endpoint_location(locations, true).unwrap(),
            Some("grpc+tls://secure:443")
        );
    }

    #[test]
    fn tls_rejects_insecure_only_locations() {
        let error = select_endpoint_location(["grpc://insecure:80", "grpc+tcp://other:80"], true)
            .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("--tls requires a secure endpoint location")
        );
    }

    #[test]
    fn reuse_falls_back_to_original_connection() {
        for tls in [false, true] {
            assert_eq!(select_endpoint_location([], tls).unwrap(), None);
            assert_eq!(select_endpoint_location([""], tls).unwrap(), None);
            assert_eq!(
                select_endpoint_location([REUSE_CONNECTION_URI], tls).unwrap(),
                None
            );
        }
        for reuse in ["", REUSE_CONNECTION_URI] {
            for locations in [
                [reuse, "grpc+tcp://insecure:80"],
                ["grpc+tcp://insecure:80", reuse],
            ] {
                assert_eq!(select_endpoint_location(locations, true).unwrap(), None);
            }
        }
    }

    #[test]
    fn without_tls_selects_first_nonreuse_location() {
        assert_eq!(
            select_endpoint_location(
                [
                    "",
                    REUSE_CONNECTION_URI,
                    "grpc+tcp://first:80",
                    "grpc+tls://later:443"
                ],
                false,
            )
            .unwrap(),
            Some("grpc+tcp://first:80")
        );
        assert_eq!(
            select_endpoint_location(
                [
                    REUSE_CONNECTION_URI,
                    "grpc+tls://first:443",
                    "grpc://later:80"
                ],
                false,
            )
            .unwrap(),
            Some("grpc+tls://first:443")
        );
    }

    #[test]
    fn unsupported_locations_are_skipped_or_rejected() {
        for unsupported in [
            "http://download/data?signature=secret",
            "https://download/data?signature=secret",
            "grpc+unix:///tmp/flight.sock",
            "unknown://server:1234",
            "grpc+tls-extra://server:1234",
            "grpc+tcp://invalid host:1234",
            "/path/grpc+tls://server:1234",
            "grpc://user:secret@server:1234",
            "grpc+tls://server:1234?signature=secret",
            "grpc+tcp://server:1234/download",
            "grpc://server:",
            "grpc+tcp://server:not-a-port",
            "grpc+tls://server:65536",
            "grpc+tls://[::1]:",
            "grpc+tls://[::1]:not-a-port",
            "grpc+tls://[::1]:65536",
        ] {
            for tls in [false, true] {
                let error = select_endpoint_location([unsupported], tls).unwrap_err();
                assert!(error.to_string().contains("unsupported endpoint location"));
                assert!(!error.to_string().contains("secret"));

                let supported = "grpc+tls://secure:443";
                for locations in [[unsupported, supported], [supported, unsupported]] {
                    assert_eq!(
                        select_endpoint_location(locations, tls).unwrap(),
                        Some(supported)
                    );
                }
                for reuse in ["", REUSE_CONNECTION_URI] {
                    for locations in [[unsupported, reuse], [reuse, unsupported]] {
                        assert_eq!(select_endpoint_location(locations, tls).unwrap(), None);
                    }
                }
            }
            for locations in [
                [unsupported, "grpc+tcp://plain:80"],
                ["grpc+tcp://plain:80", unsupported],
            ] {
                assert_eq!(
                    select_endpoint_location(locations, false).unwrap(),
                    Some("grpc+tcp://plain:80")
                );
            }
        }
    }

    #[tokio::test]
    async fn http_without_ticket_reports_unsupported_location() {
        let args = ClientArgs::try_parse_from(["test", "--host", "localhost"]).unwrap();
        let channel = Channel::from_static("http://localhost:1234").connect_lazy();
        let mut client = FlightSqlServiceClient::new(channel);
        let info = FlightInfo::new()
            .try_with_schema(&Schema::empty())
            .unwrap()
            .with_endpoint(
                FlightEndpoint::new().with_location("https://download/data?signature=secret"),
            );
        let error = execute_flight(&mut client, &args, info).await.unwrap_err();
        assert!(error.to_string().contains("unsupported endpoint location"));
        assert!(!error.to_string().contains("secret"));
    }
}
