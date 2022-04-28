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

use std::cell::RefCell;
use std::ops::Deref;
use clap::{Args, Parser, Subcommand};
use tonic::transport::Channel;
use tonic::Streaming;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow_flight::{FlightData, FlightInfo};
use arrow_flight::sql::*;
use arrow_flight::sql::client::*;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::ProstMessageExt;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Execute(ExecuteArgs),
    ExecuteUpdate(ExecuteUpdateArgs),
    GetCatalogs(GetCatalogsArgs),
    GetTableTypes(GetTableTypesArgs),
    GetSchemas(GetSchemasArgs),
    GetTables(GetTablesArgs),
    GetExportedKeys(GetExportedKeysArgs),
    GetImportedKeys(GetImportedKeysArgs),
    GetPrimaryKeys(GetPrimaryKeysArgs),
}

#[derive(Args, Debug)]
struct Common {
    #[clap(long, default_value_t = String::from("localhost"))]
    hostname: String,
    #[clap(short, long, default_value_t = 52358, parse(try_from_str))]
    port: usize,
}

#[derive(Args, Debug)]
struct ExecuteArgs {
    #[clap(flatten)]
    common: Common,
    #[clap(short, long)]
    query: String
}

#[derive(Args, Debug)]
struct ExecuteUpdateArgs {
    #[clap(flatten)]
    common: Common,
    #[clap(short, long)]
    query: String
}

#[derive(Args, Debug)]
struct GetCatalogsArgs {
    #[clap(flatten)]
    common: Common,
}

#[derive(Args, Debug)]
struct GetTableTypesArgs {
    #[clap(flatten)]
    common: Common,
}

#[derive(Args, Debug)]
struct GetSchemasArgs {
    #[clap(flatten)]
    common: Common,
    #[clap(short, long)]
    catalog: Option<String>,
    #[clap(short, long)]
    db_schema_filter_pattern: Option<String>,
}

#[derive(Args, Debug)]
struct GetTablesArgs {
    #[clap(flatten)]
    common: Common,
    #[clap(short, long)]
    catalog: Option<String>,
    #[clap(short, long)]
    db_schema_filter_pattern: Option<String>,
    #[clap(short, long)]
    table_name_filter_pattern: Option<String>,
    #[clap(short, long)]
    include_schema: bool,
}

#[derive(Args, Debug)]
struct GetExportedKeysArgs {
    #[clap(flatten)]
    common: Common,
    #[clap(short, long)]
    catalog: Option<String>,
    #[clap(short, long)]
    db_schema: Option<String>,
    #[clap(short, long)]
    table: String,
}

#[derive(Args, Debug)]
struct GetImportedKeysArgs {
    #[clap(flatten)]
    common: Common,
    #[clap(short, long)]
    catalog: Option<String>,
    #[clap(short, long)]
    db_schema: Option<String>,
    #[clap(short, long)]
    table: String,
}

#[derive(Args, Debug)]
struct GetPrimaryKeysArgs {
    #[clap(flatten)]
    common: Common,
    #[clap(short, long)]
    catalog: Option<String>,
    #[clap(short, long)]
    db_schema: Option<String>,
    #[clap(short, long)]
    table: String,
}

async fn new_client(hostname: &String, port: &usize) -> Result<FlightSqlServiceClient<Channel>, ArrowError> {
    let client_address = format!("http://{}:{}", hostname, port);
    let inner = FlightServiceClient::connect(client_address)
        .await
        .map_err(transport_error_to_arrow_erorr)?;
    Ok(FlightSqlServiceClient::new(RefCell::new(inner)))
}

async fn get_and_print(mut client: FlightSqlServiceClient<Channel>, fi: FlightInfo) -> Result<(), ArrowError> {

    let first_endpoint = fi.endpoint.first()
        .ok_or(ArrowError::ComputeError("Failed to get first endpoint".to_string()))?;

    let first_ticket = first_endpoint.ticket.clone()
        .ok_or(ArrowError::ComputeError("Failed to get first ticket".to_string()))?;

    let mut flight_data_stream = client
        .do_get(first_ticket)
        .await?;

    let arrow_schema = arrow_schema_from_flight_info(&fi)?;
    let arrow_schema_ref = SchemaRef::new(arrow_schema);

    print_flight_data_stream(arrow_schema_ref, &mut flight_data_stream)
        .await
}

#[tokio::main]
async fn main() -> Result<(), ArrowError> {

    let cli = Cli::parse();

    match &cli.command {
        Commands::Execute (ExecuteArgs { common: Common{hostname, port}, query}) => {
            let mut client = new_client(hostname, port).await?;
            let fi = client.execute(query.to_string()).await?;
            get_and_print(client, fi).await
        }
        Commands::ExecuteUpdate (ExecuteUpdateArgs { common: Common{hostname, port}, query}) => {
            let mut client = new_client(hostname, port).await?;
            let record_count = client.execute_update(query.to_string()).await?;
            println!("Updated {} records.", record_count);
            Ok(())
        }
        Commands::GetCatalogs (GetCatalogsArgs { common: Common{hostname, port}}) => {
            let mut client = new_client(hostname, port).await?;
            let fi = client.get_catalogs().await?;
            get_and_print(client, fi).await
        }
        Commands::GetTableTypes (GetTableTypesArgs { common: Common{hostname, port}}) => {
            let mut client = new_client(hostname, port).await?;
            let fi = client.get_table_types().await?;
            get_and_print(client, fi).await
        }
        Commands::GetSchemas (GetSchemasArgs { common: Common{hostname, port}, catalog, db_schema_filter_pattern: schema }) => {
            let mut client = new_client(hostname, port).await?;
            let fi = client.get_db_schemas(CommandGetDbSchemas {
                catalog: catalog.as_deref().map(|x| x.to_string()),
                db_schema_filter_pattern: schema.as_deref().map(|x| x.to_string())
            }).await?;
            get_and_print(client, fi).await
        }
        Commands::GetTables (GetTablesArgs { common: Common{hostname, port}, catalog, db_schema_filter_pattern, table_name_filter_pattern, include_schema }) => {
            let mut client = new_client(hostname, port).await?;
            let fi = client.get_tables(CommandGetTables {
                catalog: catalog.as_deref().map(|x| x.to_string()),
                db_schema_filter_pattern: db_schema_filter_pattern.as_deref().map(|x| x.to_string()),
                table_name_filter_pattern: table_name_filter_pattern.as_deref().map(|x| x.to_string()),
                table_types: vec![],
                include_schema: *include_schema,
            }).await?;
            get_and_print(client, fi).await
        }
        Commands::GetExportedKeys (GetExportedKeysArgs { common: Common{hostname, port}, catalog, db_schema, table}) => {
            let mut client = new_client(hostname, port).await?;
            let fi = client.get_exported_keys(CommandGetExportedKeys {
                catalog: catalog.as_deref().map(|x| x.to_string()),
                db_schema: db_schema.as_deref().map(|x| x.to_string()),
                table: table.to_string(),
            }).await?;
            get_and_print(client, fi).await
        }
        Commands::GetImportedKeys (GetImportedKeysArgs { common: Common{hostname, port}, catalog, db_schema, table}) => {
            let mut client = new_client(hostname, port).await?;
            let fi = client.get_imported_keys(CommandGetImportedKeys {
                catalog: catalog.as_deref().map(|x| x.to_string()),
                db_schema: db_schema.as_deref().map(|x| x.to_string()),
                table: table.to_string(),
            }).await?;
            get_and_print(client, fi).await
        }
        Commands::GetPrimaryKeys (GetPrimaryKeysArgs { common: Common{hostname, port}, catalog, db_schema, table}) => {
            let mut client = new_client(hostname, port).await?;
            let fi = client.get_primary_keys(CommandGetPrimaryKeys {
                catalog: catalog.as_deref().map(|x| x.to_string()),
                db_schema: db_schema.as_deref().map(|x| x.to_string()),
                table: table.to_string(),
            }).await?;
            get_and_print(client, fi).await
        }
        _ => Err(ArrowError::NotYetImplemented("not implemented yet".to_string()))
    }
}

async fn print_flight_data_stream(arrow_schema_ref: SchemaRef, flight_data_stream: &mut Streaming<FlightData>) -> Result<(), ArrowError> {

    while let Some(flight_data) = flight_data_stream.message()
        .await
        .map_err(status_to_arrow_error)? {
        let arrow_data = arrow_data_from_flight_data(flight_data, &arrow_schema_ref)?;
        match arrow_data {
            ArrowFlightData::RecordBatch(record_batch) => {
                arrow::util::pretty::print_batches(&[record_batch])?;
            }
            _ => {} // no data to print..
        }
    }

    Ok(())
}