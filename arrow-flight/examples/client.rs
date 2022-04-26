use std::cell::RefCell;
use std::ops::Deref;
use clap::{Args, Parser, Subcommand};
use tonic::transport::Channel;
use tonic::Streaming;
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow_flight::{FlightData, FlightInfo};
use arrow_flight::sql::gen::*;
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
    schema: Option<String>,
    #[clap(short, long)]
    table: Option<String>,
}

#[derive(Args, Debug)]
struct GetExportedKeysArgs {
    #[clap(flatten)]
    common: Common,
    #[clap(short, long)]
    catalog: Option<String>,
    #[clap(short, long)]
    schema: Option<String>,
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
    schema: Option<String>,
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
    schema: Option<String>,
    #[clap(short, long)]
    table: String,
}

async fn new_client(hostname: &String, port: &usize) -> Result<FlightSqlServiceClient, ArrowError> {
    let client_address = format!("http://{}:{}", hostname, port);
    let inner = FlightServiceClient::connect(client_address)
        .await
        .map_err(transport_error_to_arrow_erorr)?;
    Ok(FlightSqlServiceClient::new(RefCell::new(inner)))
}

async fn get_and_print(mut client: FlightSqlServiceClient, fi: FlightInfo) -> Result<(), ArrowError> {

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
        /*
        Commands::GetTables (GetTablesArgs { common: Common{hostname, port}, catalog, schema, table}) => {
            let mut client = new_client(hostname, port).await?;
            let fi = client.get_tables().await?;
            get_and_print(client, fi).await
        }
        Commands::GetExportedKeys (GetExportedKeysArgs { common: Common{hostname, port}, catalog, schema, table}) => {
            let client = new_client(hostname.to_string(), port).await?;
            get_exported_keys(&client,
                              catalog.as_deref().map(|x| x.to_string()),
                              schema.as_deref().map(|x|x.to_string()),
                              table.to_string()
            ).await
        }
        Commands::GetImportedKeys (GetImportedKeysArgs { common: Common{hostname, port}, catalog, schema, table}) => {
            let client = new_client(hostname.to_string(), port).await?;
            get_imported_keys(&client,
                              catalog.as_deref().map(|x| x.to_string()),
                              schema.as_deref().map(|x|x.to_string()),
                              table.to_string()
            ).await
        }
        Commands::GetPrimaryKeys (GetPrimaryKeysArgs { common: Common{hostname, port}, catalog, schema, table}) => {
            let client = new_client(hostname.to_string(), port).await?;
            get_primary_keys(&client,
                             catalog.as_deref().map(|x| x.to_string()),
                             schema.as_deref().map(|x|x.to_string()),
                             table.to_string()
            ).await

         */
        _ => Err(ArrowError::NotYetImplemented("not implemented yet".to_string()))
    }
}

async fn print_flight_info_results(client: &FlightServiceClient<Channel>, fi: &FlightInfo) -> Result<(), ArrowError> {

    /*
    let arrow_schema = arrow_schema_from_flight_info(fi)?;
    let arrow_schema_ref = SchemaRef::new(arrow_schema);

    let first_endpoint = fi.endpoint.first()
        .ok_or(ArrowError::ComputeError("Failed to get first endpoint".to_string()))?;

    let first_ticket = first_endpoint.ticket.clone()
        .ok_or(ArrowError::ComputeError("Failed to get first ticket".to_string()))?;

    let mut flight_data_stream = client
        .clone()
        .do_get(first_ticket)
        .await
        .map_err(status_to_arrow_error)?
        .into_inner();

    print_flight_data_stream(arrow_schema_ref, &mut flight_data_stream)
        .await*/
    todo!();
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