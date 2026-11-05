//! Scenario for testing basic auth.

use crate::{AUTH_PASSWORD, AUTH_USERNAME};

use arrow_flight::{BasicAuth, HandshakeRequest, flight_service_client::FlightServiceClient};
use futures::{StreamExt, stream};
use prost::Message;
use tonic::{Request, Status, metadata::MetadataValue, transport::Endpoint};

type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
type Result<T = (), E = Error> = std::result::Result<T, E>;

type Client = FlightServiceClient<tonic::transport::Channel>;

/// Run a scenario that tests basic auth.
pub async fn run_scenario(host: &str, port: u16) -> Result {
    let url = format!("http://{host}:{port}");
    let endpoint = Endpoint::new(url)?;
    let channel = endpoint.connect().await?;
    let mut client = FlightServiceClient::new(channel);

    let action = arrow_flight::Action::default();

    let resp = client.do_action(Request::new(action.clone())).await;
    // This client is unauthenticated and should fail.
    match resp {
        Err(e) => {
            if e.code() != tonic::Code::Unauthenticated {
                return Err(Box::new(Status::internal(format!(
                    "Expected UNAUTHENTICATED but got {e:?}"
                ))));
            }
        }
        Ok(other) => {
            return Err(Box::new(Status::internal(format!(
                "Expected UNAUTHENTICATED but got {other:?}"
            ))));
        }
    }

    let token = authenticate(&mut client, AUTH_USERNAME, AUTH_PASSWORD)
        .await
        .expect("must respond successfully from handshake");

    let mut request = Request::new(action);
    let metadata = request.metadata_mut();
    metadata.insert_bin(
        "auth-token-bin",
        MetadataValue::from_bytes(token.as_bytes()),
    );

    let resp = client.do_action(request).await?;
    let mut resp = resp.into_inner();

    let r = resp
        .next()
        .await
        .expect("No response received")
        .expect("Invalid response received");

    let body = std::str::from_utf8(&r.body).unwrap();
    assert_eq!(body, AUTH_USERNAME);

    Ok(())
}

async fn authenticate(client: &mut Client, username: &str, password: &str) -> Result<String> {
    let auth = BasicAuth {
        username: username.into(),
        password: password.into(),
    };
    let mut payload = vec![];
    auth.encode(&mut payload)?;

    let req = stream::once(async {
        HandshakeRequest {
            payload: payload.into(),
            ..HandshakeRequest::default()
        }
    });

    let rx = client.handshake(Request::new(req)).await?;
    let mut rx = rx.into_inner();

    let r = rx.next().await.expect("must respond from handshake")?;
    assert!(rx.next().await.is_none(), "must not respond a second time");

    Ok(std::str::from_utf8(&r.payload).unwrap().into())
}
