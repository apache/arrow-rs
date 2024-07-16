use crate::common::trailers_layer::TrailersLayer;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use http::Uri;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tonic::transport::Channel;

/// All tests must complete within this many seconds or else the test server is shutdown
const DEFAULT_TIMEOUT_SECONDS: u64 = 30;

/// Creates and manages a running TestServer with a background task
pub struct TestFixture {
    /// channel to send shutdown command
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,

    /// Address the server is listening on
    addr: SocketAddr,

    /// handle for the server task
    handle: Option<JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl TestFixture {
    /// create a new test fixture from the server
    pub async fn new<T: FlightService>(test_server: FlightServiceServer<T>) -> Self {
        // let OS choose a free port
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        println!("Listening on {addr}");

        // prepare the shutdown channel
        let (tx, rx) = tokio::sync::oneshot::channel();

        let server_timeout = Duration::from_secs(DEFAULT_TIMEOUT_SECONDS);

        let shutdown_future = async move {
            rx.await.ok();
        };

        let serve_future = tonic::transport::Server::builder()
            .timeout(server_timeout)
            .layer(TrailersLayer)
            .add_service(test_server)
            .serve_with_incoming_shutdown(
                tokio_stream::wrappers::TcpListenerStream::new(listener),
                shutdown_future,
            );

        // Run the server in its own background task
        let handle = tokio::task::spawn(serve_future);

        Self {
            shutdown: Some(tx),
            addr,
            handle: Some(handle),
        }
    }

    /// Return a [`Channel`] connected to the TestServer
    pub async fn channel(&self) -> Channel {
        let url = format!("http://{}", self.addr);
        let uri: Uri = url.parse().expect("Valid URI");
        Channel::builder(uri)
            .timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECONDS))
            .connect()
            .await
            .expect("error connecting to server")
    }

    /// Stops the test server and waits for the server to shutdown
    pub async fn shutdown_and_wait(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.send(()).expect("server quit early");
        }
        if let Some(handle) = self.handle.take() {
            println!("Waiting on server to finish");
            handle
                .await
                .expect("task join error (panic?)")
                .expect("Server Error found at shutdown");
        }
    }
}

impl Drop for TestFixture {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.send(()).ok();
        }
        if self.handle.is_some() {
            // tests should properly clean up TestFixture
            println!("TestFixture::Drop called prior to `shutdown_and_wait`");
        }
    }
}
