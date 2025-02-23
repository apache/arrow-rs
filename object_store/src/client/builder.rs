use crate::client::HttpClient;
use crate::ClientOptions;

/// A factory for [`HttpClient`]
pub trait HttpConnector {
    /// Create a new [`HttpClient`] with the provided [`ClientOptions`]
    fn connect(&self, options: ClientOptions) -> crate::Result<HttpClient>;
}

/// [`HttpConnector`] using [`reqwest::Client`]
#[derive(Debug)]
#[allow(missing_copy_implementations)]
pub struct ReqwestConnector {}

impl HttpConnector for ReqwestConnector {
    fn connect(&self, options: ClientOptions) -> crate::Result<HttpClient> {
        let client = options.client()?;
        Ok(HttpClient::new(client))
    }
}
