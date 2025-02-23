use crate::client::connection::HttpErrorKind;
use crate::client::{HttpClient, HttpError, HttpRequest, HttpRequestBody, HttpResponse};
use http::header::{
    Entry, InvalidHeaderName, InvalidHeaderValue, OccupiedEntry, AUTHORIZATION, CONTENT_TYPE,
};
use http::uri::{InvalidUri, PathAndQuery};
use http::{HeaderMap, HeaderName, HeaderValue, Method, Uri};
use serde::Serialize;

#[derive(Debug, thiserror::Error)]
pub(crate) enum RequestBuilderError {
    #[error("Invalid URI")]
    InvalidUri(#[from] InvalidUri),

    #[error("Invalid Header Value")]
    InvalidHeaderValue(#[from] InvalidHeaderValue),

    #[error("Invalid Header Name")]
    InvalidHeaderName(#[from] InvalidHeaderName),

    #[error("JSON serialization error")]
    SerdeJson(#[from] serde_json::Error),

    #[error("URL serialization error")]
    SerdeUrl(#[from] serde_urlencoded::ser::Error),
}

impl From<RequestBuilderError> for HttpError {
    fn from(value: RequestBuilderError) -> Self {
        HttpError::new(HttpErrorKind::Request, value)
    }
}

impl From<std::convert::Infallible> for RequestBuilderError {
    fn from(value: std::convert::Infallible) -> Self {
        match value {}
    }
}

pub(crate) struct HttpRequestBuilder {
    client: HttpClient,
    request: Result<HttpRequest, RequestBuilderError>,
}

impl HttpRequestBuilder {
    pub(crate) fn new(client: HttpClient) -> Self {
        Self {
            client,
            request: Ok(HttpRequest::new(HttpRequestBody::empty())),
        }
    }

    pub(crate) fn from_parts(client: HttpClient, request: HttpRequest) -> Self {
        Self {
            client,
            request: Ok(request),
        }
    }

    pub(crate) fn method(mut self, method: Method) -> Self {
        if let Ok(r) = &mut self.request {
            *r.method_mut() = method;
        }
        self
    }

    pub(crate) fn uri<U>(mut self, url: U) -> Self
    where
        U: TryInto<Uri>,
        U::Error: Into<RequestBuilderError>,
    {
        match (url.try_into(), &mut self.request) {
            (Ok(uri), Ok(r)) => *r.uri_mut() = uri,
            (Err(e), Ok(_)) => self.request = Err(e.into()),
            (_, Err(_)) => {}
        }
        self
    }

    pub(crate) fn header<K, V>(mut self, name: K, value: V) -> Self
    where
        K: TryInto<HeaderName>,
        K::Error: Into<RequestBuilderError>,
        V: TryInto<HeaderValue>,
        V::Error: Into<RequestBuilderError>,
    {
        match (name.try_into(), value.try_into(), &mut self.request) {
            (Ok(name), Ok(value), Ok(r)) => {
                r.headers_mut().insert(name, value);
            }
            (Err(e), _, Ok(_)) => self.request = Err(e.into()),
            (_, Err(e), Ok(_)) => self.request = Err(e.into()),
            (_, _, Err(_)) => {}
        }
        self
    }

    pub(crate) fn headers(mut self, headers: HeaderMap) -> Self {
        if let Ok(ref mut req) = self.request {
            // IntoIter of HeaderMap yields (Option<HeaderName>, HeaderValue).
            // The first time a name is yielded, it will be Some(name), and if
            // there are more values with the same name, the next yield will be
            // None.

            let mut prev_entry: Option<OccupiedEntry<'_, _>> = None;
            for (key, value) in headers {
                match key {
                    Some(key) => match req.headers_mut().entry(key) {
                        Entry::Occupied(mut e) => {
                            e.insert(value);
                            prev_entry = Some(e);
                        }
                        Entry::Vacant(e) => {
                            let e = e.insert_entry(value);
                            prev_entry = Some(e);
                        }
                    },
                    None => match prev_entry {
                        Some(ref mut entry) => {
                            entry.append(value);
                        }
                        None => unreachable!("HeaderMap::into_iter yielded None first"),
                    },
                }
            }
        }
        self
    }

    #[cfg(feature = "gcp")]
    pub(crate) fn bearer_auth(mut self, token: &str) -> Self {
        let value = HeaderValue::try_from(format!("Bearer {}", token));
        match (value, &mut self.request) {
            (Ok(mut v), Ok(r)) => {
                v.set_sensitive(true);
                r.headers_mut().insert(AUTHORIZATION, v);
            }
            (Err(e), Ok(_)) => self.request = Err(e.into()),
            (_, Err(_)) => {}
        }
        self
    }

    pub(crate) fn json<S: Serialize>(mut self, s: S) -> Self {
        match (serde_json::to_vec(&s), &mut self.request) {
            (Ok(json), Ok(request)) => {
                *request.body_mut() = json.into();
            }
            (Err(e), Ok(_)) => self.request = Err(e.into()),
            (_, Err(_)) => {}
        }
        self
    }

    pub(crate) fn query<T: Serialize + ?Sized>(mut self, query: &T) -> Self {
        let mut error = None;
        if let Ok(ref mut req) = self.request {
            let mut out = format!("{}?", req.uri().path());
            let mut encoder = form_urlencoded::Serializer::new(&mut out);
            let serializer = serde_urlencoded::Serializer::new(&mut encoder);

            if let Err(err) = query.serialize(serializer) {
                error = Some(err.into());
            }

            match PathAndQuery::from_maybe_shared(out) {
                Ok(p) => {
                    let mut parts = req.uri().clone().into_parts();
                    parts.path_and_query = Some(p);
                    *req.uri_mut() = Uri::from_parts(parts).unwrap();
                }
                Err(err) => error = Some(err.into()),
            }
        }
        if let Some(err) = error {
            self.request = Err(err);
        }
        self
    }

    pub(crate) fn form<T: Serialize>(mut self, form: T) -> Self {
        let mut error = None;
        if let Ok(ref mut req) = self.request {
            match serde_urlencoded::to_string(form) {
                Ok(body) => {
                    req.headers_mut().insert(
                        CONTENT_TYPE,
                        HeaderValue::from_static("application/x-www-form-urlencoded"),
                    );
                    *req.body_mut() = body.into();
                }
                Err(err) => error = Some(err.into()),
            }
        }
        if let Some(err) = error {
            self.request = Err(err);
        }
        self
    }

    pub(crate) fn body(mut self, b: impl Into<HttpRequestBody>) -> Self {
        if let Ok(r) = &mut self.request {
            *r.body_mut() = b.into();
        }
        self
    }

    pub(crate) fn into_parts(self) -> (HttpClient, Result<HttpRequest, RequestBuilderError>) {
        (self.client, self.request)
    }

    pub(crate) async fn send(self) -> Result<HttpResponse, HttpError> {
        match self.request {
            Ok(r) => Ok(self.client.execute(r).await?),
            Err(e) => Err(e.into()),
        }
    }
}
