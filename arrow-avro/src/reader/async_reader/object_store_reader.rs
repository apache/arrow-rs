use crate::reader::async_reader::{AsyncFileReader, DataFetchFutureBoxed};
use arrow_schema::ArrowError;
use std::ops::Range;
use std::sync::Arc;

/// An implementation of an AsyncFileReader using the [`object_store::ObjectStore`] API.
pub struct ObjectStoreFileReader {
    store: Arc<dyn object_store::ObjectStore>,
    location: object_store::path::Path,
}

impl ObjectStoreFileReader {
    /// Creates a new [`Self`] from a store implementation and file location.
    pub fn new(
        store: Arc<dyn object_store::ObjectStore>,
        location: object_store::path::Path,
    ) -> Self {
        Self { store, location }
    }
}

impl AsyncFileReader for ObjectStoreFileReader {
    fn fetch_range(&mut self, range: Range<u64>) -> DataFetchFutureBoxed {
        let store = Arc::clone(&self.store);
        let location = self.location.clone();
        Box::pin(async move {
            let res = store
                .get_ranges(&location, &[range])
                .await
                .map_err(|err| ArrowError::AvroError(format!("{}", err)))?;

            if res.len() != 1 {
                return Err(ArrowError::AvroError(format!(
                    "Requested 1 chunk of data, but received {}",
                    res.len()
                )));
            }
            Ok(res.into_iter().next().unwrap())
        })
    }

    fn fetch_extra_range(&mut self, range: Range<u64>) -> DataFetchFutureBoxed {
        let store = Arc::clone(&self.store);
        let location = self.location.clone();
        Box::pin(async move {
            store
                .get_range(&location, range)
                .await
                .map_err(|err| ArrowError::AvroError(format!("{}", err)))
        })
    }
}
