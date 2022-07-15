use std::future::Future;
use std::time::Instant;
use tokio::sync::Mutex;

/// A temporary authentication token with an associated expiry
#[derive(Debug, Clone)]
pub struct TemporaryToken<T> {
    /// The temporary credential
    pub token: T,
    /// The instant at which this credential is no longer valid
    pub expiry: Instant,
}

/// Provides [`TokenCache::get_or_insert_with`] which can be used to cache a
/// [`TemporaryToken`] based on its expiry
#[derive(Debug, Default)]
pub struct TokenCache<T> {
    cache: Mutex<Option<TemporaryToken<T>>>,
}

impl<T: Clone + Send> TokenCache<T> {
    pub async fn get_or_insert_with<F, Fut, E>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<TemporaryToken<T>, E>> + Send,
    {
        let now = Instant::now();
        let mut locked = self.cache.lock().await;

        if let Some(cached) = locked.as_ref() {
            let delta = cached
                .expiry
                .checked_duration_since(now)
                .unwrap_or_default();

            if delta.as_secs() > 300 {
                return Ok(cached.token.clone());
            }
        }

        let cached = f().await?;
        let token = cached.token.clone();
        *locked = Some(cached);

        Ok(token)
    }
}
