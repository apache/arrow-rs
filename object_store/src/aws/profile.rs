#![cfg(feature = "aws_profile")]

use aws_config::meta::region::ProvideRegion;
use aws_config::profile::profile_file::ProfileFiles;
use aws_config::profile::ProfileFileCredentialsProvider;
use aws_config::profile::ProfileFileRegionProvider;
use aws_config::provider_config::ProviderConfig;
use aws_credential_types::provider::ProvideCredentials;
use aws_types::region::Region;
use futures::future::BoxFuture;
use std::sync::Arc;
use std::time::Instant;
use std::time::SystemTime;

use crate::aws::credential::CredentialProvider;
use crate::aws::AwsCredential;
use crate::client::token::{TemporaryToken, TokenCache};
use crate::Result;

#[cfg(test)]
pub static TEST_PROFILE_NAME: &str = "object_store:fake_profile";

#[cfg(test)]
pub static TEST_PROFILE_REGION: &str = "object_store:fake_region_from_profile";

#[derive(Debug)]
pub struct ProfileProvider {
    name: String,
    region: Option<String>,
    cache: TokenCache<Arc<AwsCredential>>,
}

impl ProfileProvider {
    pub fn new(name: String, region: Option<String>) -> Self {
        Self {
            name,
            region,
            cache: Default::default(),
        }
    }

    #[cfg(test)]
    fn profile_files(&self) -> ProfileFiles {
        use aws_config::profile::profile_file::ProfileFileKind;

        let config = format!(
            "[profile {}]\nregion = {}",
            TEST_PROFILE_NAME, TEST_PROFILE_REGION
        );

        ProfileFiles::builder()
            .with_contents(ProfileFileKind::Config, config)
            .build()
    }

    #[cfg(not(test))]
    fn profile_files(&self) -> ProfileFiles {
        ProfileFiles::default()
    }

    pub async fn get_region(&self) -> Option<String> {
        if let Some(region) = self.region.clone() {
            return Some(region);
        }

        let provider = ProfileFileRegionProvider::builder()
            .profile_files(self.profile_files())
            .profile_name(&self.name)
            .build();

        let region = provider.region().await;

        region.map(|r| r.as_ref().to_owned())
    }
}

impl CredentialProvider for ProfileProvider {
    fn get_credential(&self) -> BoxFuture<'_, Result<Arc<AwsCredential>>> {
        Box::pin(self.cache.get_or_insert_with(move || async move {
            let region = self.region.clone().map(Region::new);

            let config = ProviderConfig::default().with_region(region);

            let credentials = ProfileFileCredentialsProvider::builder()
                .configure(&config)
                .profile_name(&self.name)
                .build();

            let c = credentials.provide_credentials().await.map_err(|source| {
                crate::Error::Generic {
                    store: "S3",
                    source: Box::new(source),
                }
            })?;
            let t_now = SystemTime::now();
            let expiry = c
                .expiry()
                .and_then(|e| e.duration_since(t_now).ok())
                .map(|ttl| Instant::now() + ttl);

            Ok(TemporaryToken {
                token: Arc::new(AwsCredential {
                    key_id: c.access_key_id().to_string(),
                    secret_key: c.secret_access_key().to_string(),
                    token: c.session_token().map(ToString::to_string),
                }),
                expiry,
            })
        }))
    }
}
