use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::azure::{MicrosoftAzureBuilder, MicrosoftAzure};
use object_store::path::Path;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore,
};
use std::fmt::Formatter;
use tokio::io::AsyncWrite;

#[derive(Debug)]
struct AzureStore(MicrosoftAzure);

impl std::fmt::Display for AzureStore {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        todo!()
    }
}

#[async_trait]
impl ObjectStore for AzureStore {
    async fn put(&self, path: &Path, data: Bytes) -> object_store::Result<()> {
        self.0.put(path, data).await
    }

    async fn put_multipart(
        &self,
        _: &Path,
    ) -> object_store::Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        todo!()
    }

    async fn abort_multipart(
        &self,
        _: &Path,
        _: &MultipartId,
    ) -> object_store::Result<()> {
        todo!()
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.0.get_opts(location, options).await
    }

    async fn head(&self, _: &Path) -> object_store::Result<ObjectMeta> {
        todo!()
    }

    async fn delete(&self, _: &Path) -> object_store::Result<()> {
        todo!()
    }

    async fn list(
        &self,
        _: Option<&Path>,
    ) -> object_store::Result<BoxStream<'_, object_store::Result<ObjectMeta>>> {
        todo!()
    }

    async fn list_with_delimiter(
        &self,
        _: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        todo!()
    }

    async fn copy(&self, _: &Path, _: &Path) -> object_store::Result<()> {
        todo!()
    }

    async fn copy_if_not_exists(&self, _: &Path, _: &Path) -> object_store::Result<()> {
        todo!()
    }
}


#[tokio::test]
async fn test_fabric() {        
    //Format:https://onelake.dfs.fabric.microsoft.com/<workspaceGUID>/<itemGUID>/Files/test.csv
    //Example:https://onelake.dfs.fabric.microsoft.com/86bc63cf-5086-42e0-b16d-6bc580d1dc87/17d3977c-d46e-4bae-8fed-ff467e674aed/Files/SampleCustomerList.csv
    //Account Name : onelake
    //Container Name : workspaceGUID

    let daily_store = AzureStore(
        MicrosoftAzureBuilder::new()
        .with_container_name("86bc63cf-5086-42e0-b16d-6bc580d1dc87")
        .with_account("onelake")
        .with_bearer_token_authorization("jwt-token")
        .build()
        .unwrap());
    
    let path = Path::from("17d3977c-d46e-4bae-8fed-ff467e674aed/Files/SampleCustomerList.csv");

    read_write_test(&daily_store, &path).await;
}


async fn read_write_test(store: &AzureStore, path: &Path) {
    let expected = Bytes::from_static(b"hello world");
    store.put(path, expected.clone()).await.unwrap();
    let fetched = store.get(path).await.unwrap().bytes().await.unwrap();
    assert_eq!(expected, fetched);

    for range in [0..10, 3..5, 0..expected.len()] {
        let data = store.get_range(path, range.clone()).await.unwrap();
        assert_eq!(&data[..], &expected[range])
    }
}

