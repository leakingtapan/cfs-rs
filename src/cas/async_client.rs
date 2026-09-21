use super::blocking::{bs_read_blob, create_bs_client, BsClient};
use anyhow::Result;
use bazel_remote_apis_rs::build::bazel::remote::execution::v2::Directory;
use prost::Message;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock, Semaphore};

const MAX_CONCURRENT_DOWNLOADS: usize = 64;

#[derive(Clone)]
pub struct CacheClient {
    inner: Arc<CacheClientInner>,
}

struct CacheClientInner {
    cas_client: BsClient,
    cache: RwLock<HashMap<String, Arc<Vec<u8>>>>,
    loads: Mutex<HashMap<String, Arc<Mutex<()>>>>,
    downloads: Semaphore,
}

impl CacheClient {
    pub async fn new() -> Result<Self> {
        Ok(Self {
            inner: Arc::new(CacheClientInner {
                cas_client: create_bs_client().await?,
                cache: RwLock::new(HashMap::new()),
                loads: Mutex::new(HashMap::new()),
                downloads: Semaphore::new(MAX_CONCURRENT_DOWNLOADS),
            }),
        })
    }

    pub async fn read_blob(&self, hash: &str, size: i64) -> Result<Arc<Vec<u8>>> {
        if let Some(blob) = self.inner.cache.read().await.get(hash).cloned() {
            return Ok(blob);
        }

        let load = {
            let mut loads = self.inner.loads.lock().await;
            loads
                .entry(hash.to_string())
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone()
        };
        let _guard = load.lock().await;

        if let Some(blob) = self.inner.cache.read().await.get(hash).cloned() {
            return Ok(blob);
        }

        let mut client = self.inner.cas_client.clone();
        let _permit = self.inner.downloads.acquire().await?;
        let blob = Arc::new(bs_read_blob(&mut client, hash, size).await?);
        self.inner
            .cache
            .write()
            .await
            .insert(hash.to_string(), blob.clone());
        Ok(blob)
    }

    pub async fn get_dir(&self, hash: &str, size: i64) -> Result<Directory> {
        let bytes = self.read_blob(hash, size).await?;
        Directory::decode(bytes.as_slice()).map_err(Into::into)
    }
}
