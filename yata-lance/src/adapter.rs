//! ObjectStore adapter: wraps yata-s3 (sync ureq+rustls) as async ObjectStore trait.
//!
//! Lance requires `object_store::ObjectStore` for all I/O.
//! This adapter bridges yata-s3's sync S3Client to the async trait
//! via `tokio::task::spawn_blocking`.

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::{
    path::Path, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOpts, PutOptions, PutPayload, PutResult, Result,
};
use std::sync::Arc;
use tokio::task::spawn_blocking;

/// Adapter wrapping yata-s3 S3Client as an async ObjectStore.
#[derive(Debug)]
pub struct YataS3ObjectStore {
    client: Arc<yata_s3::S3Client>,
}

impl YataS3ObjectStore {
    pub fn new(client: Arc<yata_s3::S3Client>) -> Self {
        Self { client }
    }
}

impl std::fmt::Display for YataS3ObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "YataS3ObjectStore")
    }
}

#[async_trait]
impl ObjectStore for YataS3ObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        _opts: PutOptions,
    ) -> Result<PutResult> {
        let client = self.client.clone();
        let key = location.to_string();
        let data: Bytes = payload.into();
        spawn_blocking(move || {
            client
                .put_sync(&key, data)
                .map_err(|e| object_store::Error::Generic {
                    store: "yata-s3",
                    source: Box::new(std::io::Error::new(std::io::ErrorKind::Other, e)),
                })?;
            Ok(PutResult {
                e_tag: None,
                version: None,
            })
        })
        .await
        .map_err(|e| object_store::Error::Generic {
            store: "yata-s3",
            source: Box::new(e),
        })?
    }

    async fn get_opts(
        &self,
        location: &Path,
        _opts: GetOptions,
    ) -> Result<GetResult> {
        let client = self.client.clone();
        let key = location.to_string();
        let data = spawn_blocking(move || {
            client
                .get_sync(&key)
                .map_err(|e| object_store::Error::Generic {
                    store: "yata-s3",
                    source: Box::new(std::io::Error::new(std::io::ErrorKind::Other, e)),
                })
        })
        .await
        .map_err(|e| object_store::Error::Generic {
            store: "yata-s3",
            source: Box::new(e),
        })??;

        match data {
            Some(bytes) => {
                let len = bytes.len();
                Ok(GetResult {
                    payload: GetResultPayload::Stream(Box::pin(futures::stream::once(
                        async move { Ok(bytes) },
                    ))),
                    meta: ObjectMeta {
                        location: location.clone(),
                        last_modified: chrono::Utc::now(),
                        size: len,
                        e_tag: None,
                        version: None,
                    },
                    range: 0..len,
                    attributes: Default::default(),
                })
            }
            None => Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: Box::new(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "key not found",
                )),
            }),
        }
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let client = self.client.clone();
        let key = location.to_string();
        spawn_blocking(move || {
            client
                .delete_sync(&key)
                .map_err(|e| object_store::Error::Generic {
                    store: "yata-s3",
                    source: Box::new(std::io::Error::new(std::io::ErrorKind::Other, e)),
                })
        })
        .await
        .map_err(|e| object_store::Error::Generic {
            store: "yata-s3",
            source: Box::new(e),
        })?
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, Result<ObjectMeta>> {
        let client = self.client.clone();
        let prefix_str = prefix.map(|p| p.to_string()).unwrap_or_default();
        Box::pin(futures::stream::once(async move {
            let keys = spawn_blocking(move || {
                client
                    .list_sync(&prefix_str)
                    .map_err(|e| object_store::Error::Generic {
                        store: "yata-s3",
                        source: Box::new(std::io::Error::new(std::io::ErrorKind::Other, e)),
                    })
            })
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "yata-s3",
                source: Box::new(e),
            })??;

            Ok(futures::stream::iter(keys.into_iter().map(|key| {
                Ok(ObjectMeta {
                    location: Path::from(key),
                    last_modified: chrono::Utc::now(),
                    size: 0,
                    e_tag: None,
                    version: None,
                })
            })))
        })
        .flat_map(|result| match result {
            Ok(stream) => stream,
            Err(e) => Box::pin(futures::stream::once(async move { Err(e) })),
        }))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        use futures::StreamExt;
        let mut objects = Vec::new();
        let mut stream = self.list(prefix);
        while let Some(item) = stream.next().await {
            objects.push(item?);
        }
        Ok(ListResult {
            common_prefixes: Vec::new(),
            objects,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let data = self.get_opts(from, GetOptions::default()).await?;
        let bytes = data.bytes().await?;
        self.put_opts(to, bytes.into(), PutOptions::default()).await?;
        Ok(())
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        self.copy(from, to).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        _opts: PutMultipartOpts,
    ) -> Result<Box<dyn MultipartUpload>> {
        Err(object_store::Error::NotImplemented)
    }
}
