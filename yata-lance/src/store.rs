//! UreqObjectStore — `object_store::ObjectStore` implementation backed by ureq S3Client.
//!
//! Bridges the sync ureq-based S3Client to the async ObjectStore trait that Lance requires.
//! This avoids adding `reqwest` (prohibited for cross-compile) while allowing Lance Dataset
//! operations to work directly with R2/S3 storage.
//!
//! All async methods delegate to S3Client sync methods via `tokio::task::spawn_blocking`.

use std::fmt;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{
    Attributes, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};

use yata_s3::s3::S3Client;

/// ObjectStore implementation backed by ureq S3Client (sync HTTP, no reqwest).
///
/// Allows Lance Dataset to read/write directly to R2/S3 using the existing
/// ureq transport that works in Cloudflare Container environments.
pub struct UreqObjectStore {
    client: Arc<S3Client>,
}

impl fmt::Debug for UreqObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UreqObjectStore").finish()
    }
}

impl fmt::Display for UreqObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "UreqObjectStore")
    }
}

impl UreqObjectStore {
    pub fn new(client: Arc<S3Client>) -> Self {
        Self { client }
    }

    /// Build from YATA_S3_* environment variables. Returns None if not configured.
    pub fn from_env() -> Option<Self> {
        let endpoint = std::env::var("YATA_S3_ENDPOINT").ok()?;
        let bucket = std::env::var("YATA_S3_BUCKET").unwrap_or_default();
        let key_id = std::env::var("YATA_S3_ACCESS_KEY_ID")
            .or_else(|_| std::env::var("YATA_S3_KEY_ID"))
            .unwrap_or_default();
        let secret = std::env::var("YATA_S3_SECRET_ACCESS_KEY")
            .or_else(|_| std::env::var("YATA_S3_SECRET_KEY"))
            .or_else(|_| std::env::var("YATA_S3_APPLICATION_KEY"))
            .unwrap_or_default();
        let region = std::env::var("YATA_S3_REGION").unwrap_or_else(|_| "auto".to_string());
        if endpoint.is_empty() || bucket.is_empty() || key_id.is_empty() || secret.is_empty() {
            return None;
        }
        let client = Arc::new(S3Client::new(&endpoint, &bucket, &key_id, &secret, &region));
        Some(Self { client })
    }

    /// Access the underlying S3Client.
    pub fn client(&self) -> &Arc<S3Client> {
        &self.client
    }
}

fn to_object_store_error(e: yata_s3::s3::S3Error) -> object_store::Error {
    object_store::Error::Generic {
        store: "UreqObjectStore",
        source: Box::new(e),
    }
}

impl ObjectStore for UreqObjectStore {
    fn put_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjectPath,
        payload: PutPayload,
        _opts: PutOptions,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<PutResult>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let key = location.to_string();
        let client = self.client.clone();
        Box::pin(async move {
            let data: Bytes = payload.into();
            tokio::task::spawn_blocking(move || {
                client.put_sync(&key, data).map_err(to_object_store_error)?;
                Ok(PutResult {
                    e_tag: None,
                    version: None,
                })
            })
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "UreqObjectStore",
                source: Box::new(e),
            })?
        })
    }

    fn put_multipart_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _location: &'life1 ObjectPath,
        _opts: PutMultipartOptions,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = object_store::Result<Box<dyn MultipartUpload>>>
                + Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        Box::pin(async {
            Err(object_store::Error::NotImplemented)
        })
    }

    fn get_opts<'life0, 'life1, 'async_trait>(
        &'life0 self,
        location: &'life1 ObjectPath,
        options: GetOptions,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<GetResult>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let key = location.to_string();
        let client = self.client.clone();
        let location = location.clone();
        Box::pin(async move {
            if options.head {
                // HEAD request
                let exists = tokio::task::spawn_blocking({
                    let key = key.clone();
                    let client = client.clone();
                    move || client.head_sync(&key).map_err(to_object_store_error)
                })
                .await
                .map_err(|e| object_store::Error::Generic {
                    store: "UreqObjectStore",
                    source: Box::new(e),
                })??;

                if !exists {
                    return Err(object_store::Error::NotFound {
                        path: key,
                        source: "object not found".into(),
                    });
                }
                return Ok(GetResult {
                    payload: GetResultPayload::Stream(futures::stream::empty().boxed()),
                    meta: ObjectMeta {
                        location,
                        last_modified: chrono::Utc::now(),
                        size: 0,
                        e_tag: None,
                        version: None,
                    },
                    range: 0..0,
                    attributes: Attributes::new(),
                });
            }

            // Handle range requests
            if let Some(ref range) = options.range {
                let (offset, length) = match range {
                    object_store::GetRange::Bounded(r) => (r.start, r.end - r.start),
                    object_store::GetRange::Offset(o) => (*o, u64::MAX),
                    object_store::GetRange::Suffix(s) => (0, *s),
                };
                let data = tokio::task::spawn_blocking({
                    let key = key.clone();
                    let client = client.clone();
                    move || {
                        client
                            .get_range_sync(&key, offset, length)
                            .map_err(to_object_store_error)
                    }
                })
                .await
                .map_err(|e| object_store::Error::Generic {
                    store: "UreqObjectStore",
                    source: Box::new(e),
                })??;

                match data {
                    Some(bytes) => {
                        let len = bytes.len() as u64;
                        Ok(GetResult {
                            payload: GetResultPayload::Stream(
                                futures::stream::once(async move { Ok(bytes) }).boxed(),
                            ),
                            meta: ObjectMeta {
                                location,
                                last_modified: chrono::Utc::now(),
                                size: len,
                                e_tag: None,
                                version: None,
                            },
                            range: offset..offset + len,
                            attributes: Attributes::new(),
                        })
                    }
                    None => Err(object_store::Error::NotFound {
                        path: key,
                        source: "object not found".into(),
                    }),
                }
            } else {
                // Full GET
                let data = tokio::task::spawn_blocking({
                    let key = key.clone();
                    let client = client.clone();
                    move || client.get_sync(&key).map_err(to_object_store_error)
                })
                .await
                .map_err(|e| object_store::Error::Generic {
                    store: "UreqObjectStore",
                    source: Box::new(e),
                })??;

                match data {
                    Some(bytes) => {
                        let len = bytes.len() as u64;
                        Ok(GetResult {
                            payload: GetResultPayload::Stream(
                                futures::stream::once(async move { Ok(bytes) }).boxed(),
                            ),
                            meta: ObjectMeta {
                                location,
                                last_modified: chrono::Utc::now(),
                                size: len,
                                e_tag: None,
                                version: None,
                            },
                            range: 0..len,
                            attributes: Attributes::new(),
                        })
                    }
                    None => Err(object_store::Error::NotFound {
                        path: key,
                        source: "object not found".into(),
                    }),
                }
            }
        })
    }

    fn delete<'life0, 'life1, 'async_trait>(
        &'life0 self,
        _location: &'life1 ObjectPath,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<()>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        // S3Client doesn't have delete — not needed for Lance read/append workloads
        Box::pin(async { Err(object_store::Error::NotImplemented) })
    }

    fn list(&self, prefix: Option<&ObjectPath>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let prefix_str = prefix.map(|p| p.to_string()).unwrap_or_default();
        let client = self.client.clone();
        let items = match client.list_sync(&prefix_str) {
            Ok(objects) => objects
                .into_iter()
                .map(|obj| {
                    Ok(ObjectMeta {
                        location: ObjectPath::from(obj.key),
                        last_modified: chrono::Utc::now(),
                        size: obj.size,
                        e_tag: None,
                        version: None,
                    })
                })
                .collect::<Vec<_>>(),
            Err(e) => vec![Err(to_object_store_error(e))],
        };
        futures::stream::iter(items).boxed()
    }

    fn list_with_delimiter<'life0, 'life1, 'async_trait>(
        &'life0 self,
        prefix: Option<&'life1 ObjectPath>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<ListResult>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        let prefix_str = prefix.map(|p| p.to_string()).unwrap_or_default();
        let client = self.client.clone();
        Box::pin(async move {
            let objects = tokio::task::spawn_blocking(move || {
                client.list_sync(&prefix_str).map_err(to_object_store_error)
            })
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "UreqObjectStore",
                source: Box::new(e),
            })??;

            let items: Vec<ObjectMeta> = objects
                .into_iter()
                .map(|obj| ObjectMeta {
                    location: ObjectPath::from(obj.key),
                    last_modified: chrono::Utc::now(),
                    size: obj.size,
                    e_tag: None,
                    version: None,
                })
                .collect();

            Ok(ListResult {
                common_prefixes: Vec::new(),
                objects: items,
            })
        })
    }

    fn copy<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        from: &'life1 ObjectPath,
        to: &'life2 ObjectPath,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<()>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        let from_key = from.to_string();
        let to_key = to.to_string();
        let client = self.client.clone();
        Box::pin(async move {
            // Implement as get + put (S3 copy not available in ureq client)
            let data = tokio::task::spawn_blocking({
                let client = client.clone();
                let from_key = from_key.clone();
                move || client.get_sync(&from_key).map_err(to_object_store_error)
            })
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "UreqObjectStore",
                source: Box::new(e),
            })??;

            let data = data.ok_or_else(|| object_store::Error::NotFound {
                path: from_key,
                source: "source object not found".into(),
            })?;

            tokio::task::spawn_blocking(move || {
                client.put_sync(&to_key, data).map_err(to_object_store_error)
            })
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "UreqObjectStore",
                source: Box::new(e),
            })?
        })
    }

    fn copy_if_not_exists<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        from: &'life1 ObjectPath,
        to: &'life2 ObjectPath,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = object_store::Result<()>> + Send + 'async_trait>,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait,
    {
        let to_key = to.to_string();
        let client = self.client.clone();
        let from = from.clone();
        let to = to.clone();
        Box::pin(async move {
            // Check if destination exists
            let exists = tokio::task::spawn_blocking({
                let client = client.clone();
                let to_key = to_key.clone();
                move || client.head_sync(&to_key).map_err(to_object_store_error)
            })
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "UreqObjectStore",
                source: Box::new(e),
            })??;

            if exists {
                return Err(object_store::Error::AlreadyExists {
                    path: to_key,
                    source: "destination already exists".into(),
                });
            }
            // Delegate to copy
            self.copy(&from, &to).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_env_returns_none_without_env() {
        // Ensure no YATA_S3_ENDPOINT set in test env
        unsafe { std::env::remove_var("YATA_S3_ENDPOINT") };
        assert!(UreqObjectStore::from_env().is_none());
    }

    #[test]
    fn display_and_debug() {
        let client = Arc::new(S3Client::new(
            "https://example.com",
            "bucket",
            "key",
            "secret",
            "auto",
        ));
        let store = UreqObjectStore::new(client);
        assert_eq!(format!("{store}"), "UreqObjectStore");
        assert!(format!("{store:?}").contains("UreqObjectStore"));
    }

    #[test]
    fn client_accessor() {
        let client = Arc::new(S3Client::new(
            "https://example.com",
            "bucket",
            "key",
            "secret",
            "auto",
        ));
        let store = UreqObjectStore::new(client.clone());
        // Verify it returns the same Arc
        assert!(Arc::ptr_eq(store.client(), &client));
    }
}
