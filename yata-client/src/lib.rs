#![allow(dead_code)]

//! YATA client SDK.
//!
//! In Phase 1, this wraps Broker directly (in-process).
//! Phase 2: add TCP/UCX transport and make YataClient a remote handle.

use async_trait::async_trait;
use std::sync::Arc;
use yata_arrow::ArrowBatchHandle;
use yata_core::{
    Ack, KvAck, KvEntry, ObjectManifest, ObjectMeta, Result, Revision, Sequence,
};
use yata_ocel::{OcelEventDraft, OcelLog};

/// Options for publishing.
#[derive(Clone, Debug, Default)]
pub struct PublishOpts {
    pub expected_last_seq: Option<Sequence>,
}

/// The YATA client handle.
///
/// Clone-able, cheap to copy (Arc-backed).
#[derive(Clone)]
pub struct YataClient {
    inner: Arc<dyn ClientBackend>,
}

#[async_trait]
pub trait ClientBackend: Send + Sync + 'static {
    async fn publish_arrow(
        &self,
        stream: &str,
        subject: &str,
        batch: ArrowBatchHandle,
        opts: PublishOpts,
    ) -> Result<Ack>;
    async fn publish_ocel_event(
        &self,
        stream: &str,
        event: OcelEventDraft,
        payload: Option<ArrowBatchHandle>,
    ) -> Result<Ack>;
    async fn kv_put(
        &self,
        bucket: &str,
        key: &str,
        value: bytes::Bytes,
        expected_rev: Option<Revision>,
    ) -> Result<KvAck>;
    async fn kv_get(&self, bucket: &str, key: &str) -> Result<Option<KvEntry>>;
    async fn kv_delete(
        &self,
        bucket: &str,
        key: &str,
        expected_rev: Option<Revision>,
    ) -> Result<KvAck>;
    async fn put_object(&self, data: bytes::Bytes, meta: ObjectMeta) -> Result<ObjectManifest>;
    async fn get_object(&self, id: &yata_core::ObjectId) -> Result<bytes::Bytes>;
    async fn export_ocel_json(&self, dataset: &str) -> Result<String>;
}

impl YataClient {
    pub fn new(backend: Arc<dyn ClientBackend>) -> Self {
        Self { inner: backend }
    }

    pub async fn publish_arrow(
        &self,
        stream: &str,
        subject: &str,
        batch: ArrowBatchHandle,
        opts: PublishOpts,
    ) -> Result<Ack> {
        self.inner.publish_arrow(stream, subject, batch, opts).await
    }

    pub async fn publish_ocel_event(
        &self,
        stream: &str,
        event: OcelEventDraft,
        payload: Option<ArrowBatchHandle>,
    ) -> Result<Ack> {
        self.inner.publish_ocel_event(stream, event, payload).await
    }

    pub async fn kv_put(
        &self,
        bucket: &str,
        key: &str,
        value: bytes::Bytes,
        expected_rev: Option<Revision>,
    ) -> Result<KvAck> {
        self.inner.kv_put(bucket, key, value, expected_rev).await
    }

    pub async fn kv_get(&self, bucket: &str, key: &str) -> Result<Option<KvEntry>> {
        self.inner.kv_get(bucket, key).await
    }

    pub async fn kv_delete(
        &self,
        bucket: &str,
        key: &str,
        expected_rev: Option<Revision>,
    ) -> Result<KvAck> {
        self.inner.kv_delete(bucket, key, expected_rev).await
    }

    pub async fn put_object(
        &self,
        data: bytes::Bytes,
        meta: ObjectMeta,
    ) -> Result<ObjectManifest> {
        self.inner.put_object(data, meta).await
    }

    pub async fn get_object(&self, id: &yata_core::ObjectId) -> Result<bytes::Bytes> {
        self.inner.get_object(id).await
    }

    pub async fn export_ocel_json(&self, dataset: &str) -> Result<String> {
        self.inner.export_ocel_json(dataset).await
    }
}
