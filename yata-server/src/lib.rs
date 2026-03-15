#![allow(dead_code)]

//! YATA broker/runtime — wires together log, kv, object, ocel, lance.

use std::path::PathBuf;
use std::sync::Arc;
use yata_arrow::ArrowBatchHandle;
use yata_core::{
    Ack, AppendLog, Blake3Hash, Envelope, PayloadRef, PublishRequest, Result, SchemaId, StreamId,
    Subject,
};
use yata_kv::KvBucketStore;
use yata_lance::{LocalLanceSink, LanceSink, SyncStats};
use yata_log::{LocalLog, PayloadStore};
use yata_object::LocalObjectStore;
use yata_ocel::{MemoryOcelProjector, OcelEventDraft, OcelProjector};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BrokerConfig {
    /// Base directory for log segments and CAS.
    pub data_dir: PathBuf,
    /// Lance dataset URI (local path or s3-compatible URL).
    pub lance_uri: String,
    /// Lance flush interval (milliseconds).
    pub lance_flush_interval_ms: u64,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./yata-data"),
            lance_uri: "./yata-lance".to_string(),
            lance_flush_interval_ms: 5000,
        }
    }
}

/// The central broker: owns all subsystems.
pub struct Broker {
    pub config: BrokerConfig,
    pub log: Arc<LocalLog>,
    pub kv: Arc<KvBucketStore>,
    pub objects: Arc<LocalObjectStore>,
    pub ocel: Arc<MemoryOcelProjector>,
    pub lance: Arc<LocalLanceSink>,
}

impl Broker {
    /// Initialize all subsystems from config.
    pub async fn new(config: BrokerConfig) -> anyhow::Result<Self> {
        let data_dir = &config.data_dir;
        tokio::fs::create_dir_all(data_dir).await?;

        let log = Arc::new(LocalLog::new(data_dir.join("log")).await?);
        log.recover().await?;

        let kv_payload_store = Arc::new(
            PayloadStore::new(data_dir.join("kv_payloads"))
                .await
                .map_err(anyhow::Error::from)?,
        );
        let kv = Arc::new(KvBucketStore::new(log.clone(), kv_payload_store).await?);
        let objects = Arc::new(
            LocalObjectStore::new(data_dir.join("objects")).await?,
        );
        let ocel = Arc::new(MemoryOcelProjector::new());
        let lance = Arc::new(LocalLanceSink::new(&config.lance_uri).await?);

        Ok(Self {
            config,
            log,
            kv,
            objects,
            ocel,
            lance,
        })
    }

    /// Start background tasks: Lance flush, GC scheduler.
    pub async fn start_background_tasks(self: Arc<Self>) -> anyhow::Result<()> {
        let flush_interval = self.config.lance_flush_interval_ms;
        let broker = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(flush_interval));
            loop {
                interval.tick().await;
                if let Err(e) = broker.flush_lance().await {
                    tracing::warn!("lance flush error: {}", e);
                }
            }
        });
        Ok(())
    }

    /// Publish an Arrow batch to a stream.
    pub async fn publish_arrow(
        &self,
        stream: &str,
        subject: &str,
        batch: ArrowBatchHandle,
    ) -> Result<Ack> {
        let stream_id = StreamId::from(stream);
        let subject_id = Subject::from(subject);
        let payload = batch.as_payload_ref();
        let content_hash = batch.content_hash.clone();
        let schema_id = SchemaId("yata.arrow.batch".to_string());
        let envelope = Envelope::new(subject_id.clone(), schema_id, content_hash);

        let req = PublishRequest {
            stream: stream_id,
            subject: subject_id,
            envelope,
            payload,
            expected_last_seq: None,
        };
        self.log.append(req).await
    }

    /// Publish an OCEL event (with optional Arrow payload).
    pub async fn publish_ocel_event(
        &self,
        stream: &str,
        event: OcelEventDraft,
        payload: Option<ArrowBatchHandle>,
    ) -> Result<Ack> {
        let ts_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let event_id = uuid::Uuid::new_v4().to_string();

        // Build OCEL event and project it
        let ocel_event = yata_ocel::OcelEvent {
            event_id: event_id.clone(),
            event_type: event.event_type.clone(),
            timestamp: chrono::Utc::now(),
            attrs: event.attrs.clone(),
            message_id: None,
            stream_id: Some(stream.to_owned()),
            seq: None,
        };
        self.ocel.project_event(ocel_event).await?;

        // Build E2O edges
        for obj_ref in &event.object_refs {
            let edge = yata_ocel::OcelEventObjectEdge {
                event_id: event_id.clone(),
                object_id: obj_ref.object_id.clone(),
                qualifier: obj_ref.qualifier.clone().unwrap_or_default(),
                role: obj_ref.role.clone(),
            };
            self.ocel.add_e2o(edge).await?;
        }

        let stream_id = StreamId::from(stream);
        let subject_id = Subject(format!("ocel.event.{}", event.event_type));

        let (payload_ref, content_hash) = if let Some(batch) = payload {
            let h = batch.content_hash.clone();
            (batch.as_payload_ref(), h)
        } else {
            let data = event_id.as_bytes().to_vec();
            let h = Blake3Hash::of(&data);
            (PayloadRef::InlineBytes(bytes::Bytes::from(data)), h)
        };

        let mut envelope = Envelope::new(
            subject_id.clone(),
            SchemaId("yata.ocel.event".to_string()),
            content_hash,
        );
        envelope.ocel_event_type = Some(event.event_type.clone());
        for obj_ref in &event.object_refs {
            envelope.ocel_object_refs.push(obj_ref.clone());
        }

        let req = PublishRequest {
            stream: stream_id,
            subject: subject_id,
            envelope,
            payload: payload_ref,
            expected_last_seq: None,
        };
        self.log.append(req).await
    }

    /// Flush pending OCEL projection to Lance.
    pub async fn flush_lance(&self) -> Result<SyncStats> {
        let snapshot = self.ocel.snapshot().await?;

        if !snapshot.events.is_empty() {
            self.lance.write_ocel_events(&snapshot.events).await?;
        }
        if !snapshot.objects.is_empty() {
            self.lance.write_ocel_objects(&snapshot.objects).await?;
        }
        if !snapshot.event_object_edges.is_empty() {
            self.lance.write_e2o_edges(&snapshot.event_object_edges).await?;
        }
        if !snapshot.object_object_edges.is_empty() {
            self.lance.write_o2o_edges(&snapshot.object_object_edges).await?;
        }

        let stats = self.lance.sync_stats().await?;
        tracing::debug!(?stats, "lance flush complete");
        Ok(stats)
    }
}

/// In-process ClientBackend backed by a Broker.
pub struct BrokerBackend {
    broker: Arc<Broker>,
}

impl BrokerBackend {
    pub fn new(broker: Arc<Broker>) -> Self {
        Self { broker }
    }

    pub fn into_client(self) -> yata_client::YataClient {
        yata_client::YataClient::new(Arc::new(self))
    }
}

#[async_trait::async_trait]
impl yata_client::ClientBackend for BrokerBackend {
    async fn publish_arrow(
        &self,
        stream: &str,
        subject: &str,
        batch: yata_arrow::ArrowBatchHandle,
        _opts: yata_client::PublishOpts,
    ) -> yata_core::Result<yata_core::Ack> {
        self.broker.publish_arrow(stream, subject, batch).await
    }

    async fn publish_ocel_event(
        &self,
        stream: &str,
        event: yata_ocel::OcelEventDraft,
        payload: Option<yata_arrow::ArrowBatchHandle>,
    ) -> yata_core::Result<yata_core::Ack> {
        self.broker.publish_ocel_event(stream, event, payload).await
    }

    async fn kv_put(
        &self,
        bucket: &str,
        key: &str,
        value: bytes::Bytes,
        expected_rev: Option<yata_core::Revision>,
    ) -> yata_core::Result<yata_core::KvAck> {
        use yata_core::{BucketId, KvPutRequest, KvStore};
        self.broker.kv.put(KvPutRequest {
            bucket: BucketId::from(bucket),
            key: key.to_owned(),
            value,
            expected_revision: expected_rev,
            ttl_secs: None,
        }).await
    }

    async fn kv_get(
        &self,
        bucket: &str,
        key: &str,
    ) -> yata_core::Result<Option<yata_core::KvEntry>> {
        use yata_core::{BucketId, KvStore};
        self.broker.kv.get(&BucketId::from(bucket), key).await
    }

    async fn kv_delete(
        &self,
        bucket: &str,
        key: &str,
        expected_rev: Option<yata_core::Revision>,
    ) -> yata_core::Result<yata_core::KvAck> {
        use yata_core::{BucketId, KvStore};
        self.broker.kv.delete(&BucketId::from(bucket), key, expected_rev).await
    }

    async fn put_object(
        &self,
        data: bytes::Bytes,
        meta: yata_core::ObjectMeta,
    ) -> yata_core::Result<yata_core::ObjectManifest> {
        use yata_core::ObjectStorage;
        self.broker.objects.put_object(data, meta).await
    }

    async fn get_object(
        &self,
        id: &yata_core::ObjectId,
    ) -> yata_core::Result<bytes::Bytes> {
        use yata_core::ObjectStorage;
        self.broker.objects.get_object(id).await
    }

    async fn export_ocel_json(&self, _dataset: &str) -> yata_core::Result<String> {
        use yata_ocel::OcelProjector;
        let snapshot = self.broker.ocel.snapshot().await?;
        yata_ocel::export_json(&snapshot)
            .map_err(|e| yata_core::YataError::Serialization(e.to_string()))
    }
}
