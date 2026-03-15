#![allow(dead_code)]

//! YATA broker/runtime — wires together log, kv, object, ocel, lance.
//!
//! Tiered storage is always active when `BrokerConfig.b2` is set:
//! - ObjectStore: local CAS + async B2 sync, read-through fallback
//! - KV: flushed to Lance `yata_kv_history` + log segments synced to B2
//! - Flight/Lance: datasets synced to B2 periodically
//! - Cypher: implicitly tiered via OCEL → Lance → B2

use std::path::PathBuf;
use std::sync::Arc;
use yata_arrow::ArrowBatchHandle;
use yata_b2::{B2Config, B2Sync, TieredObjectStore};
use yata_core::{
    Ack, AppendLog, Blake3Hash, Envelope, KvStore, ObjectStorage, PayloadRef, PublishRequest,
    Result, SchemaId, StreamId, Subject,
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
    /// B2 credentials. When set, enables tiered storage for all subsystems.
    pub b2: Option<B2Config>,
    /// How often to sync log/kv_payloads/lance dirs to B2 (milliseconds).
    pub b2_sync_interval_ms: u64,
    /// Graph store base URI. When set, initializes a LanceGraphStore at this path.
    pub graph_uri: Option<String>,
    /// NATS config. When set, uses NATS JetStream for log/kv/object instead of local.
    pub nats: Option<yata_nats::NatsConfig>,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./yata-data"),
            lance_uri: "./yata-lance".to_string(),
            lance_flush_interval_ms: 5000,
            b2: None,
            b2_sync_interval_ms: 30_000,
            graph_uri: None,
            nats: None,
        }
    }
}

/// The central broker: owns all subsystems.
pub struct Broker {
    pub config: BrokerConfig,
    pub log: Arc<dyn AppendLog>,
    pub kv: Arc<dyn KvStore>,
    /// ObjectStore — NATS / TieredObjectStore / LocalObjectStore.
    pub objects: Arc<dyn ObjectStorage>,
    pub ocel: Arc<MemoryOcelProjector>,
    pub lance: Arc<LocalLanceSink>,
    /// Graph store — initialized when `BrokerConfig.graph_uri` is set.
    pub graph: Option<Arc<yata_graph::LanceGraphStore>>,
    /// Local KV handle for drain_pending (None when NATS is used).
    local_kv: Option<Arc<KvBucketStore>>,
    /// B2 sync handle; used by background tasks for log/kv/lance dir sync.
    b2_sync: Option<Arc<B2Sync>>,
    /// NATS backend (for Arrow pub/sub when NATS is enabled).
    pub nats: Option<Arc<yata_nats::NatsBackend>>,
}

impl Broker {
    /// Initialize all subsystems from config.
    pub async fn new(config: BrokerConfig) -> anyhow::Result<Self> {
        let data_dir = &config.data_dir;
        tokio::fs::create_dir_all(data_dir).await?;

        // NATS backend (when configured, replaces local log/kv/object)
        let nats_backend = if let Some(ref nats_cfg) = config.nats {
            Some(Arc::new(
                yata_nats::NatsBackend::connect(nats_cfg)
                    .await
                    .map_err(|e| anyhow::anyhow!("nats connect: {e}"))?,
            ))
        } else {
            None
        };

        let (log, kv, local_kv_handle, objects, b2_sync): (
            Arc<dyn AppendLog>,
            Arc<dyn KvStore>,
            Option<Arc<KvBucketStore>>,
            Arc<dyn ObjectStorage>,
            Option<Arc<B2Sync>>,
        ) = if let Some(ref nats) = nats_backend {
            // NATS JetStream backends
            let log: Arc<dyn AppendLog> = Arc::new(nats.append_log());
            let kv: Arc<dyn KvStore> = Arc::new(nats.kv_store());
            let objects: Arc<dyn ObjectStorage> = Arc::new(nats.object_store());
            (log, kv, None, objects, None)
        } else {
            // Local backends (original path)
            let local_log = Arc::new(LocalLog::new(data_dir.join("log")).await?);
            local_log.recover().await?;

            let kv_payload_store = Arc::new(
                PayloadStore::new(data_dir.join("kv_payloads"))
                    .await
                    .map_err(anyhow::Error::from)?,
            );
            let local_kv = Arc::new(KvBucketStore::new(local_log.clone(), kv_payload_store).await?);

            let local_objects =
                Arc::new(LocalObjectStore::new(data_dir.join("objects")).await?);

            let (objects, b2_sync): (Arc<dyn ObjectStorage>, Option<Arc<B2Sync>>) =
                if let Some(ref b2_cfg) = config.b2 {
                    let b2 = Arc::new(
                        B2Sync::new(b2_cfg.clone(), local_objects.clone())
                            .map_err(anyhow::Error::from)?,
                    );
                    let tiered = Arc::new(TieredObjectStore::new(local_objects, b2.clone()));
                    (tiered, Some(b2))
                } else {
                    (local_objects as Arc<dyn ObjectStorage>, None)
                };

            (local_log as Arc<dyn AppendLog>, local_kv.clone() as Arc<dyn KvStore>, Some(local_kv), objects, b2_sync)
        };

        let ocel = Arc::new(MemoryOcelProjector::new());
        let lance = Arc::new(LocalLanceSink::new(&config.lance_uri).await?);

        let graph = if let Some(ref graph_uri) = config.graph_uri {
            Some(Arc::new(
                yata_graph::LanceGraphStore::new(graph_uri)
                    .await
                    .map_err(|e| anyhow::anyhow!("graph store init: {e}"))?,
            ))
        } else {
            None
        };

        Ok(Self {
            config,
            log,
            kv,
            objects,
            ocel,
            lance,
            graph,
            local_kv: local_kv_handle,
            b2_sync,
            nats: nats_backend,
        })
    }

    /// Start background tasks: Lance flush, B2 data sync.
    pub async fn start_background_tasks(self: Arc<Self>) -> anyhow::Result<()> {
        // Lance flush loop
        {
            let broker = self.clone();
            let flush_ms = self.config.lance_flush_interval_ms;
            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(tokio::time::Duration::from_millis(flush_ms));
                loop {
                    interval.tick().await;
                    if let Err(e) = broker.flush_lance().await {
                        tracing::warn!("lance flush error: {}", e);
                    }
                }
            });
        }

        // NATS → Lance consumer: subscribes to yata.arrow.> and writes to LanceDB
        if let Some(ref nats) = self.nats {
            let writer = yata_nats::NatsLanceWriter::new(
                nats.jetstream.clone(),
                self.lance.connection().clone(),
            );
            tokio::spawn(async move {
                if let Err(e) = writer.run().await {
                    tracing::error!("NatsLanceWriter stopped: {e}");
                }
            });
        }

        // B2 data sync loop: log segments, kv_payloads, lance datasets
        if let Some(ref b2) = self.b2_sync {
            let b2 = b2.clone();
            let data_dir = self.config.data_dir.clone();
            let lance_uri = self.config.lance_uri.clone();
            let sync_ms = self.config.b2_sync_interval_ms;
            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(tokio::time::Duration::from_millis(sync_ms));
                loop {
                    interval.tick().await;

                    // Sync append-only log segments
                    if let Err(e) = b2.sync_dir(&data_dir.join("log"), "log/").await {
                        tracing::warn!("b2 log sync error: {}", e);
                    }
                    // Sync KV payload blobs
                    if let Err(e) =
                        b2.sync_dir(&data_dir.join("kv_payloads"), "kv_payloads/").await
                    {
                        tracing::warn!("b2 kv_payloads sync error: {}", e);
                    }
                    // Sync Lance datasets (when lance_uri is a local path)
                    if !lance_uri.starts_with("s3://") && !lance_uri.starts_with("gs://") {
                        let lance_path = std::path::Path::new(&lance_uri);
                        if let Err(e) = b2.sync_dir(lance_path, "lance/").await {
                            tracing::warn!("b2 lance sync error: {}", e);
                        }
                    }
                }
            });
        }

        Ok(())
    }

    /// Publish an Arrow batch to a stream.
    ///
    /// When NATS is configured, also publishes as Arrow IPC to
    /// `yata.arrow.yata_messages` for Lance ingestion.
    pub async fn publish_arrow(
        &self,
        stream: &str,
        subject: &str,
        batch: ArrowBatchHandle,
    ) -> Result<Ack> {
        // Produce to NATS Arrow subject for Lance consumer
        if let Some(ref nats) = self.nats {
            let publisher = nats.arrow_publisher();
            let _ = publisher
                .publish_batch(
                    "yata.arrow.yata_messages",
                    &batch.batch,
                    Some("yata_messages"),
                )
                .await;
        }

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

    /// Flush pending OCEL projection + KV history.
    ///
    /// When NATS is configured: produce Arrow IPC batches to NATS subjects.
    /// NatsLanceWriter consumer subscribes and writes to LanceDB.
    ///
    /// When NATS is not configured: write directly to Lance (original path).
    pub async fn flush_lance(&self) -> Result<SyncStats> {
        let snapshot = self.ocel.snapshot().await?;

        if let Some(ref nats) = self.nats {
            // NATS path: produce Arrow IPC batches to NATS subjects.
            // NatsLanceWriter consumer handles the Lance append.
            let publisher = nats.arrow_publisher();
            use yata_lance::sink::*;

            if !snapshot.events.is_empty() {
                let batch = ocel_events_to_batch(&snapshot.events)?;
                let _ = publisher
                    .publish_batch("yata.arrow.yata_events", &batch, Some("yata_events"))
                    .await;
            }
            if !snapshot.objects.is_empty() {
                let batch = ocel_objects_to_batch(&snapshot.objects)?;
                let _ = publisher
                    .publish_batch("yata.arrow.yata_objects", &batch, Some("yata_objects"))
                    .await;
            }
            if !snapshot.event_object_edges.is_empty() {
                let batch = e2o_edges_to_batch(&snapshot.event_object_edges)?;
                let _ = publisher
                    .publish_batch(
                        "yata.arrow.yata_event_object_edges",
                        &batch,
                        Some("yata_event_object_edges"),
                    )
                    .await;
            }
            if !snapshot.object_object_edges.is_empty() {
                let batch = o2o_edges_to_batch(&snapshot.object_object_edges)?;
                let _ = publisher
                    .publish_batch(
                        "yata.arrow.yata_object_object_edges",
                        &batch,
                        Some("yata_object_object_edges"),
                    )
                    .await;
            }
            // KV history is already published per-operation via NatsKvStore dual-write
        } else {
            // Direct Lance path (no NATS)
            if !snapshot.events.is_empty() {
                self.lance.write_ocel_events(&snapshot.events).await?;
            }
            if !snapshot.objects.is_empty() {
                self.lance.write_ocel_objects(&snapshot.objects).await?;
            }
            if !snapshot.event_object_edges.is_empty() {
                self.lance
                    .write_e2o_edges(&snapshot.event_object_edges)
                    .await?;
            }
            if !snapshot.object_object_edges.is_empty() {
                self.lance
                    .write_o2o_edges(&snapshot.object_object_edges)
                    .await?;
            }

            // Local KV history drain
            if let Some(ref local_kv) = self.local_kv {
                let kv_entries = local_kv.drain_pending();
                if !kv_entries.is_empty() {
                    self.lance.write_kv_history(&kv_entries).await?;
                }
            }
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
        self.broker.objects.put_object(data, meta).await
    }

    async fn get_object(
        &self,
        id: &yata_core::ObjectId,
    ) -> yata_core::Result<bytes::Bytes> {
        self.broker.objects.get_object(id).await
    }

    async fn export_ocel_json(&self, _dataset: &str) -> yata_core::Result<String> {
        use yata_ocel::OcelProjector;
        let snapshot = self.broker.ocel.snapshot().await?;
        yata_ocel::export_json(&snapshot)
            .map_err(|e| yata_core::YataError::Serialization(e.to_string()))
    }
}
