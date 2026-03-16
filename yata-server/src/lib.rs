#![allow(dead_code)]

//! YATA broker/runtime — wires together log, kv, object, ocel, lance + Raft consensus.
//!
//! Tiered storage is always active when `BrokerConfig.b2` is set:
//! - ObjectStore: local CAS + async B2 sync, read-through fallback
//! - KV: flushed to Lance `yata_kv_history` + log segments synced to B2
//! - Flight/Lance: datasets synced to B2 periodically
//! - Cypher: implicitly tiered via OCEL → Lance → B2

pub mod metrics;

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

/// Raft cluster peer configuration.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RaftPeer {
    pub node_id: u64,
    pub addr: String,
}

/// Raft configuration.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RaftConfig {
    /// This node's ID. 0 = standalone (no Raft).
    pub node_id: u64,
    /// Peer nodes for Raft cluster. Empty = single-node.
    #[serde(default)]
    pub peers: Vec<RaftPeer>,
}

impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            node_id: 1,
            peers: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BrokerConfig {
    /// Base directory for log segments and CAS.
    pub data_dir: PathBuf,
    /// Lance dataset URI (local path or s3-compatible URL).
    pub lance_uri: String,
    /// Lance flush interval (milliseconds).
    pub lance_flush_interval_ms: u64,
    /// B2 credentials (REQUIRED). Tiered storage for all subsystems.
    pub b2: B2Config,
    /// How often to sync log/kv_payloads/lance dirs to B2 (milliseconds).
    pub b2_sync_interval_ms: u64,
    /// Graph store base URI. When set, initializes a LanceGraphStore at this path.
    pub graph_uri: Option<String>,
    /// Raft consensus config.
    #[serde(default)]
    pub raft: RaftConfig,
    /// Log segment rotation and compaction config.
    #[serde(default)]
    pub log: yata_log::config::LogConfig,
    /// Log compaction interval (milliseconds). 0 = disabled.
    pub log_compact_interval_ms: u64,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./yata-data"),
            lance_uri: "./yata-lance".to_string(),
            lance_flush_interval_ms: 5000,
            b2: B2Config {
                endpoint: "https://s3.us-west-004.backblazeb2.com".to_string(),
                bucket: "ai-gftd-lanceb".to_string(),
                key_id: String::new(),
                application_key: String::new(),
                region: "us-west-004".to_string(),
                prefix: "yata/".to_string(),
            },
            b2_sync_interval_ms: 30_000,
            graph_uri: None,
            raft: RaftConfig::default(),
            log: yata_log::config::LogConfig::default(),
            log_compact_interval_ms: 60_000,
        }
    }
}

/// The central broker: owns all subsystems + Raft node.
pub struct Broker {
    pub config: BrokerConfig,
    pub log: Arc<dyn AppendLog>,
    pub kv: Arc<dyn KvStore>,
    pub schema_registry: Arc<tokio::sync::RwLock<yata_arrow::SchemaRegistry>>,
    pub objects: Arc<dyn ObjectStorage>,
    pub ocel: Arc<MemoryOcelProjector>,
    pub lance: Arc<LocalLanceSink>,
    /// Graph store — initialized when `BrokerConfig.graph_uri` is set.
    pub graph: Option<Arc<yata_graph::LanceGraphStore>>,
    /// Local KV handle for drain_pending and snapshot loading.
    pub local_kv: Arc<KvBucketStore>,
    /// Local log handle for compaction.
    local_log: Arc<LocalLog>,
    /// B2 sync handle (REQUIRED); used by background tasks for log/kv/lance dir sync.
    b2_sync: Arc<B2Sync>,
    /// Raft consensus node. Single-node starts as leader immediately.
    pub raft: Arc<yata_raft::RaftNode>,
}

impl Broker {
    /// Initialize all subsystems from config.
    pub async fn new(config: BrokerConfig) -> anyhow::Result<Self> {
        let data_dir = &config.data_dir;
        tokio::fs::create_dir_all(data_dir).await?;

        // Local backends (always — NATS removed)
        let local_log = Arc::new(
            LocalLog::with_config(data_dir.join("log"), config.log.clone()).await?,
        );
        local_log.recover().await?;

        let kv_payload_store = Arc::new(
            PayloadStore::new(data_dir.join("kv_payloads"))
                .await
                .map_err(anyhow::Error::from)?,
        );
        let local_kv =
            Arc::new(KvBucketStore::new(local_log.clone(), kv_payload_store).await?);

        let local_objects =
            Arc::new(LocalObjectStore::new(data_dir.join("objects")).await?);

        // B2 tiered storage (REQUIRED)
        let b2 = Arc::new(
            B2Sync::new(config.b2.clone(), local_objects.clone())
                .map_err(anyhow::Error::from)?,
        );
        let objects: Arc<dyn ObjectStorage> =
            Arc::new(TieredObjectStore::new(local_objects, b2.clone()));
        let b2_sync = b2;

        let ocel = Arc::new(MemoryOcelProjector::new());
        let lance = Arc::new(LocalLanceSink::new(&config.lance_uri).await?);

        // Initialize schema registry with built-in table schemas.
        let mut registry = yata_arrow::SchemaRegistry::new();
        let _ = registry.register("yata_messages", yata_lance::messages_schema());
        let _ = registry.register("yata_events", yata_lance::ocel_events_schema());
        let _ = registry.register("yata_objects", yata_lance::ocel_objects_schema());
        let _ = registry.register(
            "yata_event_object_edges",
            yata_lance::event_object_edges_schema(),
        );
        let _ = registry.register(
            "yata_object_object_edges",
            yata_lance::object_object_edges_schema(),
        );
        let _ = registry.register("yata_kv_history", yata_lance::kv_history_schema());
        let _ = registry.register("yata_blobs", yata_lance::blobs_schema());
        let schema_registry = Arc::new(tokio::sync::RwLock::new(registry));

        let graph = if let Some(ref graph_uri) = config.graph_uri {
            Some(Arc::new(
                yata_graph::LanceGraphStore::new(graph_uri)
                    .await
                    .map_err(|e| anyhow::anyhow!("graph store init: {e}"))?,
            ))
        } else {
            None
        };

        // Initialize Raft node
        let peers: Vec<yata_raft::PeerAddr> = config
            .raft
            .peers
            .iter()
            .map(|p| yata_raft::PeerAddr {
                node_id: p.node_id,
                addr: p.addr.clone(),
            })
            .collect();

        // NoopApplier for now — Broker methods handle storage directly.
        // Raft consensus gates write operations: only leader can write.
        let applier = Arc::new(NoopApplier);
        let raft = Arc::new(yata_raft::RaftNode::new(
            config.raft.node_id,
            peers,
            applier,
        ));

        Ok(Self {
            config,
            log: local_log.clone() as Arc<dyn AppendLog>,
            kv: local_kv.clone() as Arc<dyn KvStore>,
            schema_registry,
            objects,
            ocel,
            lance,
            graph,
            local_kv,
            local_log,
            b2_sync,
            raft,
        })
    }

    /// Start background tasks: Raft, Lance flush, log compaction, TTL reaper, B2 sync.
    pub async fn start_background_tasks(self: Arc<Self>) -> anyhow::Result<()> {
        // Start Raft node (single-node becomes leader immediately)
        self.raft
            .start()
            .await
            .map_err(|e| anyhow::anyhow!("raft start: {e}"))?;
        tracing::info!(
            node_id = self.config.raft.node_id,
            is_leader = self.raft.is_leader(),
            "raft node started"
        );

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

        // Log compaction
        {
            let compact_ms = self.config.log_compact_interval_ms;
            if compact_ms > 0 {
                let log = self.local_log.clone();
                tokio::spawn(async move {
                    let mut interval =
                        tokio::time::interval(tokio::time::Duration::from_millis(compact_ms));
                    loop {
                        interval.tick().await;
                        if let Err(e) = log.compact().await {
                            tracing::warn!("log compaction error: {}", e);
                        }
                    }
                });
            }
        }

        // TTL reaper
        {
            let kv = self.local_kv.clone();
            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(tokio::time::Duration::from_secs(5));
                loop {
                    interval.tick().await;
                    kv.reap_expired().await;
                }
            });
        }

        // B2 data sync loop (REQUIRED — tiered storage)
        {
            let b2 = self.b2_sync.clone();
            let data_dir = self.config.data_dir.clone();
            let lance_uri = self.config.lance_uri.clone();
            let sync_ms = self.config.b2_sync_interval_ms;
            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(tokio::time::Duration::from_millis(sync_ms));
                loop {
                    interval.tick().await;
                    tracing::info!("b2 sync cycle start");
                    if let Err(e) = b2.sync_dir(&data_dir.join("log"), "log/").await {
                        tracing::warn!("b2 log sync error: {}", e);
                    }
                    if let Err(e) =
                        b2.sync_dir(&data_dir.join("kv_payloads"), "kv_payloads/").await
                    {
                        tracing::warn!("b2 kv_payloads sync error: {}", e);
                    }
                    if !lance_uri.starts_with("s3://") && !lance_uri.starts_with("gs://") {
                        let lance_path = std::path::Path::new(&lance_uri);
                        match b2.sync_dir(lance_path, "lance/").await {
                            Ok(n) => if n > 0 { tracing::info!(uploaded = n, "b2 lance sync"); },
                            Err(e) => tracing::warn!("b2 lance sync error: {}", e),
                        }
                    }
                }
            });
        }

        Ok(())
    }

    /// Publish an Arrow batch to a stream (local write).
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

    /// Flush pending OCEL projection + KV history to Lance (direct path).
    pub async fn flush_lance(&self) -> Result<SyncStats> {
        let start = std::time::Instant::now();
        ::metrics::counter!(crate::metrics::names::LANCE_FLUSHES).increment(1);
        let snapshot = self.ocel.snapshot().await?;

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

        let kv_entries = self.local_kv.drain_pending();
        if !kv_entries.is_empty() {
            self.lance.write_kv_history(&kv_entries).await?;
        }

        let stats = self.lance.sync_stats().await?;
        let elapsed = start.elapsed().as_secs_f64();
        ::metrics::histogram!(crate::metrics::names::LANCE_FLUSH_DURATION).record(elapsed);
        tracing::debug!(?stats, elapsed_ms = elapsed * 1000.0, "lance flush complete");
        Ok(stats)
    }
}

/// No-op applier — Broker handles storage directly; Raft gates write access.
struct NoopApplier;

#[async_trait::async_trait]
impl yata_raft::StateMachineApplier for NoopApplier {
    async fn apply_publish(
        &self, _: &str, _: u32, _: &str, _: Option<&str>, _: &str, _: Option<&str>, ts_ns: i64, _: &[u8],
    ) -> std::result::Result<(u64, i64), String> {
        Ok((0, ts_ns))
    }
    async fn apply_create_topic(&self, _: &str, _: u32, _: u32, _: Option<u64>) -> std::result::Result<(), String> { Ok(()) }
    async fn apply_delete_topic(&self, _: &str) -> std::result::Result<(), String> { Ok(()) }
    async fn apply_commit_offset(&self, _: &str, _: &str, _: u32, _: u64) -> std::result::Result<(), String> { Ok(()) }
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
