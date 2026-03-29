#![allow(dead_code)]

//! YATA broker/runtime — graph (Cypher) + objects (CAS).
//!
//! Single write path: CypherExec -> CSR -> DurableWal (fsync) -> async MDAG CAS -> R2.
//! Per-app single-writer. MDAG commit chain is the replication mechanism.
//! No consensus protocol — R2 is the source of truth for durability.

pub mod jwt;
pub mod metrics;
pub mod rest;

use std::path::PathBuf;
use std::sync::Arc;
use yata_object::cas::{CasStore, LocalCasStore};
use yata_core::OcelEventDraft;
use yata_core::{ObjectStorage, Result};
use yata_object::LocalObjectStore;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct BrokerConfig {
    pub data_dir: PathBuf,
    pub graph_uri: Option<String>,
    /// FUSE mount mode: all paths point to a FUSE-backed filesystem.
    /// When true, S3 API write-through is disabled (FUSE handles persistence).
    #[serde(default = "default_true")]
    pub fuse: bool,
    /// Optional mount root for FUSE adapter (e.g., tigrisfs mountpoint).
    #[serde(default)]
    pub fuse_mount_dir: Option<PathBuf>,
    /// S3-compatible cold storage config for write-through when FUSE is unavailable.
    /// When set (non-empty endpoint), CAS uses AsyncS3CasStore (local + R2 async write-through).
    #[serde(default)]
    pub s3: Option<yata_s3::S3Config>,
}

fn default_true() -> bool {
    true
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./yata-data"),
            graph_uri: None,
            fuse: true,
            fuse_mount_dir: None,
            s3: None,
        }
    }
}

/// The central broker: graph (Cypher) + objects (CAS).
/// Per-app single-writer. MDAG commit chain + R2 = durability.
pub struct Broker {
    pub config: BrokerConfig,
    /// Object storage (CAS + manifest). S3 write-through when configured.
    pub objects: Arc<dyn ObjectStorage>,
    /// Graph store — all data persists here via Cypher.
    pub graph: Option<Arc<yata_graph::GraphStore>>,
    /// Shared CAS store. AsyncS3CasStore when S3 configured.
    pub cas: Arc<dyn CasStore>,
    /// S3 sync handle for MDAG HEAD push/restore. None when using FUSE.
    pub s3_sync: Option<Arc<yata_s3::S3Sync>>,
}

impl Broker {
    pub async fn new(config: BrokerConfig) -> anyhow::Result<Self> {
        let data_dir = &config.data_dir;
        tokio::fs::create_dir_all(data_dir).await?;

        let storage_root = config
            .fuse_mount_dir
            .clone()
            .unwrap_or_else(|| data_dir.clone());
        tokio::fs::create_dir_all(&storage_root).await?;

        let local_cas = Arc::new(LocalCasStore::new(storage_root.join("cas")).await?);
        let local_objects = Arc::new(
            LocalObjectStore::with_cas(
                storage_root.join("objects"),
                local_cas.clone() as Arc<dyn CasStore>,
            )
            .await?,
        );

        // S3 write-through: when S3 config has a non-empty endpoint and FUSE is not active,
        // wrap local CAS/objects with S3 write-through for persistence across container restarts.
        let s3_active = config
            .s3
            .as_ref()
            .map(|s| !s.endpoint.is_empty())
            .unwrap_or(false)
            && !config.fuse;

        let mut s3_sync_handle: Option<Arc<yata_s3::S3Sync>> = None;
        let (cas, objects): (Arc<dyn CasStore>, Arc<dyn ObjectStorage>) = if s3_active {
            let s3_config = config.s3.as_ref().unwrap().clone();
            let eager = s3_config.eager;
            let s3_sync = Arc::new(yata_s3::S3Sync::with_cas(
                s3_config.clone(),
                local_objects.clone(),
                local_cas.clone(),
            )?);
            let async_s3_channel_size: usize = std::env::var("YATA_ASYNC_S3_CHANNEL_SIZE")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1024);

            let s3_cas: Arc<dyn CasStore> = Arc::new(yata_s3::async_cas::AsyncS3CasStore::new(
                local_cas.clone(),
                s3_sync.remote_store().clone(),
                s3_config.prefix.clone(),
                async_s3_channel_size,
            ));
            let tiered_objects: Arc<dyn ObjectStorage> =
                Arc::new(yata_s3::TieredObjectStore::new_eager(
                    local_objects.clone(),
                    s3_sync.clone(),
                    eager,
                ));

            // Restore MDAG HEAD from R2 if local disk lost it (container restart).
            let head_file = data_dir.join("MDAG_HEAD");
            if !head_file.exists() {
                if let Some(head) = s3_sync.restore_head().await {
                    tracing::info!(head = %head.hex(), "restored MDAG HEAD from R2");
                    tokio::fs::write(&head_file, head.hex()).await?;
                }
            }

            tracing::info!(
                bucket = %s3_config.bucket,
                prefix = %s3_config.prefix,
                async_s3_channel_size,
                "S3 write-through active (AsyncS3CasStore)"
            );
            s3_sync_handle = Some(s3_sync);
            (s3_cas, tiered_objects)
        } else {
            tracing::info!(
                storage_root = %storage_root.display(),
                fuse = config.fuse,
                "local/FUSE storage mode active"
            );
            (
                local_cas as Arc<dyn CasStore>,
                local_objects as Arc<dyn ObjectStorage>,
            )
        };

        let graph = if let Some(ref graph_uri) = config.graph_uri {
            Some(Arc::new(
                yata_graph::GraphStore::new(graph_uri)
                    .await
                    .map_err(|e| anyhow::anyhow!("graph store init: {e}"))?,
            ))
        } else {
            None
        };

        Ok(Self {
            config,
            objects,
            graph,
            cas,
            s3_sync: s3_sync_handle,
        })
    }

    /// Start background tasks.
    pub async fn start_background_tasks(self: Arc<Self>) -> anyhow::Result<()> {
        tracing::info!("broker started (single-writer, MDAG-native replication)");
        Ok(())
    }

    /// Per-app single-writer: always the authority for its own graph partition.
    pub fn is_leader(&self) -> bool {
        true
    }

    /// Publish an OCEL event — persists as `:OcelEvent` graph node via Cypher.
    pub async fn publish_ocel_event(
        &self,
        stream: &str,
        event: OcelEventDraft,
        _payload: Option<yata_arrow::ArrowBatchHandle>,
    ) -> Result<yata_core::Ack> {
        let ts_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let event_id = uuid::Uuid::new_v4().to_string();

        if let Some(ref graph) = self.graph {
            let attrs_json = serde_json::to_string(&event.attrs).unwrap_or_default();
            let cypher = format!(
                "CREATE (e:OcelEvent {{event_id: '{}', event_type: '{}', stream: '{}', attrs_json: '{}', ts_ns: {}}})",
                event_id.replace('\'', "\\'"),
                event.event_type.replace('\'', "\\'"),
                stream.replace('\'', "\\'"),
                attrs_json.replace('\'', "\\'"),
                ts_ns,
            );
            if let Err(e) = graph.cached_query(&cypher, &[]).await {
                tracing::warn!("ocel graph create failed: {e}");
            }

            for obj_ref in &event.object_refs {
                let obj_cypher = format!(
                    "MERGE (o:OcelObject {{object_id: '{}', object_type: '{}'}}) \
                     WITH o MATCH (e:OcelEvent {{event_id: '{}'}}) \
                     CREATE (e)-[:INVOLVES {{qualifier: '{}', role: '{}'}}]->(o)",
                    obj_ref.object_id.replace('\'', "\\'"),
                    obj_ref.object_type.replace('\'', "\\'"),
                    event_id.replace('\'', "\\'"),
                    obj_ref
                        .qualifier
                        .as_deref()
                        .unwrap_or("")
                        .replace('\'', "\\'"),
                    obj_ref.role.as_deref().unwrap_or("").replace('\'', "\\'"),
                );
                if let Err(e) = graph.cached_query(&obj_cypher, &[]).await {
                    tracing::warn!("ocel graph edge failed: {e}");
                }
            }
        }

        Ok(yata_core::Ack {
            message_id: yata_core::MessageId::new(),
            stream_id: yata_core::StreamId::from(stream),
            seq: yata_core::Sequence(ts_ns as u64),
            ts_ns,
        })
    }

    /// Export OCEL events as JSON — queries graph `:OcelEvent` nodes.
    pub async fn export_ocel_json(&self) -> Result<String> {
        if let Some(ref graph) = self.graph {
            let result = graph
                .cached_query(
                    "MATCH (e:OcelEvent) RETURN e.event_id, e.event_type, e.attrs_json, e.ts_ns ORDER BY e.ts_ns",
                    &[],
                )
                .await
                .map_err(|e| yata_core::YataError::Storage(e.to_string()))?;
            let json = serde_json::to_string(&result.rows)
                .map_err(|e| yata_core::YataError::Serialization(e.to_string()))?;
            Ok(json)
        } else {
            Ok("[]".to_string())
        }
    }
}

#[async_trait::async_trait]
impl yata_core::WrpcBroker for Broker {
    async fn cas_put(&self, data: bytes::Bytes) -> Result<yata_core::Blake3Hash> {
        self.cas
            .put(data)
            .await
            .map_err(|e| yata_core::YataError::Storage(e.to_string()))
    }

    fn is_leader(&self) -> bool {
        true
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
        _stream: &str,
        _subject: &str,
        _batch: yata_arrow::ArrowBatchHandle,
        _opts: yata_client::PublishOpts,
    ) -> yata_core::Result<yata_core::Ack> {
        Ok(yata_core::Ack {
            message_id: yata_core::MessageId::new(),
            stream_id: yata_core::StreamId::from(_stream),
            seq: yata_core::Sequence(0),
            ts_ns: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
        })
    }

    async fn publish_ocel_event(
        &self,
        stream: &str,
        event: yata_core::OcelEventDraft,
        payload: Option<yata_arrow::ArrowBatchHandle>,
    ) -> yata_core::Result<yata_core::Ack> {
        self.broker.publish_ocel_event(stream, event, payload).await
    }

    async fn put_object(
        &self,
        data: bytes::Bytes,
        meta: yata_core::ObjectMeta,
    ) -> yata_core::Result<yata_core::ObjectManifest> {
        self.broker.objects.put_object(data, meta).await
    }

    async fn get_object(&self, id: &yata_core::ObjectId) -> yata_core::Result<bytes::Bytes> {
        self.broker.objects.get_object(id).await
    }

    async fn export_ocel_json(&self, _dataset: &str) -> yata_core::Result<String> {
        self.broker.export_ocel_json().await
    }
}
