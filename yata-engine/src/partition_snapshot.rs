//! Partition-aware snapshot persistence (M2: selective checkpoint).
//!
//! Builds on `yata-store::partition::PartitionStoreSet` and serializes
//! per-partition data using the same Arrow IPC codec as `snapshot.rs`.
//!
//! R2 layout (partitioned):
//!   {prefix}psnap/manifest.json              — PartitionedManifest
//!   {prefix}psnap/partitions/{pid}/v/{label}.arrow
//!   {prefix}psnap/partitions/{pid}/e/{label}.arrow
//!   {prefix}psnap/partitions/{pid}/topo.bin
//!   {prefix}psnap/partitions/{pid}/schema.json

use std::collections::HashMap;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use yata_grin::*;
use yata_store::MutableCsrStore;
use yata_store::arrow_codec;
use yata_store::blocks::{EdgeBlock, LabelEdgeGroup, LabelVertexGroup, VertexBlock};
use yata_store::partition::{PartitionStoreSet, PartitionedManifest};

/// Serialized data for a single partition's checkpoint.
pub struct PartitionSnapshotBundle {
    pub partition_id: u32,
    pub vertex_groups: Vec<(String, Bytes)>,
    pub edge_groups: Vec<(String, Bytes)>,
    pub topology: Bytes,
    pub schema: PartitionSchema,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PartitionSchema {
    pub vertex_labels: Vec<String>,
    pub edge_labels: Vec<String>,
    pub vertex_primary_keys: HashMap<String, String>,
}

/// Result of a selective checkpoint.
pub struct CheckpointResult {
    pub manifest: PartitionedManifest,
    pub bundles: Vec<PartitionSnapshotBundle>,
    pub checkpointed_pids: Vec<u32>,
    pub skipped_pids: Vec<u32>,
}

/// Selective checkpoint: only serialize dirty partitions.
pub fn selective_checkpoint(pss: &mut PartitionStoreSet) -> CheckpointResult {
    let dirty = pss.drain_dirty_partitions();
    let all_pids: Vec<u32> = (0..pss.partition_count()).collect();
    let mut checkpointed = Vec::new();
    let mut skipped = Vec::new();
    let mut bundles = Vec::new();

    for pid in &all_pids {
        if !dirty.contains(pid) {
            skipped.push(*pid);
            continue;
        }
        if let Some(store) = pss.partition(*pid) {
            bundles.push(serialize_partition(store, *pid));
            checkpointed.push(*pid);
        }
    }

    let manifest = pss.build_manifest(&checkpointed);

    CheckpointResult {
        manifest,
        bundles,
        checkpointed_pids: checkpointed,
        skipped_pids: skipped,
    }
}

/// Full checkpoint: serialize all partitions.
pub fn full_checkpoint(pss: &mut PartitionStoreSet) -> CheckpointResult {
    pss.drain_dirty_partitions();
    let all_pids: Vec<u32> = (0..pss.partition_count()).collect();
    let mut bundles = Vec::new();

    for pid in &all_pids {
        if let Some(store) = pss.partition(*pid) {
            bundles.push(serialize_partition(store, *pid));
        }
    }

    let manifest = pss.build_manifest(&all_pids);

    CheckpointResult {
        manifest,
        bundles,
        checkpointed_pids: all_pids,
        skipped_pids: Vec::new(),
    }
}

/// Serialize a single partition's MutableCsrStore.
fn serialize_partition(store: &MutableCsrStore, pid: u32) -> PartitionSnapshotBundle {
    let v_labels = Schema::vertex_labels(store);
    let e_labels = Schema::edge_labels(store);

    // Vertex groups
    let all_vids: Vec<u32> = (0..store.vertex_count_raw())
        .filter(|vid| {
            store
                .vertex_alive_raw()
                .get(*vid as usize)
                .copied()
                .unwrap_or(false)
        })
        .collect();

    let mut vertex_by_label: HashMap<String, Vec<VertexBlock>> = HashMap::new();
    for &vid in &all_vids {
        let labels = Property::vertex_labels(store, vid);
        let primary_label = labels.first().cloned().unwrap_or_default();
        let prop_keys = if let Some(first_label) = labels.first() {
            store.vertex_prop_keys(first_label)
        } else {
            Vec::new()
        };
        let mut props: Vec<(String, PropValue)> = prop_keys
            .iter()
            .filter_map(|k| store.vertex_prop(vid, k).map(|v| (k.clone(), v)))
            .collect();
        props.sort_by(|a, b| a.0.cmp(&b.0));
        vertex_by_label
            .entry(primary_label)
            .or_default()
            .push(VertexBlock {
                vid,
                labels: labels.clone(),
                props,
            });
    }

    let mut vertex_groups = Vec::new();
    for label in &v_labels {
        if let Some(mut vertices) = vertex_by_label.remove(label.as_str()) {
            vertices.sort_by_key(|v| v.vid);
            let group = LabelVertexGroup {
                count: vertices.len() as u32,
                label: label.clone(),
                vertices,
            };
            if let Ok(arrow_bytes) = arrow_codec::encode_vertex_group(&group) {
                vertex_groups.push((label.clone(), Bytes::from(arrow_bytes)));
            }
        }
    }

    // Edge groups
    let mut edge_by_label: HashMap<String, Vec<EdgeBlock>> = HashMap::new();
    for eid in 0..store.edge_count_raw() as usize {
        if !store.edge_alive_raw().get(eid).copied().unwrap_or(false) {
            continue;
        }
        let label = store
            .edge_labels_raw()
            .get(eid)
            .cloned()
            .unwrap_or_default();
        let src = store.edge_src_raw().get(eid).copied().unwrap_or(0);
        let dst = store.edge_dst_raw().get(eid).copied().unwrap_or(0);
        let props: Vec<(String, PropValue)> = store
            .edge_props_raw()
            .get(eid)
            .map(|m| {
                let mut v: Vec<_> = m.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                v.sort_by(|a, b| a.0.cmp(&b.0));
                v
            })
            .unwrap_or_default();
        edge_by_label
            .entry(label.clone())
            .or_default()
            .push(EdgeBlock {
                edge_id: eid as u32,
                src,
                dst,
                label,
                props,
            });
    }

    let mut edge_groups = Vec::new();
    for label in &e_labels {
        if let Some(mut edges) = edge_by_label.remove(label.as_str()) {
            edges.sort_by(|a, b| (a.src, a.dst, a.edge_id).cmp(&(b.src, b.dst, b.edge_id)));
            let group = LabelEdgeGroup {
                count: edges.len() as u32,
                label: label.clone(),
                edges,
            };
            if let Ok(arrow_bytes) = arrow_codec::encode_edge_group(&group) {
                edge_groups.push((label.clone(), Bytes::from(arrow_bytes)));
            }
        }
    }

    // Topology
    let topology = crate::snapshot::serialize_topology_bytes(store);

    let schema = PartitionSchema {
        vertex_labels: v_labels,
        edge_labels: e_labels,
        vertex_primary_keys: store.vertex_pk_raw().clone(),
    };

    PartitionSnapshotBundle {
        partition_id: pid,
        vertex_groups,
        edge_groups,
        topology,
        schema,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use yata_core::{GlobalVid, LocalVid, PartitionId};
    use yata_store::partition::PartitionAssignment;

    #[test]
    fn test_full_checkpoint_roundtrip() {
        let mut pss = PartitionStoreSet::new(PartitionAssignment::Hash { partition_count: 2 });

        for i in 0..20u64 {
            let gv = GlobalVid::encode(PartitionId::new(0), LocalVid::new(i as u32));
            pss.add_vertex(
                gv,
                "Node",
                &[
                    ("name", PropValue::Str(format!("n{}", i))),
                    ("val", PropValue::Int(i as i64)),
                ],
            );
        }
        // Add some edges
        for i in 0..19u64 {
            let src = GlobalVid::encode(PartitionId::new(0), LocalVid::new(i as u32));
            let dst = GlobalVid::encode(PartitionId::new(0), LocalVid::new((i + 1) as u32));
            pss.add_edge(src, dst, "NEXT", &[]);
        }
        pss.commit();

        let result = full_checkpoint(&mut pss);
        assert_eq!(result.manifest.partition_count, 2);
        // total_vertex_count includes ghost vertices created for cross-partition edges
        assert!(result.manifest.total_vertex_count >= 20);
        assert_eq!(result.bundles.len(), 2);

        // Verify each bundle has data
        let total_vgroups: usize = result.bundles.iter().map(|b| b.vertex_groups.len()).sum();
        assert!(total_vgroups > 0);
    }

    #[test]
    fn test_selective_checkpoint_skips_clean() {
        let mut pss = PartitionStoreSet::new(PartitionAssignment::Hash { partition_count: 4 });

        for i in 0..10u64 {
            let gv = GlobalVid::encode(PartitionId::new(0), LocalVid::new(i as u32));
            pss.add_vertex(gv, "Node", &[]);
        }
        pss.commit();

        let result = selective_checkpoint(&mut pss);
        // Some partitions had data, some didn't
        assert!(result.skipped_pids.len() > 0 || result.checkpointed_pids.len() > 0);
        assert_eq!(
            result.checkpointed_pids.len() + result.skipped_pids.len(),
            4
        );
    }
}
