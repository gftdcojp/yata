//! MDAG CRDT peer sync — AT Protocol-style federation via Merkle diff.
//!
//! Each node maintains an independent MDAG commit chain persisted to R2.
//! Sync merges peer commits into the local graph via content-addressed
//! block transfer + LWW conflict resolution.

use yata_grin::{Mutable, PropValue, Property, Scannable, Topology};
use yata_store::MutableCsrStore;

/// Merge a peer's CSR into the local CSR.
///
/// Conflict resolution:
/// - OcelEvent: idempotent union (skip if event_id exists locally)
/// - Other nodes: LWW using `_updated_at` timestamp
/// - New nodes: always added
pub fn merge_csr(local: &mut MutableCsrStore, peer: &MutableCsrStore) -> (usize, usize) {
    let mut added = 0usize;
    let mut updated = 0usize;

    let peer_vids = peer.scan_all_vertices();

    for pvid in peer_vids {
        let peer_labels = peer.vertex_labels(pvid);
        if peer_labels.is_empty() {
            continue;
        }
        let is_ocel = peer_labels.iter().any(|l| l == "OcelEvent");

        // Identify peer vertex
        let peer_id = vid_prop(peer, pvid);
        let peer_id_str = match peer_id {
            Some(s) => s,
            None => continue,
        };

        // Find matching local vertex
        let local_vid = find_vertex_by_id(local, &peer_labels[0], &peer_id_str);

        match local_vid {
            Some(lvid) => {
                if is_ocel {
                    continue; // OcelEvent: idempotent
                }
                // LWW: compare _updated_at
                let local_ts = str_prop(local, lvid, "_updated_at");
                let peer_ts = str_prop(peer, pvid, "_updated_at");
                if peer_ts > local_ts {
                    let all_props = peer.vertex_all_props(pvid);
                    for (k, v) in all_props {
                        local.set_vertex_prop(lvid, &k, v);
                    }
                    updated += 1;
                }
            }
            None => {
                let all_props = peer.vertex_all_props(pvid);
                let props_ref: Vec<(&str, PropValue)> = all_props
                    .iter()
                    .map(|(k, v)| (k.as_str(), v.clone()))
                    .collect();
                local.add_vertex(&peer_labels[0], &props_ref);
                added += 1;
            }
        }
    }

    if added > 0 || updated > 0 {
        local.commit();
    }
    (added, updated)
}

fn vid_prop(store: &MutableCsrStore, vid: u32) -> Option<String> {
    for key in &["_vid", "event_id", "vehicle_id", "article_id", "review_id"] {
        if let Some(PropValue::Str(s)) = store.vertex_prop(vid, key) {
            return Some(s);
        }
    }
    None
}

fn find_vertex_by_id(store: &MutableCsrStore, label: &str, id: &str) -> Option<u32> {
    for vid in store.scan_vertices_by_label(label) {
        if vid_prop(store, vid).as_deref() == Some(id) {
            return Some(vid);
        }
    }
    None
}

fn str_prop(store: &MutableCsrStore, vid: u32, key: &str) -> String {
    match store.vertex_prop(vid, key) {
        Some(PropValue::Str(s)) => s,
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_csr_union() {
        let mut local = MutableCsrStore::new();
        local.add_vertex(
            "Person",
            &[
                ("_vid", PropValue::Str("p1".into())),
                ("name", PropValue::Str("Alice".into())),
                ("_updated_at", PropValue::Str("2026-01-01T00:00:00Z".into())),
            ],
        );
        local.commit();

        let mut peer = MutableCsrStore::new();
        peer.add_vertex(
            "Person",
            &[
                ("_vid", PropValue::Str("p2".into())),
                ("name", PropValue::Str("Bob".into())),
                ("_updated_at", PropValue::Str("2026-01-01T00:00:00Z".into())),
            ],
        );
        peer.commit();

        let (added, updated) = merge_csr(&mut local, &peer);
        assert_eq!(added, 1);
        assert_eq!(updated, 0);
        assert_eq!(local.vertex_count(), 2);
    }

    #[test]
    fn test_merge_csr_lww() {
        let mut local = MutableCsrStore::new();
        local.add_vertex(
            "Person",
            &[
                ("_vid", PropValue::Str("p1".into())),
                ("name", PropValue::Str("Alice".into())),
                ("_updated_at", PropValue::Str("2026-01-01T00:00:00Z".into())),
            ],
        );
        local.commit();

        let mut peer = MutableCsrStore::new();
        peer.add_vertex(
            "Person",
            &[
                ("_vid", PropValue::Str("p1".into())),
                ("name", PropValue::Str("Alice Updated".into())),
                ("_updated_at", PropValue::Str("2026-03-18T00:00:00Z".into())),
            ],
        );
        peer.commit();

        let (added, updated) = merge_csr(&mut local, &peer);
        assert_eq!(added, 0);
        assert_eq!(updated, 1);
        let vids = local.scan_vertices_by_label("Person");
        assert_eq!(vids.len(), 1);
        assert_eq!(
            local.vertex_prop(vids[0], "name"),
            Some(PropValue::Str("Alice Updated".into()))
        );
    }

    #[test]
    fn test_merge_csr_ocel_idempotent() {
        let mut local = MutableCsrStore::new();
        local.add_vertex(
            "OcelEvent",
            &[
                ("_vid", PropValue::Str("ocel-1".into())),
                ("event_id", PropValue::Str("ocel-1".into())),
                ("activity", PropValue::Str("CreateVehicle".into())),
            ],
        );
        local.commit();

        let mut peer = MutableCsrStore::new();
        peer.add_vertex(
            "OcelEvent",
            &[
                ("_vid", PropValue::Str("ocel-1".into())),
                ("event_id", PropValue::Str("ocel-1".into())),
                ("activity", PropValue::Str("CreateVehicle".into())),
            ],
        );
        peer.add_vertex(
            "OcelEvent",
            &[
                ("_vid", PropValue::Str("ocel-2".into())),
                ("event_id", PropValue::Str("ocel-2".into())),
                ("activity", PropValue::Str("UpdateVehicle".into())),
            ],
        );
        peer.commit();

        let (added, _) = merge_csr(&mut local, &peer);
        assert_eq!(added, 1);
        assert_eq!(local.vertex_count(), 2);
    }
}
