//! Mirror vertex registry for cross-partition 2-hop local expansion.
//!
//! Ghost vertices are stubs created when a cross-partition edge is added (src on P0,
//! dst on P1 → ghost of dst created on P0). Ghosts have label `_ghost` and NO outgoing
//! edges, blocking 2-hop traversal from expanding through them locally.
//!
//! Mirror vertices extend ghosts by holding a copy of the vertex's outgoing edge metadata
//! from the home partition, enabling 2-hop expansion without coordinator round-trips.

use std::collections::HashMap;

use yata_core::{GlobalVid, LocalVid, PartitionId};
use yata_grin::Topology;

use crate::partition::PartitionStoreSet;

/// Outgoing edge metadata stored on a mirror vertex.
#[derive(Debug, Clone)]
pub struct MirrorEdge {
    pub dst_global_vid: GlobalVid,
    pub label: String,
}

/// A cross-partition vertex replica that holds outgoing edge metadata.
/// Unlike ghost vertices (stub only), mirrors have a copy of the vertex's outgoing edges
/// from the home partition, enabling 2-hop local expansion.
#[derive(Debug, Clone)]
pub struct MirrorVertex {
    pub global_vid: GlobalVid,
    pub home_partition: u32,
    /// Outgoing edges: (dst_global_vid, edge_label)
    pub outgoing_edges: Vec<MirrorEdge>,
}

/// Registry of mirror vertices for cross-partition 2-hop expansion.
pub struct MirrorRegistry {
    /// global_vid.0 -> MirrorVertex
    mirrors: HashMap<u64, MirrorVertex>,
}

impl MirrorRegistry {
    pub fn new() -> Self {
        Self {
            mirrors: HashMap::new(),
        }
    }

    /// Register a mirror vertex with its outgoing edges.
    pub fn register(&mut self, mirror: MirrorVertex) {
        self.mirrors.insert(mirror.global_vid.0, mirror);
    }

    /// Get outgoing edges for a mirror vertex (for 2-hop expansion).
    pub fn get_outgoing(&self, gvid: GlobalVid) -> Option<&[MirrorEdge]> {
        self.mirrors.get(&gvid.0).map(|m| m.outgoing_edges.as_slice())
    }

    /// Check if a vertex is a mirror.
    pub fn is_mirror(&self, gvid: GlobalVid) -> bool {
        self.mirrors.contains_key(&gvid.0)
    }

    /// Upgrade a ghost vertex to a mirror by providing its outgoing edges.
    pub fn upgrade_ghost_to_mirror(
        &mut self,
        gvid: GlobalVid,
        home_partition: u32,
        outgoing_edges: Vec<MirrorEdge>,
    ) {
        self.mirrors.insert(
            gvid.0,
            MirrorVertex {
                global_vid: gvid,
                home_partition,
                outgoing_edges,
            },
        );
    }

    /// Number of mirror vertices.
    pub fn len(&self) -> usize {
        self.mirrors.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.mirrors.is_empty()
    }

    /// Remove a mirror vertex.
    pub fn remove(&mut self, gvid: GlobalVid) -> Option<MirrorVertex> {
        self.mirrors.remove(&gvid.0)
    }

    /// Iterate over all mirror vertices.
    pub fn iter(&self) -> impl Iterator<Item = &MirrorVertex> {
        self.mirrors.values()
    }
}

impl Default for MirrorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Ensure that ghost vertices for the given GlobalVids are upgraded to mirrors.
/// Fetches outgoing edge metadata from the home partition's store.
pub fn ensure_mirrors(
    pss: &PartitionStoreSet,
    mirrors: &mut MirrorRegistry,
    gvids: &[GlobalVid],
) {
    for &gvid in gvids {
        if mirrors.is_mirror(gvid) {
            continue;
        }
        // Find the home partition (non-ghost location)
        let home_pid = match pss.find_vertex_home(gvid) {
            Some(pid) => pid,
            None => continue,
        };
        let home_store = match pss.partition(home_pid) {
            Some(s) => s,
            None => continue,
        };
        let local_vid = match home_store.global_map().to_local(gvid) {
            Some(lv) => lv,
            None => continue,
        };
        let neighbors = Topology::out_neighbors(home_store, local_vid);
        let edges: Vec<MirrorEdge> = neighbors
            .into_iter()
            .map(|n| {
                let dst_global = home_store
                    .global_map()
                    .to_global(n.vid)
                    .unwrap_or(GlobalVid::encode(
                        PartitionId::new(home_pid),
                        LocalVid::new(n.vid),
                    ));
                MirrorEdge {
                    dst_global_vid: dst_global,
                    label: n.edge_label,
                }
            })
            .collect();
        mirrors.upgrade_ghost_to_mirror(gvid, home_pid, edges);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::partition::{PartitionAssignment, PartitionStoreSet};
    use yata_grin::{PropValue, Property, Topology};

    #[test]
    fn test_mirror_registry_basic() {
        let mut reg = MirrorRegistry::new();
        assert!(reg.is_empty());
        assert_eq!(reg.len(), 0);

        let gvid = GlobalVid::from_local(42);
        assert!(!reg.is_mirror(gvid));
        assert!(reg.get_outgoing(gvid).is_none());

        // Register a mirror
        reg.register(MirrorVertex {
            global_vid: gvid,
            home_partition: 1,
            outgoing_edges: vec![
                MirrorEdge {
                    dst_global_vid: GlobalVid::from_local(100),
                    label: "KNOWS".to_string(),
                },
                MirrorEdge {
                    dst_global_vid: GlobalVid::from_local(200),
                    label: "LIKES".to_string(),
                },
            ],
        });

        assert!(reg.is_mirror(gvid));
        assert_eq!(reg.len(), 1);
        assert!(!reg.is_empty());

        let edges = reg.get_outgoing(gvid).unwrap();
        assert_eq!(edges.len(), 2);
        assert_eq!(edges[0].label, "KNOWS");
        assert_eq!(edges[0].dst_global_vid, GlobalVid::from_local(100));
        assert_eq!(edges[1].label, "LIKES");
        assert_eq!(edges[1].dst_global_vid, GlobalVid::from_local(200));
    }

    #[test]
    fn test_upgrade_ghost_to_mirror() {
        let mut reg = MirrorRegistry::new();
        let gvid = GlobalVid::from_local(10);

        assert!(!reg.is_mirror(gvid));

        reg.upgrade_ghost_to_mirror(
            gvid,
            2,
            vec![MirrorEdge {
                dst_global_vid: GlobalVid::from_local(20),
                label: "EDGE_A".to_string(),
            }],
        );

        assert!(reg.is_mirror(gvid));
        let edges = reg.get_outgoing(gvid).unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].dst_global_vid, GlobalVid::from_local(20));
        assert_eq!(edges[0].label, "EDGE_A");
    }

    #[test]
    fn test_mirror_not_found() {
        let reg = MirrorRegistry::new();
        let gvid = GlobalVid::from_local(999);
        assert!(!reg.is_mirror(gvid));
        assert!(reg.get_outgoing(gvid).is_none());
    }

    #[test]
    fn test_ensure_mirrors() {
        // Build a 2-partition graph: Person on P0, Company on P1
        // Alice (P0) -[:WORKS_AT]-> GFTD (P1) -[:LOCATED_IN]-> Tokyo (P1)
        let mut label_map = std::collections::HashMap::new();
        label_map.insert("Person".to_string(), 0);
        label_map.insert("Company".to_string(), 1);
        label_map.insert("City".to_string(), 1);
        let assignment = PartitionAssignment::Label {
            label_map,
            default_partition: 0,
        };

        let mut pss = PartitionStoreSet::new(assignment);
        let alice = GlobalVid::from_local(0);
        let gftd = GlobalVid::from_local(1);
        let tokyo = GlobalVid::from_local(2);

        pss.add_vertex(alice, "Person", &[("name", PropValue::Str("Alice".into()))]);
        pss.add_vertex(gftd, "Company", &[("name", PropValue::Str("GFTD".into()))]);
        pss.add_vertex(tokyo, "City", &[("name", PropValue::Str("Tokyo".into()))]);

        // Alice -> GFTD (cross-partition: creates ghost of GFTD on P0)
        pss.add_edge(alice, gftd, "WORKS_AT", &[]);
        // GFTD -> Tokyo (same partition P1)
        pss.add_edge(gftd, tokyo, "LOCATED_IN", &[]);
        pss.commit();

        let mut mirrors = MirrorRegistry::new();

        // Upgrade the ghost of GFTD (which exists on P0) to a mirror
        ensure_mirrors(&pss, &mut mirrors, &[gftd]);

        assert!(mirrors.is_mirror(gftd));
        let edges = mirrors.get_outgoing(gftd).unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].label, "LOCATED_IN");
        assert_eq!(edges[0].dst_global_vid, tokyo);
    }

    #[test]
    fn test_ensure_mirrors_skips_existing() {
        let mut mirrors = MirrorRegistry::new();
        let gvid = GlobalVid::from_local(5);

        // Pre-register a mirror with specific edges
        mirrors.register(MirrorVertex {
            global_vid: gvid,
            home_partition: 0,
            outgoing_edges: vec![MirrorEdge {
                dst_global_vid: GlobalVid::from_local(99),
                label: "ORIGINAL".to_string(),
            }],
        });

        // Build a trivial PSS
        let pss = PartitionStoreSet::single();

        // ensure_mirrors should skip the already-registered mirror
        ensure_mirrors(&pss, &mut mirrors, &[gvid]);

        // Edges should still be the original, not overwritten
        let edges = mirrors.get_outgoing(gvid).unwrap();
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].label, "ORIGINAL");
    }

    #[test]
    fn test_mirror_2hop_expansion() {
        // Set up: Alice (P0) -[:WORKS_AT]-> GFTD (P1) -[:LOCATED_IN]-> Tokyo (P1)
        // Without mirror: 2-hop from Alice hits ghost of GFTD on P0, cannot expand.
        // With mirror: ghost upgraded to mirror, 2-hop can expand GFTD's outgoing edges.
        let mut label_map = std::collections::HashMap::new();
        label_map.insert("Person".to_string(), 0);
        label_map.insert("Company".to_string(), 1);
        label_map.insert("City".to_string(), 1);
        let assignment = PartitionAssignment::Label {
            label_map,
            default_partition: 0,
        };

        let mut pss = PartitionStoreSet::new(assignment);
        let alice = GlobalVid::from_local(0);
        let gftd = GlobalVid::from_local(1);
        let tokyo = GlobalVid::from_local(2);

        pss.add_vertex(alice, "Person", &[("name", PropValue::Str("Alice".into()))]);
        pss.add_vertex(gftd, "Company", &[("name", PropValue::Str("GFTD".into()))]);
        pss.add_vertex(tokyo, "City", &[("name", PropValue::Str("Tokyo".into()))]);
        pss.add_edge(alice, gftd, "WORKS_AT", &[]);
        pss.add_edge(gftd, tokyo, "LOCATED_IN", &[]);
        pss.commit();

        // Verify GFTD is a ghost on P0
        let p0 = pss.partition(0).unwrap();
        if let Some(gftd_local_on_p0) = p0.global_map().to_local(gftd) {
            let labels = Property::vertex_labels(p0, gftd_local_on_p0);
            assert_eq!(labels.first().map(|l: &String| l.as_str()), Some("_ghost"));
            // Ghost has no outgoing edges on P0
            let ghost_neighbors = Topology::out_neighbors(p0, gftd_local_on_p0);
            assert_eq!(ghost_neighbors.len(), 0);
        }

        // Now upgrade ghost to mirror
        let mut mirrors = MirrorRegistry::new();
        ensure_mirrors(&pss, &mut mirrors, &[gftd]);

        // Mirror should expose GFTD's outgoing edges
        assert!(mirrors.is_mirror(gftd));
        let mirror_edges = mirrors.get_outgoing(gftd).unwrap();
        assert_eq!(mirror_edges.len(), 1);
        assert_eq!(mirror_edges[0].label, "LOCATED_IN");
        assert_eq!(mirror_edges[0].dst_global_vid, tokyo);

        // Simulate 2-hop expansion:
        // Hop 1: Alice -> GFTD (via normal edge on P0)
        let alice_neighbors = pss.out_neighbors(alice);
        assert_eq!(alice_neighbors.len(), 1);
        let (hop1_dst, _, hop1_label) = &alice_neighbors[0];
        assert_eq!(hop1_label, "WORKS_AT");
        assert_eq!(*hop1_dst, gftd);

        // Hop 2: GFTD -> ? (via mirror, no coordinator round-trip)
        let hop2_edges = mirrors.get_outgoing(*hop1_dst).unwrap();
        assert_eq!(hop2_edges.len(), 1);
        assert_eq!(hop2_edges[0].dst_global_vid, tokyo);
        assert_eq!(hop2_edges[0].label, "LOCATED_IN");
    }
}
