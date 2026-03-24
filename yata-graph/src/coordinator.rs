//! Graph query coordinator — routes queries to partitions and merges results.
//! Single-node mode: all partitions are local (passthrough).
//! Future: multi-node routing via Workers RPC.

use std::collections::HashMap;

/// Partition metadata for a graph fragment.
#[derive(Debug, Clone)]
pub struct Partition {
    pub id: u32,
    /// Labels owned by this partition (empty = all labels).
    pub labels: Vec<String>,
    /// URI for this partition.
    pub store_uri: String,
}

/// Query coordinator that routes Cypher queries to appropriate partitions.
pub struct Coordinator {
    partitions: Vec<Partition>,
    label_to_partition: HashMap<String, u32>,
}

impl Coordinator {
    /// Create a single-partition coordinator (current default).
    pub fn single(store_uri: &str) -> Self {
        let p = Partition {
            id: 0,
            labels: vec![],
            store_uri: store_uri.to_string(),
        };
        Self {
            partitions: vec![p],
            label_to_partition: HashMap::new(),
        }
    }

    /// Create a label-partitioned coordinator.
    pub fn label_partitioned(base_uri: &str, labels: &[String]) -> Self {
        let mut partitions = Vec::new();
        let mut label_to_partition = HashMap::new();

        for (i, label) in labels.iter().enumerate() {
            let uri = format!("{}/partition_{}", base_uri, label.to_lowercase());
            partitions.push(Partition {
                id: i as u32,
                labels: vec![label.clone()],
                store_uri: uri,
            });
            label_to_partition.insert(label.clone(), i as u32);
        }

        // Default partition for unlabeled data
        partitions.push(Partition {
            id: labels.len() as u32,
            labels: vec![],
            store_uri: base_uri.to_string(),
        });

        Self {
            partitions,
            label_to_partition,
        }
    }

    /// Route a query: given required labels, return partition IDs to query.
    pub fn route(&self, required_labels: &[String]) -> Vec<u32> {
        if required_labels.is_empty() || self.label_to_partition.is_empty() {
            return self.partitions.iter().map(|p| p.id).collect();
        }

        let mut pids: Vec<u32> = required_labels
            .iter()
            .filter_map(|l| self.label_to_partition.get(l))
            .copied()
            .collect();
        pids.sort_unstable();
        pids.dedup();

        // Always include default partition (may have cross-label edges)
        let default_id = self.partitions.last().map(|p| p.id).unwrap_or(0);
        if !pids.contains(&default_id) {
            pids.push(default_id);
        }

        pids
    }

    /// Get partition by ID.
    pub fn partition(&self, id: u32) -> Option<&Partition> {
        self.partitions.iter().find(|p| p.id == id)
    }

    /// Get all partitions.
    pub fn partitions(&self) -> &[Partition] {
        &self.partitions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_partition() {
        let coord = Coordinator::single("/data/graph");
        assert_eq!(coord.route(&[]).len(), 1);
        assert_eq!(coord.route(&["Person".into()]).len(), 1);
        assert_eq!(coord.partition(0).unwrap().store_uri, "/data/graph");
    }

    #[test]
    fn test_label_partitioned() {
        let labels = vec!["Person".into(), "Company".into()];
        let coord = Coordinator::label_partitioned("/data/graph", &labels);

        // Person query -> Person partition + default
        let pids = coord.route(&["Person".into()]);
        assert!(pids.len() <= 2);
        assert!(pids.contains(&0)); // Person partition
        assert!(pids.contains(&2)); // default partition

        // Company query -> Company partition + default
        let pids = coord.route(&["Company".into()]);
        assert!(pids.contains(&1)); // Company partition

        // No label -> all partitions
        let all = coord.route(&[]);
        assert_eq!(all.len(), 3); // Person + Company + default
    }

    #[test]
    fn test_multi_label_query() {
        let labels = vec!["Person".into(), "Company".into(), "Project".into()];
        let coord = Coordinator::label_partitioned("/data/graph", &labels);

        let pids = coord.route(&["Person".into(), "Company".into()]);
        assert!(pids.contains(&0)); // Person
        assert!(pids.contains(&1)); // Company
        assert!(pids.contains(&3)); // default
    }

    #[test]
    fn test_unknown_label_routes_to_default() {
        let labels = vec!["Person".into()];
        let coord = Coordinator::label_partitioned("/data/graph", &labels);

        let pids = coord.route(&["Unknown".into()]);
        // Only default partition (unknown label not mapped)
        assert_eq!(pids, vec![1]); // default partition id
    }

    #[test]
    fn test_partition_uris() {
        let labels = vec!["Person".into(), "Company".into()];
        let coord = Coordinator::label_partitioned("/data/graph", &labels);

        assert_eq!(
            coord.partition(0).unwrap().store_uri,
            "/data/graph/partition_person"
        );
        assert_eq!(
            coord.partition(1).unwrap().store_uri,
            "/data/graph/partition_company"
        );
        assert_eq!(coord.partition(2).unwrap().store_uri, "/data/graph");
    }
}
