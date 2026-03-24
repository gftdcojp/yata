//! Sharded coordinator — hash-based routing for coordinator-level scale-out.
//! Long-term: multiple coordinator Workers, each responsible for a shard of partitions.

/// Configuration for sharded coordinator routing.
#[derive(Debug, Clone)]
pub struct ShardConfig {
    /// Total number of coordinator shards.
    pub shard_count: u32,
    /// Total number of data partitions.
    pub partition_count: u32,
    /// Partitions per shard (derived, ceiling division).
    pub partitions_per_shard: u32,
}

impl ShardConfig {
    pub fn new(shard_count: u32, partition_count: u32) -> Self {
        let per_shard = (partition_count + shard_count - 1) / shard_count;
        Self {
            shard_count,
            partition_count,
            partitions_per_shard: per_shard,
        }
    }

    /// Which shard handles a given partition?
    pub fn shard_for_partition(&self, partition_id: u32) -> u32 {
        (partition_id / self.partitions_per_shard).min(self.shard_count - 1)
    }

    /// Which partitions does a shard manage?
    pub fn partitions_for_shard(&self, shard_id: u32) -> Vec<u32> {
        let start = shard_id * self.partitions_per_shard;
        let end = ((shard_id + 1) * self.partitions_per_shard).min(self.partition_count);
        (start..end).collect()
    }

    /// Route a vertex (by partition from GlobalVid) to its shard.
    pub fn shard_for_vertex(&self, partition_id: u16) -> u32 {
        self.shard_for_partition(partition_id as u32)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_config_basic() {
        let cfg = ShardConfig::new(4, 16);
        assert_eq!(cfg.shard_count, 4);
        assert_eq!(cfg.partition_count, 16);
        assert_eq!(cfg.partitions_per_shard, 4);
    }

    #[test]
    fn test_shard_for_partition() {
        let cfg = ShardConfig::new(4, 16);
        assert_eq!(cfg.shard_for_partition(0), 0);
        assert_eq!(cfg.shard_for_partition(3), 0);
        assert_eq!(cfg.shard_for_partition(4), 1);
        assert_eq!(cfg.shard_for_partition(7), 1);
        assert_eq!(cfg.shard_for_partition(12), 3);
        assert_eq!(cfg.shard_for_partition(15), 3);
    }

    #[test]
    fn test_partitions_for_shard() {
        let cfg = ShardConfig::new(4, 16);
        assert_eq!(cfg.partitions_for_shard(0), vec![0, 1, 2, 3]);
        assert_eq!(cfg.partitions_for_shard(1), vec![4, 5, 6, 7]);
        assert_eq!(cfg.partitions_for_shard(3), vec![12, 13, 14, 15]);
    }

    #[test]
    fn test_shard_for_vertex() {
        let cfg = ShardConfig::new(4, 16);
        assert_eq!(cfg.shard_for_vertex(0), 0);
        assert_eq!(cfg.shard_for_vertex(5), 1);
        assert_eq!(cfg.shard_for_vertex(14), 3);
    }

    #[test]
    fn test_single_shard_covers_all() {
        let cfg = ShardConfig::new(1, 10);
        assert_eq!(cfg.partitions_per_shard, 10);
        for p in 0..10 {
            assert_eq!(cfg.shard_for_partition(p), 0);
        }
        assert_eq!(cfg.partitions_for_shard(0), (0..10).collect::<Vec<u32>>());
    }
}
