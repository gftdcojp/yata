//! Federated query engine stub.
//!
//! MDAG CAS-based federated query has been removed. This module retains
//! the public API types as stubs for downstream compatibility.

use yata_core::PartitionId;

/// A registered app source for federated queries (stub — CAS removed).
pub struct AppSource {
    pub app_id: String,
}

/// Federated query engine stub (MDAG CAS removed).
pub struct FederatedQueryEngine {
    _private: (),
}

impl FederatedQueryEngine {
    /// Lookup which apps contain a given property value (stub).
    pub fn lookup(&self, _key: &str, _value: &str) -> Vec<(&str, &str, u32)> {
        Vec::new()
    }

    /// Lookup with partition (stub).
    pub fn lookup_with_partition(
        &self,
        _key: &str,
        _value: &str,
    ) -> Vec<(&str, PartitionId, &str, u32)> {
        Vec::new()
    }

    /// List all registered app IDs (stub).
    pub fn app_ids(&self) -> Vec<&str> {
        Vec::new()
    }

    /// List all registered app IDs with partition IDs (stub).
    pub fn app_partitions(&self) -> Vec<(&str, PartitionId)> {
        Vec::new()
    }
}
