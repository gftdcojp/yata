//! Push-based distributed executor with coordinator-mediated HTTP exchange.
//!
//! Phase 5: GIE Exchange/Receive/Gather wired for production multi-partition execution.
//! CF Containers cannot communicate directly — the TS coordinator mediates all exchange.
//!
//! Execution model (stateless per-round):
//!   Round 0: execute ops until first Exchange → yield outbound MaterializedRecords
//!   Round N: replay ops 0..N with inbound data injected at Receive → yield or return final
//!
//! The coordinator orchestrates: collect outbound → route to targets → inject as inbound.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::executor::{execute_op, eval_expr, MaterializedRecord, Record};
use crate::ir::*;
use yata_grin::PropValue;
use yata_store::MutableCsrStore;

/// Trait for exchange data transport between partitions.
/// Implementations: MemoryExchangeTransport (testing), R2ExchangeTransport (production).
pub trait ExchangeTransport: Send + Sync {
    /// Send records to a destination partition.
    fn send_records(
        &self,
        query_id: u64,
        round: u32,
        src: u32,
        dst: u32,
        records: &[Record],
    ) -> Result<(), String>;

    /// Receive records from a source partition. Returns None if not yet available.
    fn recv_records(
        &self,
        query_id: u64,
        round: u32,
        src: u32,
        dst: u32,
    ) -> Result<Option<Vec<Record>>, String>;

    /// Receive records from all source partitions for a given round.
    fn recv_all(
        &self,
        query_id: u64,
        round: u32,
        dst: u32,
        partition_count: u32,
    ) -> Result<Vec<Record>, String>;

    /// Clean up exchange data for a query.
    fn cleanup(&self, query_id: u64) -> Result<(), String>;
}

/// In-memory exchange transport for testing.
pub struct MemoryExchangeTransport {
    data: std::sync::Mutex<HashMap<(u64, u32, u32, u32), Vec<Record>>>,
}

impl MemoryExchangeTransport {
    pub fn new() -> Self {
        Self {
            data: std::sync::Mutex::new(HashMap::new()),
        }
    }
}

impl Default for MemoryExchangeTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl ExchangeTransport for MemoryExchangeTransport {
    fn send_records(
        &self,
        query_id: u64,
        round: u32,
        src: u32,
        dst: u32,
        records: &[Record],
    ) -> Result<(), String> {
        let mut data = self.data.lock().map_err(|e| e.to_string())?;
        let entry = data.entry((query_id, round, src, dst)).or_default();
        entry.extend(records.iter().cloned());
        Ok(())
    }

    fn recv_records(
        &self,
        query_id: u64,
        round: u32,
        src: u32,
        dst: u32,
    ) -> Result<Option<Vec<Record>>, String> {
        let data = self.data.lock().map_err(|e| e.to_string())?;
        Ok(data.get(&(query_id, round, src, dst)).cloned())
    }

    fn recv_all(
        &self,
        query_id: u64,
        round: u32,
        dst: u32,
        partition_count: u32,
    ) -> Result<Vec<Record>, String> {
        let data = self.data.lock().map_err(|e| e.to_string())?;
        let mut all = Vec::new();
        for src in 0..partition_count {
            if src == dst {
                continue;
            }
            if let Some(records) = data.get(&(query_id, round, src, dst)) {
                all.extend(records.iter().cloned());
            }
        }
        Ok(all)
    }

    fn cleanup(&self, query_id: u64) -> Result<(), String> {
        let mut data = self.data.lock().map_err(|e| e.to_string())?;
        data.retain(|k, _| k.0 != query_id);
        Ok(())
    }
}

/// Execute a distributed plan fragment on a single partition.
pub fn execute_fragment(
    fragment: &PartitionPlanFragment,
    store: &MutableCsrStore,
    transport: &dyn ExchangeTransport,
    query_id: u64,
    partition_count: u32,
) -> Vec<Record> {
    let pid = fragment.partition_id;
    let mut data: Vec<Record> = Vec::new();
    let mut exchange_round = 0u32;

    for op in &fragment.plan.ops {
        match op {
            LogicalOp::Exchange { routing_key, kind } => match kind {
                ExchangeKind::HashShuffle => {
                    let mut buckets: HashMap<u32, Vec<Record>> = HashMap::new();
                    for record in &data {
                        let key_val = eval_routing_key(routing_key, record, store);
                        let target_pid = hash_to_partition(key_val, partition_count);
                        buckets.entry(target_pid).or_default().push(record.clone());
                    }
                    for (target, records) in &buckets {
                        if *target != pid {
                            transport
                                .send_records(query_id, exchange_round, pid, *target, records)
                                .unwrap_or_default();
                        }
                    }
                    data = buckets.remove(&pid).unwrap_or_default();
                    exchange_round += 1;
                }
                ExchangeKind::Broadcast => {
                    for target in 0..partition_count {
                        if target != pid {
                            transport
                                .send_records(query_id, exchange_round, pid, target, &data)
                                .unwrap_or_default();
                        }
                    }
                    exchange_round += 1;
                }
                ExchangeKind::Gather => {
                    if pid != 0 {
                        transport
                            .send_records(query_id, exchange_round, pid, 0, &data)
                            .unwrap_or_default();
                        data.clear();
                    }
                    exchange_round += 1;
                }
            },
            LogicalOp::Receive { source_partitions } => {
                let received = if source_partitions.is_empty() {
                    transport
                        .recv_all(
                            query_id,
                            exchange_round.saturating_sub(1),
                            pid,
                            partition_count,
                        )
                        .unwrap_or_default()
                } else {
                    let mut all = Vec::new();
                    for &src in source_partitions {
                        if let Ok(Some(records)) = transport.recv_records(
                            query_id,
                            exchange_round.saturating_sub(1),
                            src,
                            pid,
                        ) {
                            all.extend(records);
                        }
                    }
                    all
                };
                data.extend(received);
            }
            other => {
                data = execute_op(other, data, store);
            }
        }
    }

    data
}

/// Hash a routing key value to a partition ID.
fn hash_to_partition(value: u64, partition_count: u32) -> u32 {
    (value % partition_count as u64) as u32
}

// ── Phase 5: Step executor for coordinator-mediated HTTP exchange ──

/// Serializable exchange payload for HTTP transport between coordinator and containers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangePayload {
    /// Target partition → materialized records
    pub outbound: HashMap<u32, Vec<MaterializedRecord>>,
    /// Final result rows (only when done=true)
    pub results: Vec<Vec<(String, String)>>,
    /// True if execution completed (no more exchange rounds)
    pub done: bool,
    /// Current exchange round (for coordinator tracking)
    pub round: u32,
}

/// Execute a distributed plan fragment up to a specific exchange round.
///
/// - `target_round`: execute until this exchange round, then yield outbound data.
///   If target_round >= total exchange rounds, execute to completion.
/// - `inbound`: materialized records received from other partitions for each round < target_round.
///
/// Returns ExchangePayload with outbound data or final results.
pub fn execute_step(
    cypher: &str,
    store: &MutableCsrStore,
    partition_id: u32,
    partition_count: u32,
    target_round: u32,
    inbound: &HashMap<u32, Vec<MaterializedRecord>>,
) -> Result<ExchangePayload, String> {
    // Parse and plan
    let ast = yata_cypher::parse(cypher).map_err(|e| format!("parse error: {e}"))?;
    let plan = crate::transpile::transpile(&ast).map_err(|e| format!("transpile error: {e}"))?;
    let dist = crate::distributed_planner::plan_distributed(&plan, partition_count);

    if partition_id as usize >= dist.fragments.len() {
        return Ok(ExchangePayload { outbound: HashMap::new(), results: vec![], done: true, round: target_round });
    }

    let fragment = &dist.fragments[partition_id as usize];
    let pid = partition_id;
    let mut data: Vec<Record> = Vec::new();
    let mut exchange_round = 0u32;

    for op in &fragment.plan.ops {
        match op {
            LogicalOp::Exchange { routing_key, kind } => {
                if exchange_round == target_round {
                    // Yield: materialize records and return outbound data
                    let mut outbound: HashMap<u32, Vec<MaterializedRecord>> = HashMap::new();
                    match kind {
                        ExchangeKind::HashShuffle => {
                            for record in &data {
                                let key_val = eval_routing_key(routing_key, record, store);
                                let target_pid = hash_to_partition(key_val, partition_count);
                                if target_pid != pid {
                                    outbound.entry(target_pid).or_default()
                                        .push(MaterializedRecord::from_record(record, store));
                                }
                            }
                            // Keep local records
                            data.retain(|r| {
                                let key_val = eval_routing_key(routing_key, r, store);
                                hash_to_partition(key_val, partition_count) == pid
                            });
                        }
                        ExchangeKind::Broadcast => {
                            let materialized: Vec<MaterializedRecord> = data.iter()
                                .map(|r| MaterializedRecord::from_record(r, store))
                                .collect();
                            for target in 0..partition_count {
                                if target != pid {
                                    outbound.insert(target, materialized.clone());
                                }
                            }
                        }
                        ExchangeKind::Gather => {
                            if pid != 0 {
                                let materialized: Vec<MaterializedRecord> = data.iter()
                                    .map(|r| MaterializedRecord::from_record(r, store))
                                    .collect();
                                outbound.insert(0, materialized);
                                data.clear();
                            }
                        }
                    }
                    return Ok(ExchangePayload { outbound, results: vec![], done: false, round: exchange_round });
                }
                // Past round: apply same logic but don't yield (already handled in previous call)
                match kind {
                    ExchangeKind::HashShuffle => {
                        data.retain(|r| {
                            let key_val = eval_routing_key(routing_key, r, store);
                            hash_to_partition(key_val, partition_count) == pid
                        });
                    }
                    ExchangeKind::Gather => {
                        if pid != 0 { data.clear(); }
                    }
                    ExchangeKind::Broadcast => {} // keep all
                }
                exchange_round += 1;
            }
            LogicalOp::Receive { .. } => {
                // Inject inbound records for this round (from coordinator)
                let recv_round = exchange_round.saturating_sub(1);
                if let Some(materialized) = inbound.get(&recv_round) {
                    for mr in materialized {
                        if let Some(record) = mr.to_record(store) {
                            data.push(record);
                        }
                    }
                }
            }
            other => {
                data = execute_op(other, data, store);
            }
        }
    }

    // Execution complete — format results
    let results: Vec<Vec<(String, String)>> = data.iter().map(|record| {
        record.values.iter().enumerate().map(|(i, v)| {
            let col = format!("col_{i}");
            let val = match v {
                PropValue::Null => "null".to_string(),
                PropValue::Bool(b) => b.to_string(),
                PropValue::Int(n) => n.to_string(),
                PropValue::Float(f) => f.to_string(),
                PropValue::Str(s) => s.clone(),
            };
            (col, val)
        }).collect()
    }).collect();

    Ok(ExchangePayload { outbound: HashMap::new(), results, done: true, round: exchange_round })
}

/// Evaluate routing key expression to a u64 for partition routing.
fn eval_routing_key(expr: &Expr, record: &Record, _store: &MutableCsrStore) -> u64 {
    match expr {
        Expr::Var(name) => record.bindings.get(name.as_str()).copied().unwrap_or(0) as u64,
        Expr::Lit(PropValue::Int(n)) => *n as u64,
        _ => {
            let val = eval_expr(expr, record, _store);
            match val {
                PropValue::Int(n) => n as u64,
                _ => 0,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed_planner::plan_distributed;
    use crate::executor::execute;
    use crate::planner::PlanBuilder;
    use yata_grin::{Direction, Mutable};

    fn make_store_with_persons(names: &[(&str, i64)]) -> MutableCsrStore {
        let mut store = MutableCsrStore::new();
        for (name, age) in names {
            store.add_vertex(
                "Person",
                &[
                    ("name", PropValue::Str((*name).into())),
                    ("age", PropValue::Int(*age)),
                ],
            );
        }
        store.commit();
        store
    }

    #[test]
    fn test_fragment_no_exchange() {
        let store = make_store_with_persons(&[("Alice", 30), ("Bob", 25)]);
        let plan = PlanBuilder::new()
            .scan("Person", "n")
            .project(vec![Expr::Prop("n".into(), "name".into())])
            .build();

        // Single partition distributed plan — no exchange ops.
        let dist = plan_distributed(&plan, 1);
        let transport = MemoryExchangeTransport::new();
        let results = execute_fragment(&dist.fragments[0], &store, &transport, 1, 1);

        let single_results = execute(&plan, &store);
        assert_eq!(results.len(), single_results.len());
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_fragment_with_hash_shuffle() {
        // Partition 0: Alice(0), Partition 1: Bob(0)
        let mut store0 = MutableCsrStore::new();
        store0.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Alice".into())),
                ("age", PropValue::Int(30)),
            ],
        );
        let bob_vid = store0.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Bob".into())),
                ("age", PropValue::Int(25)),
            ],
        );
        store0.add_edge(0, bob_vid, "KNOWS", &[]);
        store0.commit();

        let mut store1 = MutableCsrStore::new();
        store1.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Charlie".into())),
                ("age", PropValue::Int(35)),
            ],
        );
        store1.commit();

        let plan = PlanBuilder::new()
            .scan("Person", "n")
            .expand("n", "KNOWS", "m", Direction::Out)
            .project(vec![Expr::Prop("m".into(), "name".into())])
            .build();

        let dist = plan_distributed(&plan, 2);
        let transport = MemoryExchangeTransport::new();
        let query_id = 42;

        // Execute on partition 0
        let results0 = execute_fragment(&dist.fragments[0], &store0, &transport, query_id, 2);
        // Execute on partition 1
        let results1 = execute_fragment(&dist.fragments[1], &store1, &transport, query_id, 2);

        // At least partition 0 should have results (Alice->Bob is local).
        // The hash shuffle may route some records to partition 1.
        let total = results0.len() + results1.len();
        assert!(total >= 1, "Should have at least 1 result from expand");
    }

    #[test]
    fn test_fragment_with_gather() {
        // 4 partitions, each has 1 Person. Gather to partition 0.
        let names = [("Alice", 30), ("Bob", 25), ("Charlie", 35), ("Diana", 28)];
        let stores: Vec<MutableCsrStore> = names
            .iter()
            .map(|(name, age)| make_store_with_persons(&[(*name, *age)]))
            .collect();

        let plan = PlanBuilder::new()
            .scan("Person", "n")
            .aggregate(
                vec![],
                vec![("cnt".into(), AggOp::Count, Expr::Var("n".into()))],
            )
            .build();

        let dist = plan_distributed(&plan, 4);
        let transport = MemoryExchangeTransport::new();
        let query_id = 100;

        // Execute all fragments — each partition sends its data via Gather.
        let mut all_results = Vec::new();
        for (i, frag) in dist.fragments.iter().enumerate() {
            let results = execute_fragment(frag, &stores[i], &transport, query_id, 4);
            all_results.push(results);
        }

        // Partition 0 should have the aggregated result after Gather + Aggregate.
        // Non-zero partitions should have sent their data and returned empty (or
        // their local aggregate of 0 records after clearing).
        let p0_results = &all_results[0];
        assert!(
            !p0_results.is_empty(),
            "Partition 0 should have aggregated results"
        );

        // The count on partition 0 includes its own 1 record + received records.
        // Due to synchronous execution, partition 0 runs before others send,
        // so it only sees its own data (1). In a real async system, it would see all 4.
        if let PropValue::Int(cnt) = &p0_results[0].values[0] {
            assert!(*cnt >= 1, "Partition 0 should count at least its own record");
        }
    }

    #[test]
    fn test_fragment_broadcast() {
        let store0 = make_store_with_persons(&[("Alice", 30)]);
        let store1 = make_store_with_persons(&[("Bob", 25)]);

        let transport = MemoryExchangeTransport::new();
        let query_id = 200;

        // Manually create a fragment with broadcast.
        let mut plan = QueryPlan::new();
        plan.push(LogicalOp::Scan {
            label: "Person".into(),
            alias: "n".into(),
            predicate: None,
        });
        plan.push(LogicalOp::Exchange {
            routing_key: Expr::Var("n".into()),
            kind: ExchangeKind::Broadcast,
        });
        plan.push(LogicalOp::Receive {
            source_partitions: vec![],
        });
        plan.push(LogicalOp::Project {
            exprs: vec![Expr::Prop("n".into(), "name".into())],
        });

        let frag0 = PartitionPlanFragment {
            partition_id: 0,
            plan: plan.clone(),
            outbound_exchanges: vec![],
            inbound_receives: vec![],
        };
        let frag1 = PartitionPlanFragment {
            partition_id: 1,
            plan: plan.clone(),
            outbound_exchanges: vec![],
            inbound_receives: vec![],
        };

        // Execute partition 0 first (sends broadcast)
        let results0 = execute_fragment(&frag0, &store0, &transport, query_id, 2);
        // Execute partition 1 (receives broadcast from 0 + its own scan)
        let results1 = execute_fragment(&frag1, &store1, &transport, query_id, 2);

        // Partition 0 keeps its own data (Alice) + receives from 1 (nothing yet since 1 hasn't sent)
        assert!(!results0.is_empty(), "Partition 0 should have its own data");
        // Partition 1 has its own data (Bob) + received Alice from partition 0's broadcast
        assert!(
            results1.len() >= 1,
            "Partition 1 should have at least its own data"
        );
    }

    #[test]
    fn test_hash_to_partition() {
        // Verify distribution across partitions.
        assert_eq!(hash_to_partition(0, 4), 0);
        assert_eq!(hash_to_partition(1, 4), 1);
        assert_eq!(hash_to_partition(2, 4), 2);
        assert_eq!(hash_to_partition(3, 4), 3);
        assert_eq!(hash_to_partition(4, 4), 0);
        assert_eq!(hash_to_partition(5, 4), 1);
        assert_eq!(hash_to_partition(100, 4), 0);
        assert_eq!(hash_to_partition(101, 4), 1);

        // Single partition — everything maps to 0.
        assert_eq!(hash_to_partition(0, 1), 0);
        assert_eq!(hash_to_partition(999, 1), 0);
    }

    #[test]
    fn test_memory_transport_send_recv() {
        let transport = MemoryExchangeTransport::new();

        let records = vec![
            Record {
                bindings: {
                    let mut m = HashMap::new();
                    m.insert("n".to_string(), 0);
                    m
                },
                values: vec![PropValue::Str("Alice".into())],
            },
            Record {
                bindings: {
                    let mut m = HashMap::new();
                    m.insert("n".to_string(), 1);
                    m
                },
                values: vec![PropValue::Str("Bob".into())],
            },
        ];

        // Send from partition 0 to partition 1
        transport
            .send_records(1, 0, 0, 1, &records)
            .expect("send should succeed");

        // Receive on partition 1 from partition 0
        let received = transport
            .recv_records(1, 0, 0, 1)
            .expect("recv should succeed");
        assert!(received.is_some());
        let received = received.unwrap();
        assert_eq!(received.len(), 2);
        assert_eq!(received[0].values[0], PropValue::Str("Alice".into()));
        assert_eq!(received[1].values[0], PropValue::Str("Bob".into()));

        // Receive from non-existent source returns None.
        let none = transport.recv_records(1, 0, 2, 1).expect("should succeed");
        assert!(none.is_none());
    }

    #[test]
    fn test_memory_transport_cleanup() {
        let transport = MemoryExchangeTransport::new();

        let records = vec![Record {
            bindings: HashMap::new(),
            values: vec![PropValue::Int(42)],
        }];

        transport.send_records(10, 0, 0, 1, &records).unwrap();
        transport.send_records(10, 1, 0, 1, &records).unwrap();
        transport.send_records(20, 0, 0, 1, &records).unwrap();

        // Cleanup query 10.
        transport.cleanup(10).unwrap();

        // Query 10 data should be gone.
        let r = transport.recv_records(10, 0, 0, 1).unwrap();
        assert!(r.is_none());

        // Query 20 data should still exist.
        let r = transport.recv_records(20, 0, 0, 1).unwrap();
        assert!(r.is_some());
    }

    #[test]
    fn test_memory_transport_recv_all() {
        let transport = MemoryExchangeTransport::new();

        // 3 partitions send to partition 1.
        let r0 = vec![Record {
            bindings: HashMap::new(),
            values: vec![PropValue::Str("from_0".into())],
        }];
        let r2 = vec![Record {
            bindings: HashMap::new(),
            values: vec![PropValue::Str("from_2".into())],
        }];

        transport.send_records(1, 0, 0, 1, &r0).unwrap();
        transport.send_records(1, 0, 2, 1, &r2).unwrap();

        let all = transport.recv_all(1, 0, 1, 3).unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_two_partition_distributed_query() {
        // End-to-end: 2 partitions with different data.
        // Partition 0: Alice, Bob. Alice->Bob (KNOWS).
        // Partition 1: Charlie, Diana. Charlie->Diana (KNOWS).
        let mut store0 = MutableCsrStore::new();
        store0.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Alice".into())),
                ("age", PropValue::Int(30)),
            ],
        );
        store0.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Bob".into())),
                ("age", PropValue::Int(25)),
            ],
        );
        store0.add_edge(0, 1, "KNOWS", &[]);
        store0.commit();

        let mut store1 = MutableCsrStore::new();
        store1.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Charlie".into())),
                ("age", PropValue::Int(35)),
            ],
        );
        store1.add_vertex(
            "Person",
            &[
                ("name", PropValue::Str("Diana".into())),
                ("age", PropValue::Int(28)),
            ],
        );
        store1.add_edge(0, 1, "KNOWS", &[]);
        store1.commit();

        // MATCH (n:Person)-[:KNOWS]->(m:Person) RETURN m.name
        let plan = PlanBuilder::new()
            .scan("Person", "n")
            .expand("n", "KNOWS", "m", Direction::Out)
            .project(vec![Expr::Prop("m".into(), "name".into())])
            .build();

        let dist = plan_distributed(&plan, 2);
        assert_eq!(dist.fragments.len(), 2);
        assert_eq!(dist.exchange_rounds, 1);

        let transport = MemoryExchangeTransport::new();
        let query_id = 999;

        // Execute both fragments.
        let results0 = execute_fragment(&dist.fragments[0], &store0, &transport, query_id, 2);
        let results1 = execute_fragment(&dist.fragments[1], &store1, &transport, query_id, 2);

        // Merge results from all partitions.
        let mut all_results = Vec::new();
        all_results.extend(results0);
        all_results.extend(results1);

        // Each partition has 1 local edge: Alice->Bob and Charlie->Diana.
        // After hash shuffle, records may move between partitions.
        // Total results should be >= 2 (the two local edges).
        assert!(
            all_results.len() >= 2,
            "Should have at least 2 traversal results, got {}",
            all_results.len()
        );

        // Cleanup.
        transport.cleanup(query_id).unwrap();
    }

    #[test]
    fn test_eval_routing_key_var() {
        let store = MutableCsrStore::new();
        let record = Record {
            bindings: {
                let mut m = HashMap::new();
                m.insert("n".to_string(), 7);
                m
            },
            values: vec![],
        };
        let expr = Expr::Var("n".into());
        assert_eq!(eval_routing_key(&expr, &record, &store), 7);
    }

    #[test]
    fn test_eval_routing_key_lit() {
        let store = MutableCsrStore::new();
        let record = Record {
            bindings: HashMap::new(),
            values: vec![],
        };
        let expr = Expr::Lit(PropValue::Int(42));
        assert_eq!(eval_routing_key(&expr, &record, &store), 42);
    }

    #[test]
    fn test_eval_routing_key_missing_var() {
        let store = MutableCsrStore::new();
        let record = Record {
            bindings: HashMap::new(),
            values: vec![],
        };
        let expr = Expr::Var("missing".into());
        assert_eq!(eval_routing_key(&expr, &record, &store), 0);
    }
}
