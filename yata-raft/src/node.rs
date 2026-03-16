use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::{Mutex, Notify};

use crate::network::{PeerAddr, RaftMessage, RaftTransport};
use crate::store::{LogEntry, MemLogStore, StateMachineApplier, YataStateMachine};
use crate::types::{YataRequest, YataResponse};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Follower,
    Candidate,
    Leader,
}

pub struct RaftNode {
    id: u64,
    peers: Vec<PeerAddr>,
    role: Mutex<Role>,
    leader: AtomicBool,
    current_term: AtomicU64,
    commit_index: AtomicU64,
    log_store: Arc<MemLogStore>,
    state_machine: Arc<YataStateMachine>,
    transport: Arc<dyn RaftTransport>,
    _stop_notify: Notify,
}

impl RaftNode {
    pub fn new(
        id: u64,
        peers: Vec<PeerAddr>,
        applier: Arc<dyn StateMachineApplier>,
    ) -> Self {
        Self {
            id,
            peers,
            role: Mutex::new(Role::Follower),
            leader: AtomicBool::new(false),
            current_term: AtomicU64::new(0),
            commit_index: AtomicU64::new(0),
            log_store: Arc::new(MemLogStore::new()),
            state_machine: Arc::new(YataStateMachine::new(applier)),
            transport: Arc::new(NoopTransport),
            _stop_notify: Notify::new(),
        }
    }

    pub fn with_transport(
        id: u64,
        peers: Vec<PeerAddr>,
        applier: Arc<dyn StateMachineApplier>,
        transport: Arc<dyn RaftTransport>,
    ) -> Self {
        Self {
            id,
            peers,
            role: Mutex::new(Role::Follower),
            leader: AtomicBool::new(false),
            current_term: AtomicU64::new(0),
            commit_index: AtomicU64::new(0),
            log_store: Arc::new(MemLogStore::new()),
            state_machine: Arc::new(YataStateMachine::new(applier)),
            transport,
            _stop_notify: Notify::new(),
        }
    }

    pub fn is_leader(&self) -> bool {
        self.leader.load(Ordering::SeqCst)
    }

    pub fn id(&self) -> u64 { self.id }

    pub async fn propose(&self, req: YataRequest) -> Result<YataResponse, String> {
        if !self.is_leader() {
            return Err("not leader".into());
        }

        let term = self.current_term.load(Ordering::SeqCst);
        let index = self.log_store.last_index().await + 1;

        let entry = LogEntry { index, term, request: req };
        self.log_store.append(entry.clone()).await;

        let majority = (self.peers.len() + 1) / 2 + 1;
        let mut ack_count = 1u64;

        for peer in &self.peers {
            let prev_index = index.saturating_sub(1);
            let prev_term = if prev_index == 0 { 0 }
                else { self.log_store.get(prev_index).await.map(|e| e.term).unwrap_or(0) };

            let msg = RaftMessage::AppendEntries {
                term,
                leader_id: self.id,
                prev_log_index: prev_index,
                prev_log_term: prev_term,
                entries: vec![entry.clone()],
                leader_commit: self.commit_index.load(Ordering::SeqCst),
            };

            if let Ok(RaftMessage::AppendEntriesResponse { success: true, .. }) = self.transport.send(peer, msg).await {
                ack_count += 1;
            }
        }

        if ack_count >= majority as u64 {
            self.commit_index.store(index, Ordering::SeqCst);
            self.log_store.set_last_applied(index).await;
            let resp = self.state_machine.apply(&entry.request).await;
            Ok(resp)
        } else {
            Err("failed to reach majority".into())
        }
    }

    pub async fn start(&self) -> Result<(), String> {
        if self.peers.is_empty() {
            let mut role = self.role.lock().await;
            *role = Role::Leader;
            self.leader.store(true, Ordering::SeqCst);
            self.current_term.fetch_add(1, Ordering::SeqCst);
            tracing::info!(id = self.id, "single-node cluster, became leader");
            return Ok(());
        }
        self.run_election().await
    }

    async fn run_election(&self) -> Result<(), String> {
        { let mut role = self.role.lock().await; *role = Role::Candidate; }

        let new_term = self.current_term.fetch_add(1, Ordering::SeqCst) + 1;
        self.log_store.set_term(new_term).await;
        self.log_store.set_voted_for(Some(self.id)).await;

        let last_log_index = self.log_store.last_index().await;
        let last_log_term = self.log_store.last_term().await;

        let mut votes: u64 = 1;
        let majority = (self.peers.len() + 1) / 2 + 1;

        for peer in &self.peers {
            let msg = RaftMessage::VoteRequest {
                term: new_term,
                candidate_id: self.id,
                last_log_index,
                last_log_term,
            };
            if let Ok(RaftMessage::VoteResponse { vote_granted: true, term }) = self.transport.send(peer, msg).await {
                if term == new_term { votes += 1; }
            }
        }

        if votes >= majority as u64 {
            let mut role = self.role.lock().await;
            *role = Role::Leader;
            self.leader.store(true, Ordering::SeqCst);
            tracing::info!(id = self.id, term = new_term, "became leader");
            Ok(())
        } else {
            let mut role = self.role.lock().await;
            *role = Role::Follower;
            self.leader.store(false, Ordering::SeqCst);
            Err("election failed".into())
        }
    }

    pub async fn handle_message(&self, msg: RaftMessage) -> RaftMessage {
        match msg {
            RaftMessage::VoteRequest { term, candidate_id, last_log_index, last_log_term } => {
                let current_term = self.current_term.load(Ordering::SeqCst);
                if term < current_term {
                    return RaftMessage::VoteResponse { term: current_term, vote_granted: false };
                }
                if term > current_term {
                    self.current_term.store(term, Ordering::SeqCst);
                    self.log_store.set_term(term).await;
                    self.log_store.set_voted_for(None).await;
                    let mut role = self.role.lock().await;
                    *role = Role::Follower;
                    self.leader.store(false, Ordering::SeqCst);
                }

                let voted_for = self.log_store.voted_for().await;
                let my_last_index = self.log_store.last_index().await;
                let my_last_term = self.log_store.last_term().await;

                let log_ok = last_log_term > my_last_term
                    || (last_log_term == my_last_term && last_log_index >= my_last_index);
                let can_vote = voted_for.is_none() || voted_for == Some(candidate_id);

                if log_ok && can_vote {
                    self.log_store.set_voted_for(Some(candidate_id)).await;
                    RaftMessage::VoteResponse { term, vote_granted: true }
                } else {
                    RaftMessage::VoteResponse { term, vote_granted: false }
                }
            }
            RaftMessage::AppendEntries { term, leader_id: _, prev_log_index, prev_log_term, entries, leader_commit } => {
                let current_term = self.current_term.load(Ordering::SeqCst);
                if term < current_term {
                    return RaftMessage::AppendEntriesResponse { term: current_term, success: false, match_index: 0 };
                }
                if term > current_term {
                    self.current_term.store(term, Ordering::SeqCst);
                    self.log_store.set_term(term).await;
                }
                {
                    let mut role = self.role.lock().await;
                    *role = Role::Follower;
                    self.leader.store(false, Ordering::SeqCst);
                }

                if prev_log_index > 0 {
                    if let Some(prev_entry) = self.log_store.get(prev_log_index).await {
                        if prev_entry.term != prev_log_term {
                            self.log_store.truncate_from(prev_log_index).await;
                            return RaftMessage::AppendEntriesResponse { term, success: false, match_index: 0 };
                        }
                    } else {
                        return RaftMessage::AppendEntriesResponse { term, success: false, match_index: 0 };
                    }
                }

                let mut last_new_index = prev_log_index;
                for entry in entries {
                    self.log_store.append(entry.clone()).await;
                    last_new_index = entry.index;
                }

                let old_commit = self.commit_index.load(Ordering::SeqCst);
                if leader_commit > old_commit {
                    let new_commit = std::cmp::min(leader_commit, last_new_index);
                    self.commit_index.store(new_commit, Ordering::SeqCst);
                    let last_applied = self.log_store.last_applied().await;
                    for idx in (last_applied + 1)..=new_commit {
                        if let Some(entry) = self.log_store.get(idx).await {
                            self.state_machine.apply(&entry.request).await;
                            self.log_store.set_last_applied(idx).await;
                        }
                    }
                }

                RaftMessage::AppendEntriesResponse { term, success: true, match_index: last_new_index }
            }
            other => {
                tracing::warn!(?other, "unexpected message type in handle_message");
                RaftMessage::VoteResponse { term: 0, vote_granted: false }
            }
        }
    }
}

struct NoopTransport;

#[async_trait::async_trait]
impl RaftTransport for NoopTransport {
    async fn send(&self, _target: &PeerAddr, _msg: RaftMessage) -> Result<RaftMessage, String> {
        Err("no transport configured".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockApplier;

    #[async_trait::async_trait]
    impl StateMachineApplier for MockApplier {
        async fn apply_publish(&self, _: &str, _: u32, _: &str, _: Option<&str>, _: &str, _: Option<&str>, ts_ns: i64, _: &[u8]) -> Result<(u64, i64), String> {
            Ok((1, ts_ns))
        }
        async fn apply_create_topic(&self, _: &str, _: u32, _: u32, _: Option<u64>) -> Result<(), String> { Ok(()) }
        async fn apply_delete_topic(&self, _: &str) -> Result<(), String> { Ok(()) }
        async fn apply_commit_offset(&self, _: &str, _: &str, _: u32, _: u64) -> Result<(), String> { Ok(()) }
    }

    #[tokio::test]
    async fn test_single_node_becomes_leader() {
        let node = RaftNode::new(1, vec![], Arc::new(MockApplier));
        node.start().await.unwrap();
        assert!(node.is_leader());
    }

    #[tokio::test]
    async fn test_propose_on_leader() {
        let node = RaftNode::new(1, vec![], Arc::new(MockApplier));
        node.start().await.unwrap();

        let req = YataRequest::CreateTopic {
            name: "test".into(),
            num_partitions: 1,
            replication_factor: 1,
            retention_hours: None,
        };
        let resp = node.propose(req).await.unwrap();
        assert!(matches!(resp, YataResponse::TopicCreated));
    }

    #[tokio::test]
    async fn test_propose_on_follower_fails() {
        let node = RaftNode::new(1, vec![PeerAddr { node_id: 2, addr: "fake:4222".into() }], Arc::new(MockApplier));
        let req = YataRequest::DeleteTopic { name: "x".into() };
        let err = node.propose(req).await.unwrap_err();
        assert_eq!(err, "not leader");
    }
}
