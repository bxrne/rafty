//! Transport layer for Raft communication between nodes

use crate::node::{LogEntry, NodeId, Term};

// Message types for communication between nodes
#[derive(Debug, Clone)]
pub enum Message {
    RequestVote {
        term: Term,
        from: NodeId,
        last_log_index: u64,
        last_log_term: Term,
    },
    AppendEntries {
        term: Term,
        from: NodeId,
        prev_log_index: u64,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    },
    VoteResponse {
        term: Term,
        from: NodeId,
        vote_granted: bool,
    },
    AppendEntriesResponse {
        term: Term,
        from: NodeId,
        success: bool,
        // Highest log index now matched on the responder; lets the leader
        // update `match_index[from]` without recomputing from prev_log_index.
        match_index: u64,
    },
}

impl Message {
    pub fn term(&self) -> Term {
        match self {
            Message::RequestVote { term, .. }
            | Message::AppendEntries { term, .. }
            | Message::VoteResponse { term, .. }
            | Message::AppendEntriesResponse { term, .. } => *term,
        }
    }

    pub fn from(&self) -> NodeId {
        match self {
            Message::RequestVote { from, .. }
            | Message::AppendEntries { from, .. }
            | Message::VoteResponse { from, .. }
            | Message::AppendEntriesResponse { from, .. } => *from,
        }
    }
}

// RPC trait for sending messages between nodes.
// `&self` + `Send + Sync` so a single handle can be shared across node threads
// via `Arc<dyn RPC>`, regardless of transport (in-memory, TCP, ...).
pub trait RPC: Send + Sync {
    fn send(&self, target: NodeId, msg: Message);
    fn broadcast(&self, msg: Message);
}
