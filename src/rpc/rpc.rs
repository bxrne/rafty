//! Transport layer for Raft communication between nodes

use crate::node::{NodeId, Term};

// Message types for communication between nodes
#[derive(Debug, Clone)]
pub enum Message {
    RequestVote {
        term: Term,
        from: NodeId,
    },
    AppendEntries {
        term: Term,
        from: NodeId,
        entries: Vec<String>,
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
}

// RPC trait for sending messages between nodes.
// `&self` + `Send + Sync` so a single handle can be shared across node threads
// via `Arc<dyn RPC>`, regardless of transport (in-memory, TCP, ...).
pub trait RPC: Send + Sync {
    fn send(&self, target: NodeId, msg: Message);
    fn broadcast(&self, msg: Message);
}
