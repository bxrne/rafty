//! Node states and behaviour logic

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::rpc::{Message, RPC};

// State of node in consensus
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    Candidate,
    Follower,
    Leader,
}

// A Term represents a period of elected leadership in the consensus
pub type Term = u64;

// Id wrapper for clarity
pub type NodeId = u64;

// A log entry represents a command to be applied to the state machine
pub struct LogEntry {
    term: Term,
    command: String, // Command to be applied to the state machine
}

// Tunables
const ELECTION_TIMEOUT_MIN_MS: u64 = 150;
const ELECTION_TIMEOUT_JITTER_MS: u64 = 150; // actual range: [150, 300)
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);

// A node represents a server in the consensus cluster
pub struct Node {
    id: NodeId,
    state: NodeState,
    current_term: Term,
    voted_for: Option<NodeId>,

    // Persistent state on all servers
    log: Vec<LogEntry>,
    last_applied: u64,
    commit_index: u64,

    // Cluster knowledge
    cluster_size: usize,

    // Election bookkeeping
    last_heard: Instant,
    election_timeout: Duration,
    votes_received: HashSet<NodeId>, // valid only while in Candidate state

    // Tiny LCG so we don't need the `rand` crate
    rng_state: u64,

    // Transport seam
    inbox: Receiver<Message>,
    rpc: Arc<dyn RPC>,
}

impl Node {
    pub fn new(
        id: NodeId,
        cluster_size: usize,
        inbox: Receiver<Message>,
        rpc: Arc<dyn RPC>,
    ) -> Self {
        // Per-node seed mixed with wall-clock so different runs differ.
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as u64)
            .unwrap_or(0);
        let mut node = Node {
            id,
            state: NodeState::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            last_applied: 0,
            commit_index: 0,
            cluster_size,
            last_heard: Instant::now(),
            election_timeout: Duration::from_millis(ELECTION_TIMEOUT_MIN_MS),
            votes_received: HashSet::new(),
            rng_state: id.wrapping_mul(2654435761).wrapping_add(nanos),
            inbox,
            rpc,
        };
        node.reset_election_timer();
        node
    }

    // ---------- main loop ----------

    pub fn run(&mut self, shutdown: Arc<AtomicBool>) {
        while !shutdown.load(Ordering::Relaxed) {
            match self.state {
                NodeState::Follower => self.tick_follower(),
                NodeState::Candidate => self.tick_candidate(),
                NodeState::Leader => self.tick_leader(),
            }
        }
        println!(
            "Node {} stop (state={:?}, term={}, voted_for={:?})",
            self.id, self.state, self.current_term, self.voted_for
        );
    }

    fn tick_follower(&mut self) {
        let timeout = self.election_deadline_remaining();
        match self.inbox.recv_timeout(timeout) {
            Ok(msg) => self.handle_message(msg),
            Err(RecvTimeoutError::Timeout) => self.start_election(),
            Err(RecvTimeoutError::Disconnected) => self.state = NodeState::Follower, // noop, loop will exit on shutdown
        }
    }

    fn tick_candidate(&mut self) {
        let timeout = self.election_deadline_remaining();
        match self.inbox.recv_timeout(timeout) {
            Ok(msg) => self.handle_message(msg),
            Err(RecvTimeoutError::Timeout) => self.start_election(), // start a fresh election
            Err(RecvTimeoutError::Disconnected) => {}
        }
    }

    fn tick_leader(&mut self) {
        // Heartbeat
        self.rpc.broadcast(Message::AppendEntries {
            term: self.current_term,
            from: self.id,
            entries: Vec::new(),
        });
        // Drain inbox up to next heartbeat
        let deadline = Instant::now() + HEARTBEAT_INTERVAL;
        loop {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .unwrap_or_default();
            if remaining.is_zero() {
                break;
            }
            match self.inbox.recv_timeout(remaining) {
                Ok(msg) => {
                    self.handle_message(msg);
                    if self.state != NodeState::Leader {
                        return; // stepped down
                    }
                }
                Err(_) => break,
            }
        }
    }

    // ---------- message handling ----------

    fn handle_message(&mut self, msg: Message) {
        // Universal "higher term seen" rule.
        let msg_term = msg.term();
        if msg_term > self.current_term {
            self.current_term = msg_term;
            self.voted_for = None;
            self.state = NodeState::Follower;
        }

        match msg {
            Message::RequestVote { term, from } => {
                let grant = term >= self.current_term
                    && (self.voted_for.is_none() || self.voted_for == Some(from));
                if grant {
                    self.voted_for = Some(from);
                    self.reset_election_timer();
                }
                self.rpc.send(
                    from,
                    Message::VoteResponse {
                        term: self.current_term,
                        from: self.id,
                        vote_granted: grant,
                    },
                );
            }
            Message::AppendEntries { term, from, .. } => {
                let success = term >= self.current_term;
                if success {
                    if self.state == NodeState::Candidate {
                        self.state = NodeState::Follower;
                    }
                    self.reset_election_timer();
                }
                self.rpc.send(
                    from,
                    Message::AppendEntriesResponse {
                        term: self.current_term,
                        from: self.id,
                        success,
                    },
                );
            }
            Message::VoteResponse {
                term,
                from,
                vote_granted,
            } => {
                if self.state == NodeState::Candidate && term == self.current_term && vote_granted {
                    self.votes_received.insert(from);
                    if self.has_majority() {
                        self.become_leader();
                    }
                }
            }
            Message::AppendEntriesResponse { .. } => {
                // Log replication not implemented yet.
            }
        }
    }

    // ---------- transitions ----------

    fn start_election(&mut self) {
        self.state = NodeState::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.votes_received.clear();
        self.votes_received.insert(self.id);
        self.reset_election_timer();
        println!("Node {} -> Candidate (term {})", self.id, self.current_term);
        self.rpc.broadcast(Message::RequestVote {
            term: self.current_term,
            from: self.id,
        });
        // Single-node clusters elect themselves immediately.
        if self.has_majority() {
            self.become_leader();
        }
    }

    fn become_leader(&mut self) {
        self.state = NodeState::Leader;
        println!("Node {} -> Leader   (term {})", self.id, self.current_term);
        // Immediate heartbeat to assert authority.
        self.rpc.broadcast(Message::AppendEntries {
            term: self.current_term,
            from: self.id,
            entries: Vec::new(),
        });
    }

    fn has_majority(&self) -> bool {
        self.votes_received.len() > self.cluster_size / 2
    }

    // ---------- timers / rng ----------

    fn reset_election_timer(&mut self) {
        self.last_heard = Instant::now();
        let jitter = self.next_rand() % ELECTION_TIMEOUT_JITTER_MS;
        self.election_timeout = Duration::from_millis(ELECTION_TIMEOUT_MIN_MS + jitter);
    }

    fn election_deadline_remaining(&self) -> Duration {
        self.election_timeout
            .checked_sub(self.last_heard.elapsed())
            .unwrap_or(Duration::from_millis(0))
    }

    fn next_rand(&mut self) -> u64 {
        // Knuth LCG constants
        self.rng_state = self
            .rng_state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.rng_state >> 33
    }
}
