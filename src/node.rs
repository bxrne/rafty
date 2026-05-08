//! Node states and behaviour logic

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rand::prelude::*;
use tracing::{debug, info, info_span, warn};

use crate::rpc::{Message, RPC};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    Follower,
    PreCandidate,
    Candidate,
    Leader,
}

pub type Term = u64;
pub type NodeId = u64;

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub term: Term,
    pub command: String,
}

// Tunables
const ELECTION_TIMEOUT_MIN_MS: u64 = 150;
const ELECTION_TIMEOUT_JITTER_MS: u64 = 150; // [150, 300)
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(50);

pub struct Node {
    id: NodeId,
    state: NodeState,
    current_term: Term,
    voted_for: Option<NodeId>,

    // Log + state-machine application
    log: Vec<LogEntry>,
    last_applied: u64,
    commit_index: u64,
    state_machine: HashMap<String, String>,

    // Cluster knowledge (includes self)
    peers: Vec<NodeId>,

    // Leader-only bookkeeping (cleared on step-down, populated on become_leader).
    next_index: HashMap<NodeId, u64>,
    match_index: HashMap<NodeId, u64>,

    // Election bookkeeping
    last_heard: Instant,
    election_timeout: Duration,
    votes_received: HashSet<NodeId>,

    // Transport seam + client surface + leader command intake
    inbox: Receiver<Message>,
    rpc: Arc<dyn RPC>,
    client_rx: Arc<Mutex<Receiver<String>>>,
}

impl Node {
    pub fn new(
        id: NodeId,
        peers: Vec<NodeId>,
        inbox: Receiver<Message>,
        rpc: Arc<dyn RPC>,
        client_rx: Arc<Mutex<Receiver<String>>>,
    ) -> Self {
        let mut node = Node {
            id,
            state: NodeState::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            last_applied: 0,
            commit_index: 0,
            state_machine: HashMap::new(),
            peers,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            last_heard: Instant::now(),
            election_timeout: Duration::from_millis(ELECTION_TIMEOUT_MIN_MS),
            votes_received: HashSet::new(),
            inbox,
            rpc,
            client_rx,
        };
        node.reset_election_timer();
        node
    }

    pub fn run(&mut self, shutdown: Arc<AtomicBool>) {
        let span = info_span!("node", id = self.id);
        let _enter = span.enter();
        info!("started");
        while !shutdown.load(Ordering::Relaxed) {
            match self.state {
                NodeState::Follower => self.tick_follower(),
                NodeState::PreCandidate => self.tick_pre_candidate(),
                NodeState::Candidate => self.tick_candidate(),
                NodeState::Leader => self.tick_leader(),
            }
        }
        info!(
            term = self.current_term,
            state = ?self.state,
            log_len = self.log.len(),
            kv_size = self.state_machine.len(),
            "stop"
        );
    }

    fn tick_follower(&mut self) {
        let timeout = self.election_deadline_remaining();
        match self.inbox.recv_timeout(timeout) {
            Ok(msg) => self.handle_message(msg),
            Err(RecvTimeoutError::Timeout) => self.start_pre_election(),
            Err(RecvTimeoutError::Disconnected) => {}
        }
    }

    fn tick_pre_candidate(&mut self) {
        let timeout = self.election_deadline_remaining();
        match self.inbox.recv_timeout(timeout) {
            Ok(msg) => self.handle_message(msg),
            Err(RecvTimeoutError::Timeout) => self.start_pre_election(),
            Err(RecvTimeoutError::Disconnected) => {}
        }
    }

    fn tick_candidate(&mut self) {
        let timeout = self.election_deadline_remaining();
        match self.inbox.recv_timeout(timeout) {
            Ok(msg) => self.handle_message(msg),
            Err(RecvTimeoutError::Timeout) => self.start_pre_election(),
            Err(RecvTimeoutError::Disconnected) => {}
        }
    }

    fn tick_leader(&mut self) {
        // Pull any client commands and append to our log first.
        self.drain_client_queue();

        // Send per-peer AppendEntries (heartbeat or real).
        let peers: Vec<NodeId> = self
            .peers
            .iter()
            .copied()
            .filter(|&p| p != self.id)
            .collect();
        for peer in peers {
            self.send_append_entries(peer);
        }

        // Drain inbox up to the next heartbeat.
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
                        return;
                    }
                }
                Err(_) => break,
            }
        }
    }

    fn drain_client_queue(&mut self) {
        // Collect first to release the lock before mutating self.log.
        let cmds: Vec<String> = {
            let rx = self.client_rx.lock().unwrap();
            let mut v = Vec::new();
            while let Ok(c) = rx.try_recv() {
                v.push(c);
            }
            v
        };
        for command in cmds {
            let term = self.current_term;
            self.log.push(LogEntry {
                term,
                command: command.clone(),
            });
            debug!(idx = self.log.len(), term, command = %command, "leader appended client cmd");
        }
    }

    fn send_append_entries(&self, peer: NodeId) {
        let next = self.next_index.get(&peer).copied().unwrap_or(1);
        let prev_log_index = next.saturating_sub(1);
        let prev_log_term = if prev_log_index == 0 {
            0
        } else {
            self.log[(prev_log_index - 1) as usize].term
        };
        let entries: Vec<LogEntry> = if (next as usize) <= self.log.len() {
            self.log[(next - 1) as usize..].to_vec()
        } else {
            Vec::new()
        };
        self.rpc.send(
            peer,
            Message::AppendEntries {
                term: self.current_term,
                from: self.id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit: self.commit_index,
            },
        );
    }

    fn handle_message(&mut self, msg: Message) {
        // Universal "higher term seen" rule, EXCEPT for PreVote messages —
        // ignoring them here is the whole point of PreVote (no term bump from
        // a returning isolated node that has been spinning elections alone).
        let is_prevote = matches!(
            msg,
            Message::PreVoteRequest { .. } | Message::PreVoteResponse { .. }
        );
        if !is_prevote {
            let msg_term = msg.term();
            if msg_term > self.current_term {
                self.step_down(msg_term);
            }
        }

        match msg {
            Message::RequestVote {
                term,
                from,
                last_log_index,
                last_log_term,
            } => self.handle_request_vote(term, from, last_log_index, last_log_term),
            Message::AppendEntries {
                term,
                from,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.handle_append_entries(
                term,
                from,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            ),
            Message::VoteResponse {
                term,
                from,
                vote_granted,
            } => self.handle_vote_response(term, from, vote_granted),
            Message::AppendEntriesResponse {
                term,
                from,
                success,
                match_index,
            } => self.handle_append_entries_response(term, from, success, match_index),
            Message::PreVoteRequest {
                term,
                from,
                last_log_index,
                last_log_term,
            } => self.handle_pre_vote_request(term, from, last_log_index, last_log_term),
            Message::PreVoteResponse {
                term,
                from,
                vote_granted,
            } => self.handle_pre_vote_response(term, from, vote_granted),
        }
    }

    fn handle_pre_vote_request(
        &mut self,
        proposed_term: Term,
        from: NodeId,
        last_log_index: u64,
        last_log_term: Term,
    ) {
        let our_last_idx = self.log.len() as u64;
        let our_last_term = self.log.last().map(|e| e.term).unwrap_or(0);
        let log_ok = last_log_term > our_last_term
            || (last_log_term == our_last_term && last_log_index >= our_last_idx);
        // Only grant if we'd be willing to start our own election — i.e. we
        // haven't heard from a leader within the minimum election window.
        let timer_ok = self.last_heard.elapsed() >= Duration::from_millis(ELECTION_TIMEOUT_MIN_MS);
        let grant = proposed_term > self.current_term && log_ok && timer_ok;

        if !grant {
            debug!(
                candidate = from,
                proposed_term, log_ok, timer_ok, "denied pre-vote"
            );
        }

        self.rpc.send(
            from,
            Message::PreVoteResponse {
                term: proposed_term, // echo, not our current_term
                from: self.id,
                vote_granted: grant,
            },
        );
    }

    fn handle_pre_vote_response(&mut self, proposed_term: Term, from: NodeId, vote_granted: bool) {
        if self.state != NodeState::PreCandidate {
            return;
        }
        // The proposed_term we sent was current_term + 1.
        if proposed_term != self.current_term + 1 || !vote_granted {
            return;
        }
        self.votes_received.insert(from);
        if self.has_majority() {
            // Pre-flight passed: now run a real election.
            self.start_election();
        }
    }

    fn handle_request_vote(
        &mut self,
        term: Term,
        from: NodeId,
        last_log_index: u64,
        last_log_term: Term,
    ) {
        let our_last_idx = self.log.len() as u64;
        let our_last_term = self.log.last().map(|e| e.term).unwrap_or(0);
        let log_ok = last_log_term > our_last_term
            || (last_log_term == our_last_term && last_log_index >= our_last_idx);

        let grant = term >= self.current_term
            && (self.voted_for.is_none() || self.voted_for == Some(from))
            && log_ok;

        if grant {
            self.voted_for = Some(from);
            self.reset_election_timer();
            info!(term, candidate = from, "granted vote");
        } else {
            debug!(
                term,
                candidate = from,
                our_term = self.current_term,
                voted_for = ?self.voted_for,
                log_ok,
                "denied vote"
            );
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

    fn handle_append_entries(
        &mut self,
        term: Term,
        from: NodeId,
        prev_log_index: u64,
        prev_log_term: Term,
        entries: Vec<LogEntry>,
        leader_commit: u64,
    ) {
        // Stale leader.
        if term < self.current_term {
            self.rpc.send(
                from,
                Message::AppendEntriesResponse {
                    term: self.current_term,
                    from: self.id,
                    success: false,
                    match_index: 0,
                },
            );
            return;
        }

        // Valid leader for our term: refresh timer, ensure we're a follower.
        self.reset_election_timer();
        if self.state == NodeState::Candidate {
            self.state = NodeState::Follower;
        }

        // Log-consistency check at prev_log_index.
        if prev_log_index > 0 {
            let prev_idx = (prev_log_index - 1) as usize;
            let consistent = self
                .log
                .get(prev_idx)
                .map(|e| e.term == prev_log_term)
                .unwrap_or(false);
            if !consistent {
                debug!(
                    leader = from,
                    prev_log_index,
                    prev_log_term,
                    have_len = self.log.len(),
                    "rejecting AppendEntries: prev mismatch"
                );
                self.rpc.send(
                    from,
                    Message::AppendEntriesResponse {
                        term: self.current_term,
                        from: self.id,
                        success: false,
                        match_index: 0,
                    },
                );
                return;
            }
        }

        // Walk entries; truncate on conflict, then append the rest.
        let mut idx = prev_log_index;
        for entry in entries.into_iter() {
            idx += 1;
            let pos = (idx - 1) as usize;
            match self.log.get(pos) {
                Some(existing) if existing.term == entry.term => {
                    // Already in sync at this slot.
                }
                Some(_) => {
                    warn!(idx, "truncating divergent log tail");
                    self.log.truncate(pos);
                    self.log.push(entry);
                }
                None => {
                    self.log.push(entry);
                }
            }
        }
        let new_match = idx; // highest index now in our log from this RPC

        // Advance commit using the leader's commit, capped by what we just added.
        if leader_commit > self.commit_index {
            self.commit_index = leader_commit.min(self.log.len() as u64);
            self.apply_log();
        }

        self.rpc.send(
            from,
            Message::AppendEntriesResponse {
                term: self.current_term,
                from: self.id,
                success: true,
                match_index: new_match,
            },
        );
    }

    fn handle_vote_response(&mut self, term: Term, from: NodeId, vote_granted: bool) {
        if self.state != NodeState::Candidate || term != self.current_term {
            return;
        }
        if vote_granted {
            self.votes_received.insert(from);
            if self.has_majority() {
                self.become_leader();
            }
        }
    }

    fn handle_append_entries_response(
        &mut self,
        term: Term,
        from: NodeId,
        success: bool,
        match_index: u64,
    ) {
        if self.state != NodeState::Leader || term != self.current_term {
            return;
        }
        if success {
            self.match_index.insert(from, match_index);
            self.next_index.insert(from, match_index + 1);
            self.maybe_advance_commit();
        } else {
            // Back off and retry next heartbeat.
            let cur = self.next_index.get(&from).copied().unwrap_or(1);
            let new = cur.saturating_sub(1).max(1);
            self.next_index.insert(from, new);
            warn!(
                peer = from,
                next_index = new,
                "append rejected, backing off"
            );
        }
    }

    fn start_pre_election(&mut self) {
        self.state = NodeState::PreCandidate;
        self.votes_received.clear();
        self.votes_received.insert(self.id);
        self.reset_election_timer();
        let proposed_term = self.current_term + 1;
        info!(proposed_term, "starting pre-vote");

        let last_log_index = self.log.len() as u64;
        let last_log_term = self.log.last().map(|e| e.term).unwrap_or(0);
        self.rpc.broadcast(Message::PreVoteRequest {
            term: proposed_term,
            from: self.id,
            last_log_index,
            last_log_term,
        });
        // Single-node clusters: skip straight to the real election.
        if self.has_majority() {
            self.start_election();
        }
    }

    fn start_election(&mut self) {
        self.state = NodeState::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        self.votes_received.clear();
        self.votes_received.insert(self.id);
        self.reset_election_timer();
        info!(term = self.current_term, "starting election");

        let last_log_index = self.log.len() as u64;
        let last_log_term = self.log.last().map(|e| e.term).unwrap_or(0);
        self.rpc.broadcast(Message::RequestVote {
            term: self.current_term,
            from: self.id,
            last_log_index,
            last_log_term,
        });

        if self.has_majority() {
            self.become_leader();
        }
    }

    fn become_leader(&mut self) {
        self.state = NodeState::Leader;
        let last = self.log.len() as u64;
        self.next_index.clear();
        self.match_index.clear();
        for &peer in &self.peers {
            if peer == self.id {
                continue;
            }
            self.next_index.insert(peer, last + 1);
            self.match_index.insert(peer, 0);
        }
        info!(
            term = self.current_term,
            last_log_index = last,
            "became leader"
        );
        // Immediate heartbeat to assert authority. (Per-peer because next_index varies.)
        let peers: Vec<NodeId> = self
            .peers
            .iter()
            .copied()
            .filter(|&p| p != self.id)
            .collect();
        for peer in peers {
            self.send_append_entries(peer);
        }
    }

    fn step_down(&mut self, new_term: Term) {
        if self.state == NodeState::Leader {
            info!(
                from_term = self.current_term,
                to_term = new_term,
                "stepping down"
            );
        }
        self.current_term = new_term;
        self.voted_for = None;
        self.state = NodeState::Follower;
        self.next_index.clear();
        self.match_index.clear();
    }

    fn has_majority(&self) -> bool {
        self.votes_received.len() > self.peers.len() / 2
    }

    fn maybe_advance_commit(&mut self) {
        let last = self.log.len() as u64;
        let mut new_commit = self.commit_index;
        for n in (self.commit_index + 1)..=last {
            // Figure 8 rule: only commit entries from the current term.
            if self.log[(n - 1) as usize].term != self.current_term {
                continue;
            }
            let mut count = 1; // self
            for &peer in &self.peers {
                if peer == self.id {
                    continue;
                }
                if self.match_index.get(&peer).copied().unwrap_or(0) >= n {
                    count += 1;
                }
            }
            if count > self.peers.len() / 2 {
                new_commit = n;
            }
        }
        if new_commit > self.commit_index {
            self.commit_index = new_commit;
            self.apply_log();
        }
    }

    fn apply_log(&mut self) {
        while self.last_applied < self.commit_index {
            let idx = self.last_applied as usize;
            let (term, command) = match self.log.get(idx) {
                Some(e) => (e.term, e.command.clone()),
                None => break,
            };
            self.apply_to_state_machine(&command);
            info!(
                idx = idx + 1,
                term,
                command = %command,
                kv_size = self.state_machine.len(),
                "applied"
            );
            self.last_applied += 1;
        }
    }

    fn apply_to_state_machine(&mut self, command: &str) {
        // Tiny KV: "key=value" upserts; anything else is ignored.
        if let Some((k, v)) = command.split_once('=') {
            self.state_machine.insert(k.to_string(), v.to_string());
        }
    }

    fn reset_election_timer(&mut self) {
        self.last_heard = Instant::now();
        let jitter = rand::rng().random_range(0..ELECTION_TIMEOUT_JITTER_MS);
        self.election_timeout = Duration::from_millis(ELECTION_TIMEOUT_MIN_MS + jitter);
    }

    fn election_deadline_remaining(&self) -> Duration {
        self.election_timeout
            .checked_sub(self.last_heard.elapsed())
            .unwrap_or(Duration::from_millis(0))
    }
}
