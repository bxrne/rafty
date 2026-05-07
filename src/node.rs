use std::thread::sleep;
use std::time::Duration;

// State of node in consensus
#[derive(Debug)]
pub enum NodeState {
    Candidate,
    Follower,
    Leader,
}

// A Term represents a period of elected leadership in the consensus
pub type Term = u64;

// A log entry represents a command to be applied to the state machine
pub struct LogEntry {
    term: Term,
    command: String, // Command to be applied to the state machine
}

// A node represents a server in the consensus cluster
pub struct Node {
    id: u64,
    state: NodeState,
    current_term: Term,
    voted_for: Option<u64>, // ID of the candidate this node voted for in the current term

    // Persistent state on all servers
    log: Vec<LogEntry>, // Log entries for state machine replication
    last_applied: u64,  // Index of the last applied log entry
    commit_index: u64,  // Index of the highest log entry known to be committed
}

impl Node {
    // Create a new node with the given ID
    pub fn new(id: u64) -> Self {
        Node {
            id,
            state: NodeState::Follower,
            current_term: 0,
            voted_for: None,
            log: Vec::new(),
            last_applied: 0,
            commit_index: 0,
        }
    }

    fn transition_to_candidate(&mut self) {
        self.state = NodeState::Candidate;
        self.current_term += 1;
        self.voted_for = Some(self.id);
        // reset election timer and start election
    }

    // Main loop of the node, handling state transitions and timeouts
    pub fn run(&mut self) {
        let timer = Duration::from_millis(150); // Election timeout duration
        loop {
            match self.state {
                NodeState::Follower => {
                    // Wait for messages from the leader or candidates
                    // If timeout occurs, transition to candidate
                    sleep(timer);
                    self.transition_to_candidate();
                }
                NodeState::Candidate => {
                    // Start election by sending RequestVote RPCs to other nodes
                    // Wait for votes and transition to leader if majority is received
                    // If timeout occurs, start a new election
                }
                NodeState::Leader => {
                    // Send AppendEntries RPCs to followers to replicate log entries
                    // Handle client commands and update state machine
                }
            }
        }
    }
}
