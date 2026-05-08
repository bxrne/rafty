//! Fault-injecting RPC wrapper for testing partitions, message loss, and latency.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use rand::prelude::*;

use crate::node::NodeId;
use crate::rpc::{Message, RPC};

/// Wraps another `RPC` and applies the configured faults to every message.
/// Use [`FaultController`] to mutate the fault set at runtime.
pub struct FaultyRPC {
    inner: Arc<dyn RPC>,
    peers: Vec<NodeId>,
    state: Arc<Mutex<FaultState>>,
}

struct FaultState {
    isolated: HashSet<NodeId>,
    drop_rate: f32,
    delay_min_ms: u64,
    delay_max_ms: u64,
    /// Group id per node; two nodes can talk only if they share a group.
    /// Empty map = no partition active.
    partition: HashMap<NodeId, u32>,
}

impl FaultState {
    fn new() -> Self {
        FaultState {
            isolated: HashSet::new(),
            drop_rate: 0.0,
            delay_min_ms: 0,
            delay_max_ms: 0,
            partition: HashMap::new(),
        }
    }

    fn allowed(&self, from: NodeId, to: NodeId) -> bool {
        if self.isolated.contains(&from) || self.isolated.contains(&to) {
            return false;
        }
        match (self.partition.get(&from), self.partition.get(&to)) {
            (Some(a), Some(b)) => a == b,
            _ => true,
        }
    }
}

impl FaultyRPC {
    pub fn new(inner: Arc<dyn RPC>, peers: Vec<NodeId>) -> (Arc<Self>, FaultController) {
        let state = Arc::new(Mutex::new(FaultState::new()));
        let rpc = Arc::new(FaultyRPC {
            inner,
            peers,
            state: state.clone(),
        });
        (rpc, FaultController { state })
    }

    /// Decide what to do with `(from -> to, msg)`: drop it, delay it, or pass it.
    fn dispatch(&self, from: NodeId, to: NodeId, msg: Message) {
        let (drop_rate, delay_min, delay_max) = {
            let s = self.state.lock().unwrap();
            if !s.allowed(from, to) {
                return;
            }
            (s.drop_rate, s.delay_min_ms, s.delay_max_ms)
        };

        let mut rng = rand::rng();
        if drop_rate > 0.0 && rng.random::<f32>() < drop_rate {
            return;
        }

        let delay = if delay_max > delay_min {
            Some(rng.random_range(delay_min..=delay_max))
        } else if delay_max > 0 {
            Some(delay_max)
        } else {
            None
        };

        match delay {
            None => self.inner.send(to, msg),
            Some(ms) => {
                // Spawn so we don't block the caller's tick.
                let inner = self.inner.clone();
                thread::spawn(move || {
                    thread::sleep(Duration::from_millis(ms));
                    inner.send(to, msg);
                });
            }
        }
    }
}

impl RPC for FaultyRPC {
    fn send(&self, target: NodeId, msg: Message) {
        self.dispatch(msg.from(), target, msg);
    }

    fn broadcast(&self, msg: Message) {
        for &peer in &self.peers {
            self.dispatch(msg.from(), peer, msg.clone());
        }
    }
}

/// Handle for mutating the active fault set at runtime.
#[derive(Clone)]
pub struct FaultController {
    state: Arc<Mutex<FaultState>>,
}

impl FaultController {
    pub fn isolate(&self, id: NodeId) {
        self.state.lock().unwrap().isolated.insert(id);
    }

    pub fn heal(&self, id: NodeId) {
        self.state.lock().unwrap().isolated.remove(&id);
    }

    /// Drop each message with probability `p` (0.0 = none, 1.0 = all).
    pub fn set_drop_rate(&self, p: f32) {
        self.state.lock().unwrap().drop_rate = p.clamp(0.0, 1.0);
    }

    /// Add a uniform random latency `[min_ms, max_ms]` to every message.
    pub fn set_delay(&self, min_ms: u64, max_ms: u64) {
        let mut s = self.state.lock().unwrap();
        s.delay_min_ms = min_ms;
        s.delay_max_ms = max_ms.max(min_ms);
    }

    /// Partition the cluster into the given groups. Messages flow only within
    /// a group. Nodes not listed in any group are unrestricted.
    pub fn partition(&self, groups: Vec<Vec<NodeId>>) {
        let mut map = HashMap::new();
        for (gid, nodes) in groups.into_iter().enumerate() {
            for n in nodes {
                map.insert(n, gid as u32);
            }
        }
        self.state.lock().unwrap().partition = map;
    }

    pub fn clear_partition(&self) {
        self.state.lock().unwrap().partition.clear();
    }

    /// Clear every active fault.
    pub fn reset(&self) {
        let mut s = self.state.lock().unwrap();
        s.isolated.clear();
        s.drop_rate = 0.0;
        s.delay_min_ms = 0;
        s.delay_max_ms = 0;
        s.partition.clear();
    }
}
