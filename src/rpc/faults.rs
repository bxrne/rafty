//! Fault-injecting RPC wrapper for testing partitions / unresponsive nodes.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use crate::node::NodeId;
use crate::rpc::{Message, RPC};

/// Wraps another `RPC` and silently drops any message whose source or target
/// is currently isolated. Use [`FaultController`] to mutate the fault set.
pub struct FaultyRPC {
    inner: Arc<dyn RPC>,
    peers: Vec<NodeId>,
    isolated: Arc<Mutex<HashSet<NodeId>>>,
}

impl FaultyRPC {
    pub fn new(inner: Arc<dyn RPC>, peers: Vec<NodeId>) -> (Arc<Self>, FaultController) {
        let isolated = Arc::new(Mutex::new(HashSet::new()));
        let rpc = Arc::new(FaultyRPC {
            inner,
            peers,
            isolated: isolated.clone(),
        });
        (rpc, FaultController { isolated })
    }

    fn blocked(&self, from: NodeId, to: NodeId) -> bool {
        let g = self.isolated.lock().unwrap();
        g.contains(&from) || g.contains(&to)
    }
}

impl RPC for FaultyRPC {
    fn send(&self, target: NodeId, msg: Message) {
        if self.blocked(msg.from(), target) {
            return;
        }
        self.inner.send(target, msg);
    }

    fn broadcast(&self, msg: Message) {
        // Iterate per-peer so we can drop selectively.
        for &peer in &self.peers {
            if self.blocked(msg.from(), peer) {
                continue;
            }
            self.inner.send(peer, msg.clone());
        }
    }
}

/// Handle for flipping nodes in and out of the cluster at runtime.
#[derive(Clone)]
pub struct FaultController {
    isolated: Arc<Mutex<HashSet<NodeId>>>,
}

impl FaultController {
    pub fn isolate(&self, id: NodeId) {
        self.isolated.lock().unwrap().insert(id);
    }

    pub fn heal(&self, id: NodeId) {
        self.isolated.lock().unwrap().remove(&id);
    }
}
