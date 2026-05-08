//! In memory RPC

use std::collections::HashMap;
use std::sync::mpsc::Sender;

use crate::node::NodeId;
use crate::rpc::{Message, RPC};

pub struct MemoryRPC {
    peers: HashMap<NodeId, Sender<Message>>,
}

impl MemoryRPC {
    pub fn new(peers: HashMap<NodeId, Sender<Message>>) -> Self {
        MemoryRPC { peers }
    }
}

impl RPC for MemoryRPC {
    fn send(&self, target: NodeId, msg: Message) {
        if let Some(tx) = self.peers.get(&target) {
            // Ignore send errors: peer may have shut down.
            let _ = tx.send(msg);
        }
    }

    fn broadcast(&self, msg: Message) {
        for tx in self.peers.values() {
            let _ = tx.send(msg.clone());
        }
    }
}
