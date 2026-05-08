use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;
use std::thread::spawn;

#[allow(dead_code)]
mod node;

#[allow(dead_code)]
mod rpc;

use node::{Node, NodeId};
use rpc::{MemoryRPC, RPC};

fn main() {
    let cluster_size: usize = 3;

    // Channels
    let mut senders = HashMap::new();
    let mut inboxes = Vec::new();
    for id in 1..=cluster_size as NodeId {
        let (tx, rx) = mpsc::channel();
        senders.insert(id, tx);
        inboxes.push((id, rx));
    }

    let rpc: Arc<dyn RPC> = Arc::new(MemoryRPC::new(senders));
    let shutdown = Arc::new(AtomicBool::new(false));

    // 4. Spawn nodes; each holds its inbox + a shared RPC handle.
    let handles: Vec<_> = inboxes
        .into_iter()
        .map(|(id, rx)| {
            let rpc = rpc.clone();
            let shutdown = shutdown.clone();
            spawn(move || {
                let mut node = Node::new(id, cluster_size, rx, rpc);
                node.run(shutdown);
            })
        })
        .collect();

    // 5. Run the cluster indefinitely.
    for h in handles {
        h.join().expect("node thread panicked");
    }
}
