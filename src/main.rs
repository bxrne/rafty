use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;
use std::thread::{sleep, spawn};
use std::time::Duration;

mod node;
mod rpc;

use node::{Node, NodeId};
use rpc::{FaultyRPC, MemoryRPC, RPC};

fn main() {
    let cluster_size: usize = 3;

    // Channels first: each node gets a Sender for its inbox, and we keep the Receiver to give to
    // the node.
    let mut senders = HashMap::new();
    let mut inboxes = Vec::new();
    for id in 1..=cluster_size as NodeId {
        let (tx, rx) = mpsc::channel();
        senders.insert(id, tx);
        inboxes.push((id, rx));
    }

    // Wrapped RPC: base is in mem, wrapped with fault injection.
    let peer_ids: Vec<NodeId> = (1..=cluster_size as NodeId).collect();
    let memory: Arc<dyn RPC> = Arc::new(MemoryRPC::new(senders));
    let (faulty, faults) = FaultyRPC::new(memory, peer_ids.clone());
    let rpc: Arc<dyn RPC> = faulty;

    // TODO: actually use this
    let shutdown = Arc::new(AtomicBool::new(false));

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

    // Fault injector: cycle through nodes, isolate each for a while, then heal.
    spawn(move || {
        let isolate_for = Duration::from_secs(3);
        let healthy_for = Duration::from_secs(3);
        sleep(healthy_for); // let the initial election settle
        for id in peer_ids.into_iter().cycle() {
            println!("[injector] isolate node {id}");
            faults.isolate(id);
            sleep(isolate_for);
            println!("[injector] heal    node {id}");
            faults.heal(id);
            sleep(healthy_for);
        }
    });

    for h in handles {
        h.join().expect("node thread panicked");
    }
}
