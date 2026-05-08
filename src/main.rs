use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread::{sleep, spawn};
use std::time::Duration;

use tracing::info;

mod node;
mod rpc;

use node::{Node, NodeId};
use rpc::{FaultyRPC, MemoryRPC, RPC};

fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let cluster_size: usize = 3;
    let peers: Vec<NodeId> = (1..=cluster_size as NodeId).collect();

    // Channels first: each node gets a Sender for its inbox, and we keep the Receiver to give to
    // the node.
    let mut senders = HashMap::new();
    let mut inboxes = Vec::new();
    for &id in &peers {
        let (tx, rx) = mpsc::channel();
        senders.insert(id, tx);
        inboxes.push((id, rx));
    }

    // Wrapped RPC: base is in mem, wrapped with fault injection.
    let memory: Arc<dyn RPC> = Arc::new(MemoryRPC::new(senders));
    let (faulty, faults) = FaultyRPC::new(memory, peers.clone());
    let rpc: Arc<dyn RPC> = faulty;

    // Shared client-command queue. Only the current leader pulls from it.
    let (client_tx, client_rx) = mpsc::channel::<String>();
    let client_rx = Arc::new(Mutex::new(client_rx));

    // TODO: actually use this
    let shutdown = Arc::new(AtomicBool::new(false));

    let handles: Vec<_> = inboxes
        .into_iter()
        .map(|(id, rx)| {
            let rpc = rpc.clone();
            let shutdown = shutdown.clone();
            let peers = peers.clone();
            let client_rx = client_rx.clone();
            spawn(move || {
                let mut node = Node::new(id, peers, rx, rpc, client_rx);
                node.run(shutdown);
            })
        })
        .collect();

    // Client thread: feed key=value commands into the cluster.
    spawn(move || {
        let mut counter: u64 = 0;
        loop {
            sleep(Duration::from_secs(1));
            counter += 1;
            let cmd = format!("k{counter}=v{counter}");
            let _ = client_tx.send(cmd);
        }
    });

    // Fault injector: cycle through nodes, isolate each for a while, then heal.
    spawn(move || {
        let isolate_for = Duration::from_secs(4);
        let healthy_for = Duration::from_secs(4);
        sleep(healthy_for); // let the initial election settle
        for id in peers.into_iter().cycle() {
            info!(target: "injector", node = id, "isolate");
            faults.isolate(id);
            sleep(isolate_for);
            info!(target: "injector", node = id, "heal");
            faults.heal(id);
            sleep(healthy_for);
        }
    });

    for h in handles {
        h.join().expect("node thread panicked");
    }
}
