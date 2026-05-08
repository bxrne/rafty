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

    // Fault injector: cycle through several fault patterns so we exercise
    // isolation, packet loss, latency, and split-brain partitions.
    spawn(move || {
        let phase = Duration::from_secs(5);
        sleep(phase); // let the initial election settle

        loop {
            // Phase 1: isolate one node entirely.
            info!(target: "injector", "isolate node 1");
            faults.isolate(1);
            sleep(phase);
            faults.heal(1);

            // Phase 2: 30% packet loss across the whole cluster.
            info!(target: "injector", "drop_rate 0.3");
            faults.set_drop_rate(0.3);
            sleep(phase);
            faults.set_drop_rate(0.0);

            // Phase 3: 50-200ms latency on every message.
            info!(target: "injector", "delay 50..200ms");
            faults.set_delay(50, 200);
            sleep(phase);
            faults.set_delay(0, 0);

            // Phase 4: split into a quorum side and a minority side.
            info!(target: "injector", "partition {{1,2}} | {{3}}");
            faults.partition(vec![vec![1, 2], vec![3]]);
            sleep(phase);
            faults.clear_partition();

            // Phase 5: total split, no quorum anywhere.
            info!(target: "injector", "partition {{1}} | {{2}} | {{3}}");
            faults.partition(vec![vec![1], vec![2], vec![3]]);
            sleep(phase);
            faults.clear_partition();

            // Belt and braces.
            faults.reset();

            info!(target: "injector", "healthy");
            sleep(phase);
        }
    });

    for h in handles {
        h.join().expect("node thread panicked");
    }
}
