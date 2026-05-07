use std::thread::{park, spawn};

#[allow(dead_code)]
mod node;

use node::Node;

fn main() {
    for mut node in vec![Node::new(1), Node::new(2), Node::new(3)] {
        spawn(move || {
            node.run();
        });
    }

    loop {
        park(); // block main thread
    }
}
