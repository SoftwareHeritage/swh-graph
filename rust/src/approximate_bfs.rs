// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! A parallel almost-BFS traversal
//!
//! This implements a graph traversal that is like a BFS from many sources, but traversal
//! from a source may steal a node from another traversal

use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use dsi_progress_logger::{progress_logger, ProgressLog};
use num_cpus;
use rayon::prelude::*;
use sux::prelude::AtomicBitVec;
use thread_local::ThreadLocal;
use webgraph::prelude::*;

use crate::map::OwnedPermutation;

type NodeId = usize;

pub fn almost_bfs_order<G: RandomAccessGraph + Send + Sync>(
    graph: &G,
    start_nodes: &[NodeId],
) -> OwnedPermutation<Vec<NodeId>> {
    let num_nodes = graph.num_nodes();

    let visited = AtomicBitVec::new(num_nodes);

    let visit_from_root_node = |thread_order: &mut Vec<NodeId>, root_node| -> Option<usize> {
        if visited.get(root_node, Ordering::Relaxed) {
            // Skip VecDeque allocation
            return None;
        }
        let mut visited_nodes = 0;
        let mut queue = VecDeque::new();
        queue.push_back(root_node);
        while let Some(node) = queue.pop_front() {
            // As we are not atomically getting and setting 'visited' bit, other
            // threads may also visit it at the same time. We will deduplicate that
            // at the end, so the only effect is for some nodes to be double-counted
            // by the progress logger.
            if visited.get(node, Ordering::Relaxed) {
                continue;
            }
            visited.set(node, true, Ordering::Relaxed);
            visited_nodes += 1;
            thread_order.push(node);

            for succ in graph.successors(node) {
                queue.push_back(succ);
            }
        }
        Some(visited_nodes)
    };

    let mut pl = progress_logger!(
        display_memory = true,
        item_name = "node",
        local_speed = true,
        expected_updates = Some(num_nodes),
    );
    pl.start("[step 1/2] Visiting graph in pseudo-BFS order...");
    let pl = Arc::new(Mutex::new(pl));

    let thread_orders = ThreadLocal::new();

    let num_threads = num_cpus::get();

    if start_nodes.is_empty() {
        log::info!("No initial starting nodes; starting from arbitrary nodes...");
    } else {
        log::info!(
            "Traversing from {} given initial nodes...",
            start_nodes.len()
        );
        let visited_initial_nodes = AtomicUsize::new(0);
        start_nodes.into_par_iter().for_each_init(
            || {
                thread_orders
                    .get_or(|| RefCell::new(Vec::with_capacity(num_nodes / num_threads)))
                    .borrow_mut()
            },
            |thread_order, &root_node| {
                if let Some(visited_nodes) = visit_from_root_node(thread_order, root_node) {
                    pl.lock().unwrap().update_with_count(visited_nodes);
                }
                let i = visited_initial_nodes.fetch_add(1, Ordering::Relaxed);
                if start_nodes.len() > 100 && i % (start_nodes.len() / 100) == 0 {
                    log::info!(
                        "Finished traversals from {}% of initial nodes.",
                        i * 100 / start_nodes.len()
                    );
                }
            },
        );
        log::info!("Done traversing from given initial nodes.");
        log::info!("Traversing from arbitrary nodes...");
    }

    crate::utils::shuffle::par_iter_shuffled_range(0..num_nodes).for_each_init(
        || {
            thread_orders
                .get_or(|| RefCell::new(Vec::with_capacity(num_nodes / num_threads)))
                .borrow_mut()
        },
        |thread_order, root_node| {
            if let Some(visited_nodes) = visit_from_root_node(thread_order, root_node) {
                pl.lock().unwrap().update_with_count(visited_nodes);
            }
        },
    );

    pl.lock().unwrap().done();

    let mut pl = progress_logger!(
        display_memory = true,
        item_name = "node",
        local_speed = true,
        expected_updates = Some(num_nodes),
    );
    pl.start("[step 2/2] Concatenating orders...");

    // "Concatenate" orders from each thread.
    let mut order = vec![NodeId::MAX; num_nodes];
    let mut i = 0;
    for thread_order in thread_orders.into_iter() {
        for node in thread_order.into_inner().into_iter() {
            if order[node] == NodeId::MAX {
                pl.light_update();
                order[node] = i;
                i += 1
            }
        }
    }

    assert_eq!(
        i, num_nodes,
        "graph has {num_nodes} nodes, permutation has {i}"
    );

    pl.done();
    OwnedPermutation::new(order).unwrap()
}
