// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

/// A parallel almost-BFS traversal
///
/// This implements a graph traversal that is like a BFS from many sources, but traversal
/// from a source may steal a node from another traversal
use std::cell::RefCell;
use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use dsi_progress_logger::ProgressLogger;
use num_cpus;
use rayon::prelude::*;
use sux::prelude::AtomicBitVec;
use thread_local::ThreadLocal;
use webgraph::prelude::*;

use crate::map::OwnedPermutation;

pub fn almost_bfs_order<G: RandomAccessGraph + Send + Sync>(
    graph: &G,
) -> OwnedPermutation<Vec<usize>> {
    let num_nodes = graph.num_nodes();

    let visited = AtomicBitVec::new(num_nodes);

    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "node";
    pl.local_speed = true;
    pl.expected_updates = Some(num_nodes);
    pl.start("[step 1/2] Visiting graph in pseudo-BFS order...");
    let pl = Arc::new(Mutex::new(pl));

    let thread_orders = ThreadLocal::new();

    let num_threads = num_cpus::get();

    crate::utils::shuffle::par_iter_shuffled_range(0..num_nodes).for_each_init(
        || {
            thread_orders
                .get_or(|| RefCell::new(Vec::with_capacity(num_nodes / num_threads)))
                .borrow_mut()
        },
        |thread_order, root_node| {
            if visited.get(root_node, Ordering::Relaxed) {
                // Skip VecDeque allocation
                return;
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

            pl.lock().unwrap().update_with_count(visited_nodes);
        },
    );

    pl.lock().unwrap().done();

    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "node";
    pl.local_speed = true;
    pl.expected_updates = Some(num_nodes);
    pl.start("[step 2/2] Concatenating orders...");

    // "Concatenate" orders from each thread.
    let mut order = vec![usize::MAX; num_nodes];
    let mut i = 0;
    for thread_order in thread_orders.into_iter() {
        for node in thread_order.into_inner().into_iter() {
            if order[node] == usize::MAX {
                pl.light_update();
                order[node] = i;
                i += 1
            }
        }
    }

    assert_eq!(
        i, num_nodes,
        "graph has {} nodes, permutation has {}",
        num_nodes, i
    );

    pl.done();
    OwnedPermutation::new(order).unwrap()
}
