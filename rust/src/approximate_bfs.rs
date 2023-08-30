// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

/// A parallel almost-BFS traversal
///
/// This implements a graph traversal that is like a BFS, but with a small likelihood
/// of duplicating nodes in order to allow efficient parallelization
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use dsi_progress_logger::ProgressLogger;
use num_cpus;
use webgraph::prelude::*;

use crate::permutation::OwnedPermutation;

pub fn almost_bfs_order<'a, G: RandomAccessGraph + Send + Sync>(graph: &'a G) -> OwnedPermutation {
    let num_nodes = graph.num_nodes();

    println!("Allocating array");
    // Non-atomic booleans, which mean we may visit some nodes twice.
    let visited = Mutex::new(vec![false; num_nodes]);

    let mut pl = ProgressLogger::default().display_memory();
    pl.item_name = "node";
    pl.local_speed = true;
    pl.expected_updates = Some(num_nodes);
    pl.start("Visiting graph in pseudo-BFS order...");
    let pl = Arc::new(Mutex::new(pl));

    let num_threads = num_cpus::get();
    let next_start = Mutex::new(0usize);

    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        for _ in 0..num_threads {
            handles.push(scope.spawn(|| {
                let visited_ptr = visited.lock().unwrap().as_mut_ptr();
                let mut thread_queue = VecDeque::new();
                let mut queued_updates = 0;
                let mut thread_order = Vec::with_capacity(num_nodes / num_threads);
                loop {
                    // Get the next node from the thread queue.
                    // If the thread queue is empty, get the next start.
                    // If there is none, return
                    let current_node = thread_queue.pop_front().or_else(|| {
                        let mut next_start_ref = next_start.lock().unwrap();

                        while unsafe { *visited_ptr.offset(*next_start_ref as isize) } {
                            if *next_start_ref + 1 >= graph.num_nodes() {
                                pl.lock().unwrap().update_with_count(queued_updates);
                                return None;
                            }
                            *next_start_ref += 1;
                        }
                        unsafe { *visited_ptr.offset(*next_start_ref as isize) = true };

                        Some(*next_start_ref)
                    });
                    let Some(current_node) = current_node else {
                        return thread_order;
                    };

                    thread_order.push(current_node);

                    for succ in graph.successors(current_node) {
                        if unsafe { !*visited_ptr.offset(succ as isize) } {
                            thread_queue.push_back(succ);
                            unsafe { *visited_ptr.offset(succ as isize) = true };
                        }
                    }

                    queued_updates += 1;
                    if queued_updates >= 10000 {
                        pl.lock().unwrap().update_with_count(queued_updates);
                        queued_updates = 0;
                    }
                }
            }));
        }

        // "Concatenate" orders from each thread.
        let mut order = vec![usize::MAX; num_nodes];
        let mut i = 0;
        for handle in handles {
            let thread_order: Vec<usize> = handle.join().expect("Error in BFS thread");
            for node in thread_order.into_iter() {
                if order[node] == usize::MAX {
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

        pl.lock().unwrap().done();

        OwnedPermutation::new(order).unwrap()
    })
}
