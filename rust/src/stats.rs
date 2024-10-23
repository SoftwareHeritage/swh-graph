/*
 * Copyright (C) 2024  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

//! Computes statistics on the graph

use rayon::prelude::*;

use crate::graph::*;

pub struct DegreeStats {
    pub min: u64,
    pub max: u64,
    pub avg: f64,
}

pub fn outdegree<G: SwhForwardGraph + Sync>(graph: &G) -> DegreeStats {
    let identity = (u64::MAX, u64::MIN, 0u64);
    let (min, max, total) = crate::utils::shuffle::par_iter_shuffled_range(0..graph.num_nodes())
        .into_par_iter()
        // Compute stats in parallel
        .fold(
            || identity,
            |(min, max, total), node: usize| {
                let outdegree =
                    u64::try_from(graph.outdegree(node)).expect("outdegree overflowed u64");
                (
                    std::cmp::min(min, outdegree),
                    std::cmp::max(max, outdegree),
                    total.saturating_add(outdegree),
                )
            },
        )
        // Merge each thread's work
        .reduce(
            || identity,
            |(min1, max1, total1), (min2, max2, total2)| {
                (
                    std::cmp::min(min1, min2),
                    std::cmp::max(max1, max2),
                    total1.saturating_add(total2),
                )
            },
        );
    DegreeStats {
        min,
        max,
        avg: (total as f64) / (graph.num_nodes() as f64),
    }
}
