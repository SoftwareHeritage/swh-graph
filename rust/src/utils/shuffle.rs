// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::ops::{Add, Range};

use rand::prelude::*;
use rayon::prelude::*;

/// Returns a parallel iterator on the range where items are pseudo-shuffled
///
/// When running an operation on all nodes of the graph, the naive approach is
/// to use `(0..graph.num_nodes()).into_par_iter().map(f)`.
///
/// However, nodes are not uniformly distributed over the range of ids, meaning that
/// some sections of the range of ids can be harder for `f` than others.
/// This can be an issue when some instances are harder than others, or simply because
/// it makes ETAs unreliable.
///
/// Instead, this function returns the same set of nodes as the naive approach would,
/// but in a somewhat-random order.
///
/// # Example
///
/// ```
/// # #[cfg(not(miri))] // very slow to run on Miri
/// # {
/// use rayon::prelude::*;
/// use swh_graph::utils::shuffle::par_iter_shuffled_range;
///
/// let mut integers: Vec<usize> = par_iter_shuffled_range(0..10000).collect();
/// assert_ne!(integers, (0..10000).collect::<Vec<_>>(), "integers are still sorted");
/// integers.sort();
/// assert_eq!(integers, (0..10000).collect::<Vec<_>>(), "integers do not match the input range");
/// # }
/// ```
pub fn par_iter_shuffled_range<Item>(
    range: Range<Item>,
) -> impl ParallelIterator<Item = <Range<Item> as IntoParallelIterator>::Item>
where
    Range<Item>: ExactSizeIterator,
    usize: Add<Item, Output = Item>,
    Item: Ord + Sync + Send + Copy,
    std::ops::Range<Item>: rayon::iter::IntoParallelIterator,
{
    let num_chunks = 100_000; // Arbitrary value
    let chunk_size = range.len().div_ceil(num_chunks);
    let mut chunks: Vec<usize> = (0..num_chunks).collect();

    chunks.shuffle(&mut rand::thread_rng());

    chunks
        .into_par_iter()
        // Lazily rebuild the list of nodes from the shuffled chunks
        .flat_map(move |chunk_id| {
            ((chunk_id * chunk_size) + range.start)
                ..Item::min(
                    (chunk_id.checked_add(1).unwrap()) * chunk_size + range.start,
                    range.end,
                )
        })
}
