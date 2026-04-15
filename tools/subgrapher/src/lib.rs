// Copyright (C) 2025-2026  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use swh_graph::SWHID;

use log::{debug, error, warn};

use anyhow::{Context, Result};
use dashmap::DashSet;
use dsi_progress_logger::ProgressLog;
use rayon::prelude::*;

use swh_graph::graph::SwhForwardGraph;
use swh_graph::graph::SwhGraphWithProperties;
use swh_graph::properties;

pub fn process_origins_and_build_subgraph<G, I>(
    graph: &G,
    origins: I,
    allow_protocol_variations: bool,
) -> Result<(DashSet<usize>, DashSet<String>)>
where
    G: SwhGraphWithProperties<Maps: properties::Maps> + SwhForwardGraph + Sync + Send,
    I: Iterator<Item = Result<String, std::io::Error>> + ParallelBridge + Send,
{
    let num_nodes = graph.num_nodes();

    let visited = DashSet::new();
    let unknown_origins = DashSet::new();

    #[cfg(miri)]
    let node_pl = dsi_progress_logger::no_logging!();
    #[cfg(not(miri))] // uses sysinfo which is not supported by Miri
    let mut node_pl = dsi_progress_logger::concurrent_progress_logger!(
        display_memory = true,
        item_name = "node",
        local_speed = true,
        expected_updates = Some(num_nodes),
    );
    node_pl.start("visiting graph ...");

    #[cfg(miri)]
    let origin_pl = dsi_progress_logger::no_logging!();
    #[cfg(not(miri))] // uses sysinfo which is not supported by Miri
    let mut origin_pl = dsi_progress_logger::concurrent_progress_logger!(
        display_memory = true,
        item_name = "origin",
        local_speed = true,
    );
    origin_pl.start("visiting graph ...");

    origins.par_bridge().try_for_each_with(
        (node_pl.clone(), origin_pl.clone()),
        |(node_pl, origin_pl), origin_result| -> Result<()> {
            origin_pl.light_update();
            let origin = origin_result.context("Could not decode input line")?;
            let mut origin_swhid = SWHID::from_origin_url(&origin);

            // Lookup SWHID
            debug!("looking up SWHID {} ...", origin);
            let mut node_id_lookup = graph.properties().node_id(origin_swhid);

            if node_id_lookup.is_err() && allow_protocol_variations {
                warn!("origin {origin} not in graph. Will look for other protocols");
                // try with other protocols
                if origin.contains("git://") || origin.contains("https://") {
                    // try to switch the protocol. Only https and git available
                    let alternative_origin = if origin.contains("git://") {
                        origin.replace("git://", "https://")
                    } else if origin.contains("https://") {
                        origin.replace("https://", "git://")
                    } else {
                        origin.to_owned()
                    };

                    origin_swhid = SWHID::from_origin_url(alternative_origin);

                    node_id_lookup = graph.properties().node_id(origin_swhid);
                    if node_id_lookup.is_ok() {
                        debug!("origin found with different protocol: {origin}");
                    }
                }
            }

            // if node_id is still err, attempts to switch protocols failed
            // the original url from the origins file should be logged
            let Ok(node_id) = node_id_lookup else {
                error!("origin {origin} not in graph");
                unknown_origins.insert(origin);
                return Ok(());
            };
            debug!("obtained node ID {node_id} ...");
            assert!(node_id < num_nodes);

            let mut todo = vec![node_id];

            debug!("starting bfs for the origin: {origin}");

            // iterative BFS
            while let Some(current_node) = todo.pop() {
                let new = visited.insert(current_node);
                if new {
                    node_pl.light_update();
                    for succ in graph.successors(current_node) {
                        todo.push(succ);
                    }
                }
            }

            Ok(())
        },
    )?;
    origin_pl.done();
    node_pl.done();

    Ok((visited, unknown_origins))
}
