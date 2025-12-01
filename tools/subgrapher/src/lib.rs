// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use swh_graph::SWHID;

use std::collections::{HashSet, VecDeque};

use anyhow::{Context, Result};
use dsi_progress_logger::ProgressLog;
use log::{debug, error, info, warn, Level};

use swh_graph::graph::SwhForwardGraph;
use swh_graph::graph::SwhGraphWithProperties;
use swh_graph::properties;

pub fn process_origins_and_build_subgraph<G, I>(
    graph: &G,
    origins: I,
    allow_protocol_variations: bool,
) -> Result<(HashSet<usize>, Vec<String>)>
where
    G: SwhGraphWithProperties + SwhForwardGraph,
    G::Maps: properties::Maps,
    I: Iterator<Item = Result<String, std::io::Error>>,
{
    let graph_props = graph.properties();
    let num_nodes = graph.num_nodes();

    let mut visited = HashSet::new();
    let mut unknown_origins = vec![];

    #[cfg(miri)]
    let pl = dsi_progress_logger::no_logging!();
    #[cfg(not(miri))] // uses sysinfo which is not supported by Miri
    let mut pl = dsi_progress_logger::progress_logger!(
        display_memory = true,
        item_name = "node",
        local_speed = true,
        expected_updates = Some(num_nodes),
    );
    pl.start("visiting graph ...");

    for origin_result in origins {
        let origin = origin_result.context("Could not decode input line")?;
        let mut origin_swhid = SWHID::from_origin_url(&origin);

        // Lookup SWHID
        info!("looking up SWHID {} ...", origin);
        let mut node_id_lookup = graph_props.node_id(origin_swhid);

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

                node_id_lookup = graph_props.node_id(origin_swhid);
                if node_id_lookup.is_ok() {
                    debug!("origin found with different protocol: {origin}");
                }
            }
        }

        // if node_id is still err, attempts to switch protocols failed
        // the original url from the origins file should be logged
        let Ok(node_id) = node_id_lookup else {
            error!("origin {origin} not in graph");
            unknown_origins.push(origin);
            continue;
        };
        debug!("obtained node ID {node_id} ...");
        assert!(node_id < num_nodes);

        // Setup a queue
        let mut queue: VecDeque<usize> = VecDeque::new();

        queue.push_back(node_id);

        // Setup the progress logger for
        let mut visited_nodes = 0;

        debug!("starting bfs for the origin: {origin}");

        // iterative BFS
        while let Some(current_node) = queue.pop_front() {
            if log::log_enabled!(Level::Debug) {
                let id = graph.properties().swhid(current_node);
                debug!("visited: {id}");
            } // add current_node to the external results hashset
            let new = visited.insert(current_node);
            //  only visit children if this node is new
            if new {
                visited_nodes += 1;
                for succ in graph.successors(current_node) {
                    queue.push_back(succ);
                    pl.light_update();
                }
            } else {
                debug!(
                    "stopping bfs because this node was foud in a previous bfs run (from another origin) {current_node}"
                );
            }
        }

        pl.update_and_display();
        info!("visit from {origin} completed after visiting {visited_nodes} nodes.");
    }
    pl.done();

    Ok((visited, unknown_origins))
}
