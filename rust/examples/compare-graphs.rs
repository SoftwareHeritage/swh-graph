/*
 * Copyright (C) 2023-2024  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

use std::collections::HashSet;
use std::path::PathBuf;

use anyhow::{bail, ensure, Context, Result};
use clap::Parser;
use dsi_progress_logger::{concurrent_progress_logger, ProgressLog};
use rayon::prelude::*;

use swh_graph::graph::*;
use swh_graph::mph::DynMphf;
use swh_graph::utils::shuffle::par_iter_shuffled_range;
use swh_graph::views::Transposed;
use swh_graph::NodeType;

#[derive(Parser, Debug)]
/// Reads two graphs and check they have the same labels and properties
struct Args {
    graph1: PathBuf,
    graph2: PathBuf,
    #[arg(long)]
    /// Assume without checking SWHIDs are the same in both graphs
    skip_swhids: bool,
    #[arg(long)]
    /// Assume without checking successors are the same in both graphs
    skip_successors: bool,
    #[arg(long)]
    /// Do not check arc labels are the same in both graphs
    skip_arc_labels: bool,
    #[arg(long)]
    /// Do not check node properties are the same in both graphs
    skip_properties: bool,
    #[arg(long)]
    /// Needed when comparing graphs with origin->snapshot labels *and* generated with Java
    /// to compensate for the Java implementation dropping all but one labels when there are
    /// multiple visits of the same origin with the same timestamp at the same timestamp.
    allow_missing_duplicate_origin_arc_labels: bool,
    #[arg(long)]
    /// Check backwards graphs instead of forwards graphs
    transposed: bool,
}

pub fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();

    let graph1 = SwhBidirectionalGraph::new(&args.graph1)
        .with_context(|| format!("Could not load graph {}", args.graph1.display()))?
        .load_all_properties::<DynMphf>()
        .with_context(|| {
            format!(
                "Could not load properties from graph {}",
                args.graph1.display()
            )
        })?
        .load_labels()
        .with_context(|| format!("Could not load labels from graph {}", args.graph1.display()))?;

    let graph2 = SwhBidirectionalGraph::new(&args.graph2)
        .with_context(|| format!("Could not load graph {}", args.graph2.display()))?
        .load_all_properties::<DynMphf>()
        .with_context(|| {
            format!(
                "Could not load properties from graph {}",
                args.graph2.display()
            )
        })?
        .load_labels()
        .with_context(|| format!("Could not load labels from graph {}", args.graph2.display()))?;

    ensure!(
        graph1.num_nodes() == graph2.num_nodes(),
        "{} has {} nodes, {} has {} nodes",
        args.graph1.display(),
        graph1.num_nodes(),
        args.graph2.display(),
        graph2.num_nodes()
    );

    if args.transposed {
        let graph1 = Transposed(graph1);
        let graph2 = Transposed(graph2);

        if !args.skip_swhids {
            check_swhids(&graph1, &graph2)?;
        }

        if !args.skip_successors {
            check_successors(&graph1, &graph2)?;
        }

        if !args.skip_arc_labels {
            check_labels(
                &graph1,
                &graph2,
                args.allow_missing_duplicate_origin_arc_labels,
            )?;
        }

        if !args.skip_properties {
            check_properties(&graph1, &graph2)?;
        }
    } else {
        if !args.skip_swhids {
            check_swhids(&graph1, &graph2)?;
        }

        if !args.skip_successors {
            check_successors(&graph1, &graph2)?;
        }

        if !args.skip_arc_labels {
            check_labels(
                &graph1,
                &graph2,
                args.allow_missing_duplicate_origin_arc_labels,
            )?;
        }

        if !args.skip_properties {
            check_properties(&graph1, &graph2)?;
        }
    }

    // TODO: check label names

    Ok(())
}

fn check_swhids<G>(graph1: &G, graph2: &G) -> Result<()>
where
    G: SwhGraph + SwhGraphWithProperties + Sync,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let num_nodes = graph2.num_nodes();
    let mut pl = concurrent_progress_logger!(
        display_memory = true,
        local_speed = true,
        item_name = "node",
        expected_updates = Some(num_nodes),
    );
    pl.start("Checking SWHIDs...");
    (0..num_nodes).into_par_iter().try_for_each_with(
        pl.clone(),
        |thread_pl, node| -> Result<()> {
            if graph1.properties().swhid(node) != graph1.properties().swhid(node) {
                log::error!(
                    "node {} is {} in {} and {} in {}",
                    node,
                    graph1.properties().swhid(node),
                    graph1.path().display(),
                    graph2.properties().swhid(node),
                    graph2.path().display()
                );
                bail!("SWHID mismatch")
            }
            thread_pl.light_update();
            Ok(())
        },
    )?;
    pl.done();

    Ok(())
}

fn check_successors<G>(graph1: &G, graph2: &G) -> Result<()>
where
    G: SwhGraph + SwhForwardGraph + SwhGraphWithProperties + Sync,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let num_nodes = graph2.num_nodes();
    let mut pl = concurrent_progress_logger!(
        display_memory = true,
        local_speed = true,
        item_name = "node",
        expected_updates = Some(num_nodes),
    );
    pl.start("Checking successors...");
    par_iter_shuffled_range(0..num_nodes).try_for_each_with(
        pl.clone(),
        |thread_pl, node| -> Result<()> {
            let successors1: Vec<_> = graph1.successors(node).into_iter().collect();
            let successors2: Vec<_> = graph2.successors(node).into_iter().collect();
            if successors1 != successors2 {
                log::error!(
                    "node {} has successors {} in {} and {} in {}",
                    node,
                    successors1
                        .into_iter()
                        .map(|succ| graph1.properties().swhid(succ).to_string())
                        .collect::<Vec<_>>()
                        .join(", "),
                    graph1.path().display(),
                    successors2
                        .into_iter()
                        .map(|succ| graph2.properties().swhid(succ).to_string())
                        .collect::<Vec<_>>()
                        .join(", "),
                    graph2.path().display()
                );
                bail!("Successors mismatch")
            }
            thread_pl.light_update();
            Ok(())
        },
    )?;
    pl.done();

    Ok(())
}

fn check_labels<G>(
    graph1: &G,
    graph2: &G,
    allow_missing_duplicate_origin_arc_labels: bool,
) -> Result<()>
where
    G: SwhGraph + SwhLabeledForwardGraph + SwhGraphWithProperties + Sync,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    assert_eq!(graph1.is_transposed(), graph2.is_transposed());
    let num_nodes = graph2.num_nodes();
    let mut pl = concurrent_progress_logger!(
        display_memory = true,
        local_speed = true,
        item_name = "node",
        expected_updates = Some(num_nodes),
    );
    pl.start("Checking arc labels...");
    par_iter_shuffled_range(0..num_nodes).try_for_each_with(
        pl.clone(),
        |thread_pl, node| -> Result<()> {
        for ((succ1, labels1), (succ2, labels2)) in graph1
            .labeled_successors(node)
            .zip(graph2.labeled_successors(node))
        {
            ensure!(succ1 == succ2, "Successors mismatch");
            let labels1: Vec<_> = labels1.collect();
            let labels2: Vec<_> = labels2.collect();
            if labels1 != labels2 {
                let is_ori_to_snp_arc = if graph1.is_transposed() {
                    graph1.properties().node_type(succ1) == NodeType::Origin
                } else {
                    graph1.properties().node_type(node) == NodeType::Origin
                };
                if allow_missing_duplicate_origin_arc_labels && is_ori_to_snp_arc && labels1.iter().copied().collect::<HashSet<_>>() == labels2.iter().copied().collect::<HashSet<_>>() {
                    log::warn!(
                        "arc {} ({}) -> {} ({}) has same set of labels, but different counts ({} in {} and {} in {})",
                        graph1.properties().swhid(node),
                        node,
                        graph1.properties().swhid(succ1),
                        succ1,
                        labels1.len(),
                        graph1.path().display(),
                        labels2.len(),
                        graph2.path().display()
                    );
                } else {
                    log::error!(
                        "arc {} ({}) -> {} ({}) has labels {:?} in {} and {:?} in {}",
                        graph1.properties().swhid(node),
                        node,
                        graph1.properties().swhid(succ1),
                        succ1,
                        labels1,
                        graph1.path().display(),
                        labels2,
                        graph2.path().display(),
                    );
                    bail!("Labels mismatch")
                }
            }
        }
        thread_pl.light_update();
        Ok(())
    })?;
    pl.done();
    Ok(())
}

fn check_properties<G>(graph1: &G, graph2: &G) -> Result<()>
where
    G: SwhGraph + SwhForwardGraph + SwhGraphWithProperties + Sync,
    <G as SwhGraphWithProperties>::Contents: swh_graph::properties::Contents,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
    <G as SwhGraphWithProperties>::Persons: swh_graph::properties::Persons,
    <G as SwhGraphWithProperties>::Strings: swh_graph::properties::Strings,
    <G as SwhGraphWithProperties>::Timestamps: swh_graph::properties::Timestamps,
{
    let num_nodes = graph1.num_nodes();

    macro_rules! check_property {
        ($property:ident) => {{
            let mut pl = concurrent_progress_logger!(
                display_memory = true,
                local_speed = true,
                item_name = "node",
                expected_updates = Some(num_nodes),
            );
            pl.start(&format!("Checking {}...", stringify!($property)));
            (0..num_nodes).into_par_iter().try_for_each_with(
                pl.clone(),
                |thread_pl, node| -> Result<()> {
                    if graph1.properties().$property(node) != graph2.properties().$property(node) {
                        log::error!(
                            "node {} ({}) has {} {:?} in {} and {:?} in {}",
                            node,
                            graph1.properties().swhid(node),
                            stringify!($property),
                            graph1.properties().$property(node),
                            graph1.path().display(),
                            graph2.properties().$property(node),
                            graph2.path().display()
                        );
                    }
                    thread_pl.light_update();
                    Ok(())
                },
            )?;

            pl.done();
        }};
    }

    check_property!(author_timestamp);
    check_property!(author_timestamp_offset);
    check_property!(committer_timestamp);
    check_property!(committer_timestamp_offset);
    check_property!(author_id);
    check_property!(committer_id);
    check_property!(content_length);
    check_property!(is_skipped_content);
    check_property!(message);
    check_property!(tag_name);

    Ok(())
}
