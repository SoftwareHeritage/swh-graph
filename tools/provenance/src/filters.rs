// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use swh_graph::graph::*;
use swh_graph::SWHType;

/// Returns whether the node is a release or a revision with a release/snapshot predecessor
///
/// # Example
///
/// ```
/// use swh_graph::graph_builder::GraphBuilder;
/// use swh_graph::swhid;
///
/// use swh_graph_provenance::filters::is_head;
/// let mut builder = GraphBuilder::default();
/// let rev0 = builder
///     .node(swhid!(swh:1:rev:0000000000000000000000000000000000000000))
///     .unwrap()
///     .done();
/// let rev1 = builder
///     .node(swhid!(swh:1:rev:0000000000000000000000000000000000000001))
///     .unwrap()
///     .done();
/// let rev2 = builder
///     .node(swhid!(swh:1:rev:0000000000000000000000000000000000000002))
///     .unwrap()
///     .done();
/// let rev3 = builder
///     .node(swhid!(swh:1:rev:0000000000000000000000000000000000000003))
///     .unwrap()
///     .done();
/// let rel4 = builder
///     .node(swhid!(swh:1:rel:0000000000000000000000000000000000000004))
///     .unwrap()
///     .done();
/// let rel5 = builder
///     .node(swhid!(swh:1:rel:0000000000000000000000000000000000000005))
///     .unwrap()
///     .done();
/// let snp6 = builder
///     .node(swhid!(swh:1:snp:0000000000000000000000000000000000000006))
///     .unwrap()
///     .done();
///
/// /*
///  *  snp6 -> rev1 -> rev0
///  *    \
///  *     +--> rel5
///  *
///  *  rel4 -> rev3 -> rev2
///  */
///
/// builder.arc(snp6, rev1, None); // snp6 has rev1 as branch tip
/// builder.arc(rev1, rev0, None); // rev0 is parent of rev1
/// builder.arc(snp6, rel5, None); // snp6 has rel5 as branch tip
///
/// builder.arc(rel4, rev3, None); // rel4 points to rev3
/// builder.arc(rev3, rev2, None); // rev2 is parent of rev3
///
/// let graph = builder.done().unwrap();
///
/// assert!(!is_head(&graph, rev0), "rev0 should not be a head revision");
/// assert!(is_head(&graph, rev1), "rev1 should be a head revision (pointed by a snapshot)");
/// assert!(!is_head(&graph, rev2), "rev2 should not be a head revision");
/// assert!(is_head(&graph, rev3), "rev3 should be a head revision (pointed by a release)");
/// assert!(is_head(&graph, rel4), "rev4 should be a head (it's a release)");
/// assert!(is_head(&graph, rel5), "rel5 should be a head (it's a release)");
/// ```
pub fn is_head<G>(graph: &G, node_id: NodeId) -> bool
where
    G: SwhBackwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::Maps: swh_graph::properties::Maps,
{
    let node_type = graph.properties().node_type(node_id);

    match node_type {
        SWHType::Release => true,
        SWHType::Revision => graph.predecessors(node_id).into_iter().any(|pred| {
            let pred_type = graph.properties().node_type(pred);
            pred_type == SWHType::Snapshot || pred_type == SWHType::Release
        }),
        SWHType::Origin | SWHType::Snapshot | SWHType::Directory | SWHType::Content => false,
    }
}
