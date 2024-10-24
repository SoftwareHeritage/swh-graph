// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use std::sync::Arc;

use anyhow::{Context, Result};
use futures::stream::StreamExt;
use prost_types::FieldMask;
use tonic::Request;

use swh_graph::graph_builder::GraphBuilder;
use swh_graph::swhid;
use swh_graph::views::GraphSpy;
use swh_graph_grpc_server::proto::traversal_service_server::TraversalService as _TraversalService;
use swh_graph_grpc_server::proto::*;
use swh_graph_grpc_server::TraversalService;

#[tokio::test(flavor = "multi_thread")]
/// Counting the number of nodes reachable from another one
async fn test_count_nodes() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let a = builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000010))?
        .done();
    let b = builder
        .node(swhid!(swh:1:rel:0000000000000000000000000000000000000020))?
        .done();
    let c = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000030))?
        .done();
    builder.arc(a, b);
    builder.arc(a, c);
    builder.arc(b, c);
    let graph = builder.done().context("Could not make graph")?;

    let spy_graph = Arc::new(GraphSpy::new(graph));

    let traversal_service = TraversalService::new(spy_graph.clone(), None);

    let res = traversal_service
        .count_nodes(Request::new(TraversalRequest {
            src: vec!["swh:1:snp:0000000000000000000000000000000000000010".into()],
            direction: GraphDirection::Forward.into(),
            ..Default::default()
        }))
        .await
        .context("Request failed")?;

    assert_eq!(
        *spy_graph.history.lock().unwrap(),
        vec![
            ("properties", "()".into()), // swhid -> id
            ("has_node", "(0,)".into()), // check not masked
            ("successors", "(0,)".into()),
            ("successors", "(1,)".into()),
            ("successors", "(2,)".into())
        ]
    );
    spy_graph.history.lock().unwrap().clear();

    assert_eq!(res.into_inner(), CountResponse { count: 3 });

    let res = traversal_service
        .count_nodes(Request::new(TraversalRequest {
            src: vec!["swh:1:rel:0000000000000000000000000000000000000020".into()],
            direction: GraphDirection::Forward.into(),
            ..Default::default()
        }))
        .await
        .context("Request failed")?;

    assert_eq!(
        *spy_graph.history.lock().unwrap(),
        vec![
            ("properties", "()".into()), // swhid -> id
            ("has_node", "(1,)".into()), // check not masked
            ("successors", "(1,)".into()),
            ("successors", "(2,)".into())
        ]
    );

    assert_eq!(res.into_inner(), CountResponse { count: 2 });

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
/// Counting the number of nodes reachable with a path of length exactly 1
async fn test_count_neighbors() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let a = builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000010))?
        .done();
    let b = builder
        .node(swhid!(swh:1:rel:0000000000000000000000000000000000000020))?
        .done();
    let c = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000030))?
        .done();
    builder.arc(a, b);
    builder.arc(b, c);
    let graph = builder.done().context("Could not make graph")?;

    let spy_graph = Arc::new(GraphSpy::new(graph));

    let traversal_service = TraversalService::new(spy_graph.clone(), None);

    let res = traversal_service
        .count_nodes(Request::new(TraversalRequest {
            src: vec!["swh:1:snp:0000000000000000000000000000000000000010".into()],
            direction: GraphDirection::Forward.into(),
            min_depth: Some(1),
            max_depth: Some(1),
            ..Default::default()
        }))
        .await
        .context("Request failed")?;

    assert_eq!(
        *spy_graph.history.lock().unwrap(),
        vec![
            ("properties", "()".into()), // swhid -> id
            ("has_node", "(0,)".into()), // check not masked
            ("successors", "(0,)".into())
        ]
    );

    assert_eq!(res.into_inner(), CountResponse { count: 1 });

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
/// Counting the number of nodes reachable with a path of length exactly 1,
/// and a maximum number of matches
async fn test_count_neighbors_max() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let a = builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000010))?
        .done();
    let b = builder
        .node(swhid!(swh:1:rel:0000000000000000000000000000000000000020))?
        .done();
    let c = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000030))?
        .done();
    let d = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000040))?
        .done();
    let e = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000050))?
        .done();
    builder.arc(a, b);
    builder.arc(a, c);
    builder.arc(a, d);
    builder.arc(a, e);
    let graph = builder.done().context("Could not make graph")?;

    let spy_graph = Arc::new(GraphSpy::new(graph));

    let traversal_service = TraversalService::new(spy_graph.clone(), None);

    let res = traversal_service
        .count_nodes(Request::new(TraversalRequest {
            src: vec!["swh:1:snp:0000000000000000000000000000000000000010".into()],
            direction: GraphDirection::Forward.into(),
            min_depth: Some(1),
            max_depth: Some(1),
            max_matching_nodes: Some(2),
            ..Default::default()
        }))
        .await
        .context("Request failed")?;

    assert_eq!(
        *spy_graph.history.lock().unwrap(),
        vec![
            ("properties", "()".into()), // swhid -> id
            ("has_node", "(0,)".into()), // check not masked
            ("successors", "(0,)".into()),
        ]
    );

    assert_eq!(res.into_inner(), CountResponse { count: 2 });

    Ok(())
}

#[tokio::test]
/// List all nodes reachable with a path of length exactly 1,
/// and a maximum number of matches
async fn test_neighbors() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let a = builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000010))?
        .done();
    let b = builder
        .node(swhid!(swh:1:rel:0000000000000000000000000000000000000020))?
        .done();
    let c = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000030))?
        .done();
    let d = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000040))?
        .done();
    builder.arc(a, b);
    builder.arc(a, c);
    builder.arc(a, d);
    let graph = builder.done().context("Could not make graph")?;

    let spy_graph = Arc::new(GraphSpy::new(graph));

    let traversal_service = TraversalService::new(spy_graph.clone(), None);

    let res = traversal_service
        .traverse(Request::new(TraversalRequest {
            src: vec!["swh:1:snp:0000000000000000000000000000000000000010".into()],
            direction: GraphDirection::Forward.into(),
            min_depth: Some(1),
            max_depth: Some(1),
            mask: Some(FieldMask {
                paths: vec!["swhid".into()],
            }),
            ..Default::default()
        }))
        .await
        .context("Request failed")?;

    let mut nodes = vec![];
    let mut stream = res.into_inner();
    while let Some(res) = stream.next().await {
        nodes.push(res?);
    }

    assert_eq!(
        *spy_graph.history.lock().unwrap(),
        vec![
            ("properties", "()".into()), // swhid -> id
            ("has_node", "(0,)".into()), // check not masked
            ("successors", "(0,)".into()),
            ("properties", "()".into()), // id -> swhids
            ("properties", "()".into()), // id -> swhids
            ("properties", "()".into()), // id -> swhids
        ]
    );

    assert_eq!(
        nodes,
        vec![
            Node {
                swhid: "swh:1:rel:0000000000000000000000000000000000000020".into(),
                ..Default::default()
            },
            Node {
                swhid: "swh:1:rev:0000000000000000000000000000000000000030".into(),
                ..Default::default()
            },
            Node {
                swhid: "swh:1:rev:0000000000000000000000000000000000000040".into(),
                ..Default::default()
            }
        ]
    );

    Ok(())
}

#[tokio::test]
/// List all nodes reachable with a path of length exactly 1,
/// and a maximum number of matches
async fn test_neighbors_max() -> Result<()> {
    let mut builder = GraphBuilder::default();
    let a = builder
        .node(swhid!(swh:1:snp:0000000000000000000000000000000000000010))?
        .done();
    let b = builder
        .node(swhid!(swh:1:rel:0000000000000000000000000000000000000020))?
        .done();
    let c = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000030))?
        .done();
    let d = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000040))?
        .done();
    let e = builder
        .node(swhid!(swh:1:rev:0000000000000000000000000000000000000050))?
        .done();
    builder.arc(a, b);
    builder.arc(a, c);
    builder.arc(a, d);
    builder.arc(a, e);
    let graph = builder.done().context("Could not make graph")?;

    let spy_graph = Arc::new(GraphSpy::new(graph));

    let traversal_service = TraversalService::new(spy_graph.clone(), None);

    let res = traversal_service
        .traverse(Request::new(TraversalRequest {
            src: vec!["swh:1:snp:0000000000000000000000000000000000000010".into()],
            direction: GraphDirection::Forward.into(),
            min_depth: Some(1),
            max_depth: Some(1),
            max_matching_nodes: Some(2),
            mask: Some(FieldMask {
                paths: vec!["swhid".into()],
            }),
            ..Default::default()
        }))
        .await
        .context("Request failed")?;

    let mut nodes = vec![];
    let mut stream = res.into_inner();
    while let Some(res) = stream.next().await {
        nodes.push(res?);
    }

    assert_eq!(
        *spy_graph.history.lock().unwrap(),
        vec![
            ("properties", "()".into()), // swhid -> id
            ("has_node", "(0,)".into()), // check not masked
            ("successors", "(0,)".into()),
            ("properties", "()".into()), // id -> swhids
            ("properties", "()".into()), // id -> swhids
        ]
    );

    assert_eq!(
        nodes,
        vec![
            Node {
                swhid: "swh:1:rel:0000000000000000000000000000000000000020".into(),
                ..Default::default()
            },
            Node {
                swhid: "swh:1:rev:0000000000000000000000000000000000000030".into(),
                ..Default::default()
            }
        ]
    );

    Ok(())
}
