// Copyright (C) 2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Filesystem manipulation functions.

use std::collections::HashMap;

use anyhow::{ensure, Context, Result};
use log::warn;

use swh_graph::graph::*;
use swh_graph::labels::{EdgeLabel, LabelNameId, Permission};
use swh_graph::properties;
use swh_graph::NodeType;

fn msg_no_label_name_id(name: impl AsRef<[u8]>) -> String {
    format!(
        "no label_name id found for entry \"{}\"",
        String::from_utf8_lossy(name.as_ref())
    )
}

/// Given a graph and a directory node, return the node id of a named directory
/// entry located (not recursively) in that directory, if it exists.
///
/// See [fs_resolve_path] for a version of this function that traverses
/// sub-directories recursively.
///
/// ```ignore
/// if let Ok(Some(node)) == fs_resolve_name(&graph, 42, "README.md") {
///     // do something with node
/// }
/// ```
pub fn fs_resolve_name<G>(graph: &G, dir: NodeId, name: impl AsRef<[u8]>) -> Result<Option<NodeId>>
where
    G: SwhLabeledForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::LabelNames: properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    let props = graph.properties();
    let name_id = props
        .label_name_id(name.as_ref())
        .with_context(|| msg_no_label_name_id(name))?;
    fs_resolve_name_by_id(&graph, dir, name_id)
}

/// Same as [fs_resolve_name], but using a pre-resolved [LabelNameId] as entry
/// name. Using this function is more efficient in case the same name (e.g.,
/// "README.md") is to be looked up in many directories.
pub fn fs_resolve_name_by_id<G>(graph: &G, dir: NodeId, name: LabelNameId) -> Result<Option<NodeId>>
where
    G: SwhLabeledForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::LabelNames: properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    let node_type = graph.properties().node_type(dir);
    ensure!(
        node_type == NodeType::Directory,
        "Type of {dir} should be dir, but is {node_type} instead"
    );

    for (succ, label) in graph.labeled_successors(dir).flatten_labels() {
        if let EdgeLabel::DirEntry(dentry) = label {
            if dentry.label_name_id() == name {
                return Ok(Some(succ));
            }
        }
    }
    Ok(None)
}

/// Given a graph and a directory node, return the node id of a directory entry
/// located at a given path within that directory, if it exists.
///
/// Slashes (`/`) contained in `path` are interpreted as path separators.
///
/// See [fs_resolve_name] for a non-recursive version of this function.
///
/// ```ignore
/// if let Ok(Some(node)) == fs_resolve_path(&graph, 42, "src/main.c") {
///     // do something with node
/// }
/// ```
pub fn fs_resolve_path<G>(graph: &G, dir: NodeId, path: impl AsRef<[u8]>) -> Result<Option<NodeId>>
where
    G: SwhLabeledForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::LabelNames: properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    let props = graph.properties();
    let path = path
        .as_ref()
        .split(|byte| *byte == b'/')
        .map(|name| {
            props
                .label_name_id(name)
                .with_context(|| msg_no_label_name_id(name))
        })
        .collect::<Result<Vec<LabelNameId>, _>>()?;
    fs_resolve_path_by_id(&graph, dir, &path)
}

/// Same as [fs_resolve_path], but using as path a sequence of pre-resolved
/// [LabelNameId]-s. Using this function is more efficient in case the same path
/// (e.g., "src/main.c") is to be looked up in many directories.
pub fn fs_resolve_path_by_id<G>(
    graph: &G,
    dir: NodeId,
    path: &[LabelNameId],
) -> Result<Option<NodeId>>
where
    G: SwhLabeledForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::LabelNames: properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    let mut cur_entry = dir;
    for name in path {
        match fs_resolve_name_by_id(graph, cur_entry, *name)? {
            None => return Ok(None),
            Some(entry) => cur_entry = entry,
        }
    }
    Ok(Some(cur_entry))
}

/// Recursive representation of a directory tree, ignoring sharing.
///
/// Note that a `Revision` variant can in fact point to either revision or
/// release nodes.
#[derive(Debug, Default, PartialEq)]
pub enum FsTree {
    #[default]
    Content,
    Directory(HashMap<Vec<u8>, (FsTree, Option<Permission>)>),
    Revision(NodeId),
}

/// Given a graph and a directory node in it (usually, but not necessarily, the
/// *root* directory of a repository), return a recursive list of the contained
/// files and directories.
///
/// Note that symlinks are not followed during listing and are reported as
/// files in the returned tree. To recognize them as links, check the
/// permissions of the associated directory entries.
pub fn fs_ls_tree<G>(graph: &G, dir: NodeId) -> Result<FsTree>
where
    G: SwhLabeledForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::LabelNames: properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    let props = graph.properties();
    let node_type = props.node_type(dir);
    ensure!(
        node_type == NodeType::Directory,
        "Type of {dir} should be dir, but is {node_type} instead"
    );

    let mut dir_entries = HashMap::new();
    for (succ, labels) in graph.labeled_successors(dir) {
        let node_type = props.node_type(succ);
        for label in labels {
            if let EdgeLabel::DirEntry(dentry) = label {
                let file_name = props.label_name(dentry.label_name_id());
                let perm = dentry.permission();
                match node_type {
                    NodeType::Content => {
                        dir_entries.insert(file_name, (FsTree::Content, perm));
                    }
                    NodeType::Directory => {
                        // recurse into subdir
                        if let Ok(subdir) = fs_ls_tree(graph, succ) {
                            dir_entries.insert(file_name, (subdir, perm));
                        } else {
                            warn!("Cannot list (sub-)directory {succ}, skipping it");
                        }
                    }
                    NodeType::Revision | NodeType::Release => {
                        dir_entries.insert(file_name, (FsTree::Revision(succ), perm));
                    }
                    NodeType::Origin | NodeType::Snapshot => {
                        warn!("Ignoring dir entry with unexpected type {node_type}");
                    }
                }
            }
        }
    }

    Ok(FsTree::Directory(dir_entries))
}
