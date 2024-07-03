//! Filesystem manipulation functions.

use anyhow::{bail, Context, Result};

use crate::graph::*;
use crate::labels::{EdgeLabel, FilenameId};
use crate::properties;
use crate::NodeType;

fn msg_no_filename_id(name: impl AsRef<[u8]>) -> String {
    format!(
        "no filename id found for entry \"{}\"",
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
        .with_context(|| msg_no_filename_id(name))?;
    fs_resolve_name_by_id(&graph, dir, name_id)
}

/// Same as [fs_resolve_name], but using a pre-resolved [FilenameId] as entry
/// name. Using this function is more efficient in case the same name (e.g.,
/// "README.md") is to be looked up in many directories.
pub fn fs_resolve_name_by_id<G>(graph: &G, dir: NodeId, name: FilenameId) -> Result<Option<NodeId>>
where
    G: SwhLabeledForwardGraph + SwhGraphWithProperties,
    <G as SwhGraphWithProperties>::LabelNames: properties::LabelNames,
    <G as SwhGraphWithProperties>::Maps: properties::Maps,
{
    let node_type = graph.properties().node_type(dir);
    if graph.properties().node_type(dir) != NodeType::Directory {
        bail!("Type of {dir} should be directory, but is {node_type} instead");
    }

    for (succ, labels) in graph.labeled_successors(dir) {
        for label in labels {
            if let EdgeLabel::DirEntry(dentry) = label {
                if dentry.filename_id() == name {
                    return Ok(Some(succ));
                }
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
                .with_context(|| msg_no_filename_id(name))
        })
        .collect::<Result<Vec<FilenameId>, _>>()?;
    fs_resolve_path_by_id(&graph, dir, &path)
}

/// Same as [fs_resolve_path], but using as path a sequence of pre-resolved
/// [FilenameId]-s. Using this function is more efficient in case the same path
/// (e.g., "src/main.c") is to be looked up in many directories.
pub fn fs_resolve_path_by_id<G>(
    graph: &G,
    dir: NodeId,
    path: &[FilenameId],
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
