// Copyright (C) 2025  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

//! Recursively lists differences between two directory nodes.

use anyhow::{bail, ensure, Result};

use swh_graph::{
    graph::{NodeId, SwhGraphWithProperties, SwhLabeledForwardGraph},
    labels::{EdgeLabel, LabelNameId, Permission},
    properties, NodeType,
};

/// An operation made between two revisions on a single directory entry
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TreeDiffOperation<Path: AsRef<[LabelNameId]> = Box<[LabelNameId]>> {
    /// Adds a single file or directory
    Added {
        /// Full path (from repo root) to the added file
        path: Path,
        new_file: NodeId,
        new_perm: Option<Permission>,
    },
    /// Removes a single file or directory
    Deleted {
        /// Full path (from repo root) to the removed file
        path: Path,
        old_file: NodeId,
        old_perm: Option<Permission>,
    },
    /// Edits a single file (it can't be a directory)
    Modified {
        /// Full path (from the repo root) to the removed file
        path: Path,
        new_file: NodeId,
        old_file: NodeId,
        new_perm: Option<Permission>,
        old_perm: Option<Permission>,
    },
    /// Moves a file from one place to another.
    /// The file may have been edited in the process,
    /// in which case the two supplied node ids will differ.
    Moved {
        old_path: Path,
        new_path: Path,
        old_file: NodeId,
        new_file: NodeId,
        old_perm: Option<Permission>,
        new_perm: Option<Permission>,
    },
}

impl<Path: AsRef<[LabelNameId]>> TreeDiffOperation<Path> {
    pub fn shallow_copy(&self) -> TreeDiffOperation<&[LabelNameId]> {
        use TreeDiffOperation::*;

        match self {
            Added {
                path,
                new_file,
                new_perm,
            } => Added {
                path: path.as_ref(),
                new_file: *new_file,
                new_perm: *new_perm,
            },
            Deleted {
                path,
                old_file,
                old_perm,
            } => Deleted {
                path: path.as_ref(),
                old_file: *old_file,
                old_perm: *old_perm,
            },
            Modified {
                path,
                new_file,
                old_file,
                new_perm,
                old_perm,
            } => Modified {
                path: path.as_ref(),
                new_file: *new_file,
                old_file: *old_file,
                new_perm: *new_perm,
                old_perm: *old_perm,
            },
            Moved {
                old_path,
                new_path,
                new_file,
                old_file,
                new_perm,
                old_perm,
            } => Moved {
                old_path: old_path.as_ref(),
                new_path: new_path.as_ref(),
                new_file: *new_file,
                old_file: *old_file,
                new_perm: *new_perm,
                old_perm: *old_perm,
            },
        }
    }
}

/// A diff between two trees, treating files as atomic units
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeDiff {
    operations: Vec<TreeDiffOperation>,
}

impl TreeDiff {
    pub fn operations(&self) -> impl Iterator<Item = TreeDiffOperation<&[LabelNameId]>> + use<'_> {
        self.operations.iter().map(TreeDiffOperation::shallow_copy)
    }
}

/// Compute a tree diff between the old and new trees rooted in
/// the nodes supplied, which are required to be directory nodes.
///
/// The limit parameter can optionally limit the number of diff
/// elements to return (to avoid returning very big diffs from
/// git bombs). If this limit is reached, an error is returned.
///
/// WARNING: rename detection is currently not supported. The `Moved` variant is never returned.
pub fn tree_diff_dirs<G>(
    graph: &G,
    old_tree: NodeId,
    new_tree: NodeId,
    limit: Option<usize>,
) -> Result<TreeDiff>
where
    G: SwhLabeledForwardGraph + SwhGraphWithProperties<Maps: properties::Maps>,
{
    let mut operations = Vec::new();
    let mut path_prefix = Vec::new();
    let mut stack: Vec<StackItem> = Vec::new();
    stack.push(StackItem::Visit {
        old: old_tree,
        new: new_tree,
        path: None,
    });
    while let Some(stack_item) = stack.pop() {
        match stack_item {
            StackItem::Leave => {
                path_prefix.pop();
            }
            StackItem::Visit { path, old, new } => {
                if let Some(path) = path {
                    path_prefix.push(path);
                }
                tree_diff_internal(
                    graph,
                    old,
                    new,
                    &limit,
                    &mut operations,
                    &mut path_prefix,
                    &mut stack,
                )?;
            }
        }
    }
    Ok(TreeDiff { operations })
}

/// Internal representation of our stack to recurse into directories,
/// to avoid overflowing Rust's call stack.
enum StackItem {
    /// marker to update the path to the directory currently being visited
    Leave,
    // task to visit a pair of nodes
    Visit {
        path: Option<LabelNameId>,
        old: NodeId,
        new: NodeId,
    },
}

fn tree_diff_internal<G>(
    graph: &G,
    old_tree: NodeId,
    new_tree: NodeId,
    limit: &Option<usize>,
    result: &mut Vec<TreeDiffOperation>,
    path_prefix: &mut Vec<LabelNameId>,
    stack: &mut Vec<StackItem>,
) -> Result<()>
where
    G: SwhLabeledForwardGraph + SwhGraphWithProperties<Maps: properties::Maps>,
{
    if old_tree == new_tree {
        // nothing to diff!
        return Ok(());
    }

    let props = graph.properties();

    // Iterate jointly on the listing of both directories.
    // The listings are known to be sorted on their label name ids.
    let mut old_dir_iter = list_dir(graph, old_tree)?.into_iter().peekable();
    let mut new_dir_iter = list_dir(graph, new_tree)?.into_iter().peekable();
    loop {
        if limit.is_some_and(|limit| result.len() >= limit) {
            // TODO convert to if let-chains when they become available to avoid the `unwrap`
            bail!("Diff has more than {} changes", limit.unwrap())
        }
        // concatenates file names to obtain full relative path from the repo's root
        let full_path = |file_name: &LabelNameId| {
            [path_prefix.as_slice(), &[*file_name]]
                .concat()
                .into_boxed_slice()
        };
        match (old_dir_iter.peek(), new_dir_iter.peek()) {
            (None, None) => break,
            (None, Some(new)) => {
                add_directory_recursively(graph, new, limit, result, path_prefix)?;
                new_dir_iter.next();
            }
            (Some(old), None) => {
                delete_directory_recursively(graph, old, limit, result, path_prefix)?;
                old_dir_iter.next();
            }
            (Some(old), Some(new)) => {
                match old.path.0.cmp(&new.path.0) {
                    std::cmp::Ordering::Less => {
                        delete_directory_recursively(graph, old, limit, result, path_prefix)?;
                        old_dir_iter.next();
                    }
                    std::cmp::Ordering::Greater => {
                        add_directory_recursively(graph, new, limit, result, path_prefix)?;
                        new_dir_iter.next();
                    }
                    std::cmp::Ordering::Equal => {
                        // If the two ids are equal, we know the contents are recursively equal,
                        // in which case there is nothing to compare. Otherwise:
                        if old.id != new.id || old.perm != new.perm {
                            let old_type = props.node_type(old.id);
                            let new_type = props.node_type(new.id);
                            let content_types = [NodeType::Content, NodeType::Revision];

                            if content_types.contains(&old_type)
                                && content_types.contains(&new_type)
                            {
                                // Both entries point to file-like things, so we register the difference as an edit on them
                                result.push(TreeDiffOperation::Modified {
                                    path: full_path(&new.path),
                                    new_file: new.id,
                                    old_file: old.id,
                                    new_perm: new.perm,
                                    old_perm: old.perm,
                                });
                            } else if content_types.contains(&old_type)
                                && new_type == NodeType::Directory
                            {
                                // A file got changed into a directory.
                                result.push(TreeDiffOperation::Deleted {
                                    path: full_path(&old.path),
                                    old_file: old.id,
                                    old_perm: old.perm,
                                });
                                add_directory_recursively(graph, new, limit, result, path_prefix)?;
                            } else if old_type == NodeType::Directory
                                && content_types.contains(&new_type)
                            {
                                // A directory changed into a file
                                delete_directory_recursively(
                                    graph,
                                    old,
                                    limit,
                                    result,
                                    path_prefix,
                                )?;
                                result.push(TreeDiffOperation::Added {
                                    path: full_path(&new.path),
                                    new_file: new.id,
                                    new_perm: new.perm,
                                });
                            } else if old_type == NodeType::Directory
                                && new_type == NodeType::Directory
                            {
                                // Two directories with different contents: let's remember
                                // to recurse into this directory later, by pushing the task
                                // to visit it on our stack.
                                // We also add a `Leave` marker to update the current path once
                                // we're done visiting that subdirectory. In order for it to be
                                // processed after the visit, we add it on the stack before the
                                // visit (since it's Last-In-First-Out)
                                stack.push(StackItem::Leave);
                                stack.push(StackItem::Visit {
                                    path: Some(new.path),
                                    old: old.id,
                                    new: new.id,
                                });
                            }
                        };
                        old_dir_iter.next();
                        new_dir_iter.next();
                    }
                }
            }
        }
    }
    Ok(())
}

/// Recursively browse a part of a file system and mark all of its contents as added
fn add_directory_recursively<G>(
    graph: &G,
    dir_entry: &DirEntry,
    limit: &Option<usize>,
    result: &mut Vec<TreeDiffOperation>,
    path_prefix: &[LabelNameId],
) -> Result<()>
where
    G: SwhLabeledForwardGraph + SwhGraphWithProperties<Maps: properties::Maps>,
{
    add_or_delete_directory_recursively(graph, dir_entry, limit, result, path_prefix, true)
}

/// Recursively browse a part of a file system and mark all of its contents as deleted
fn delete_directory_recursively<G>(
    graph: &G,
    dir_entry: &DirEntry,
    limit: &Option<usize>,
    result: &mut Vec<TreeDiffOperation>,
    path_prefix: &[LabelNameId],
) -> Result<()>
where
    G: SwhLabeledForwardGraph + SwhGraphWithProperties<Maps: properties::Maps>,
{
    add_or_delete_directory_recursively(graph, dir_entry, limit, result, path_prefix, false)
}

enum TraverseStackItem {
    Leave,
    Visit {
        node_id: NodeId,
        path: LabelNameId,
        perm: Option<Permission>,
    },
}

/// Recursively browse a part of a file system and mark all of its contents as added
/// or deleted (as indicated by the last parameter).
fn add_or_delete_directory_recursively<G>(
    graph: &G,
    dir_entry: &DirEntry,
    limit: &Option<usize>,
    result: &mut Vec<TreeDiffOperation>,
    path_prefix: &[LabelNameId],
    add: bool,
) -> Result<()>
where
    G: SwhLabeledForwardGraph + SwhGraphWithProperties<Maps: properties::Maps>,
{
    let props = graph.properties();
    let mut stack = Vec::new();
    let mut path_prefix = path_prefix.to_vec();
    stack.push(TraverseStackItem::Visit {
        node_id: dir_entry.id,
        path: dir_entry.path,
        perm: dir_entry.perm,
    });
    while let Some(task) = stack.pop() {
        if limit.is_some_and(|limit| result.len() >= limit) {
            bail!("Diff has more than {} changes", limit.unwrap());
        }
        match task {
            TraverseStackItem::Leave => {
                path_prefix.pop();
            }
            TraverseStackItem::Visit {
                node_id,
                path,
                perm,
            } => {
                path_prefix.push(path);
                match props.node_type(node_id) {
                    NodeType::Content | NodeType::Revision => {
                        let diff_item = if add {
                            TreeDiffOperation::Added {
                                path: path_prefix.to_vec().into_boxed_slice(),
                                new_file: node_id,
                                new_perm: perm,
                            }
                        } else {
                            TreeDiffOperation::Deleted {
                                path: path_prefix.to_vec().into_boxed_slice(),
                                old_file: node_id,
                                old_perm: perm,
                            }
                        };
                        result.push(diff_item);
                    }
                    NodeType::Directory => {
                        let dir_listing = list_dir(graph, node_id)?;
                        for entry in &dir_listing {
                            stack.push(TraverseStackItem::Leave);
                            stack.push(TraverseStackItem::Visit {
                                node_id: entry.id,
                                path: entry.path,
                                perm: entry.perm,
                            });
                        }
                    }
                    x => {
                        bail!("unexpected node type {x} when browsing a directory");
                    }
                }
            }
        }
    }
    Ok(())
}

/// Internal struct to represent an entry in the list of contents of a directory
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DirEntry {
    path: LabelNameId,
    perm: Option<Permission>,
    id: NodeId,
}

/// Lists the contents of a directory, sorting the results by label name id.
fn list_dir<G>(graph: &G, dir_node: NodeId) -> Result<Vec<DirEntry>>
where
    G: SwhLabeledForwardGraph + SwhGraphWithProperties<Maps: properties::Maps>,
{
    let props = graph.properties();
    let node_type = props.node_type(dir_node);
    ensure!(
        node_type == NodeType::Directory,
        "Type of {dir_node} should be dir, but is {node_type} instead"
    );
    let mut listing = Vec::new();
    for (succ, labels) in graph.labeled_successors(dir_node) {
        let node_type = props.node_type(succ);
        if node_type == NodeType::Content
            || node_type == NodeType::Directory
            || node_type == NodeType::Revision
        {
            for label in labels {
                if let EdgeLabel::DirEntry(dentry) = label {
                    let perm = dentry.permission();
                    listing.push(DirEntry {
                        path: dentry.label_name_id(),
                        perm,
                        id: succ,
                    });
                }
            }
        }
    }
    listing.sort_unstable_by_key(|dir_entry| dir_entry.path.0);
    Ok(listing)
}

#[cfg(test)]
mod tests {

    use swh_graph::{
        graph::*,
        graph_builder::{BuiltGraph, GraphBuilder},
        NodeType, SWHID,
    };

    use super::*;

    /// Convenience type to define test cases.
    /// The `u32` fields are used to generate SWHIDs
    /// when converting the trees to graphs.
    enum Tree {
        File(u32),
        Executable(u32),
        Dir(u32, Vec<(&'static str, Tree)>),
        Revision(u32),
    }

    #[rustfmt::skip] // to keep the tree representation more compact and readable
    fn base_tree() -> Tree {
        Tree::Dir(1, vec![
            ("books", Tree::Dir(2, vec![
                ("disparition.txt", Tree::File(3)),
                ("revision_link", Tree::Revision(9)),
                ("war_and_peace.doc", Tree::File(4)),
            ])),
            ("music", Tree::Dir(5, vec![
                ("lemon_tree.mp3", Tree::File(6)),
                ("on_reflection.wav", Tree::File(7)),
            ])),
            ("readme.txt", Tree::File(8)),
        ])
    }

    fn parse_path(graph: &BuiltGraph, path: &str) -> Box<[LabelNameId]> {
        path.split('/')
            .map(|part| {
                graph
                    .properties()
                    .label_name_id(part.as_bytes())
                    .expect("label name does not exist in the graph")
            })
            .collect()
    }

    #[test]
    fn test_list_dir() -> Result<()> {
        let tree = base_tree();
        let mut builder = GraphBuilder::default();
        let root_id = build_graph_recursively(&tree, &mut builder)?.0;
        let graph = builder.done()?;
        let props = graph.properties();
        let books_subdir_id =
            props.node_id("swh:1:dir:0000000000000000000000000000000000000002")?;
        let music_subdir_id =
            props.node_id("swh:1:dir:0000000000000000000000000000000000000005")?;
        let readme_id = props.node_id("swh:1:cnt:0000000000000000000000000000000000000008")?;
        let lemon_tree_id = props.node_id("swh:1:cnt:0000000000000000000000000000000000000006")?;
        let on_reflection_id =
            props.node_id("swh:1:cnt:0000000000000000000000000000000000000007")?;

        assert_eq!(
            list_dir(&graph, root_id)?,
            vec![
                DirEntry {
                    path: parse_path(&graph, "books")[0],
                    perm: Some(Permission::Directory),
                    id: books_subdir_id,
                },
                DirEntry {
                    path: parse_path(&graph, "music")[0],
                    perm: Some(Permission::Directory),
                    id: music_subdir_id,
                },
                DirEntry {
                    path: parse_path(&graph, "readme.txt")[0],
                    perm: Some(Permission::Content),
                    id: readme_id,
                }
            ]
        );
        assert_eq!(
            list_dir(&graph, music_subdir_id)?,
            vec![
                DirEntry {
                    path: parse_path(&graph, "lemon_tree.mp3")[0],
                    perm: Some(Permission::Content),
                    id: lemon_tree_id,
                },
                DirEntry {
                    path: parse_path(&graph, "on_reflection.wav")[0],
                    perm: Some(Permission::Content),
                    id: on_reflection_id,
                }
            ]
        );
        Ok(())
    }

    #[test]
    fn test_edit_file() -> Result<()> {
        let base_tree = base_tree();

        #[rustfmt::skip]
        let tree_with_edit =
            Tree::Dir(15, vec![
                ("books", Tree::Dir(14, vec![
                    ("disparition.txt", Tree::File(13)),
                    ("revision_link", Tree::Revision(9)),
                    ("war_and_peace.doc", Tree::File(4)),
                ])),
                ("music", Tree::Dir(10, vec![
                    ("lemon_tree.mp3", Tree::File(6)),
                    ("on_reflection.wav", Tree::File(7)),
                ])),
                ("readme.txt", Tree::File(8)),
        ]);

        let (graph, diff) = diff_trees(&base_tree, &tree_with_edit, None)?;

        assert_eq!(
            diff.operations,
            vec![TreeDiffOperation::Modified {
                path: parse_path(&graph, "books/disparition.txt"),
                old_file: graph
                    .properties()
                    .node_id("swh:1:cnt:0000000000000000000000000000000000000003")?,
                new_file: graph
                    .properties()
                    .node_id("swh:1:cnt:000000000000000000000000000000000000000d")?,
                old_perm: Some(Permission::Content),
                new_perm: Some(Permission::Content),
            }]
        );
        Ok(())
    }

    #[test]
    fn test_change_permissions() -> Result<()> {
        let base_tree = base_tree();

        #[rustfmt::skip]
        let tree_with_edit =
            Tree::Dir(15, vec![
                ("books", Tree::Dir(14, vec![
                    ("disparition.txt", Tree::Executable(3)),
                    ("revision_link", Tree::Revision(9)),
                    ("war_and_peace.doc", Tree::File(4)),
                ])),
                ("music", Tree::Dir(10, vec![
                    ("lemon_tree.mp3", Tree::File(6)),
                    ("on_reflection.wav", Tree::File(7)),
                ])),
                ("readme.txt", Tree::File(8)),
        ]);

        let (graph, diff) = diff_trees(&base_tree, &tree_with_edit, None)?;

        assert_eq!(
            diff.operations,
            vec![TreeDiffOperation::Modified {
                path: parse_path(&graph, "books/disparition.txt"),
                old_file: graph
                    .properties()
                    .node_id("swh:1:cnt:0000000000000000000000000000000000000003")?,
                new_file: graph
                    .properties()
                    .node_id("swh:1:cnt:0000000000000000000000000000000000000003")?,
                old_perm: Some(Permission::Content),
                new_perm: Some(Permission::ExecutableContent),
            }]
        );
        Ok(())
    }

    #[test]
    fn test_edit_revision() -> Result<()> {
        let base_tree = base_tree();

        #[rustfmt::skip]
        let tree_with_edit =
            Tree::Dir(15, vec![
                ("books", Tree::Dir(14, vec![
                    ("disparition.txt", Tree::File(3)),
                    ("revision_link", Tree::Revision(16)),
                    ("war_and_peace.doc", Tree::File(4)),
                ])),
                ("music", Tree::Dir(10, vec![
                    ("lemon_tree.mp3", Tree::File(6)),
                    ("on_reflection.wav", Tree::File(7)),
                ])),
                ("readme.txt", Tree::File(8)),
        ]);

        let (graph, diff) = diff_trees(&base_tree, &tree_with_edit, None)?;

        assert_eq!(
            diff.operations,
            vec![TreeDiffOperation::Modified {
                path: parse_path(&graph, "books/revision_link"),
                old_file: graph
                    .properties()
                    .node_id("swh:1:rev:0000000000000000000000000000000000000009")?,
                new_file: graph
                    .properties()
                    .node_id("swh:1:rev:0000000000000000000000000000000000000010")?,
                old_perm: Some(Permission::Revision),
                new_perm: Some(Permission::Revision),
            }]
        );
        Ok(())
    }

    #[test]
    fn test_additions_and_deletions() -> Result<()> {
        let base_tree = base_tree();

        #[rustfmt::skip]
        let tree_with_addition =
            Tree::Dir(11, vec![
                ("books", Tree::Dir(2, vec![
                    ("disparition.txt", Tree::File(3)),
                    ("revision_link", Tree::Revision(9)),
                    ("war_and_peace.doc", Tree::File(4)),
                ])),
                ("music", Tree::Dir(10, vec![
                    ("lemon_tree.mp3", Tree::File(6)),
                    ("gavotte_aven.wma", Tree::File(12)),
                    ("on_reflection.wav", Tree::File(7)),
                ])),
                ("readme.txt", Tree::File(8)),
        ]);

        #[rustfmt::skip]
        let tree_with_directory_deletion =
            Tree::Dir(13, vec![
                ("music", Tree::Dir(5, vec![
                    ("lemon_tree.mp3", Tree::File(6)),
                    ("on_reflection.wav", Tree::File(7)),
                ])),
                ("readme.txt", Tree::File(8)),
        ]);

        let (graph, diff) = diff_trees(&base_tree, &tree_with_addition, None)?;

        assert_eq!(
            diff.operations().collect::<Vec<_>>(),
            vec![TreeDiffOperation::Added {
                path: parse_path(&graph, "music/gavotte_aven.wma").as_ref(),
                new_file: graph
                    .properties()
                    .node_id("swh:1:cnt:000000000000000000000000000000000000000c")?,
                new_perm: Some(Permission::Content),
            }]
        );

        let (graph, diff) = diff_trees(&base_tree, &tree_with_directory_deletion, None)?;

        assert_eq!(
            diff.operations().collect::<Vec<_>>(),
            vec![
                TreeDiffOperation::Deleted {
                    path: parse_path(&graph, "books/war_and_peace.doc").as_ref(),
                    old_file: graph
                        .properties()
                        .node_id("swh:1:cnt:0000000000000000000000000000000000000004")?,
                    old_perm: Some(Permission::Content),
                },
                TreeDiffOperation::Deleted {
                    path: parse_path(&graph, "books/revision_link").as_ref(),
                    old_file: graph
                        .properties()
                        .node_id("swh:1:rev:0000000000000000000000000000000000000009")?,
                    old_perm: Some(Permission::Revision),
                },
                TreeDiffOperation::Deleted {
                    path: parse_path(&graph, "books/disparition.txt").as_ref(),
                    old_file: graph
                        .properties()
                        .node_id("swh:1:cnt:0000000000000000000000000000000000000003")?,
                    old_perm: Some(Permission::Content),
                },
            ]
        );

        let (graph, diff) = diff_trees(&tree_with_addition, &tree_with_directory_deletion, None)?;

        assert_eq!(
            diff.operations().collect::<Vec<_>>(),
            vec![
                TreeDiffOperation::Deleted {
                    path: parse_path(&graph, "books/war_and_peace.doc").as_ref(),
                    old_file: graph
                        .properties()
                        .node_id("swh:1:cnt:0000000000000000000000000000000000000004")?,
                    old_perm: Some(Permission::Content),
                },
                TreeDiffOperation::Deleted {
                    path: parse_path(&graph, "books/revision_link").as_ref(),
                    old_file: graph
                        .properties()
                        .node_id("swh:1:rev:0000000000000000000000000000000000000009")?,
                    old_perm: Some(Permission::Revision),
                },
                TreeDiffOperation::Deleted {
                    path: parse_path(&graph, "books/disparition.txt").as_ref(),
                    old_file: graph
                        .properties()
                        .node_id("swh:1:cnt:0000000000000000000000000000000000000003")?,
                    old_perm: Some(Permission::Content),
                },
                TreeDiffOperation::Deleted {
                    path: parse_path(&graph, "music/gavotte_aven.wma").as_ref(),
                    old_file: graph
                        .properties()
                        .node_id("swh:1:cnt:000000000000000000000000000000000000000c")?,
                    old_perm: Some(Permission::Content),
                }
            ]
        );
        Ok(())
    }

    #[test]
    fn test_move_file() -> Result<()> {
        let base_tree = base_tree();

        #[rustfmt::skip]
        let tree_with_edit =
            Tree::Dir(17, vec![
                ("books", Tree::Dir(16, vec![
                    ("disparition.txt", Tree::File(3)),
                    ("revision_link", Tree::Revision(9)),
                    ("war_and_peace.doc", Tree::File(4)),
                    ("readme.txt", Tree::File(8)),
                ])),
                ("music", Tree::Dir(10, vec![
                    ("lemon_tree.mp3", Tree::File(6)),
                    ("on_reflection.wav", Tree::File(7)),
                ])),
        ]);

        let (graph, diff) = diff_trees(&base_tree, &tree_with_edit, None)?;

        // TODO: in the future we want to detect renames,
        // instead of registering this as an addition and deletion
        assert_eq!(
            diff.operations().collect::<Vec<_>>(),
            vec![
                TreeDiffOperation::Deleted {
                    path: parse_path(&graph, "readme.txt").as_ref(),
                    old_file: graph
                        .properties()
                        .node_id("swh:1:cnt:0000000000000000000000000000000000000008")?,
                    old_perm: Some(Permission::Content),
                },
                TreeDiffOperation::Added {
                    path: parse_path(&graph, "books/readme.txt").as_ref(),
                    new_file: graph
                        .properties()
                        .node_id("swh:1:cnt:0000000000000000000000000000000000000008")?,
                    new_perm: Some(Permission::Content),
                },
            ]
        );
        Ok(())
    }

    #[test]
    fn test_change_dir_to_file() -> Result<()> {
        let base_tree = base_tree();

        #[rustfmt::skip]
        let modified_tree =
            Tree::Dir(11, vec![
                ("books", Tree::Dir(2, vec![
                    ("disparition.txt", Tree::File(3)),
                    ("revision_link", Tree::Revision(9)),
                    ("war_and_peace.doc", Tree::File(4)),
                ])),
                ("music", Tree::Revision(10)),
                ("readme.txt", Tree::File(8)),
        ]);

        let (graph, diff) = diff_trees(&base_tree, &modified_tree, None)?;

        assert_eq!(
            diff.operations().collect::<Vec<_>>(),
            vec![
                TreeDiffOperation::Deleted {
                    path: parse_path(&graph, "music/on_reflection.wav").as_ref(),
                    old_file: graph
                        .properties()
                        .node_id("swh:1:cnt:0000000000000000000000000000000000000007")?,
                    old_perm: Some(Permission::Content),
                },
                TreeDiffOperation::Deleted {
                    path: parse_path(&graph, "music/lemon_tree.mp3").as_ref(),
                    old_file: graph
                        .properties()
                        .node_id("swh:1:cnt:0000000000000000000000000000000000000006")?,
                    old_perm: Some(Permission::Content),
                },
                TreeDiffOperation::Added {
                    path: parse_path(&graph, "music").as_ref(),
                    new_file: graph
                        .properties()
                        .node_id("swh:1:rev:000000000000000000000000000000000000000a")?,
                    new_perm: Some(Permission::Revision),
                },
            ]
        );
        Ok(())
    }

    #[test]
    fn test_change_file_to_dir() -> Result<()> {
        let base_tree = base_tree();

        #[rustfmt::skip]
        let tree_with_edit =
            Tree::Dir(11, vec![
                ("books", Tree::Dir(12, vec![
                    ("disparition.txt", Tree::File(3)),
                    ("revision_link", Tree::Dir(10, vec![
                        ("readme.txt", Tree::File(8)),
                    ])),
                    ("war_and_peace.doc", Tree::File(4)),
                ])),
                ("music", Tree::Dir(5, vec![
                    ("lemon_tree.mp3", Tree::File(6)),
                    ("on_reflection.wav", Tree::File(7)),
                ])),
                ("readme.txt", Tree::File(8)),
        ]);

        let (graph, diff) = diff_trees(&base_tree, &tree_with_edit, None)?;

        assert_eq!(
            diff.operations().collect::<Vec<_>>(),
            vec![
                TreeDiffOperation::Deleted {
                    path: parse_path(&graph, "books/revision_link").as_ref(),
                    old_file: graph
                        .properties()
                        .node_id("swh:1:rev:0000000000000000000000000000000000000009")?,
                    old_perm: Some(Permission::Revision),
                },
                TreeDiffOperation::Added {
                    path: parse_path(&graph, "books/revision_link/readme.txt").as_ref(),
                    new_file: graph
                        .properties()
                        .node_id("swh:1:cnt:0000000000000000000000000000000000000008")?,
                    new_perm: Some(Permission::Content),
                }
            ]
        );
        Ok(())
    }

    #[test]
    fn test_limit() -> Result<()> {
        let base_tree = base_tree();

        #[rustfmt::skip]
        let tree_with_edit =
            Tree::Dir(15, vec![
                ("books", Tree::Dir(14, vec![
                    ("disparition.txt", Tree::File(3)),
                    ("revision_link", Tree::Revision(16)),
                    ("war_and_peace.doc", Tree::File(4)),
                ])),
        ]);

        assert!(
            diff_trees(&base_tree, &tree_with_edit, Some(2)).is_err(),
            "Expected the diff to error out because there are too many changes"
        );
        Ok(())
    }

    /// Helper function to call the [`diff_tree`] function more easily on graphs
    /// generated from synthetic trees
    fn diff_trees(
        tree_1: &Tree,
        tree_2: &Tree,
        limit: Option<usize>,
    ) -> Result<(BuiltGraph, TreeDiff)> {
        let mut builder = GraphBuilder::default();
        let old_root = build_graph_recursively(tree_1, &mut builder)?.0;
        let new_root = build_graph_recursively(tree_2, &mut builder)?.0;
        let graph = builder.done()?;
        let diff = tree_diff_dirs(&graph, old_root, new_root, limit)?;
        Ok((graph, diff))
    }

    fn make_node(id: &u32, node_type: NodeType, builder: &mut GraphBuilder) -> Result<NodeId> {
        let mut hash = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        hash[16..].copy_from_slice(&id.to_be_bytes());
        let swhid = SWHID {
            namespace_version: 1,
            node_type,
            hash,
        };
        if let Some(node_id) = builder.node_id(swhid) {
            Ok(node_id)
        } else {
            Ok(builder.node(swhid)?.done())
        }
    }

    fn build_graph_recursively(
        tree: &Tree,
        builder: &mut GraphBuilder,
    ) -> Result<(NodeId, Permission)> {
        match tree {
            Tree::File(node_id) => Ok((
                make_node(node_id, NodeType::Content, builder)?,
                Permission::Content,
            )),
            Tree::Executable(node_id) => Ok((
                make_node(node_id, NodeType::Content, builder)?,
                Permission::ExecutableContent,
            )),
            Tree::Dir(node_id, items) => {
                let dir = make_node(node_id, NodeType::Directory, builder)?;
                for (name, child) in items {
                    let (child_node, permission) = build_graph_recursively(child, builder)?;
                    builder.dir_arc(dir, child_node, permission, name.as_bytes());
                }
                Ok((dir, Permission::Directory))
            }
            Tree::Revision(node_id) => Ok((
                make_node(node_id, NodeType::Revision, builder)?,
                Permission::Revision,
            )),
        }
    }
}
