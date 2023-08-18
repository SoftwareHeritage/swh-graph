// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

/// Readers for the ORC dataset.
use std::path::PathBuf;

use orcxx::deserialize::{CheckableKind, OrcDeserialize, OrcStruct};
use orcxx::reader::Reader;
use orcxx::row_iterator::RowIterator;
use orcxx_derive::OrcDeserialize;
use rayon::prelude::*;

use crate::SWHType;

const SWHID_TXT_SIZE: usize = 50;
type TextSwhid = [u8; SWHID_TXT_SIZE];

const ORC_BATCH_SIZE: usize = 10_000; // Larger values don't seem to improve throughput

fn get_dataset_readers(mut dataset_dir: PathBuf, subdirectory: &str) -> Vec<orcxx::reader::Reader> {
    dataset_dir.push("orc");
    dataset_dir.push(subdirectory);
    std::fs::read_dir(&dataset_dir)
        .expect(&format!("Could not list {}", dataset_dir.display()))
        .map(|file_path| {
            let file_path = file_path
                .expect(&format!("Failed to list {}", dataset_dir.display()))
                .path();
            let input_stream = orcxx::reader::InputStream::from_local_file(
                file_path
                    .to_str()
                    .expect(&format!("Error decoding {}", file_path.display())),
            )
            .expect(&format!("Could not open {}", file_path.display()));
            Reader::new(input_stream).expect(&format!("Could not read {}", file_path.display()))
        })
        .collect()
}

pub fn estimate_node_count(dataset_dir: &PathBuf) -> u64 {
    [].into_par_iter()
        .chain(get_dataset_readers(dataset_dir.clone(), "directory"))
        .chain(get_dataset_readers(dataset_dir.clone(), "content"))
        .chain(get_dataset_readers(dataset_dir.clone(), "origin"))
        .chain(get_dataset_readers(dataset_dir.clone(), "release"))
        .chain(get_dataset_readers(dataset_dir.clone(), "revision"))
        .chain(get_dataset_readers(dataset_dir.clone(), "snapshot"))
        .map(|reader| reader.row_count())
        .sum()
}

pub fn estimate_edge_count(dataset_dir: &PathBuf) -> u64 {
    [].into_par_iter()
        .chain(get_dataset_readers(dataset_dir.clone(), "directory_entry"))
        .chain(get_dataset_readers(
            // Count the source...
            dataset_dir.clone(),
            "origin_visit_status",
        ))
        .chain(get_dataset_readers(
            // ... and destination of each arc
            dataset_dir.clone(),
            "origin_visit_status",
        ))
        .chain(get_dataset_readers(dataset_dir.clone(), "release"))
        .chain(get_dataset_readers(dataset_dir.clone(), "revision"))
        .chain(get_dataset_readers(dataset_dir.clone(), "revision_history"))
        .chain(get_dataset_readers(dataset_dir.clone(), "snapshot_branch"))
        .map(|reader| reader.row_count())
        .sum()
}

pub fn iter_swhids(dataset_dir: &PathBuf) -> impl ParallelIterator<Item = TextSwhid> {
    [].into_par_iter()
        .chain(
            get_dataset_readers(dataset_dir.clone(), "directory")
                .into_par_iter()
                .flat_map(iter_swhids_from_dir),
        )
        .chain(
            get_dataset_readers(dataset_dir.clone(), "directory_entry")
                .into_par_iter()
                .flat_map(iter_swhids_from_dir_entry),
        )
        .chain(
            get_dataset_readers(dataset_dir.clone(), "content")
                .into_par_iter()
                .flat_map(iter_swhids_from_cnt),
        )
        .chain(
            get_dataset_readers(dataset_dir.clone(), "origin")
                .into_par_iter()
                .flat_map(iter_swhids_from_ori),
        )
        .chain(
            get_dataset_readers(dataset_dir.clone(), "origin_visit_status")
                .into_par_iter()
                .flat_map(iter_arcs_from_ovs)
                .flat_map_iter(|(src, dst)| [src, dst].into_iter()),
        )
        .chain(
            get_dataset_readers(dataset_dir.clone(), "release")
                .into_par_iter()
                .flat_map(iter_rel_swhids_from_rel),
        )
        .chain(
            get_dataset_readers(dataset_dir.clone(), "release")
                .into_par_iter()
                .flat_map(iter_target_swhids_from_rel),
        )
        .chain(
            get_dataset_readers(dataset_dir.clone(), "revision")
                .into_par_iter()
                .flat_map(iter_rev_swhids_from_rev),
        )
        .chain(
            get_dataset_readers(dataset_dir.clone(), "revision")
                .into_par_iter()
                .flat_map(iter_dir_swhids_from_rev),
        )
        .chain(
            get_dataset_readers(dataset_dir.clone(), "revision_history")
                .into_par_iter()
                .flat_map(iter_parent_swhids_from_rev),
        )
        .chain(
            get_dataset_readers(dataset_dir.clone(), "snapshot")
                .into_par_iter()
                .flat_map(iter_swhids_from_snp),
        )
        .chain(
            get_dataset_readers(dataset_dir.clone(), "snapshot_branch")
                .into_par_iter()
                .flat_map(iter_swhids_from_snp_branch),
        )
}

fn map_swhids<T: OrcDeserialize + CheckableKind + OrcStruct + Clone + Send, F>(
    reader: Reader,
    f: F,
) -> impl ParallelIterator<Item = TextSwhid>
where
    F: Fn(T) -> Option<String> + Send + Sync,
{
    RowIterator::<T>::new(&reader, (ORC_BATCH_SIZE as u64).try_into().unwrap())
        .expect("Could not open row reader")
        .expect("Unexpected schema")
        .par_bridge()
        .flat_map(f)
        .map(|swhid| swhid.as_bytes().try_into().unwrap())
}

fn iter_swhids_from_dir_entry(reader: Reader) -> impl ParallelIterator<Item = TextSwhid> {
    #[derive(OrcDeserialize, Default, Clone)]
    struct DirectoryEntry {
        r#type: String,
        target: String,
    }

    map_swhids(reader, |entry: DirectoryEntry| {
        Some(match entry.r#type.as_bytes() {
            b"file" => format!("swh:1:cnt:{}", entry.target),
            b"dir" => format!("swh:1:dir:{}", entry.target),
            b"rev" => format!("swh:1:rev:{}", entry.target),
            _ => panic!("Unexpected directory entry type: {:?}", entry.r#type),
        })
    })
}

fn iter_swhids_from_dir(reader: Reader) -> impl ParallelIterator<Item = TextSwhid> {
    #[derive(OrcDeserialize, Default, Clone)]
    struct Directory {
        id: String,
    }

    map_swhids(reader, |dir: Directory| {
        Some(format!("swh:1:dir:{}", dir.id))
    })
}

fn iter_swhids_from_cnt(reader: Reader) -> impl ParallelIterator<Item = TextSwhid> {
    #[derive(OrcDeserialize, Default, Clone)]
    struct Content {
        sha1_git: String,
    }

    map_swhids(reader, |cnt: Content| {
        Some(format!("swh:1:cnt:{}", cnt.sha1_git))
    })
}

fn iter_swhids_from_ori(reader: Reader) -> impl ParallelIterator<Item = TextSwhid> {
    #[derive(OrcDeserialize, Default, Clone)]
    struct Origin {
        id: String,
    }

    map_swhids(reader, |ori: Origin| Some(format!("swh:1:ori:{}", ori.id)))
}

fn iter_rel_swhids_from_rel(reader: Reader) -> impl ParallelIterator<Item = TextSwhid> {
    #[derive(OrcDeserialize, Default, Clone)]
    struct Release {
        id: String,
    }

    map_swhids(reader, |rel: Release| Some(format!("swh:1:rel:{}", rel.id)))
}

fn iter_target_swhids_from_rel(reader: Reader) -> impl ParallelIterator<Item = TextSwhid> {
    #[derive(OrcDeserialize, Default, Clone)]
    struct Release {
        target: String,
        target_type: String,
    }

    map_swhids(reader, |entry: Release| {
        Some(match entry.target_type.as_bytes() {
            b"content" => format!("swh:1:cnt:{}", entry.target),
            b"directory" => format!("swh:1:dir:{}", entry.target),
            b"revision" => format!("swh:1:rev:{}", entry.target),
            b"release" => format!("swh:1:rel:{}", entry.target),
            _ => panic!("Unexpected release target type: {:?}", entry.target_type),
        })
    })
}

fn iter_rev_swhids_from_rev(reader: Reader) -> impl ParallelIterator<Item = TextSwhid> {
    #[derive(OrcDeserialize, Default, Clone)]
    struct Revision {
        id: String,
    }

    map_swhids(reader, |dir: Revision| {
        Some(format!("swh:1:rev:{}", dir.id))
    })
}

fn iter_dir_swhids_from_rev(reader: Reader) -> impl ParallelIterator<Item = TextSwhid> {
    #[derive(OrcDeserialize, Default, Clone)]
    struct Revision {
        directory: String,
    }

    map_swhids(reader, |rev: Revision| {
        Some(format!("swh:1:dir:{}", rev.directory))
    })
}

fn iter_parent_swhids_from_rev(reader: Reader) -> impl ParallelIterator<Item = TextSwhid> {
    #[derive(OrcDeserialize, Default, Clone)]
    struct RevisionParent {
        parent_id: String,
    }

    map_swhids(reader, |rev: RevisionParent| {
        Some(format!("swh:1:rev:{}", rev.parent_id))
    })
}

fn iter_swhids_from_snp(reader: Reader) -> impl ParallelIterator<Item = TextSwhid> {
    #[derive(OrcDeserialize, Default, Clone)]
    struct Snapshot {
        id: String,
    }

    map_swhids(reader, |dir: Snapshot| {
        Some(format!("swh:1:snp:{}", dir.id))
    })
}

fn iter_swhids_from_snp_branch(reader: Reader) -> impl ParallelIterator<Item = TextSwhid> {
    #[derive(OrcDeserialize, Default, Clone)]
    struct SnapshotBranch {
        target: String,
        target_type: String,
    }

    map_swhids(reader, |branch: SnapshotBranch| {
        match branch.target_type.as_bytes() {
            b"content" => Some(format!("swh:1:cnt:{}", branch.target)),
            b"directory" => Some(format!("swh:1:dir:{}", branch.target)),
            b"revision" => Some(format!("swh:1:rev:{}", branch.target)),
            b"release" => Some(format!("swh:1:rel:{}", branch.target)),
            b"alias" => None,
            _ => panic!("Unexpected snapshot target type: {:?}", branch.target_type),
        }
    })
}
pub fn iter_arcs(dataset_dir: &PathBuf) -> impl ParallelIterator<Item = (TextSwhid, TextSwhid)> {
    [].into_par_iter()
        .chain(
            get_dataset_readers(dataset_dir.clone(), "directory_entry")
                .into_par_iter()
                .flat_map(iter_arcs_from_dir_entry),
        )
        .chain(
            get_dataset_readers(dataset_dir.clone(), "origin_visit_status")
                .into_par_iter()
                .flat_map(iter_arcs_from_ovs),
        )
        .chain(
            get_dataset_readers(dataset_dir.clone(), "release")
                .into_par_iter()
                .flat_map(iter_arcs_from_rel),
        )
        .chain(
            get_dataset_readers(dataset_dir.clone(), "revision")
                .into_par_iter()
                .flat_map(iter_arcs_from_rev),
        )
        .chain(
            get_dataset_readers(dataset_dir.clone(), "revision_history")
                .into_par_iter()
                .flat_map(iter_arcs_from_rev_history),
        )
        .chain(
            get_dataset_readers(dataset_dir.clone(), "snapshot_branch")
                .into_par_iter()
                .flat_map(iter_arcs_from_snp_branch),
        )
}

fn map_arcs<T: OrcDeserialize + CheckableKind + OrcStruct + Clone + Send, F>(
    reader: Reader,
    f: F,
) -> impl ParallelIterator<Item = (TextSwhid, TextSwhid)>
where
    F: Fn(T) -> Option<(String, String)> + Send + Sync,
{
    RowIterator::<T>::new(&reader, (ORC_BATCH_SIZE as u64).try_into().unwrap())
        .expect("Could not open row reader")
        .expect("Unexpected schema")
        .par_bridge()
        .flat_map(f)
        .map(|(src_swhid, dst_swhid)| {
            (
                src_swhid.as_bytes().try_into().unwrap(),
                dst_swhid.as_bytes().try_into().unwrap(),
            )
        })
}

fn iter_arcs_from_dir_entry(
    reader: Reader,
) -> impl ParallelIterator<Item = (TextSwhid, TextSwhid)> {
    #[derive(OrcDeserialize, Default, Clone)]
    struct DirectoryEntry {
        directory_id: String,
        r#type: String,
        target: String,
    }

    map_arcs(reader, |entry: DirectoryEntry| {
        Some((
            format!("swh:1:dir:{}", entry.directory_id),
            match entry.r#type.as_bytes() {
                b"file" => format!("swh:1:cnt:{}", entry.target),
                b"dir" => format!("swh:1:dir:{}", entry.target),
                b"rev" => format!("swh:1:rev:{}", entry.target),
                _ => panic!("Unexpected directory entry type: {:?}", entry.r#type),
            },
        ))
    })
}

fn iter_arcs_from_ovs(reader: Reader) -> impl ParallelIterator<Item = (TextSwhid, TextSwhid)> {
    #[derive(OrcDeserialize, Default, Clone)]
    struct OriginVisitStatus {
        origin: String,
        snapshot: Option<String>,
    }

    map_arcs(reader, |ovs: OriginVisitStatus| {
        use sha1::Digest;
        let mut hasher = sha1::Sha1::new();
        hasher.update(ovs.origin.as_bytes());

        match ovs.snapshot {
            None => None,
            Some(ref snapshot) => Some((
                format!("swh:1:ori:{:x}", hasher.finalize(),),
                format!("swh:1:snp:{}", snapshot),
            )),
        }
    })
}

fn iter_arcs_from_rel(reader: Reader) -> impl ParallelIterator<Item = (TextSwhid, TextSwhid)> {
    #[derive(OrcDeserialize, Default, Clone)]
    struct Release {
        id: String,
        target: String,
        target_type: String,
    }

    map_arcs(reader, |entry: Release| {
        Some((
            format!("swh:1:rel:{}", entry.id),
            match entry.target_type.as_bytes() {
                b"content" => format!("swh:1:cnt:{}", entry.target),
                b"directory" => format!("swh:1:dir:{}", entry.target),
                b"revision" => format!("swh:1:rev:{}", entry.target),
                b"release" => format!("swh:1:rel:{}", entry.target),
                _ => panic!("Unexpected release target type: {:?}", entry.target_type),
            },
        ))
    })
}

fn iter_arcs_from_rev(reader: Reader) -> impl ParallelIterator<Item = (TextSwhid, TextSwhid)> {
    #[derive(OrcDeserialize, Default, Clone)]
    struct Revision {
        id: String,
        directory: String,
    }

    map_arcs(reader, |rev: Revision| {
        Some((
            format!("swh:1:rev:{}", rev.id),
            format!("swh:1:dir:{}", rev.directory),
        ))
    })
}

fn iter_arcs_from_rev_history(
    reader: Reader,
) -> impl ParallelIterator<Item = (TextSwhid, TextSwhid)> {
    #[derive(OrcDeserialize, Default, Clone)]
    struct RevisionParent {
        id: String,
        parent_id: String,
    }

    map_arcs(reader, |rev: RevisionParent| {
        Some((
            format!("swh:1:rev:{}", rev.id),
            format!("swh:1:rev:{}", rev.parent_id),
        ))
    })
}

fn iter_arcs_from_snp_branch(
    reader: Reader,
) -> impl ParallelIterator<Item = (TextSwhid, TextSwhid)> {
    #[derive(OrcDeserialize, Default, Clone)]
    struct SnapshotBranch {
        snapshot_id: String,
        target: String,
        target_type: String,
    }

    map_arcs(reader, |branch: SnapshotBranch| {
        let dst = match branch.target_type.as_bytes() {
            b"content" => Some(format!("swh:1:cnt:{}", branch.target)),
            b"directory" => Some(format!("swh:1:dir:{}", branch.target)),
            b"revision" => Some(format!("swh:1:rev:{}", branch.target)),
            b"release" => Some(format!("swh:1:rel:{}", branch.target)),
            b"alias" => None,
            _ => panic!("Unexpected snapshot target type: {:?}", branch.target_type),
        };
        dst.map(|dst| (format!("swh:1:snp:{}", branch.snapshot_id), dst))
    })
}

type EdgeStats = [[usize; SWHType::NUMBER_OF_TYPES]; SWHType::NUMBER_OF_TYPES];

pub fn count_edge_types(dataset_dir: &PathBuf) -> impl ParallelIterator<Item = EdgeStats> {
    [].into_par_iter()
        .chain(
            get_dataset_readers(dataset_dir.clone(), "directory_entry")
                .into_par_iter()
                .map(count_edge_types_from_dir),
        )
        .chain(
            get_dataset_readers(dataset_dir.clone(), "origin_visit_status")
                .into_par_iter()
                .map(count_edge_types_from_ovs),
        )
        .chain(
            get_dataset_readers(dataset_dir.clone(), "release")
                .into_par_iter()
                .map(count_edge_types_from_rel),
        )
        .chain(
            get_dataset_readers(dataset_dir.clone(), "revision")
                .into_par_iter()
                .map(count_dir_edge_types_from_rev),
        )
        .chain(
            get_dataset_readers(dataset_dir.clone(), "revision_history")
                .into_par_iter()
                .map(count_parent_edge_types_from_rev),
        )
        .chain(
            get_dataset_readers(dataset_dir.clone(), "snapshot_branch")
                .into_par_iter()
                .map(count_edge_types_from_snp),
        )
}

fn for_each_edge<T: OrcDeserialize + CheckableKind + OrcStruct + Clone, F>(reader: Reader, f: F)
where
    F: FnMut(T),
{
    RowIterator::<T>::new(&reader, (ORC_BATCH_SIZE as u64).try_into().unwrap())
        .expect("Could not open row reader")
        .expect("Unexpected schema")
        .for_each(f)
}

fn inc(stats: &mut EdgeStats, src_type: SWHType, dst_type: SWHType) {
    stats[src_type as usize][dst_type as usize] += 1;
}

fn count_edge_types_from_dir(reader: Reader) -> EdgeStats {
    let mut stats = EdgeStats::default();

    #[derive(OrcDeserialize, Default, Clone)]
    struct DirectoryEntry {
        r#type: String,
    }

    for_each_edge(reader, |entry: DirectoryEntry| {
        match entry.r#type.as_bytes() {
            b"file" => {
                inc(&mut stats, SWHType::Directory, SWHType::Content);
            }
            b"dir" => {
                inc(&mut stats, SWHType::Directory, SWHType::Directory);
            }
            b"rev" => {
                inc(&mut stats, SWHType::Directory, SWHType::Revision);
            }
            _ => panic!("Unexpected directory entry type: {:?}", entry.r#type),
        }
    });

    stats
}

fn count_edge_types_from_ovs(reader: Reader) -> EdgeStats {
    let mut stats = EdgeStats::default();

    #[derive(OrcDeserialize, Default, Clone)]
    struct OriginVisitStatus {
        snapshot: Option<String>,
    }

    for_each_edge(reader, |ovs: OriginVisitStatus| {
        if ovs.snapshot.is_some() {
            inc(&mut stats, SWHType::Origin, SWHType::Snapshot)
        }
    });

    stats
}

fn count_dir_edge_types_from_rev(reader: Reader) -> EdgeStats {
    let mut stats = EdgeStats::default();

    stats[SWHType::Revision as usize][SWHType::Directory as usize] += reader.row_count() as usize;

    stats
}

fn count_parent_edge_types_from_rev(reader: Reader) -> EdgeStats {
    let mut stats = EdgeStats::default();

    stats[SWHType::Revision as usize][SWHType::Revision as usize] += reader.row_count() as usize;

    stats
}

fn count_edge_types_from_rel(reader: Reader) -> EdgeStats {
    let mut stats = EdgeStats::default();
    #[derive(OrcDeserialize, Default, Clone)]
    struct Release {
        target_type: String,
    }

    for_each_edge(reader, |entry: Release| {
        match entry.target_type.as_bytes() {
            b"content" => {
                inc(&mut stats, SWHType::Release, SWHType::Content);
            }
            b"directory" => {
                inc(&mut stats, SWHType::Release, SWHType::Directory);
            }
            b"revision" => {
                inc(&mut stats, SWHType::Release, SWHType::Revision);
            }
            b"release" => {
                inc(&mut stats, SWHType::Release, SWHType::Release);
            }
            _ => panic!("Unexpected directory entry type: {:?}", entry.target_type),
        }
    });

    stats
}

fn count_edge_types_from_snp(reader: Reader) -> EdgeStats {
    let mut stats = EdgeStats::default();

    #[derive(OrcDeserialize, Default, Clone)]
    struct SnapshotBranch {
        target_type: String,
    }

    for_each_edge(reader, |branch: SnapshotBranch| {
        match branch.target_type.as_bytes() {
            b"content" => {
                inc(&mut stats, SWHType::Snapshot, SWHType::Content);
            }
            b"directory" => {
                inc(&mut stats, SWHType::Snapshot, SWHType::Directory);
            }
            b"revision" => {
                inc(&mut stats, SWHType::Snapshot, SWHType::Revision);
            }
            b"release" => {
                inc(&mut stats, SWHType::Snapshot, SWHType::Release);
            }
            b"alias" => {}
            _ => panic!("Unexpected snapshot branch type: {:?}", branch.target_type),
        }
    });

    stats
}
