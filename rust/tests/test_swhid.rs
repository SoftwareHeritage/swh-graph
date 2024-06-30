// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use anyhow::Result;
use swh_graph::swhid;
use swh_graph::{NodeType, SWHID};

#[test]
fn test_swhid_from_string_ok() -> Result<()> {
    assert_eq!(
        SWHID::try_from("swh:1:rel:0000000000000000000000000000000000000010").unwrap(),
        SWHID {
            namespace_version: 1,
            node_type: NodeType::Release,
            hash: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x10]
        }
    );
    Ok(())
}

#[test]
fn test_swhid_from_string_err() -> Result<()> {
    for s in [
        "ftp:1:rel:0000000000000000000000000000000000000010",
        "swh:2:rel:0000000000000000000000000000000000000010",
        "swh:1:rel:00000000000000000000000000000000000001",
        "swh:1:rel:000000000000000000000000000000000000001",
        "swh:1:rel:00000000000000000000000000000000000000100",
        "swh:1:rel:000000000000000000000000000000000000001000",
        "swh:1:rel:000000000000000000000000000000000000000g",
    ] {
        assert!(SWHID::try_from(s).is_err());
    }

    Ok(())
}

#[test]
fn test_swhid_to_bytes() -> Result<()> {
    assert_eq!(
        <[u8; SWHID::BYTES_SIZE]>::from(SWHID {
            namespace_version: 1,
            node_type: NodeType::Release,
            hash: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x10]
        }),
        [1, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x10]
    );
    Ok(())
}

#[test]
fn test_swhid_from_bytes_ok() -> Result<()> {
    assert_eq!(
        SWHID::try_from([1, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x10])
            .unwrap(),
        SWHID {
            namespace_version: 1,
            node_type: NodeType::Release,
            hash: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x10]
        }
    );
    Ok(())
}

#[test]
fn test_swhid_from_bytes_err() -> Result<()> {
    assert!(
        SWHID::try_from([2, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x10])
            .is_err(),
    );
    Ok(())
}

#[test]
fn test_macro() -> Result<()> {
    assert_eq!(
        swhid!(swh:1:cnt:0101010101010101010101010101010101010101),
        SWHID {
            namespace_version: 1,
            node_type: NodeType::Content,
            hash: [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
        }
    );

    assert_eq!(
        swhid!(swh:1:dir:0000000000000000000000000000000000000010),
        SWHID {
            namespace_version: 1,
            node_type: NodeType::Directory,
            hash: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x10]
        }
    );

    assert_eq!(
        swhid!(swh:1:rev:0000000000000000000000000000000000000010),
        SWHID {
            namespace_version: 1,
            node_type: NodeType::Revision,
            hash: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x10]
        }
    );

    assert_eq!(
        swhid!(swh:1:rel:0000000000000000000000000000000000000042),
        SWHID {
            namespace_version: 1,
            node_type: NodeType::Release,
            hash: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x42]
        }
    );

    assert_eq!(
        swhid!(swh:1:snp:0000000000000000000000000000000000000042),
        SWHID {
            namespace_version: 1,
            node_type: NodeType::Snapshot,
            hash: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x42]
        }
    );

    assert_eq!(
        swhid!(swh:1:ori:0000000000000000000000000000000000000042),
        SWHID {
            namespace_version: 1,
            node_type: NodeType::Origin,
            hash: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0x42]
        }
    );

    Ok(())
}
