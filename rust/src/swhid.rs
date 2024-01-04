// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use crate::SWHType;
use anyhow::{bail, Context};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C)]
/// SoftWare Heritage persistent IDentifiers
///
/// A SWHID consists of two separate parts, a mandatory core identifier that
/// can point to any software artifact (or “object”) available in the Software
/// Heritage archive, and an optional list of qualifiers that allows to specify
/// the context where the object is meant to be seen and point to a subpart of
/// the object itself.
///
/// # Reference
/// - <https://docs.softwareheritage.org/devel/swh-model/persistent-identifiers.html>
/// - Roberto Di Cosmo, Morane Gruenpeter, Stefano Zacchiroli. [Identifiers for Digital Objects: the Case of Software Source Code Preservation](https://hal.archives-ouvertes.fr/hal-01865790v4). In Proceedings of iPRES 2018: 15th International Conference on Digital Preservation, Boston, MA, USA, September 2018, 9 pages.
/// - Roberto Di Cosmo, Morane Gruenpeter, Stefano Zacchiroli. [Referencing Source Code Artifacts: a Separate Concern in Software Citation](https://arxiv.org/abs/2001.08647). In Computing in Science and Engineering, volume 22, issue 2, pages 33-43. ISSN 1521-9615, IEEE. March 2020.
pub struct SWHID {
    /// Namespace Version
    pub namespace_version: u8,
    /// Node type
    pub node_type: SWHType,
    /// SHA1 has of the node
    pub hash: [u8; 20],
}

impl SWHID {
    /// The size of the binary representation of a SWHID
    pub const BYTES_SIZE: usize = 22;
}

impl core::fmt::Display for SWHID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "swh:{}:{}:",
            self.namespace_version,
            self.node_type.to_str(),
        )?;
        for byte in self.hash.iter() {
            write!(f, "{:02x}", byte)?;
        }
        Ok(())
    }
}

/// Parse a SWHID from bytes, while the SWHID struct has the exact same layout
/// and thus it can be read directly from bytes, this function is provided for
/// completeness and safety because we can check the namespace version is
/// supported.
impl TryFrom<[u8; SWHID::BYTES_SIZE]> for SWHID {
    type Error = anyhow::Error;
    fn try_from(value: [u8; SWHID::BYTES_SIZE]) -> std::result::Result<Self, Self::Error> {
        let namespace_version = value[0];
        if namespace_version != 1 {
            bail!("Unsupported SWHID namespace version: {}", namespace_version);
        }
        let node_type = SWHType::try_from(value[1])?;
        let mut hash = [0; 20];
        hash.copy_from_slice(&value[2..]);
        Ok(Self {
            namespace_version,
            node_type,
            hash,
        })
    }
}

/// Parse a SWHID from the string representation
impl TryFrom<&str> for SWHID {
    type Error = anyhow::Error;
    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        let mut tokens = value.splitn(4, ':');
        let Some(namespace) = tokens.next() else {
            bail!("SWHID is empty");
        };
        if namespace != "swh" {
            bail!("Unsupported SWHID namespace: {}", namespace);
        }
        let Some(namespace_version) = tokens.next() else {
            bail!("SWHID is too short (no namespace version)")
        };
        if namespace_version != "1" {
            bail!("Unsupported SWHID namespace version: {}", namespace_version);
        }
        let Some(node_type) = tokens.next() else {
            bail!("SWHID is too short (no object type)")
        };
        let Some(hex_hash) = tokens.next() else {
            bail!("SWHID is too short (no object hash)")
        };
        if hex_hash.len() < 40 {
            bail!(
                "SWHID is too short (object hash has {} digits instead of 40)",
                hex_hash.len()
            )
        }
        if hex_hash.len() > 40 {
            bail!(
                "SWHID is too long (object hash has {} digits instead of 40)",
                hex_hash.len()
            )
        }
        let node_type = SWHType::try_from(node_type)?;
        let mut hash = [0u8; 20];
        faster_hex::hex_decode(hex_hash.as_bytes(), &mut hash)
            .with_context(|| format!("SWHID hash is not hexadecimal: {}", hex_hash))?;
        Ok(Self {
            namespace_version: 1,
            node_type,
            hash,
        })
    }
}

impl From<SWHID> for [u8; SWHID::BYTES_SIZE] {
    fn from(value: SWHID) -> Self {
        let mut result = [0; SWHID::BYTES_SIZE];
        result[0] = value.namespace_version;
        result[1] = value.node_type as u8;
        result[2..].copy_from_slice(&value.hash);
        result
    }
}
