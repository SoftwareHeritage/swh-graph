// Copyright (C) 2023-2024  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

use thiserror::Error;

use crate::SWHType;

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

#[derive(Error, Debug)]
pub enum BinSWHIDDeserializationError {
    #[error("Unsupported SWHID version: {0}")]
    Version(u8),
    #[error("Invalid SWHID type: {0}")]
    Type(u8),
}

/// Parse a SWHID from bytes, while the SWHID struct has the exact same layout
/// and thus it can be read directly from bytes, this function is provided for
/// completeness and safety because we can check the namespace version is
/// supported.
impl TryFrom<[u8; SWHID::BYTES_SIZE]> for SWHID {
    type Error = BinSWHIDDeserializationError;
    fn try_from(value: [u8; SWHID::BYTES_SIZE]) -> std::result::Result<Self, Self::Error> {
        use BinSWHIDDeserializationError::*;

        let namespace_version = value[0];
        if namespace_version != 1 {
            return Err(Version(namespace_version));
        }
        let node_type = SWHType::try_from(value[1]).map_err(Type)?;
        let mut hash = [0; 20];
        hash.copy_from_slice(&value[2..]);
        Ok(Self {
            namespace_version,
            node_type,
            hash,
        })
    }
}

#[derive(Error, Debug, PartialEq, Eq, Hash)]
pub enum StrSWHIDDeserializationError {
    #[error("Invalid syntax: {0}")]
    Syntax(&'static str),
    #[error("Unsupported SWHID namespace: {0}")]
    Namespace(String),
    #[error("Unsupported SWHID version: {0}")]
    Version(String),
    #[error("Expected hash length to be {expected}, got {got}")]
    HashLength { expected: usize, got: usize },
    #[error("Invalid SWHID type: {0}")]
    Type(String),
    #[error("SWHID hash is not hexadecimal: {0}")]
    HashAlphabet(String),
}

/// Parse a SWHID from the string representation
impl TryFrom<&str> for SWHID {
    type Error = StrSWHIDDeserializationError;
    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        use StrSWHIDDeserializationError::*;

        let mut tokens = value.splitn(4, ':');
        let Some(namespace) = tokens.next() else {
            return Err(Syntax("SWHID is empty"));
        };
        if namespace != "swh" {
            return Err(Namespace(namespace.to_string()));
        }
        let Some(namespace_version) = tokens.next() else {
            return Err(Syntax("SWHID is too short (no namespace version)"));
        };
        if namespace_version != "1" {
            return Err(Version(namespace_version.to_string()));
        }
        let Some(node_type) = tokens.next() else {
            return Err(Syntax("SWHID is too short (no object type)"));
        };
        let Some(hex_hash) = tokens.next() else {
            return Err(Syntax("SWHID is too short (no object hash)"));
        };
        if hex_hash.len() != 40 {
            return Err(HashLength {
                expected: 40,
                got: hex_hash.len(),
            });
        }
        let node_type = SWHType::try_from(node_type).map_err(|e| Type(e.to_string()))?;
        let mut hash = [0u8; 20];
        faster_hex::hex_decode(hex_hash.as_bytes(), &mut hash)
            .map_err(|_| HashAlphabet(hex_hash.to_string()))?;
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

#[cfg(feature = "serde")]
impl serde::Serialize for SWHID {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

#[cfg(feature = "serde")]
impl<'de> serde::Deserialize<'de> for SWHID {
    fn deserialize<D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        use serde::de::Error;
        <&str>::deserialize(deserializer).and_then(|s| s.try_into().map_err(D::Error::custom))
    }
}

#[doc(hidden)]
#[cfg(feature = "macros")]
/// Helper function for [`swhid!()`]
pub const fn __parse_swhid(node_type: crate::SWHType, hash: &'static str) -> SWHID {
    use const_panic::unwrap_ok;
    unwrap_ok!(match const_hex::const_decode_to_array(hash.as_bytes()) {
        Ok(hash) => Ok(SWHID {
            namespace_version: 1,
            node_type,
            hash
        }),
        Err(_) => Err("invalid SWHID hash"),
    })
}

#[cfg(feature = "macros")]
/// A SWHID literal checked at compile time
///
/// # Examples
///
/// ```
/// use swh_graph::swhid;
/// assert_eq!(
///     swhid!(swh:1:rev:0000000000000000000000000000000000000004).to_string(),
///     "swh:1:rev:0000000000000000000000000000000000000004".to_string(),
/// );
/// ```
///
/// ```compile_fail
/// use swh_graph::swhid;
/// swhid!(swh:1:rev:ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ);
/// ```
///
/// ```compile_fail
/// use swh_graph::swhid;
/// swhid!(swh:1:rev:00000000000000000000000000000000000004);
/// ```
#[macro_export]
macro_rules! swhid {
    (swh:1:cnt:$hash:literal) => {{
        const swhid: ::swh_graph::SWHID = {
            let hash: &str = stringify!($hash);
            ::swh_graph::__parse_swhid(::swh_graph::SWHType::Content, hash)
        };
        swhid
    }};
    (swh:1:dir:$hash:literal) => {{
        const swhid: ::swh_graph::SWHID = {
            let hash: &str = stringify!($hash);
            ::swh_graph::__parse_swhid(::swh_graph::SWHType::Directory, hash)
        };
        swhid
    }};
    (swh:1:rev:$hash:literal) => {{
        const swhid: ::swh_graph::SWHID = {
            let hash: &str = stringify!($hash);
            ::swh_graph::__parse_swhid(::swh_graph::SWHType::Revision, hash)
        };
        swhid
    }};
    (swh:1:rel:$hash:literal) => {{
        const swhid: ::swh_graph::SWHID = {
            let hash: &str = stringify!($hash);
            ::swh_graph::__parse_swhid(::swh_graph::SWHType::Release, hash)
        };
        swhid
    }};
    (swh:1:snp:$hash:literal) => {{
        const swhid: ::swh_graph::SWHID = {
            let hash: &str = stringify!($hash);
            ::swh_graph::__parse_swhid(::swh_graph::SWHType::Snapshot, hash)
        };
        swhid
    }};
    (swh:1:ori:$hash:literal) => {{
        const swhid: ::swh_graph::SWHID = {
            let hash: &str = stringify!($hash);
            ::swh_graph::__parse_swhid(::swh_graph::SWHType::Origin, hash)
        };
        swhid
    }};
}
