# Copyright (C) 2019-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

from collections.abc import MutableMapping
from enum import Enum
import mmap
from mmap import MAP_SHARED, PROT_READ, PROT_WRITE
import os
import struct
from typing import BinaryIO, Iterator, Tuple

from swh.model.identifiers import ExtendedObjectType, ExtendedSWHID

SWHID_BIN_FMT = "BB20s"  # 2 unsigned chars + 20 bytes
INT_BIN_FMT = ">q"  # big endian, 8-byte integer
SWHID_BIN_SIZE = 22  # in bytes
INT_BIN_SIZE = 8  # in bytes


class SwhidType(Enum):
    """types of existing SWHIDs, used to serialize ExtendedSWHID type as a (char)
    integer

    Note that the order does matter also for driving the binary search in
    SWHID-indexed maps. Integer values also matter, for compatibility with the
    Java layer.

    """

    content = 0
    directory = 1
    origin = 2
    release = 3
    revision = 4
    snapshot = 5

    @classmethod
    def from_extended_object_type(cls, object_type: ExtendedObjectType) -> SwhidType:
        return cls[object_type.name.lower()]

    def to_extended_object_type(self) -> ExtendedObjectType:
        return ExtendedObjectType[SwhidType(self).name.upper()]


def str_to_bytes(swhid_str: str) -> bytes:
    """Convert a SWHID to a byte sequence

    The binary format used to represent SWHIDs as 22-byte long byte sequences as
    follows:

    - 1 byte for the namespace version represented as a C `unsigned char`
    - 1 byte for the object type, as the int value of :class:`SwhidType` enums,
      represented as a C `unsigned char`
    - 20 bytes for the SHA1 digest as a byte sequence

    Args:
        swhid: persistent identifier

    Returns:
        bytes: byte sequence representation of swhid

    """
    swhid = ExtendedSWHID.from_string(swhid_str)
    return struct.pack(
        SWHID_BIN_FMT,
        swhid.scheme_version,
        SwhidType.from_extended_object_type(swhid.object_type).value,
        swhid.object_id,
    )


def bytes_to_str(bytes: bytes) -> str:
    """Inverse function of :func:`str_to_bytes`

    See :func:`str_to_bytes` for a description of the binary SWHID format.

    Args:
        bytes: byte sequence representation of swhid

    Returns:
        swhid: persistent identifier

    """
    (version, type, bin_digest) = struct.unpack(SWHID_BIN_FMT, bytes)
    swhid = ExtendedSWHID(
        object_type=SwhidType(type).to_extended_object_type(), object_id=bin_digest
    )
    return str(swhid)


class _OnDiskMap:
    """mmap-ed on-disk sequence of fixed size records"""

    def __init__(
        self, record_size: int, fname: str, mode: str = "rb", length: int = None
    ):
        """open an existing on-disk map

        Args:
            record_size: size of each record in bytes
            fname: path to the on-disk map
            mode: file open mode, usually either 'rb' for read-only maps, 'wb'
                for creating new maps, or 'rb+' for updating existing ones
                (default: 'rb')
            length: map size in number of logical records; used to initialize
                writable maps at creation time. Must be given when mode is 'wb'
                and the map doesn't exist on disk; ignored otherwise

        """
        os_modes = {"rb": os.O_RDONLY, "wb": os.O_RDWR | os.O_CREAT, "rb+": os.O_RDWR}
        if mode not in os_modes:
            raise ValueError("invalid file open mode: " + mode)
        new_map = mode == "wb"
        writable_map = mode in ["wb", "rb+"]

        self.record_size = record_size
        self.fd = os.open(fname, os_modes[mode])
        if new_map:
            if length is None:
                raise ValueError("missing length when creating new map")
            os.truncate(self.fd, length * self.record_size)

        self.size = os.path.getsize(fname)
        (self.length, remainder) = divmod(self.size, record_size)
        if remainder:
            raise ValueError(
                "map size {} is not a multiple of the record size {}".format(
                    self.size, record_size
                )
            )

        self.mm = mmap.mmap(
            self.fd,
            self.size,
            prot=(PROT_READ | PROT_WRITE if writable_map else PROT_READ),
            flags=MAP_SHARED,
        )

    def close(self) -> None:
        """close the map

        shuts down both the mmap and the underlying file descriptor

        """
        if not self.mm.closed:
            self.mm.close()
        os.close(self.fd)

    def __len__(self) -> int:
        return self.length

    def __delitem__(self, pos: int) -> None:
        raise NotImplementedError("cannot delete records from fixed-size map")


class SwhidToNodeMap(_OnDiskMap, MutableMapping):
    """memory mapped map from :ref:`SWHIDs <persistent-identifiers>` to a
    continuous range 0..N of (8-byte long) integers

    This is the converse mapping of :class:`NodeToSwhidMap`.

    The on-disk serialization format is a sequence of fixed length (30 bytes)
    records with the following fields:

    - SWHID (22 bytes): binary SWHID representation as per :func:`str_to_bytes`
    - long (8 bytes): big endian long integer

    The records are sorted lexicographically by SWHID type and checksum, where
    type is the integer value of :class:`SwhidType`. SWHID lookup in the map is
    performed via binary search.  Hence a huge map with, say, 11 B entries,
    will require ~30 disk seeks.

    Note that, due to fixed size + ordering, it is not possible to create these
    maps by random writing. Hence, __setitem__ can be used only to *update* the
    value associated to an existing key, rather than to add a missing item. To
    create an entire map from scratch, you should do so *sequentially*, using
    static method :meth:`write_record` (or, at your own risk, by hand via the
    mmap :attr:`mm`).

    """

    # record binary format: SWHID + a big endian 8-byte big endian integer
    RECORD_BIN_FMT = ">" + SWHID_BIN_FMT + "q"
    RECORD_SIZE = SWHID_BIN_SIZE + INT_BIN_SIZE

    def __init__(self, fname: str, mode: str = "rb", length: int = None):
        """open an existing on-disk map

        Args:
            fname: path to the on-disk map
            mode: file open mode, usually either 'rb' for read-only maps, 'wb'
                for creating new maps, or 'rb+' for updating existing ones
                (default: 'rb')
            length: map size in number of logical records; used to initialize
                read-write maps at creation time. Must be given when mode is
                'wb'; ignored otherwise

        """
        super().__init__(self.RECORD_SIZE, fname, mode=mode, length=length)

    def _get_bin_record(self, pos: int) -> Tuple[bytes, bytes]:
        """seek and return the (binary) record at a given (logical) position

        see :func:`_get_record` for an equivalent function with additional
        deserialization

        Args:
            pos: 0-based record number

        Returns:
            a pair `(swhid, int)`, where swhid and int are bytes

        """
        rec_pos = pos * self.RECORD_SIZE
        int_pos = rec_pos + SWHID_BIN_SIZE

        return (self.mm[rec_pos:int_pos], self.mm[int_pos : int_pos + INT_BIN_SIZE])

    def _get_record(self, pos: int) -> Tuple[str, int]:
        """seek and return the record at a given (logical) position

        moral equivalent of :func:`_get_bin_record`, with additional
        deserialization to non-bytes types

        Args:
            pos: 0-based record number

        Returns:
            a pair `(swhid, int)`, where swhid is a string-based SWHID and int the
            corresponding integer identifier

        """
        (swhid_bytes, int_bytes) = self._get_bin_record(pos)
        return (bytes_to_str(swhid_bytes), struct.unpack(INT_BIN_FMT, int_bytes)[0])

    @classmethod
    def write_record(cls, f: BinaryIO, swhid: str, int: int) -> None:
        """write a logical record to a file-like object

        Args:
            f: file-like object to write the record to
            swhid: textual SWHID
            int: SWHID integer identifier

        """
        f.write(str_to_bytes(swhid))
        f.write(struct.pack(INT_BIN_FMT, int))

    def _bisect_pos(self, swhid_str: str) -> int:
        """bisect the position of the given identifier. If the identifier is
        not found, the position of the swhid immediately after is returned.

        Args:
            swhid_str: the swhid as a string

        Returns:
            the logical record of the bisected position in the map

        """
        if not isinstance(swhid_str, str):
            raise TypeError("SWHID must be a str, not {}".format(type(swhid_str)))
        try:
            target = str_to_bytes(swhid_str)  # desired SWHID as bytes
        except ValueError:
            raise ValueError('invalid SWHID: "{}"'.format(swhid_str))

        lo = 0
        hi = self.length - 1
        while lo < hi:
            mid = (lo + hi) // 2
            (swhid, _value) = self._get_bin_record(mid)
            if swhid < target:
                lo = mid + 1
            else:
                hi = mid
        return lo

    def _find(self, swhid_str: str) -> Tuple[int, int]:
        """lookup the integer identifier of a swhid and its position

        Args:
            swhid_str: the swhid as a string

        Returns:
            a pair `(swhid, pos)` with swhid integer identifier and its logical
            record position in the map

        """
        pos = self._bisect_pos(swhid_str)
        swhid_found, value = self._get_record(pos)
        if swhid_found == swhid_str:
            return (value, pos)
        raise KeyError(swhid_str)

    def __getitem__(self, swhid_str: str) -> int:
        """lookup the integer identifier of a SWHID

        Args:
            swhid: the SWHID as a string

        Returns:
            the integer identifier of swhid

        """
        return self._find(swhid_str)[0]  # return element, ignore position

    def __setitem__(self, swhid_str: str, int: str) -> None:
        (_swhid, pos) = self._find(swhid_str)  # might raise KeyError and that's OK

        rec_pos = pos * self.RECORD_SIZE
        int_pos = rec_pos + SWHID_BIN_SIZE
        self.mm[rec_pos:int_pos] = str_to_bytes(swhid_str)
        self.mm[int_pos : int_pos + INT_BIN_SIZE] = struct.pack(INT_BIN_FMT, int)

    def __iter__(self) -> Iterator[Tuple[str, int]]:
        for pos in range(self.length):
            yield self._get_record(pos)

    def iter_prefix(self, prefix: str):
        swh, n, t, sha = prefix.split(":")
        sha = sha.ljust(40, "0")
        start_swhid = ":".join([swh, n, t, sha])
        start = self._bisect_pos(start_swhid)
        for pos in range(start, self.length):
            swhid, value = self._get_record(pos)
            if not swhid.startswith(prefix):
                break
            yield swhid, value

    def iter_type(self, swhid_type: str) -> Iterator[Tuple[str, int]]:
        prefix = "swh:1:{}:".format(swhid_type)
        yield from self.iter_prefix(prefix)


class NodeToSwhidMap(_OnDiskMap, MutableMapping):
    """memory mapped map from a continuous range of 0..N (8-byte long) integers to
    :ref:`SWHIDs <persistent-identifiers>`

    This is the converse mapping of :class:`SwhidToNodeMap`.

    The on-disk serialization format is a sequence of fixed length records (22
    bytes), each being the binary representation of a SWHID as per
    :func:`str_to_bytes`.

    The records are sorted by long integer, so that integer lookup is possible
    via fixed-offset seek.

    """

    RECORD_BIN_FMT = SWHID_BIN_FMT
    RECORD_SIZE = SWHID_BIN_SIZE

    def __init__(self, fname: str, mode: str = "rb", length: int = None):
        """open an existing on-disk map

        Args:
            fname: path to the on-disk map
            mode: file open mode, usually either 'rb' for read-only maps, 'wb'
                for creating new maps, or 'rb+' for updating existing ones
                (default: 'rb')
            size: map size in number of logical records; used to initialize
                read-write maps at creation time. Must be given when mode is
                'wb'; ignored otherwise
            length: passed to :class:`_OnDiskMap`

        """

        super().__init__(self.RECORD_SIZE, fname, mode=mode, length=length)

    def _get_bin_record(self, pos: int) -> bytes:
        """seek and return the (binary) SWHID at a given (logical) position

        Args:
            pos: 0-based record number

        Returns:
            SWHID as a byte sequence

        """
        rec_pos = pos * self.RECORD_SIZE

        return self.mm[rec_pos : rec_pos + self.RECORD_SIZE]

    @classmethod
    def write_record(cls, f: BinaryIO, swhid: str) -> None:
        """write a SWHID to a file-like object

        Args:
            f: file-like object to write the record to
            swhid: textual SWHID

        """
        f.write(str_to_bytes(swhid))

    def __getitem__(self, pos: int) -> str:
        orig_pos = pos
        if pos < 0:
            pos = len(self) + pos
        if not (0 <= pos < len(self)):
            raise IndexError(orig_pos)

        return bytes_to_str(self._get_bin_record(pos))

    def __setitem__(self, pos: int, swhid: str) -> None:
        rec_pos = pos * self.RECORD_SIZE
        self.mm[rec_pos : rec_pos + self.RECORD_SIZE] = str_to_bytes(swhid)

    def __iter__(self) -> Iterator[Tuple[int, str]]:
        for pos in range(self.length):
            yield (pos, self[pos])
