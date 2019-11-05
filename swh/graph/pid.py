# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import mmap
import os
import struct

from collections.abc import MutableMapping
from enum import Enum
from mmap import MAP_SHARED, MAP_PRIVATE
from typing import BinaryIO, Iterator, Tuple

from swh.model.identifiers import PersistentId, parse_persistent_identifier


PID_BIN_FMT = 'BB20s'  # 2 unsigned chars + 20 bytes
INT_BIN_FMT = '>q'     # big endian, 8-byte integer
PID_BIN_SIZE = 22      # in bytes
INT_BIN_SIZE = 8       # in bytes


class PidType(Enum):
    """types of existing PIDs, used to serialize PID type as a (char) integer

    Note that the order does matter also for driving the binary search in
    PID-indexed maps. Integer values also matter, for compatibility with the
    Java layer.

    """
    content = 0
    directory = 1
    origin = 2
    release = 3
    revision = 4
    snapshot = 5


def str_to_bytes(pid_str: str) -> bytes:
    """Convert a PID to a byte sequence

    The binary format used to represent PIDs as 22-byte long byte sequences as
    follows:

    - 1 byte for the namespace version represented as a C `unsigned char`
    - 1 byte for the object type, as the int value of :class:`PidType` enums,
      represented as a C `unsigned char`
    - 20 bytes for the SHA1 digest as a byte sequence

    Args:
        pid: persistent identifier

    Returns:
        bytes: byte sequence representation of pid

    """
    pid = parse_persistent_identifier(pid_str)
    return struct.pack(PID_BIN_FMT, pid.scheme_version,
                       PidType[pid.object_type].value,
                       bytes.fromhex(pid.object_id))


def bytes_to_str(bytes: bytes) -> str:
    """Inverse function of :func:`str_to_bytes`

    See :func:`str_to_bytes` for a description of the binary PID format.

    Args:
        bytes: byte sequence representation of pid

    Returns:
        pid: persistent identifier

    """
    (version, type, bin_digest) = struct.unpack(PID_BIN_FMT, bytes)
    pid = PersistentId(object_type=PidType(type).name, object_id=bin_digest)
    return str(pid)


class _OnDiskMap():
    """mmap-ed on-disk sequence of fixed size records

    """

    def __init__(self, record_size: int, fname: str, mode: str = 'rb',
                 length: int = None):
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
        os_modes = {
            'rb': os.O_RDONLY,
            'wb': os.O_RDWR | os.O_CREAT,
            'rb+': os.O_RDWR
        }
        if mode not in os_modes:
            raise ValueError('invalid file open mode: ' + mode)
        new_map = (mode == 'wb')
        writable_map = mode in ['wb', 'rb+']

        self.record_size = record_size
        self.fd = os.open(fname, os_modes[mode])
        if new_map:
            if length is None:
                raise ValueError('missing length when creating new map')
            os.truncate(self.fd, length * self.record_size)

        self.size = os.path.getsize(fname)
        (self.length, remainder) = divmod(self.size, record_size)
        if remainder:
            raise ValueError(
                'map size {} is not a multiple of the record size {}'.format(
                    self.size, record_size))

        self.mm = mmap.mmap(
            self.fd, self.size,
            flags=MAP_SHARED if writable_map else MAP_PRIVATE)

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
        raise NotImplementedError('cannot delete records from fixed-size map')


class PidToNodeMap(_OnDiskMap, MutableMapping):
    """memory mapped map from PID (:ref:`persistent-identifiers`) to a continuous
    range 0..N of (8-byte long) integers

    This is the converse mapping of :class:`NodeToPidMap`.

    The on-disk serialization format is a sequence of fixed length (30 bytes)
    records with the following fields:

    - PID (22 bytes): binary PID representation as per :func:`str_to_bytes`
    - long (8 bytes): big endian long integer

    The records are sorted lexicographically by PID type and checksum, where
    type is the integer value of :class:`PidType`. PID lookup in the map is
    performed via binary search.  Hence a huge map with, say, 11 B entries,
    will require ~30 disk seeks.

    Note that, due to fixed size + ordering, it is not possible to create these
    maps by random writing. Hence, __setitem__ can be used only to *update* the
    value associated to an existing key, rather than to add a missing item. To
    create an entire map from scratch, you should do so *sequentially*, using
    static method :meth:`write_record` (or, at your own risk, by hand via the
    mmap :attr:`mm`).

    """

    # record binary format: PID + a big endian 8-byte big endian integer
    RECORD_BIN_FMT = '>' + PID_BIN_FMT + 'q'
    RECORD_SIZE = PID_BIN_SIZE + INT_BIN_SIZE

    def __init__(self, fname: str, mode: str = 'rb', length: int = None):
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
            a pair `(pid, int)`, where pid and int are bytes

        """
        rec_pos = pos * self.RECORD_SIZE
        int_pos = rec_pos + PID_BIN_SIZE

        return (self.mm[rec_pos:int_pos],
                self.mm[int_pos:int_pos+INT_BIN_SIZE])

    def _get_record(self, pos: int) -> Tuple[str, int]:
        """seek and return the record at a given (logical) position

        moral equivalent of :func:`_get_bin_record`, with additional
        deserialization to non-bytes types

        Args:
            pos: 0-based record number

        Returns:
            a pair `(pid, int)`, where pid is a string-based PID and int the
            corresponding integer identifier

        """
        (pid_bytes, int_bytes) = self._get_bin_record(pos)
        return (bytes_to_str(pid_bytes),
                struct.unpack(INT_BIN_FMT, int_bytes)[0])

    @classmethod
    def write_record(cls, f: BinaryIO, pid: str, int: int) -> None:
        """write a logical record to a file-like object

        Args:
            f: file-like object to write the record to
            pid: textual PID
            int: PID integer identifier

        """
        f.write(str_to_bytes(pid))
        f.write(struct.pack(INT_BIN_FMT, int))

    def _bisect_pos(self, pid_str: str) -> int:
        """bisect the position of the given identifier. If the identifier is
        not found, the position of the pid immediately after is returned.

        Args:
            pid_str: the pid as a string

        Returns:
            the logical record of the bisected position in the map

        """
        if not isinstance(pid_str, str):
            raise TypeError('PID must be a str, not {}'.format(type(pid_str)))
        try:
            target = str_to_bytes(pid_str)  # desired PID as bytes
        except ValueError:
            raise ValueError('invalid PID: "{}"'.format(pid_str))

        lo = 0
        hi = self.length - 1
        while lo < hi:
            mid = (lo + hi) // 2
            (pid, _value) = self._get_bin_record(mid)
            if pid < target:
                lo = mid + 1
            else:
                hi = mid
        return lo

    def _find(self, pid_str: str) -> Tuple[int, int]:
        """lookup the integer identifier of a pid and its position

        Args:
            pid_str: the pid as a string

        Returns:
            a pair `(pid, pos)` with pid integer identifier and its logical
            record position in the map

        """
        pos = self._bisect_pos(pid_str)
        pid_found, value = self._get_record(pos)
        if pid_found == pid_str:
            return (value, pos)
        raise KeyError(pid_str)

    def __getitem__(self, pid_str: str) -> int:
        """lookup the integer identifier of a PID

        Args:
            pid: the PID as a string

        Returns:
            the integer identifier of pid

        """
        return self._find(pid_str)[0]  # return element, ignore position

    def __setitem__(self, pid_str: str, int: str) -> None:
        (_pid, pos) = self._find(pid_str)  # might raise KeyError and that's OK

        rec_pos = pos * self.RECORD_SIZE
        int_pos = rec_pos + PID_BIN_SIZE
        self.mm[rec_pos:int_pos] = str_to_bytes(pid_str)
        self.mm[int_pos:int_pos+INT_BIN_SIZE] = struct.pack(INT_BIN_FMT, int)

    def __iter__(self) -> Iterator[Tuple[str, int]]:
        for pos in range(self.length):
            yield self._get_record(pos)

    def iter_prefix(self, prefix: str):
        swh, n, t, sha = prefix.split(':')
        sha = sha.ljust(40, '0')
        start_pid = ':'.join([swh, n, t, sha])
        start = self._bisect_pos(start_pid)
        for pos in range(start, self.length):
            pid, value = self._get_record(pos)
            if not pid.startswith(prefix):
                break
            yield pid, value

    def iter_type(self, pid_type: str) -> Iterator[Tuple[str, int]]:
        prefix = 'swh:1:{}:'.format(pid_type)
        yield from self.iter_prefix(prefix)


class NodeToPidMap(_OnDiskMap, MutableMapping):
    """memory mapped map from a continuous range of 0..N (8-byte long) integers to
    PIDs (:ref:`persistent-identifiers`)

    This is the converse mapping of :class:`PidToNodeMap`.

    The on-disk serialization format is a sequence of fixed length records (22
    bytes), each being the binary representation of a PID as per
    :func:`str_to_bytes`.

    The records are sorted by long integer, so that integer lookup is possible
    via fixed-offset seek.

    """

    RECORD_BIN_FMT = PID_BIN_FMT
    RECORD_SIZE = PID_BIN_SIZE

    def __init__(self, fname: str, mode: str = 'rb', length: int = None):
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
        """seek and return the (binary) PID at a given (logical) position

        Args:
            pos: 0-based record number

        Returns:
            PID as a byte sequence

        """
        rec_pos = pos * self.RECORD_SIZE

        return self.mm[rec_pos:rec_pos+self.RECORD_SIZE]

    @classmethod
    def write_record(cls, f: BinaryIO, pid: str) -> None:
        """write a PID to a file-like object

        Args:
            f: file-like object to write the record to
            pid: textual PID

        """
        f.write(str_to_bytes(pid))

    def __getitem__(self, pos: int) -> str:
        orig_pos = pos
        if pos < 0:
            pos = len(self) + pos
        if not (0 <= pos < len(self)):
            raise IndexError(orig_pos)

        return bytes_to_str(self._get_bin_record(pos))

    def __setitem__(self, pos: int, pid: str) -> None:
        rec_pos = pos * self.RECORD_SIZE
        self.mm[rec_pos:rec_pos+self.RECORD_SIZE] = str_to_bytes(pid)

    def __iter__(self) -> Iterator[Tuple[int, str]]:
        for pos in range(self.length):
            yield (pos, self[pos])
