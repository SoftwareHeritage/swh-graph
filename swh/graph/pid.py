# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import mmap
import os
import struct

from collections.abc import Mapping, Sequence
from enum import Enum

from swh.model.identifiers import PersistentId, parse_persistent_identifier


PID_BIN_FMT = 'BB20s'  # 2 unsigned chars + 20 bytes
INT_BIN_FMT = '>q'     # big endian, 8-byte integer
PID_BIN_SIZE = 22      # in bytes
INT_BIN_SIZE = 8       # in bytes


class PidType(Enum):
    """types of existing PIDs, used to serialize PID type as a (char) integer

    note that the order does matter also for driving the binary search in
    PID-indexed maps

    """
    content = 1
    directory = 2
    origin = 3
    release = 4
    revision = 5
    snapshot = 6


def str_to_bytes(pid):
    """Convert a PID to a byte sequence

    The binary format used to represent PIDs as byte sequences is as follows:

    - 1 byte for the namespace version represented as a C `unsigned char`
    - 1 byte for the object type, as the int value of :class:`PidType` enums,
      represented as a C `unsigned char`
    - 20 bytes for the SHA1 digest as a byte sequence

    Args:
        pid (str): persistent identifier

    Returns:
        bytes (bytes): byte sequence representation of pid

    """
    pid = parse_persistent_identifier(pid)
    return struct.pack(PID_BIN_FMT, pid.scheme_version,
                       PidType[pid.object_type].value,
                       bytes.fromhex(pid.object_id))


def bytes_to_str(bytes):
    """Inverse function of :func:`str_to_bytes`

    See :func:`str_to_bytes` for a description of the binary PID format.

    Args:
        bytes (bytes): byte sequence representation of pid

    Returns:
        pid (str): persistent identifier

    """
    (version, type, bin_digest) = struct.unpack(PID_BIN_FMT, bytes)
    pid = PersistentId(object_type=PidType(type).name, object_id=bin_digest)
    return str(pid)


class _OnDiskMap():
    """mmap-ed on-disk sequence of fixed size records

    """

    def __init__(self, record_size, fname):
        """open an existing on-disk map

        Args:
            record_size (int): size of each record in bytes
            fname (str): path to the on-disk map

        """
        self.record_size = record_size
        self.size = os.path.getsize(fname)
        (self.length, remainder) = divmod(self.size, record_size)
        if remainder:
            raise ValueError(
                'map size {} is not a multiple of the record size {}'.format(
                    self.size, record_size))

        self.f = open(fname, 'rb')
        self.mm = mmap.mmap(self.f.fileno(), self.size, mmap.MAP_PRIVATE)

    def close(self):
        """close the map

        shuts down both the mmap and the underlying file descriptor

        """
        if not self.mm.closed:
            self.mm.close()
        if not self.f.closed:
            self.f.close()


class PidToIntMap(_OnDiskMap, Mapping):
    """memory mapped map from PID (:ref:`persistent-identifiers`) to a continuous
    range 0..N of (8-byte long) integers

    This is the converse mapping of :class:`IntToPidMap`.

    The on-disk serialization format is a sequence of fixed length (30 bytes)
    records with the following fields:

    - PID (22 bytes): binary PID representation as per :func:`str_to_bytes`
    - long (8 bytes): big endian long integer

    The records are sorted lexicographically by PID type and checksum, where
    type is the integer value of :class:`PidType`. PID lookup in the map is
    performed via binary search.  Hence a huge map with, say, 11 B entries,
    will require ~30 disk seeks.

    """

    # record binary format: PID + a big endian 8-byte big endian integer
    RECORD_BIN_FMT = '>' + PID_BIN_FMT + 'q'
    RECORD_SIZE = PID_BIN_SIZE + INT_BIN_SIZE

    def __init__(self, fname):
        """open an existing on-disk map

        Args:
            fname (str): path to the on-disk map

        """
        super().__init__(self.RECORD_SIZE, fname)

    def _get_bin_record(self, pos):
        """seek and return the (binary) record at a given (logical) position

        see :func:`_get_record` for an equivalent function with additional
        deserialization

        Args:
            pos: 0-based record number

        Returns:
            tuple: a pair `(pid, int)`, where pid and int are bytes

        """
        rec_pos = pos * self.RECORD_SIZE
        int_pos = rec_pos + PID_BIN_SIZE

        return (self.mm[rec_pos:int_pos],
                self.mm[int_pos:int_pos+INT_BIN_SIZE])

    def _get_record(self, pos):
        """seek and return the record at a given (logical) position

        moral equivalent of :func:`_get_bin_record`, with additional
        deserialization to non-bytes types

        Args:
            pos: 0-based record number

        Returns:
            tuple: a pair `(pid, int)`, where pid is a string-based PID and int
            an integer

        """
        (pid_bytes, int_bytes) = self._get_bin_record(pos)
        return (bytes_to_str(pid_bytes),
                struct.unpack(INT_BIN_FMT, int_bytes)[0])

    @classmethod
    def write_record(cls, f, pid, int):
        """write a logical record to a file-like object

        Args:
            f: file-like object to write the record to
            pid (str): textual PID
            int (int): PID integer identifier

        """
        f.write(str_to_bytes(pid))
        f.write(struct.pack(INT_BIN_FMT, int))

    def __getitem__(self, pid_str):
        """lookup the integer identifier of a PID

        Args:
            pid (str): the PID as a string

        Returns:
            int: the integer identifier of pid

        """
        if not isinstance(pid_str, str):
            raise TypeError('PID must be a str, not ' + type(pid_str))
        try:
            target = str_to_bytes(pid_str)  # desired PID as bytes
        except ValueError:
            raise ValueError('invalid PID: "{}"'.format(pid_str))

        min = 0
        max = self.length - 1
        while (min <= max):
            mid = (min + max) // 2
            (pid, int) = self._get_bin_record(mid)
            if pid < target:
                min = mid + 1
            elif pid > target:
                max = mid - 1
            else:  # pid == target
                return struct.unpack(INT_BIN_FMT, int)[0]

        raise KeyError(pid_str)

    def __iter__(self):
        for pos in range(self.length):
            yield self._get_record(pos)

    def __len__(self):
        return self.length


class IntToPidMap(_OnDiskMap, Sequence):
    """memory mapped map from a continuous range of 0..N (8-byte long) integers to
    PIDs (:ref:`persistent-identifiers`)

    This is the converse mapping of :class:`PidToIntMap`.

    The on-disk serialization format is a sequence of fixed length (22 bytes),
    where each record is the binary representation of PID as per
    :func:`str_to_bytes`.

    The records are sorted by long integer, so that integer lookup is possible
    via fixed-offset seek.

    """

    RECORD_BIN_FMT = PID_BIN_FMT
    RECORD_SIZE = PID_BIN_SIZE

    def __init__(self, fname):
        """open an existing on-disk map

        Args:
            fname (str): path to the on-disk map

        """
        super().__init__(self.RECORD_SIZE, fname)

    def _get_bin_record(self, pos):
        """seek and return the (binary) PID at a given (logical) position

        Args:
            pos: 0-based record number

        Returns:
            bytes: PID as a byte sequence

        """
        rec_pos = pos * self.RECORD_SIZE

        return self.mm[rec_pos:rec_pos+self.RECORD_SIZE]

    @classmethod
    def write_record(cls, f, pid):
        """write a PID to a file-like object

        Args:
            f: file-like object to write the record to
            pid (str): textual PID

        """
        f.write(str_to_bytes(pid))

    def __getitem__(self, pos):
        orig_pos = pos
        if pos < 0:
            pos = len(self) + pos
        if not (0 <= pos < len(self)):
            raise IndexError(orig_pos)

        return bytes_to_str(self._get_bin_record(pos))

    def __len__(self):
        return self.length
