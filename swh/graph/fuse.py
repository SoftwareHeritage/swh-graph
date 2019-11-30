# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import errno
import itertools as it
import stat
import time
import trio
import os
import pyfuse3

from functools import lru_cache
from pathlib import Path
from pyfuse3 import ROOT_INODE
from typing import Dict, Generator, Optional, Union

from swh.graph.client import RemoteGraphClient
from swh.model.identifiers import PersistentId, parse_persistent_identifier, \
    DIRECTORY, CONTENT


FILE_MODE = 0o444  # read-only file
DIR_MODE = 0o555   # read-only directory

INODE_CACHE_SIZE = 1024  # number of inode -> PersistentId pairs to cache


class GraphFs(pyfuse3.Operations):

    def __init__(self, client: RemoteGraphClient, root_pid: PersistentId):
        super(GraphFs, self).__init__()

        # TODO check if root_pid actually exists in the graph
        self._pid2inode: Dict[str, int] = {str(root_pid): ROOT_INODE}
        self._inode2pid: Dict[int, str] = {ROOT_INODE: str(root_pid)}
        self._next_inode: int = ROOT_INODE + 1
        self.time_ns: int = time.time_ns()  # start time, used as timestamp
        self.client = client

    def _alloc_inode(self, pid: Union[str, PersistentId]) -> int:
        """allocate a fresh inode for a given PID"""
        if isinstance(pid, PersistentId):
            str_pid = str(pid)
        else:
            str_pid = pid

        try:
            return self._pid2inode[str_pid]
        except KeyError:
            inode = self._next_inode
            self._next_inode += 1
            self._pid2inode[str_pid] = inode
            self._inode2pid[inode] = str_pid

            # TODO add inode recycling with invocation to invalidate_inode when
            # the dicts get too big

            return inode

    def inode_of_pid(self, pid: Union[str, PersistentId]) -> int:
        """lookup the inode corresponding to a given PID"""
        try:
            if isinstance(pid, PersistentId):
                str_pid = str(pid)
            else:
                str_pid = pid
            return self._pid2inode[str_pid]
        except KeyError:
            raise pyfuse3.FUSEError(errno.ENOENT)

    @lru_cache(maxsize=INODE_CACHE_SIZE)
    def pid_of_inode(self, inode: int) -> PersistentId:
        """lookup the PID corresponding to a given inode"""
        try:
            return parse_persistent_identifier(self._inode2pid[inode])
        except KeyError:
            raise pyfuse3.FUSEError(errno.ENOENT)

    def fsname_of_pid(self, pid: Union[str, PersistentId]) -> bytes:
        if isinstance(pid, PersistentId):
            str_pid = str(pid)
        else:
            str_pid = pid
        return os.fsencode(str_pid)

    def attrs_of_pid(self, pid: Union[str, PersistentId],
                     inode: Optional[int]) -> pyfuse3.EntryAttributes:
        attrs = pyfuse3.EntryAttributes()

        if isinstance(pid, PersistentId):
            pid_ = pid
        else:
            pid_ = parse_persistent_identifier(pid)

        if pid_.object_type == CONTENT:
            attrs.st_mode = (stat.S_IFREG | FILE_MODE)
            attrs.st_size = 0  # TODO use storage to fetch actual size
        else:
            attrs.st_mode = (stat.S_IFDIR | DIR_MODE)
            attrs.st_size = 0

        attrs.st_size = 0  # should be overridden later for files
        attrs.st_atime_ns = self.time_ns
        attrs.st_ctime_ns = self.time_ns
        attrs.st_mtime_ns = self.time_ns
        attrs.st_gid = os.getgid()
        attrs.st_uid = os.getuid()
        attrs.st_ino = inode if inode is not None else self.inode_of_pid(pid_)

        return attrs

    async def getattr(self, inode, ctx):
        return self.attrs_of_pid(self.pid_of_inode(inode), inode)

    async def opendir(self, inode, ctx):
        return inode  # (re)use inodes as directory handles

    async def readdir(self, inode, offset, token):
        pid = self.pid_of_inode(inode)

        if pid.object_type == DIRECTORY:
            # TODO we should cache neighbors() response to avoid re-fetching
            # them when offset != 0, which will be for very large dirs, the
            # worst case to re-fetch...
            entries: Generator[str] = self.client.neighbors(str(pid))
            next_id = offset + 1
            for entry_pid in it.islice(entries, offset, None):
                inode = self._alloc_inode(entry_pid)
                if not pyfuse3.readdir_reply(token,
                                             self.fsname_of_pid(entry_pid),
                                             self.attrs_of_pid(entry_pid,
                                                               inode),
                                             next_id):
                    break
                next_id += 1
        elif pid.object_type == CONTENT:
            raise pyfuse3.FUSEError(errno.ENOTDIR)
        else:  # TODO add virtual dir support for other object types
            raise pyfuse3.FUSEError(errno.ENOTDIR)

    async def lookup(self, dir_inode, fs_name, ctx):
        self.pid_of_inode(dir_inode)  # will barf if dir_inode doesn't exist
        entry_pid = os.fsdecode(fs_name)
        # TODO check if entry_pid actually exists in the graph
        inode = self._alloc_inode(entry_pid)

        return self.attrs_of_pid(entry_pid, inode)


def main(graph_cli: RemoteGraphClient, pid: PersistentId, path: Path) -> None:
    fs = GraphFs(client=graph_cli, root_pid=pid)

    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=swh')
    # if options.debug_fuse:
    #     fuse_options.add('debug')
    pyfuse3.init(fs, path, fuse_options)

    try:
        trio.run(pyfuse3.main)
    finally:
        pyfuse3.close(unmount=True)
