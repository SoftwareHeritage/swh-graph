# Copyright (C) 2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

r"""This module implements a shell-like command pipeline system in
pure-Python.

Pipelines are built like this:

>>> from swh.graph.shell import Command, Sink
>>> (
...     Command.echo("foo")
...     | Command.zstdmt()
...     | Command.cat("-", Command.echo("bar") | Command.zstdmt())
...     | Command.zstdcat()
...     > Sink()
... ).run().stdout
b'foo\nbar\n'

which is the equivalent of this bash command:

.. code-block:: bash

    echo foo \
    | zstdmt \
    | cat - <(echo bar | zstdmt) \
    | zstdcat

:class:`Sink` is mainly meant for tests; it causes ``.run().stdout`` to
return the stdout of the last process.

Actual pipelines will usually write to a file instead, using
:class:`AtomicFileSink`. This calls is similar to ``>`` in bash,
with a twist: it is only written after all other commands in the pipeline
succeeded (but unlike ``sponge`` from moreutils, it buffers to disk and
rename the file at the end).
"""

from __future__ import annotations

import dataclasses
import functools
import logging
import os
from pathlib import Path
import shlex
import signal
import subprocess
from typing import Any, BinaryIO, Dict, List, Optional, Tuple, TypeVar, Union

try:
    import luigi
    from luigi import LocalTarget
except ImportError:

    class LocalTarget:  # type: ignore
        """Placeholder for ``luigi.LocalTarget`` if it could not be imported"""

        pass


logger = logging.getLogger(__name__)


LOGBACK_CONF = b"""\
<configuration>
  <appender name="STDERR" class="ch.qos.logback.core.ConsoleAppender">
    <target>System.err</target>
    <encoder>
      <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} %msg%n</pattern>
    </encoder>
  </appender>

  <root level="debug">
    <appender-ref ref="STDERR" />
  </root>
</configuration>
"""
"""Overrides the default config, to log to stderr instead of stdout"""


class CommandException(Exception):
    def __init__(self, command, returncode):
        super().__init__(f"{command[0]} returned: {returncode}")
        self.command = command
        self.returncode = returncode


_PROC = Path("/proc/")
""":file:`/proc/`"""
_CGROUP_ROOT = Path("/sys/fs/cgroup/")
"""Base path of the cgroup filesystem"""


@functools.lru_cache(1)
def base_cgroup() -> Optional[Path]:
    """Returns the cgroup that should be used as parent for child processes.

    As `cgroups with children should not contain processes themselves
    <https://systemd.io/CGROUP_DELEGATION/#two-key-design-rules>`_, this is the parent
    of the cgroup this process was started in.
    """
    import atexit

    if not _CGROUP_ROOT.is_dir():
        logger.info("%s is not mounted", _CGROUP_ROOT)
        return None

    proc_cgroup_path = _PROC / str(os.getpid()) / "cgroup"
    if not proc_cgroup_path.is_file():
        logger.info("%s does not exist", proc_cgroup_path)
        return None

    my_cgroup = proc_cgroup_path.read_text().strip()
    if not my_cgroup.startswith("0::/"):
        # https://www.kernel.org/doc/html/latest/admin-guide/cgroup-v2.html#processes
        logger.warning("Process was started in %s which is not a cgroupv2", my_cgroup)
        return None

    # this is the cgroup that contains the current process, plus whatever process
    # spawned it (eg. pytest or bash); and neither cgroupv2 nor systemd allows a cgroup
    # to both contain processes itself and have child cgroups; so we have to use the
    # parent cgroup as root for the cgroups we are going to create.
    original_cgroup_path = _CGROUP_ROOT / my_cgroup[4:]
    if original_cgroup_path == _CGROUP_ROOT:
        # Running directly in the root cgroup, so there is no parent.
        # TODO: this means we are running in a container, so this is probably the only
        # process in the cgroup, we could try moving it to a child cgroup.
        return None
    assert (original_cgroup_path.parent / "cgroup.procs").read_text().strip() == ""

    # create a cgroup that will encapsulate both the "swh.graph.shell" cgroup and
    # all the children
    base_cgroup_path = create_cgroup(
        f"swh.graph@{os.getpid()}", original_cgroup_path.parent, add_suffix=False
    )
    if base_cgroup_path is None:
        return None

    assert (base_cgroup_path / "cgroup.procs").read_text().strip() == ""
    for controller in ("cpu", "memory"):
        try:
            with (base_cgroup_path / "cgroup.subtree_control").open("wt") as f:
                f.write(f"+{controller}\n")
        except OSError as e:
            logger.warning(
                "Failed to enable %r controller for %s: %s",
                controller,
                base_cgroup_path,
                e,
            )

    def cleanup():
        # Clean up the base cgroup we created
        base_cgroup_path.rmdir()

    atexit.register(cleanup)

    return base_cgroup_path


_num_child_cgroups = 0


def create_cgroup(
    base_name: str, parent: Optional[Path] = None, add_suffix: bool = True
) -> Optional[Path]:
    global _num_child_cgroups

    parent = parent or base_cgroup()
    if parent is None:
        return None

    if add_suffix:
        name = f"{base_name}@{_num_child_cgroups}"
        _num_child_cgroups += 1
    else:
        name = base_name

    new_cgroup_path = parent / name
    try:
        new_cgroup_path.mkdir()
    except OSError as e:
        logger.warning("Failed to create %s: %s", new_cgroup_path, e)
        return None

    return new_cgroup_path


def move_to_cgroup(cgroup: Path, pid: Optional[int] = None) -> bool:
    """Returns whether the process was successfully moved."""
    if pid is None:
        pid = os.getpid()
    try:
        with (cgroup / "cgroup.procs").open("at") as f:
            f.write(f"{pid}\n")
    except OSError as e:
        logger.warning("Failed to move process to %s: %s", cgroup, e)
        cgroup.rmdir()
        return False
    else:
        return True


class _MetaCommand(type):
    def __getattr__(self, name):
        return functools.partial(Command, name)


class Command(metaclass=_MetaCommand):
    """Runs a command with the given name and arguments. ``**kwargs`` is passed to
    :class:`subprocess.Popen`.

    If ``check`` is :const:`True` (the default), raises an exception if the command
    returns a non-zero exit code."""

    def __init__(
        self, *args: Union[str, Path, LocalTarget], check: bool = True, **kwargs
    ):
        self.args = args
        self.kwargs = dict(kwargs)
        self.preexec_fn = self.kwargs.pop("preexec_fn", lambda: None)
        self.cgroup = None
        self.check = check

    def _preexec_fn(self):
        if self.cgroup is not None:
            move_to_cgroup(self.cgroup)
        self.preexec_fn()

    def _run(self, stdin, stdout, stderr) -> _RunningCommand:
        cgroup = create_cgroup(str(self.args[0]).split("/")[-1])
        pass_fds = []
        children = []
        final_args = []
        pipes_to_close: List[int] = []
        stdout_read = None
        stderr_read = None

        if stderr is subprocess.STDOUT:
            if stdout is subprocess.PIPE:
                (r, w) = os.pipe()
                stdout = w
                stderr = w
                stdout_read = r
                pipes_to_close.append(w)
            else:
                stderr = stdout
        elif stderr is subprocess.PIPE:
            raise NotImplementedError(
                "swh.graph.shell.Command._run(..., stderr=subprocess.PIPE)"
            )

        for arg in self.args:
            if isinstance(arg, (Command, Pipe)):
                # command stdout piped to a non-stdin FD
                (r, w) = os.pipe()
                pass_fds.append(r)
                final_args.append(f"/dev/fd/{r}")
                children.append(arg._run(None, w, stderr))
                os.close(w)
            elif isinstance(arg, LocalTarget):
                final_args.append(arg.path)
            elif isinstance(arg, Path):
                final_args.append(str(arg))
            else:
                final_args.append(arg)

        proc = subprocess.Popen(
            final_args,
            stdin=stdin,
            stdout=stdout,
            stderr=stderr,
            pass_fds=pass_fds,
            preexec_fn=self._preexec_fn,
            **self.kwargs,
        )

        for pipe in pipes_to_close:
            os.close(pipe)

        return _RunningCommand(
            self,
            proc,
            children,
            cgroup,
            stdout=(
                os.fdopen(stdout_read, "rb")
                if isinstance(stdout_read, int)
                else stdout_read
            ),
            stderr=(
                os.fdopen(stderr_read, "rb")
                if isinstance(stderr_read, int)
                else stderr_read
            ),
            check=self.check,
        )

    def run(self, stderr=None) -> List[RunResult]:
        return self._run(None, None, stderr).wait()

    def __or__(self, other: Union[Command, Pipe]) -> Pipe:
        """``self | other``: pipe self's stdout to other's stdin"""
        if isinstance(other, Command):
            return Pipe([self, other])
        elif isinstance(other, Pipe):
            return Pipe([self, *other.children])
        else:
            raise NotImplementedError(
                f"{self.__class__.__name__} | {other.__class__.__name__}"
            )

    def __str__(self) -> str:
        return f"{' '.join(shlex.quote(str(arg)) for arg in self.args)}"


class Rust(Command):
    def __init__(
        self,
        bin_name,
        *args: Union[str, Path, "luigi.LocalTarget"],
        base_rust_executable_dir: Optional[Path] = None,
        conf: Optional[Dict[str, Any]] = None,
        env: Optional[Dict[str, str]] = None,
    ):
        from .config import check_config

        conf = dict(conf or {})
        conf = check_config(conf, base_rust_executable_dir=base_rust_executable_dir)
        assert conf is not None  # for mypy

        env = env or dict(os.environ)
        path = env.get("PATH")
        if path:
            env["PATH"] = f"{conf['rust_executable_dir']}:{path}"
        else:
            env["PATH"] = conf["rust_executable_dir"]
        env["RUST_MIN_STACK"] = "8388608"  # 8MiB; avoids stack overflows in LLP

        super().__init__(bin_name, *args, env=env)


class _RunningCommand:
    def __init__(
        self,
        command: Command,
        proc: subprocess.Popen,
        running_children: List[Union[_RunningCommand, _RunningPipe]],
        cgroup: Optional[Path],
        stdout: Optional[BinaryIO] = None,
        stderr: Optional[BinaryIO] = None,
        check: bool = True,
    ):
        self.command = command
        self.proc = proc
        self.running_children = running_children
        self.cgroup = cgroup
        self.check = check
        self._stdout = stdout
        self._stderr = stderr

    def stdout(self):
        return self._stdout or self.proc.stdout

    def stderr(self):
        return self._stderr or self.proc.stderr

    def is_alive(self) -> bool:
        return self.proc.poll() is None

    def wait(self) -> List[RunResult]:
        results = []
        try:
            self.proc.wait()
            results.append(
                RunResult(
                    command=tuple(map(str, self.command.args)),
                    cgroup=self.cgroup,
                    cgroup_stats={
                        p.name: p.read_text().strip()
                        for p in (self.cgroup.iterdir() if self.cgroup else [])
                        if p.name.startswith(("cpu.", "memory.", "io.", "pids."))
                        # exclude writeable files (they are for control, not statistics)
                        and p.stat().st_mode & 0o600 == 0o400
                    },
                )
            )
            self._cleanup()
            if self.check and self.proc.returncode not in (0, -int(signal.SIGPIPE)):
                raise CommandException(self.command.args, self.proc.returncode)

            for child in self.running_children:
                results.extend(child.wait())
        except BaseException:
            self.kill()
            raise

        return results

    def kill(self) -> None:
        for child in self.running_children:
            child.kill()

        if self.proc.returncode is not None:
            self.proc.kill()

    def _cleanup(self) -> None:
        if self.cgroup is not None:
            self.cgroup.rmdir()


class Pipe:
    def __init__(self, children: List[Union[Command, Pipe]]):
        self.children = children

    def _run(self, stdin, stdout, stderr) -> _RunningPipe:
        read_pipes: List[Any] = [stdin]
        write_pipes: List[Any] = []
        pipes_to_close: List[int] = []
        for _ in range(len(self.children) - 1):
            (r, w) = os.pipe()
            read_pipes.append(r)
            write_pipes.append(w)
            pipes_to_close.append(r)
            pipes_to_close.append(w)

        if stdout is subprocess.PIPE:
            (r, w) = os.pipe()
            stdout = w
            stdout_read = os.fdopen(r, "rb")
            pipes_to_close.append(w)
        elif stdout is subprocess.DEVNULL:
            stdout_read = None
        else:
            stdout_read = os.fdopen(stdout, "rb") if isinstance(stdout, int) else stdout

        write_pipes.append(stdout)

        if stderr is subprocess.STDOUT:
            stderr = stdout
        elif stderr is subprocess.PIPE:
            raise NotImplementedError(
                "swh.graph.shell.Pipe._run(..., stderr=subprocess.PIPE)"
            )

        running_children = [
            child._run(r, w, stderr)
            for (r, w, child) in zip(read_pipes, write_pipes, self.children)
        ]

        # We need to close these file descriptors in the parent process, so that the
        # corresponding pipes have an end fully closed when the corresponding process
        # dies.
        # On CPython (and when not running in tools like pytest that keep references to
        # call frames), this would be done automatically when the function exits because
        # the refcount to all pipes becomes 0, but we cannot rely
        # on this behavior.
        # Without it, the other end of the process will hang when trying to read
        # or write to it.
        for pipe in pipes_to_close:
            os.close(pipe)

        return _RunningPipe(self, running_children, stdout_read)

    def run(self) -> List[RunResult]:
        return self._run(None, None, None).wait()

    def __or__(self, other) -> Pipe:
        if isinstance(other, Pipe):
            return Pipe([*self.children, *other.children])
        elif isinstance(other, Command):
            return Pipe([*self.children, other])
        else:
            raise NotImplementedError(
                f"{self.__class__.__name__} | {other.__class__.__name__}"
            )

    def __str__(self) -> str:
        children = "\n| ".join(map(str, self.children))
        return f"( {children}\n)"


def wc(source: Union[Command, Pipe], *args: str) -> int:
    return int((source | Command.wc(*args) > Sink()).run().stdout.strip())


class _RunningPipe:
    def __init__(
        self,
        pipe: Pipe,
        children: List[Union[_RunningCommand, _RunningPipe]],
        stdout: Optional[BinaryIO],
    ):
        self.pipe = pipe
        self.children = children
        self._stdout = stdout

    def stdout(self):
        return self._stdout

    def is_alive(self) -> bool:
        return all(child.is_alive() for child in self.children)

    def wait(self) -> List[RunResult]:
        results = []
        try:
            for child in self.children:
                results.extend(child.wait())
        except BaseException:
            self.kill()
            raise

        return results

    def kill(self) -> None:
        for child in self.children:
            child.kill()


TSink = TypeVar("TSink", bound="_BaseSink")


class _BaseSink:
    def __init__(self) -> None:
        self.source_pipe: Union[None, Command, Pipe] = None

    def _run(self, stdin, stdout, stderr) -> Any:
        raise TypeError(f"{self.__class__.__name__} must be the end of a pipeline.")

    def __lt__(self: TSink, other: Union[Command, Pipe]) -> TSink:
        """``other > self``"""
        if isinstance(other, (Command, Pipe)):
            if self.source_pipe is not None:
                raise TypeError(f"{self!r} is already piped to {self.source_pipe!r}")
            self.source_pipe = other
            return self
        else:
            raise NotImplementedError(
                f"{other.__class__.__name__} > {self.__class__.__name__}"
            )


class _SinkResult:
    def __init__(self, result: List[RunResult], stdout: bytes) -> None:
        self.result = result
        self.stdout = stdout


class Sink(_BaseSink):
    """Captures the final output instead of sending it to the process' stdout"""

    def run(self, stderr=None) -> _SinkResult:
        if self.source_pipe is None:
            raise TypeError("AtomicFileSink has no stdin")
        if stderr is subprocess.STDOUT:
            raise NotImplementedError(
                "swh.graph.shell.Sink.run(stderr=subprocess.STDOUT)"
            )

        source = self.source_pipe._run(
            stdin=None, stdout=subprocess.PIPE, stderr=stderr
        )

        chunks = []
        while True:
            new_chunk = source.stdout().read(10240)
            if not new_chunk and not source.is_alive():
                break
            chunks.append(new_chunk)

        return _SinkResult(result=source.wait(), stdout=b"".join(chunks))


class _RunningAtomicFileSink:
    """Properly distributes ``stdout`` and ``stderr`` during ``AtomicFileSink``."""

    def __init__(
        self,
        source_pipe,
        stdout: Optional[BinaryIO],
        stderr: Optional[BinaryIO],
        path,
        tmp_path,
        tmp_fd,
    ):
        self.source_pipe = source_pipe
        self._stdout = stdout
        self._stderr = stderr
        self.path = path
        self.tmp_path = tmp_path
        self.tmp_fd = tmp_fd

    def stdout(self):
        return self._stdout

    def stderr(self):
        return self._stderr

    def wait(self):
        try:
            result = self.source_pipe.wait()
        except BaseException:
            self.tmp_fd.close()
            self.tmp_path.unlink()
            raise
        else:
            self.tmp_fd.close()
            self.tmp_path.replace(self.path)
            return result


class AtomicFileSink(_BaseSink):
    """Similar to ``> path`` at the end of a command, but writes only if the whole
    command succeeded."""

    def __init__(self, path: Union[Path, LocalTarget]):
        super().__init__()
        if isinstance(path, LocalTarget):
            path = Path(path.path)
        self.path = path

    def _run(self, stdin, stdout, stderr) -> _RunningAtomicFileSink:
        if self.source_pipe is None:
            raise TypeError("AtomicFileSink has no stdin")

        tmp_path = Path(f"{self.path}.tmp")
        if tmp_path.exists():
            tmp_path.unlink()
        tmp_fd = tmp_path.open("wb")

        pipes_to_close: List[int] = []

        if stderr is subprocess.STDOUT:
            (r, w) = os.pipe()
            stdout = os.fdopen(r, "rb")
            stderr_read = stdout
            stderr = w
            pipes_to_close.append(w)
        elif stderr is subprocess.PIPE:
            (r, w) = os.pipe()
            stdout = None
            stderr_read = os.fdopen(r, "rb")
            stderr = w
            pipes_to_close.append(w)
        else:
            stdout = None
            stderr_read = None

        running_sink = _RunningAtomicFileSink(
            source_pipe=self.source_pipe._run(stdin=None, stdout=tmp_fd, stderr=stderr),
            stdout=stdout,
            stderr=stderr_read,
            path=self.path,
            tmp_path=tmp_path,
            tmp_fd=tmp_fd,
        )

        for pipe in pipes_to_close:
            os.close(pipe)

        return running_sink

    def run(self, stderr=None) -> List[RunResult]:
        return self._run(None, None, stderr).wait()

    def __str__(self) -> str:
        return f"{self.source_pipe} > AtomicFileSink({self.path})"


@dataclasses.dataclass
class RunResult:
    cgroup: Optional[Path]
    command: Tuple[str, ...]
    cgroup_stats: Dict[str, str]
