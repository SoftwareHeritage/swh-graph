# Copyright (C) 2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

r"""This module implements a shell-like command pipeline system in
pure-Python.

Pipelines are built like this:

>>> from swh.graph.luigi.shell import Command, Sink
>>> (
...     Command.echo("foo")
...     | Command.zstdmt()
...     | Command.cat("-", Command.echo("bar") | Command.zstdmt())
...     | Command.zstdcat()
...     > Sink()
... ).run()
b'foo\nbar\n'

which is the equivalent of this bash command:

.. code-block:: bash

    echo foo \
    | zstdmt \
    | cat - <(echo bar | zstdmt) \
    | zstdcat

:class:`Sink` is mainly meant for tests; it causes ``.run()`` to return
the stdout of the last process.

Actual pipelines will usually write to a file instead, using
:class:`AtomicFileSink`. This calls is similar to ``>`` in bash,
with a twist: it is only written after all other commands in the pipeline
succeeded (but unlike ``sponge`` from moreutils, it buffers to disk and
rename the file at the end).
"""

from __future__ import annotations

import functools
import os
from pathlib import Path
import shlex
import signal
import subprocess
from typing import Any, Dict, List, NoReturn, Optional, TypeVar, Union

import luigi

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
    pass


class _MetaCommand(type):
    def __getattr__(self, name):
        return functools.partial(Command, name)


class Command(metaclass=_MetaCommand):
    """Runs a command with the given name and arguments. ``**kwargs`` is passed to
    :class:`subprocess.Popen`."""

    def __init__(self, *args: str, **kwargs):
        self.args = args
        self.kwargs = kwargs

    def _run(self, stdin, stdout) -> _RunningCommand:
        pass_fds = []
        children = []
        final_args = []
        for arg in self.args:
            if isinstance(arg, (Command, Pipe)):
                # command stdout piped to a non-stdin FD
                (r, w) = os.pipe()
                pass_fds.append(r)
                final_args.append(f"/dev/fd/{r}")
                children.append(arg._run(None, w))
                os.close(w)
            elif isinstance(arg, luigi.LocalTarget):
                final_args.append(arg.path)
            else:
                final_args.append(arg)

        proc = subprocess.Popen(
            final_args, stdin=stdin, stdout=stdout, pass_fds=pass_fds, **self.kwargs
        )
        return _RunningCommand(self, proc, children)

    def run(self) -> None:
        self._run(None, None).wait()

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

    def _cleanup(self) -> None:
        pass


class Java(Command):
    def __init__(self, *args: str, max_ram: Optional[int] = None):

        import tempfile

        from ..config import check_config

        conf: Dict = {}  # TODO: configurable

        if max_ram:
            conf["max_ram"] = max_ram

        conf = check_config(conf)

        self.logback_conf = tempfile.NamedTemporaryFile(
            prefix="logback_", suffix=".xml"
        )
        self.logback_conf.write(LOGBACK_CONF)
        self.logback_conf.flush()

        java_tool_options = [
            f"-Dlogback.configurationFile={self.logback_conf.name}",
            conf["java_tool_options"],
        ]

        env = {
            **os.environ.copy(),
            "JAVA_TOOL_OPTIONS": " ".join(java_tool_options),
            "CLASSPATH": conf["classpath"],
        }

        super().__init__("java", *args, env=env)

    def _cleanup(self) -> None:
        self.logback_conf.close()
        super()._cleanup()


class _RunningCommand:
    def __init__(
        self,
        command: Command,
        proc: subprocess.Popen,
        running_children: List[Union[_RunningCommand, _RunningPipe]],
    ):
        self.command = command
        self.proc = proc
        self.running_children = running_children

    def stdout(self):
        return self.proc.stdout

    def is_alive(self) -> bool:
        return self.proc.poll() is None

    def wait(self) -> None:
        try:
            self.proc.wait()
            self.command._cleanup()
            if self.proc.returncode not in (0, -int(signal.SIGPIPE)):
                raise CommandException(
                    f"{self.command.args[0]} returned: {self.proc.returncode}"
                )

            for child in self.running_children:
                child.wait()
        except BaseException:
            self.kill()
            raise

    def kill(self) -> None:
        for child in self.running_children:
            child.kill()

        if self.proc.returncode is not None:
            self.proc.kill()


class Pipe:
    def __init__(self, children: List[Union[Command, Pipe]]):
        self.children = children

    def _run(self, stdin, stdout) -> _RunningPipe:
        read_pipes: List[Any] = [stdin]
        write_pipes: List[Any] = []
        for _ in range(len(self.children) - 1):
            (r, w) = os.pipe()
            read_pipes.append(os.fdopen(r, "rb"))
            write_pipes.append(os.fdopen(w, "wb"))
        write_pipes.append(stdout)

        running_children = [
            child._run(r, w)
            for (r, w, child) in zip(read_pipes, write_pipes, self.children)
        ]

        return _RunningPipe(self, running_children)

    def run(self) -> None:
        self._run(None, None).wait()

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
    return int((source | Command.wc(*args) > Sink()).run().strip())


class _RunningPipe:
    def __init__(
        self, pipe: Pipe, children: List[Union[_RunningCommand, _RunningPipe]]
    ):
        self.pipe = pipe
        self.children = children

    def stdout(self):
        return self.children[-1].stdout()

    def is_alive(self) -> bool:
        return all(child.is_alive() for child in self.children)

    def wait(self) -> None:
        try:
            for child in self.children:
                child.wait()
        except BaseException:
            self.kill()
            raise

    def kill(self) -> None:
        for child in self.children:
            child.kill()


TSink = TypeVar("TSink", bound="_BaseSink")


class _BaseSink:
    def __init__(self) -> None:
        self.source_pipe: Union[None, Command, Pipe] = None

    def _run(self, stdin, stdout) -> NoReturn:
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


class Sink(_BaseSink):
    """Captures the final output instead of sending it to the process' stdout"""

    def run(self) -> bytes:
        if self.source_pipe is None:
            raise TypeError("AtomicFileSink has no stdin")

        source = self.source_pipe._run(stdin=None, stdout=subprocess.PIPE)

        chunks = []
        while True:
            new_chunk = source.stdout().read(10240)
            if not new_chunk and not source.is_alive():
                break
            chunks.append(new_chunk)

        source.wait()

        return b"".join(chunks)


class AtomicFileSink(_BaseSink):
    """Similar to ``> path`` at the end of a command, but writes only if the whole
    command succeeded."""

    def __init__(self, path: Union[Path, luigi.LocalTarget]):
        super().__init__()
        if isinstance(path, luigi.LocalTarget):
            path = Path(path.path)
        self.path = path

    def run(self) -> None:
        if self.source_pipe is None:
            raise TypeError("AtomicFileSink has no stdin")

        tmp_path = Path(f"{self.path}.tmp")
        if tmp_path.exists():
            tmp_path.unlink()
        tmp_fd = tmp_path.open("wb")
        running_source = self.source_pipe._run(stdin=None, stdout=tmp_fd)

        try:
            running_source.wait()
        except BaseException:
            tmp_fd.close()
            tmp_path.unlink()
            raise
        else:
            tmp_fd.close()
            tmp_path.replace(self.path)

    def __str__(self) -> str:
        return f"{self.source_pipe} > AtomicFileSink({self.path})"
