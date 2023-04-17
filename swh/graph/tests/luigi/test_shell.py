# Copyright (C) 2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import threading

import pytest
import pyzstd

from swh.graph.luigi.shell import AtomicFileSink, Command, CommandException, Sink, wc

# fmt: off


def test_basic_stdout():
    assert (Command.echo("foo") > Sink()).run() == b"foo\n"


def test_basic_file(tmp_path):
    path = tmp_path / "file.txt"
    (
        Command.echo("foo")
        > AtomicFileSink(path)
    ).run()
    assert path.read_bytes() == b"foo\n"


def test_pipe_stdout():
    res = (
        Command.echo("foo")
        | Command.zstdmt()
        > Sink()
    ).run()
    assert pyzstd.decompress(res) == b"foo\n", res


def test_large_sink():
    """Checks Sink() does not block"""
    res = None

    def f():
        nonlocal res
        res = (
            Command.yes()
            | Command.head("-n", "1000000")
            > Sink()
        ).run()
        assert res == b"y\n" * 1000000, res

    thread = threading.Thread(target=f)
    thread.start()
    thread.join(10)  # 0.1s should be enough, but let's avoid flaky tests
    assert not thread.is_alive(), "blocked or took too long"


def test_pipe_file(tmp_path):
    path = tmp_path / "file.txt"
    (
        Command.echo("foo")
        | Command.zstdmt()
        > AtomicFileSink(path)
    ).run()
    res = path.read_bytes()
    print(res)  # in case pyzstd.decompress() fails
    assert pyzstd.decompress(res) == b"foo\n", res


def test_error(tmp_path):
    path = tmp_path / "bar.txt"

    with pytest.raises(CommandException):
        (
            Command.cat(tmp_path / "foo.txt")
            > AtomicFileSink(path)
        ).run()

    assert not path.exists()
    assert not (path / "bar.txt.tmp").exists()


def test_concat_command(tmp_path):
    path = tmp_path / "foo.txt"
    path.write_bytes(b"foo\n")
    assert (
        Command.echo("bar")
        | Command.cat(path, "-", Command.echo("baz"))
        > Sink()
    ).run() == b"foo\nbar\nbaz\n"


def test_wc(tmp_path):
    path = tmp_path / "foo.txt"
    path.write_bytes(b"foo\nbar\nbaz\n")
    assert wc(Command.cat(path), "-l") == 3


def test_pv_wc(tmp_path):
    path = tmp_path / "foo.txt"
    path.write_bytes(b"foo\nbar\nbaz\n")
    assert (
        Command.cat(path)
        | Command.pv(
            "--line-mode",
            "--size",
            str(wc(Command.cat(path), "-l"))
        )
        > Sink()
    ).run() == b"foo\nbar\nbaz\n"


def test_concat_pipe(tmp_path):
    path = tmp_path / "foo.txt"
    path.write_bytes(pyzstd.compress(b"foo\n"))
    res = (
        Command.echo("bar")
        | Command.zstdmt()
        | Command.cat(path, "-", Command.echo("baz") | Command.zstdmt())
        > Sink()
    ).run()
    print(res)  # in case pyzstd.decompress() fails
    assert pyzstd.decompress(res) == b"foo\nbar\nbaz\n"


def test_associativity(tmp_path):
    assert (
        Command.echo("foo")
        | Command.cat()
        | Command.cat()
        > Sink()
    ).run() == b"foo\n"

    assert (
        (
            Command.echo("foo")
            | Command.cat()
        )
        | Command.cat()
        > Sink()
    ).run() == b"foo\n"

    assert (
        Command.echo("foo")
        | (
            Command.cat()
            | Command.cat()
        )
        > Sink()
    ).run() == b"foo\n"


def test_file_atomic(tmp_path):
    path = tmp_path / "file.txt"

    for content in ("foo", "bar"):
        (
            Command.echo(content)
            | Command.cat("-", Command.true())
            > AtomicFileSink(path)
        ).run()
        assert path.read_bytes() == f"{content}\n".encode()

    with pytest.raises(CommandException):
        (
            Command.echo("qux")
            | Command.cat("-", Command.false())
            > AtomicFileSink(path)
        ).run()
    assert path.read_bytes() == b"bar\n"  # wasn't overwritten because the pipe failed

    assert not (tmp_path / "file.txt.tmp").exists(), "temporary file was not cleaned up"
