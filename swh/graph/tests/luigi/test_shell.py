# Copyright (C) 2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os
import subprocess
import threading

import pytest
import pyzstd

from swh.graph.shell import AtomicFileSink, Command, CommandException, Pipe, Sink, wc

# fmt: off


def test_basic_stdout():
    assert (Command.echo("foo") > Sink()).run().stdout == b"foo\n"


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
    ).run().stdout
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
        ).run().stdout
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
    ).run().stdout == b"foo\nbar\nbaz\n"


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
    ).run().stdout == b"foo\nbar\nbaz\n"


def test_concat_pipe(tmp_path):
    path = tmp_path / "foo.txt"
    path.write_bytes(pyzstd.compress(b"foo\n"))
    res = (
        Command.echo("bar")
        | Command.zstdmt()
        | Command.cat(path, "-", Command.echo("baz") | Command.zstdmt())
        > Sink()
    ).run().stdout
    print(res)  # in case pyzstd.decompress() fails
    assert pyzstd.decompress(res) == b"foo\nbar\nbaz\n"


def test_associativity(tmp_path):
    assert (
        Command.echo("foo")
        | Command.cat()
        | Command.cat()
        > Sink()
    ).run().stdout == b"foo\n"

    assert (
        (
            Command.echo("foo")
            | Command.cat()
        )
        | Command.cat()
        > Sink()
    ).run().stdout == b"foo\n"

    assert (
        Command.echo("foo")
        | (
            Command.cat()
            | Command.cat()
        )
        > Sink()
    ).run().stdout == b"foo\n"


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


def test_atomic_file_sink_run_with_stderr_fd(tmp_path):
    path = tmp_path / "file.txt"
    try:
        try:
            (r, w) = os.pipe()
            (
                Command.bash("-c", "echo foo; >&2 echo bar")
                | Command.zstdmt()
                > AtomicFileSink(path)
            ).run(stderr=w)
        finally:
            os.close(w)
        res = path.read_bytes()
        assert pyzstd.decompress(res) == b"foo\n", res
    except Exception:
        os.close(r)
        raise
    with os.fdopen(r) as read_fd:
        assert read_fd.read() == "bar\n"


def test_atomic_file_sink__run_with_stderr_pipe(tmp_path):
    path = tmp_path / "file.txt"
    cmd = (
        Command.bash("-c", "echo foo; >&2 echo bar")
        | Command.zstdmt()
        > AtomicFileSink(path)
    )._run(stdin=None, stdout=None, stderr=subprocess.PIPE)
    cmd.wait()
    res = path.read_bytes()
    assert pyzstd.decompress(res) == b"foo\n", res
    assert cmd.stderr().read() == b"bar\n"


def test_atomic_file_sink__run_with_stderr_is_stdout(tmp_path):
    path = tmp_path / "file.txt"
    cmd = (
        Command.bash("-c", "echo foo; >&2 echo bar")
        | Command.cat()
        > AtomicFileSink(path)
    )._run(stdin=None, stdout=None, stderr=subprocess.STDOUT)
    cmd.wait()
    res = path.read_bytes()
    assert res == b"foo\n", res
    assert cmd.stdout().read() == b"bar\n"


def test_command__run_with_pipe_and_stdout():
    cmd = (
        Command.bash("-c", "echo foo; >&2 echo bar")
    )._run(stdin=None, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    cmd.wait()
    assert sorted(cmd.stdout().read().splitlines()) == [b"bar", b"foo"]


def test_command__run_with_same_fd():
    try:
        try:
            (r, w) = os.pipe()
            cmd = (
                Command.bash("-c", "echo foo; >&2 echo bar")
            )._run(stdin=None, stdout=w, stderr=w)
        finally:
            os.close(w)
        cmd.wait()
    except Exception:
        os.close(r)
        raise
    with os.fdopen(r) as read_fd:
        assert sorted(read_fd.read().splitlines()) == ["bar", "foo"]


def test_command__run_with_different_fd():
    try:
        try:
            (r1, w1) = os.pipe()
            (r2, w2) = os.pipe()
            cmd = (
                Command.bash("-c", "echo foo; >&2 echo bar")
            )._run(stdin=None, stdout=w1, stderr=w2)
        finally:
            os.close(w1)
            os.close(w2)
        cmd.wait()
    except Exception:
        os.close(r1)
        raise
    except Exception:
        os.close(r2)
        raise
    with os.fdopen(r1) as read_fd_1:
        assert sorted(read_fd_1.read().splitlines()) == ["foo"]
    with os.fdopen(r2) as read_fd_2:
        assert sorted(read_fd_2.read().splitlines()) == ["bar"]


def test_pipe__run_with_pipe_and_stdout():
    cmd = (
        Pipe([
            Command.bash("-c", "echo foo; >&2 echo bar"),
            Command.cat(),
        ])._run(stdin=None, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    )
    cmd.wait()
    assert sorted(cmd.stdout().read().splitlines()) == [b"bar", b"foo"]


def test_pipe__run_with_same_fd():
    try:
        try:
            (r, w) = os.pipe()
            cmd = (
                Pipe([
                    Command.bash("-c", "echo foo; >&2 echo bar"),
                    Command.cat(),
                ])._run(stdin=None, stdout=w, stderr=w)
            )
        finally:
            os.close(w)
        cmd.wait()
    except Exception:
        os.close(r)
        raise
    with os.fdopen(r) as read_fd:
        assert sorted(read_fd.read().splitlines()) == ["bar", "foo"]


def test_pipe__run_with_different_fd():
    try:
        try:
            (r1, w1) = os.pipe()
            (r2, w2) = os.pipe()
            cmd = (
                Pipe([
                    Command.bash("-c", "echo foo; >&2 echo bar"),
                    Command.cat(),
                ])._run(stdin=None, stdout=w1, stderr=w2)
            )
        finally:
            os.close(w1)
            os.close(w2)
        cmd.wait()
    except Exception:
        os.close(r1)
        raise
    except Exception:
        os.close(r2)
        raise
    with os.fdopen(r1) as read_fd_1:
        assert sorted(read_fd_1.read().splitlines()) == ["foo"]
    with os.fdopen(r2) as read_fd_2:
        assert sorted(read_fd_2.read().splitlines()) == ["bar"]


def test_sink_run_with_stdout():
    with pytest.raises(NotImplementedError):
        (
            Command.bash("-c", "echo foo; >&2 echo bar")
            > Sink()
        ).run(stderr=subprocess.STDOUT)


def test_sink__run_with_stderr_fd():
    try:
        try:
            (r, w) = os.pipe()
            res = (
                Command.bash("-c", "echo foo; >&2 echo bar")
                > Sink()
            ).run(stderr=w)
        finally:
            os.close(w)
        assert res.stdout == b"foo\n"
    except Exception:
        os.close(r)
        raise
    with os.fdopen(r) as read_fd:
        assert read_fd.read() == "bar\n"


def test_sink__run_with_pipes():
    with pytest.raises(NotImplementedError):
        (
            Command.bash("-c", "echo foo; >&2 echo bar")
            > Sink()
        ).source_pipe._run(
            stdin=None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )


def test_process_substitution_no_stderr():
    res = (
        Command.cat(
            Command.bash("-c", "echo foo1; >&2 echo bar1"),
            Command.bash("-c", "echo foo2; >&2 echo bar2"),
        )
        > Sink()
    ).run()
    assert res.stdout == b"foo1\nfoo2\n"


def test_process_substitution_with_stderr_fd():
    try:
        try:
            (r, w) = os.pipe()
            res = (
                Command.cat(
                    Command.bash("-c", "echo foo1; >&2 echo bar1"),
                    Command.bash("-c", "echo foo2; >&2 echo bar2"),
                )
                > Sink()
            ).run(stderr=w)
        finally:
            os.close(w)
        assert res.stdout == b"foo1\nfoo2\n"
    except Exception:
        os.close(r)
        raise
    with os.fdopen(r, "rb") as read_fd:
        assert sorted(read_fd.read().splitlines()) == [b"bar1", b"bar2"]


def test_process_substitution_with_stderr_to_stdout():
    with pytest.raises(NotImplementedError):
        (
            Command.cat(
                Command.bash("-c", "echo foo1; >&2 echo bar1"),
                Command.bash("-c", "echo foo2; >&2 echo bar2"),
            )
            > Sink()
        ).run(stderr=subprocess.STDOUT)
