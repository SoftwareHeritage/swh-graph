# Copyright (C) 2019-2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from click.testing import CliRunner

from swh.graph.cli import graph_cli_group


def test_find_context_content(graph_grpc_server):
    swhid = "swh:1:cnt:0000000000000000000000000000000000000001"
    runner = CliRunner()
    result = runner.invoke(
        graph_cli_group, ["find-context", "-g", graph_grpc_server, "-c", swhid]
    )
    expected_fqswhid = (
        "swh:1:cnt:0000000000000000000000000000000000000001;"
        "path=/README.md;"
        "anchor=swh:1:rev:0000000000000000000000000000000000000009;"
        "visit=swh:1:snp:0000000000000000000000000000000000000020;"
        "origin=https://example.com/swh/graph\n"
    )
    assert result.exit_code == 0, result
    assert result.output == expected_fqswhid, result.output


def test_find_context_content_in_root_directory(graph_grpc_server):
    swhid = "swh:1:cnt:0000000000000000000000000000000000000014"
    runner = CliRunner()
    result = runner.invoke(
        graph_cli_group, ["find-context", "-g", graph_grpc_server, "-c", swhid]
    )
    expected_fqswhid = (
        "swh:1:cnt:0000000000000000000000000000000000000014;"
        "path=/TODO.txt;"
        "anchor=swh:1:rev:0000000000000000000000000000000000000018;"
        "visit=swh:1:snp:0000000000000000000000000000000000000022;"
        "origin=https://example.com/swh/graph2\n"
    )
    assert result.exit_code == 0, result
    assert result.output == expected_fqswhid, result.output


def test_find_context_directory(graph_grpc_server):
    swhid = "swh:1:dir:0000000000000000000000000000000000000012"
    runner = CliRunner()
    result = runner.invoke(
        graph_cli_group, ["find-context", "-g", graph_grpc_server, "-c", swhid]
    )
    expected_fqswhid = (
        "swh:1:dir:0000000000000000000000000000000000000012;path=/;"
        "anchor=swh:1:rev:0000000000000000000000000000000000000013;"
        "visit=swh:1:snp:0000000000000000000000000000000000000022;"
        "origin=https://example.com/swh/graph2\n"
    )
    assert result.exit_code == 0, result
    assert result.output == expected_fqswhid, result.output


def test_find_context_revision(graph_grpc_server):
    swhid = "swh:1:rev:0000000000000000000000000000000000000009"
    runner = CliRunner()
    result = runner.invoke(
        graph_cli_group, ["find-context", "-g", graph_grpc_server, "-c", swhid]
    )
    expected_fqswhid = (
        "swh:1:rev:0000000000000000000000000000000000000009;"
        "visit=swh:1:snp:0000000000000000000000000000000000000020;"
        "origin=https://example.com/swh/graph\n"
    )
    assert result.exit_code == 0, result
    assert result.output == expected_fqswhid, result.output


def test_find_context_release(graph_grpc_server):
    swhid = "swh:1:rel:0000000000000000000000000000000000000010"
    runner = CliRunner()
    result = runner.invoke(
        graph_cli_group, ["find-context", "-g", graph_grpc_server, "-c", swhid]
    )
    expected_fqswhid = (
        "swh:1:rel:0000000000000000000000000000000000000010;"
        "visit=swh:1:snp:0000000000000000000000000000000000000020;"
        "origin=https://example.com/swh/graph\n"
    )
    assert result.exit_code == 0, result
    assert result.output == expected_fqswhid, result.output


def test_find_context_snapshot(graph_grpc_server):
    swhid = "swh:1:snp:0000000000000000000000000000000000000020"
    runner = CliRunner()
    result = runner.invoke(
        graph_cli_group, ["find-context", "-g", graph_grpc_server, "-c", swhid]
    )
    expected_fqswhid = (
        "swh:1:snp:0000000000000000000000000000000000000020;"
        "origin=https://example.com/swh/graph\n"
    )
    assert result.exit_code == 0, result
    assert result.output == expected_fqswhid, result.output


def test_find_context_unknown(graph_grpc_server):
    swhid = "swh:1:dir:1111111111111111111111111111111111111111"
    runner = CliRunner()
    result = runner.invoke(
        graph_cli_group, ["find-context", "-g", graph_grpc_server, "-c", swhid]
    )
    expected_fqswhid = (
        "Error from the GRPC API call: "
        "Unknown SWHID: swh:1:dir:1111111111111111111111111111111111111111\n"
    )
    assert result.exit_code == 0, result
    assert result.output == expected_fqswhid, result.output


def test_find_context_invalid(graph_grpc_server):
    swhid_1 = ""
    swhid_2 = "asdfasdfasdf"
    runner = CliRunner()
    result_1 = runner.invoke(
        graph_cli_group, ["find-context", "-g", graph_grpc_server, "-c", swhid_1]
    )
    result_2 = runner.invoke(
        graph_cli_group, ["find-context", "-g", graph_grpc_server, "-c", swhid_2]
    )
    assert result_1.exit_code == 0, result_1
    assert result_2.exit_code == 0, result_2
    assert f"'{swhid_1}' is not a valid SWHID" in result_1.output
    assert f"'{swhid_2}' is not a valid SWHID" in result_2.output


def test_find_context_dangling(graph_grpc_server):
    swhid = "swh:1:rel:0000000000000000000000000000000000000019"
    runner = CliRunner()
    result = runner.invoke(
        graph_cli_group, ["find-context", "-g", graph_grpc_server, "-c", swhid]
    )
    expected_fqswhid = (
        "Error from the GRPC API call: "
        "Could not find a path from the sources "
        "(swh:1:rel:0000000000000000000000000000000000000019) to any matching target\n"
    )
    assert result.exit_code == 0, result
    assert result.output == expected_fqswhid, result.output
