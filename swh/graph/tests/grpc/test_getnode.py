# Copyright (c) 2022-2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import hashlib

from google.protobuf.field_mask_pb2 import FieldMask
import grpc
import pytest

from swh.graph.grpc.swhgraph_pb2 import (
    GetNodeRequest,
    OriginData,
    ReleaseData,
    RevisionData,
)

TEST_ORIGIN_ID = "swh:1:ori:{}".format(
    hashlib.sha1(b"https://example.com/swh/graph").hexdigest()
)
TEST_ORIGIN_ID2 = "swh:1:ori:{}".format(
    hashlib.sha1(b"https://example.com/swh/graph2").hexdigest()
)


def test_not_found(graph_grpc_stub):
    with pytest.raises(grpc.RpcError) as excinfo:
        graph_grpc_stub.GetNode(
            GetNodeRequest(swhid="swh:1:cnt:0000000000000000000000000000000000000194")
        )
    assert excinfo.value.code() == grpc.StatusCode.NOT_FOUND


def test_invalid_swhid(graph_grpc_stub):
    with pytest.raises(grpc.RpcError) as excinfo:
        graph_grpc_stub.GetNode(
            GetNodeRequest(swhid="swh:1:lol:0000000000000000000000000000000000000001")
        )
    assert excinfo.value.code() == grpc.StatusCode.INVALID_ARGUMENT

    with pytest.raises(grpc.RpcError) as excinfo:
        graph_grpc_stub.GetNode(
            GetNodeRequest(swhid="swh:1:cnt:000000000000000000000000000000000000000z")
        )
    assert excinfo.value.code() == grpc.StatusCode.INVALID_ARGUMENT


def test_contents(graph_grpc_stub):
    expected_cnts = [1, 4, 5, 7, 11, 14, 15]
    expected_lengths = {1: 42, 4: 404, 5: 1337, 7: 666, 11: 313, 14: 14, 15: 404}
    expected_skipped = {15}

    for cnt_id in expected_cnts:
        node = graph_grpc_stub.GetNode(GetNodeRequest(swhid=f"swh:1:cnt:{cnt_id:040}"))
        assert node.HasField("cnt")
        assert node.cnt.HasField("length")
        assert expected_lengths[cnt_id] == node.cnt.length
        assert node.cnt.HasField("is_skipped")
        assert node.cnt.is_skipped == (cnt_id in expected_skipped)


def test_revisions(graph_grpc_stub):
    expected_revs = [3, 9, 13, 18]
    expected_messages = {
        3: "Initial commit",
        9: "Add parser",
        13: "Add tests",
        18: "Refactor codebase",
    }

    expected_authors = {3: "foo", 9: "bar", 13: "foo", 18: "baz"}
    expected_committers = {3: "foo", 9: "bar", 13: "bar", 18: "foo"}

    expected_author_timestamps = {
        3: 1111122220,
        9: 1111144440,
        13: 1111166660,
        18: 1111177770,
    }
    expected_committer_timestamps = {
        3: 1111122220,
        9: 1111155550,
        13: 1111166660,
        18: 1111177770,
    }
    expected_author_timestamp_offsets = {3: 120, 9: 120, 13: 120, 18: 0}
    expected_committer_timestamp_offsets = {3: 120, 9: 120, 13: 120, 18: 0}

    person_mapping = {}
    for rev_id in expected_revs:
        node = graph_grpc_stub.GetNode(GetNodeRequest(swhid=f"swh:1:rev:{rev_id:040}"))
        assert node.HasField("rev")
        assert node.rev.HasField("message")
        assert expected_messages[rev_id] == node.rev.message.decode("utf-8")

        assert node.rev.HasField("author")
        assert node.rev.HasField("committer")
        actual_persons = [node.rev.author, node.rev.committer]
        expected_persons = [expected_authors[rev_id], expected_committers[rev_id]]
        for i in range(len(actual_persons)):
            actual_person = actual_persons[i]
            expected_person = expected_persons[i]
            assert actual_person >= 0
            if actual_person in person_mapping:
                assert person_mapping[actual_person] == expected_person
            else:
                person_mapping[actual_person] = expected_person

        assert node.rev.HasField("author_date")
        assert node.rev.HasField("author_date_offset")
        assert node.rev.HasField("committer_date")
        assert node.rev.HasField("committer_date_offset")

        assert expected_author_timestamps[rev_id] == node.rev.author_date
        assert expected_author_timestamp_offsets[rev_id] == node.rev.author_date_offset
        assert expected_committer_timestamps[rev_id] == node.rev.committer_date
        assert (
            expected_committer_timestamp_offsets[rev_id]
            == node.rev.committer_date_offset
        )


@pytest.mark.parametrize("rel_id", [10, 19])
def test_releases(rel_id, graph_grpc_stub):
    expected_messages = {10: "Version 1.0", 19: "Version 2.0"}
    expected_names = {10: "v1.0", 19: "v2.0"}
    expected_authors = {10: "foo", 19: "bar"}
    expected_author_timestamps = {10: 1234567890}
    expected_author_timestamp_offsets = {10: 120}

    person_mapping = {}

    node = graph_grpc_stub.GetNode(GetNodeRequest(swhid=f"swh:1:rel:{rel_id:040}"))
    assert node.HasField("rel")
    assert node.rel.HasField("message")
    assert expected_messages[rel_id] == node.rel.message.decode("utf-8")
    assert expected_names[rel_id] == node.rel.name.decode("utf-8")

    # Persons are anonymized, we just need to check that the mapping is self-consistent
    assert node.rel.HasField("author")
    actual_person = node.rel.author
    expected_person = expected_authors[rel_id]
    assert actual_person >= 0
    if actual_person in person_mapping:
        assert person_mapping[actual_person] == expected_person
    else:
        person_mapping[actual_person] = expected_person

    if rel_id == 10:
        assert node.rel.HasField("author_date")
        assert node.rel.HasField("author_date_offset")
    elif rel_id == 19:
        assert not node.rel.HasField("author_date")
        assert not node.rel.HasField("author_date_offset")
    else:
        assert False, "Unexpected rel_id value"

    if rel_id in expected_author_timestamps:
        assert expected_author_timestamps[rel_id] == node.rel.author_date
    if rel_id in expected_author_timestamp_offsets:
        assert expected_author_timestamp_offsets[rel_id] == node.rel.author_date_offset


def test_origins(graph_grpc_stub):
    expected_oris = [TEST_ORIGIN_ID]
    expected_urls = {TEST_ORIGIN_ID: "https://example.com/swh/graph"}

    for ori_swhid in expected_oris:
        node = graph_grpc_stub.GetNode(GetNodeRequest(swhid=ori_swhid))
        assert node.HasField("ori")
        assert node.ori.HasField("url")
        assert expected_urls[ori_swhid] == node.ori.url


def test_cnt_mask(graph_grpc_stub):
    swhid = "swh:1:cnt:0000000000000000000000000000000000000001"

    # No mask, all fields present
    node = graph_grpc_stub.GetNode(GetNodeRequest(swhid=swhid))
    assert node.HasField("cnt")
    assert node.cnt.HasField("length")
    assert node.cnt.length == 42
    assert node.cnt.HasField("is_skipped")
    assert not node.cnt.is_skipped

    # Empty mask, no fields present
    node = graph_grpc_stub.GetNode(GetNodeRequest(swhid=swhid, mask=FieldMask()))
    if node.HasField("cnt"):
        assert not node.cnt.HasField("length")
        assert not node.cnt.HasField("is_skipped")

    # Mask with length, no is_skipped
    node = graph_grpc_stub.GetNode(
        GetNodeRequest(swhid=swhid, mask=FieldMask(paths=["cnt.length"]))
    )
    assert node.HasField("cnt")
    assert node.cnt.HasField("length")
    assert not node.cnt.HasField("is_skipped")

    # Mask with is_skipped, no length
    node = graph_grpc_stub.GetNode(
        GetNodeRequest(swhid=swhid, mask=FieldMask(paths=["cnt.is_skipped"]))
    )
    assert node.HasField("cnt")
    assert not node.cnt.HasField("length")
    assert node.cnt.HasField("is_skipped")


def test_rev_mask(graph_grpc_stub):
    swhid = "swh:1:rev:0000000000000000000000000000000000000003"

    # No mask, all fields present
    node = graph_grpc_stub.GetNode(GetNodeRequest(swhid=swhid))
    assert node.HasField("rev")
    assert node.rev.HasField("message")
    assert node.rev.HasField("author")
    assert node.rev.HasField("author_date")
    assert node.rev.HasField("author_date_offset")
    assert node.rev.HasField("committer")
    assert node.rev.HasField("committer_date")
    assert node.rev.HasField("committer_date_offset")

    # Empty mask, no fields present
    node = graph_grpc_stub.GetNode(GetNodeRequest(swhid=swhid, mask=FieldMask()))
    assert not node.rev.HasField("message")
    assert not node.rev.HasField("author")
    assert not node.rev.HasField("author_date")
    assert not node.rev.HasField("author_date_offset")
    assert not node.rev.HasField("committer")
    assert not node.rev.HasField("committer_date")
    assert not node.rev.HasField("committer_date_offset")

    # Test all masks with single fields
    for included_field in RevisionData.DESCRIPTOR.fields:
        mask = FieldMask(paths=[f"rev.{included_field.name}"])
        node = graph_grpc_stub.GetNode(GetNodeRequest(swhid=swhid, mask=mask))
        print(included_field.name, repr(node))
        for field in RevisionData.DESCRIPTOR.fields:
            assert node.rev.HasField(field.name) == (field.name == included_field.name)


@pytest.mark.parametrize("rel_id", [10, 19])
def test_rel_mask(rel_id, graph_grpc_stub):
    swhid = f"swh:1:rel:00000000000000000000000000000000000000{rel_id}"

    # No mask, all fields present
    node = graph_grpc_stub.GetNode(GetNodeRequest(swhid=swhid))
    assert node.HasField("rel")
    assert node.rel.HasField("message")
    assert node.rel.HasField("author")
    if rel_id == 10:
        assert node.rel.HasField("author_date")
        assert node.rel.HasField("author_date_offset")
    elif rel_id == 19:
        assert not node.rel.HasField("author_date")
        assert not node.rel.HasField("author_date_offset")
    else:
        assert False

    # Empty mask, no fields present
    node = graph_grpc_stub.GetNode(GetNodeRequest(swhid=swhid, mask=FieldMask()))
    assert not node.rel.HasField("message")
    assert not node.rel.HasField("author")
    assert not node.rel.HasField("author_date")
    assert not node.rel.HasField("author_date_offset")

    # Test all masks with single fields
    for included_field in ReleaseData.DESCRIPTOR.fields:
        mask = FieldMask(paths=[f"rel.{included_field.name}"])
        node = graph_grpc_stub.GetNode(GetNodeRequest(swhid=swhid, mask=mask))
        for field in ReleaseData.DESCRIPTOR.fields:
            if rel_id == 19 and field.name in ("author_date", "author_date_offset"):
                continue
            assert node.rel.HasField(field.name) == (field.name == included_field.name)


def test_ori_mask(graph_grpc_stub):
    swhid = TEST_ORIGIN_ID

    # No mask, all fields present
    node = graph_grpc_stub.GetNode(GetNodeRequest(swhid=swhid))
    assert node.HasField("ori")
    assert node.ori.HasField("url")

    # Empty mask, no fields present
    node = graph_grpc_stub.GetNode(GetNodeRequest(swhid=swhid, mask=FieldMask()))
    assert not node.ori.HasField("url")

    # Test all masks with single fields
    for included_field in OriginData.DESCRIPTOR.fields:
        mask = FieldMask(paths=[f"ori.{included_field.name}"])
        node = graph_grpc_stub.GetNode(GetNodeRequest(swhid=swhid, mask=mask))
        print(included_field, repr(node))
        for field in OriginData.DESCRIPTOR.fields:
            assert node.ori.HasField(field.name) == (field.name == included_field.name)
