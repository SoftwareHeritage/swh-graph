# Copyright (C) 2025  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import os

import pytest

from swh.graph.download import GraphDownloader


def check_dataset_download(graph_example_dataset_path, downloaded_dataset_path):
    for root, _, files in os.walk(graph_example_dataset_path):
        for filename in files:
            file_path = os.path.join(root, filename)
            relative_path = file_path.replace(graph_example_dataset_path + "/", "")
            downloaded_file_path = os.path.join(downloaded_dataset_path, relative_path)
            assert os.path.exists(downloaded_file_path)
            assert os.path.getsize(downloaded_file_path) == os.path.getsize(file_path)


def test_graph_downloader_ok(
    graph_example_dataset_path, s3_graph_dataset_url, tmp_path, mocked_aws
):
    """Check graph dataset can be successfully downloaded"""
    graph_downloader = GraphDownloader(
        local_graph_path=tmp_path,
        s3_graph_path=s3_graph_dataset_url,
    )
    assert graph_downloader.download_graph()

    check_dataset_download(graph_example_dataset_path, tmp_path)


def test_graph_downloader_resume_download(
    graph_example_dataset_path, s3_graph_dataset_url, tmp_path, mocked_aws
):
    """Check download of graph dataset can be successfully resumed when
    a download error happened"""

    # return a patched object.iter_chunks method that raises a ConnectionError
    # after having yielded a few bytes
    def failing_iter_chunks(iter_chunks_orig):
        def iter_chunks(chunk_size):
            for i, chunk in enumerate(iter_chunks_orig(4)):
                if i < 3:
                    yield chunk
                else:
                    raise ConnectionError("Remote disconnected")

        return iter_chunks

    # return a patched client.get_object method allowing to mock the iter_chunks
    # method of an s3 object
    def patched_get_object(client_get_object_orig, download_failure_filename):
        def get_object(**kwargs):
            obj = client_get_object_orig(**kwargs)
            if kwargs["Key"].endswith(download_failure_filename):
                obj["Body"].iter_chunks = failing_iter_chunks(obj["Body"].iter_chunks)
            return obj

        return get_object

    graph_downloader = GraphDownloader(
        local_graph_path=tmp_path,
        s3_graph_path=s3_graph_dataset_url,
    )

    # simulate download failure for a file
    download_failure_filename = "example-labelled.ef"

    orig_client_get_object = graph_downloader.client.get_object
    graph_downloader.client.get_object = patched_get_object(
        orig_client_get_object, download_failure_filename
    )

    # first download attempt should fail
    assert not graph_downloader.download_graph()

    # downloaded dataset should be incomplete
    with pytest.raises(AssertionError):
        check_dataset_download(graph_example_dataset_path, tmp_path)

    # part file should have been created
    part_file = tmp_path / (download_failure_filename + ".part")
    previous_part_size = part_file.stat().st_size
    assert part_file.exists()

    # second download attempt should still fail
    assert not graph_downloader.download_graph()

    # downloaded dataset should still be incomplete
    with pytest.raises(AssertionError):
        check_dataset_download(graph_example_dataset_path, tmp_path)

    # part file should have been updated with new bytes
    assert part_file.exists()
    assert part_file.stat().st_size > previous_part_size

    # restore original client.get_object implementation for
    # download to succeed
    graph_downloader.client.get_object = orig_client_get_object

    # third download attempt should succeed
    assert graph_downloader.download_graph()

    # no more part files exist
    assert all(
        not filename.endswith(".part")
        for _, _, files in os.walk(graph_example_dataset_path)
        for filename in files
    )

    # downloaded dataset should be complete
    check_dataset_download(graph_example_dataset_path, tmp_path)
