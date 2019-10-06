#!/usr/bin/env bats

load repo_helper

@test "export revision nodes" {
    run_git2graph "$TEST_REPO_DIR" "$TEST_TMPDIR" -N rev -E ''
    assert_equal_graphs ${DATA_DIR}/graphs/rev-nodes ${TEST_TMPDIR}
}

@test "export directory nodes" {
    run_git2graph "$TEST_REPO_DIR" "$TEST_TMPDIR" -N dir -E ''
    assert_equal_graphs ${DATA_DIR}/graphs/dir-nodes ${TEST_TMPDIR}
}

@test "export file system layer nodes" {
    run_git2graph "$TEST_REPO_DIR" "$TEST_TMPDIR" -N cnt,dir -E ''
    assert_equal_graphs ${DATA_DIR}/graphs/fs-nodes ${TEST_TMPDIR}
}
