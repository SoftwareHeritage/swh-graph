#!/usr/bin/env bats

load repo_helper

@test "export revisions" {
    run_git2graph "$TEST_REPO_DIR" "$TEST_TMPDIR" -E rev:rev
    assert_equal_graphs ${DATA_DIR}/graphs/revisions ${TEST_TMPDIR}
}

@test "export edges with revision targets" {
    run_git2graph "$TEST_REPO_DIR" "$TEST_TMPDIR" -E "*:rev"
    assert_equal_graphs ${DATA_DIR}/graphs/to-revisions ${TEST_TMPDIR}
}

@test "export directories" {
    run_git2graph "$TEST_REPO_DIR" "$TEST_TMPDIR" -E "dir:*"
    assert_equal_graphs ${DATA_DIR}/graphs/directories ${TEST_TMPDIR}
}

@test "export releases" {
    run_git2graph "$TEST_REPO_DIR" "$TEST_TMPDIR" -E "rel:*"
    assert_equal_graphs ${DATA_DIR}/graphs/releases ${TEST_TMPDIR}
}
