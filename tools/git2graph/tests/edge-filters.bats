#!/usr/bin/env bats

load repo_helper

@test "export revision self-edges" {
    run_git2graph "$TEST_REPO_DIR" "$TEST_TMPDIR" -N '' -E rev:rev
    assert_equal_graphs ${DATA_DIR}/graphs/rev-edges ${TEST_TMPDIR}
}

@test "export edges to revisions" {
    run_git2graph "$TEST_REPO_DIR" "$TEST_TMPDIR" -N '' -E "*:rev"
    assert_equal_graphs ${DATA_DIR}/graphs/to-rev-edges ${TEST_TMPDIR}
}

@test "export edges from directories" {
    run_git2graph "$TEST_REPO_DIR" "$TEST_TMPDIR" -N '' -E "dir:*"
    assert_equal_graphs ${DATA_DIR}/graphs/from-dir-edges ${TEST_TMPDIR}
}

@test "export edges from releases" {
    run_git2graph "$TEST_REPO_DIR" "$TEST_TMPDIR" -N '' -E "rel:*"
    assert_equal_graphs ${DATA_DIR}/graphs/from-rel-edges ${TEST_TMPDIR}
}
