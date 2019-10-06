#!/usr/bin/env bats

load repo_helper

@test "export entire graph" {
    run_git2graph "$TEST_REPO_DIR" "$TEST_TMPDIR"
    assert_equal_graphs ${DATA_DIR}/graphs/full ${TEST_TMPDIR}
}

@test "export entire graph (using wildcard)" {
    run_git2graph "$TEST_REPO_DIR" "$TEST_TMPDIR" -E "*:*"
    assert_equal_graphs ${DATA_DIR}/graphs/full ${TEST_TMPDIR}
}
