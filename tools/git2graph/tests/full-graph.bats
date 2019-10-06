#!/usr/bin/env bats

load repo_helper

@test "export entire graph" {
    run_git2graph "$TEST_REPO_DIR" "$TEST_TMPDIR" -o https://example.com/foo/bar.git
    assert_equal_graphs ${DATA_DIR}/graphs/full ${TEST_TMPDIR}
}
