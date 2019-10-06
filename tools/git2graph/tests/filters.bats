#!/usr/bin/env bats

load repo_helper

@test "export revisions" {
    run_git2graph "$TEST_REPO_DIR" "$TEST_TMPDIR" -E rev:rev -N rev
    assert_equal_graphs ${DATA_DIR}/graphs/revisions ${TEST_TMPDIR}
}

@test "export directories" {
    run_git2graph "$TEST_REPO_DIR" "$TEST_TMPDIR" -E dir:* -N cnt,dir
    assert_equal_graphs ${DATA_DIR}/graphs/directories ${TEST_TMPDIR}
}

@test "export releases" {
    run_git2graph "$TEST_REPO_DIR" "$TEST_TMPDIR" -E rel:* -N rel,rev
    assert_equal_graphs ${DATA_DIR}/graphs/releases ${TEST_TMPDIR}
}
