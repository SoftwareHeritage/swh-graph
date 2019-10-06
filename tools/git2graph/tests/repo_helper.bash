
DATA_DIR="${BATS_TEST_DIRNAME}/data"
TEST_REPO_TGZ="${DATA_DIR}/sample-repo.tgz"

setup () {
    TEST_TMPDIR=$(mktemp -td swh-graph-test.XXXXXXXXXX)
    (cd "$TEST_TMPDIR" ; tar xaf "$TEST_REPO_TGZ")
    TEST_REPO_DIR="${TEST_TMPDIR}/sample-repo"
}

teardown () {
    rm -rf "$TEST_TMPDIR"
}

# Invoke git2graph (SUT) on the given repo_dir and store its results in the CSV
# files nodes.csv and edges.csv located under the given dest_dir.
run_git2graph () {
    repo_dir="$1"
    dest_dir="$2"
    shift 2

    nodes_file="${dest_dir}/nodes.csv"
    edges_file="${dest_dir}/edges.csv"

    if [ ! -d "$dest_dir" ] ; then
        mkdir -p "$dest_dir"
    fi

    ./git2graph "$@" -n >(sort > "$nodes_file") -e >(sort > "$edges_file") "$repo_dir"
}

# Ensure that two graphs, each specified as a dir that should contain a pair of
# sorted, textual files called nodes.csv and edges.csv.  Comparison is done
# using diff.
assert_equal_graphs () {
    dir_1="$1"
    dir_2="$2"

    diff "${dir_1}/nodes.csv" "${dir_2}/nodes.csv" &&
    diff "${dir_1}/edges.csv" "${dir_2}/edges.csv"
}
