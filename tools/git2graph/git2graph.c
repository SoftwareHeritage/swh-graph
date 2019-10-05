/*
 * Copyright (C) 2019  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

/* Crawl a Git repository and output it as a graph, i.e., as a pair of textual
 * files <nodes, edges>. The nodes file will contain a list of graph nodes as
 * Software Heritage (SWH) Persistent Identifiers (PIDs); the edges file a list
 * of graph edges as <from, to> PID pairs.
 */

#include <assert.h>
#include <getopt.h>
#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <git2.h>


#define SWH_PREFIX  "swh:1"
#define SWH_DIR     "swh:1:dir"
#define SWH_REV     "swh:1:rev"
#define SWH_PIDSZ   (GIT_OID_HEXSZ + 10)  // size of a SWH PID

// line-lengths in nodes and edges file
#define NODES_LINELEN  (SWH_PIDSZ + 1)
#define EDGES_LINELEN  (SWH_PIDSZ * 2 + 2)

// Output buffer sizes for nodes and edges files. To guarantee atomic and
// non-interleaved writes (which matter when used concurrently writing to a
// shared FIFO), these sizes must be <= PIPE_BUF and multiples of
// {NODES,EDGES}_LINELEN.
#define NODES_OUTSZ  ((PIPE_BUF / NODES_LINELEN) * NODES_LINELEN)
#define EDGES_OUTSZ  ((PIPE_BUF / EDGES_LINELEN) * EDGES_LINELEN)

// GIT_OBJ_* constants extension for non-git objects
#define SWH_OBJ_SNP  5  // snapshots (swh:1:snp:...)
#define SWH_OBJ_ORI  6  // origins (swh:1:ori:...)
#define SWH_OBJ_LOC  7  // lines of code (swh:1:loc:...)

#define OBJ_TYPES  8

/* map from libgit2's git_otype (+ SWH-specific types above) to SWH PID type
 * qualifiers */
static char *_git_otype2swh[OBJ_TYPES] = {
	"ERR", // 0 == GIT_OBJ__EXT1 (unused)
	"rev", // 1 == GIT_OBJ_COMMIT
	"dir", // 2 == GIT_OBJ_TREE
	"cnt", // 3 == GIT_OBJ_BLOB
	"rel", // 4 == GIT_OBJ_TAG
	"snp", // 5 == SWH_OBJ_SNP
	"ori", // 6 == SWH_OBJ_ORI
	"loc", // 7 == SWH_OBJ_LOC
};

/* Convert a git object type (+ SWH-specific types above) to the corresponding
 * SWH PID type. */
#define git_otype2swh(type)  _git_otype2swh[(type)]

/* Allowed edge types matrix. Each cell denotes whether edges from a given
 * SRC_TYPE to a given DST_TYPE should be produced or not. */
static int _allowed_edges[OBJ_TYPES][OBJ_TYPES] = {
	// TO   rev    dir    cnt    rel    snp    ori    loc        |
	// ----------------------------------------------------------------
	{true,  true,  true,  true,  true,  true,  true,  true},  // | FROM
	{true,  true,  true,  true,  true,  true,  true,  true},  // | rev
	{true,  true,  true,  true,  true,  true,  true,  true},  // | dir
	{true,  true,  true,  true,  true,  true,  true,  true},  // | cnt
	{true,  true,  true,  true,  true,  true,  true,  true},  // | rel
	{true,  true,  true,  true,  true,  true,  true,  true},  // | snp
	{true,  true,  true,  true,  true,  true,  true,  true},  // | ori
	{true,  true,  true,  true,  true,  true,  true,  true},  // | loc
};

/* Allowed node type vector. */
static int _allowed_nodes[OBJ_TYPES] = {
	true,  //
	true,  // rev
	true,  // dir
	true,  // cnt
	true,  // rel
	true,  // snp
	true,  // ori
	true,  // loc
};

#define is_edge_allowed(src_type, dst_type)  _allowed_edges[(src_type)][(dst_type)]
#define is_node_allowed(type)  _allowed_nodes[(type)]

/* extra payload for callback invoked on Git objects */
typedef struct {
	git_odb *odb;  // Git object DB
	git_repository *repo;  // Git repository
	FILE *nodes_out;  // stream to write nodes to, or NULL
	FILE *edges_out;  // stream to write edges to, or NULL
} cb_payload;


/* Invoke a libgit2 method and exits with an error message in case of
 * failure.
 * 
 * Reused from libgit2 examples, specifically common.c, available under CC0.
 */
void check_lg2(int error, const char *message, const char *extra) {
	const git_error *lg2err;
	const char *lg2msg = "", *lg2spacer = "";

	if (!error)
		return;

	if ((lg2err = giterr_last()) != NULL && lg2err->message != NULL) {
		lg2msg = lg2err->message;
		lg2spacer = " - ";
	}

	if (extra)
		fprintf(stderr, "%s '%s' [%d]%s%s\n",
			message, extra, error, lg2spacer, lg2msg);
	else
		fprintf(stderr, "%s [%d]%s%s\n",
			message, error, lg2spacer, lg2msg);

	exit(1);
}


/* Compute allowed node types base on allowed edge types, which is a sane
 * default. The result should be overridden in case one wants to output
 * specific nodes, but not their outgoing edges. */
void init_allowed_nodes_from_edges(
	int allowed_edges[OBJ_TYPES][OBJ_TYPES],
	int allowed_nodes[OBJ_TYPES])
{
	for (int src_type = 0; src_type < OBJ_TYPES; src_type++) {
		allowed_nodes[src_type] = false;
		for (int dst_type = 0; dst_type < OBJ_TYPES; dst_type++) {
			allowed_nodes[src_type] = allowed_nodes[src_type] \
				|| allowed_edges[src_type][dst_type];
		}
	}
}


/* Emit commit edges. */
void emit_commit_edges(const git_commit *commit, const char *swhpid, FILE *out) {
	unsigned int i, max_i;
	char oidstr[GIT_OID_HEXSZ + 1];  // to PID

	// rev -> dir
	if (is_edge_allowed(GIT_OBJ_COMMIT, GIT_OBJ_TREE)) {
		git_oid_tostr(oidstr, sizeof(oidstr),
			      git_commit_tree_id(commit));
		fprintf(out, "%s %s:%s\n", swhpid, SWH_DIR, oidstr);
	}

	// rev -> rev
	if (is_edge_allowed(GIT_OBJ_COMMIT, GIT_OBJ_COMMIT)) {
		max_i = (unsigned int)git_commit_parentcount(commit);
		for (i = 0; i < max_i; ++i) {
			git_oid_tostr(oidstr, sizeof(oidstr),
				      git_commit_parent_id(commit, i));
			fprintf(out, "%s %s:%s\n", swhpid, SWH_REV, oidstr);
		}
	}
}

/* Emit tag edges. */
void emit_tag_edges(const git_tag *tag, const char *swhpid, FILE *out) {
	char oidstr[GIT_OID_HEXSZ + 1];
	int target_type;

	// rel -> *
	target_type = git_tag_target_type(tag);
	if (is_edge_allowed(GIT_OBJ_TAG, target_type)) {
		git_oid_tostr(oidstr, sizeof(oidstr), git_tag_target_id(tag));
		fprintf(out, "%s %s:%s:%s\n", swhpid, SWH_PREFIX,
			git_otype2swh(target_type), oidstr);
	}
}


/* Emit tree edges. */
void emit_tree_edges(const git_tree *tree, const char *swhpid, FILE *out) {
	size_t i, max_i = (int)git_tree_entrycount(tree);
	char oidstr[GIT_OID_HEXSZ + 1];
	const git_tree_entry *te;
	int entry_type;

	// dir -> *
	for (i = 0; i < max_i; ++i) {
		te = git_tree_entry_byindex(tree, i);
		entry_type = git_tree_entry_type(te);
		if (is_edge_allowed(GIT_OBJ_TREE, entry_type)) {
			git_oid_tostr(oidstr, sizeof(oidstr),
				      git_tree_entry_id(te));
			fprintf(out, "%s %s:%s:%s\n", swhpid, SWH_PREFIX,
				git_otype2swh(entry_type), oidstr);
		}
	}
}


/* Emit node and edges for current object. */
int emit_obj(const git_oid *id, void *payload) {
	char oidstr[GIT_OID_HEXSZ + 1];
	char swhpid[SWH_PIDSZ + 1];
	size_t len;
	int obj_type;
	git_commit *commit;
	git_tag *tag;
	git_tree *tree;

	git_odb *odb = ((cb_payload *) payload)->odb;
	git_repository *repo = ((cb_payload *) payload)->repo;
	FILE *nodes_out = ((cb_payload *) payload)->nodes_out;
	FILE *edges_out = ((cb_payload *) payload)->edges_out;

	check_lg2(git_odb_read_header(&len, &obj_type, odb, id),
		  "cannot read object header", NULL);
	if (!is_node_allowed(obj_type))  // no outbound edges allowed, skip node
		return 0;

	// emit node
	sprintf(swhpid, "swh:1:%s:", git_otype2swh(obj_type));
	git_oid_tostr(swhpid + 10, sizeof(oidstr), id);
	if (nodes_out != NULL)
		fprintf(nodes_out, "%s\n", swhpid);

	// emit edges
	if (edges_out != NULL) {
		switch (obj_type) {
		case GIT_OBJ_BLOB:  // graph leaf: no edges to emit
			break;
		case GIT_OBJ_COMMIT:
			check_lg2(git_commit_lookup(&commit, repo, id),
				  "cannot find commit", NULL);
			emit_commit_edges(commit, swhpid, edges_out);
			git_commit_free(commit);
			break;
		case GIT_OBJ_TAG:
			check_lg2(git_tag_lookup(&tag, repo, id),
				  "cannot find tag", NULL);
			emit_tag_edges(tag, swhpid, edges_out);
			git_tag_free(tag);
			break;
		case GIT_OBJ_TREE:
			check_lg2(git_tree_lookup(&tree, repo, id),
				  "cannot find tree", NULL);
			emit_tree_edges(tree, swhpid, edges_out);
			git_tree_free(tree);
			break;
		default:
			git_oid_tostr(oidstr, sizeof(oidstr), id);
			fprintf(stderr, "ignoring unknown object: %s\n", oidstr);
			break;
		}
	}

	return 0;
}


void exit_usage(char *msg) {
	if (msg != NULL)
		fprintf(stderr, "Error: %s\n\n", msg);

	fprintf(stderr, "Usage: git2graph [OPTION..] GIT_REPO_DIR\n");
	fprintf(stderr, "\n");
	fprintf(stderr, "Options:\n");
	fprintf(stderr, "  -e, --edges-file=PATH          file where to store edges\n");
	fprintf(stderr, "  -n, --nodes-file=PATH          file where to store nodes\n");
	fprintf(stderr, "\nNote: you can use \"-\" for stdout in file names.\n");

	exit(EXIT_FAILURE);
}


/* command line arguments */
typedef struct {
	char *nodes_path;
	char *edges_path;
	char *repo_dir;
} cli_args;


cli_args *parse_cli(int argc, char **argv) {
	int opt;

	cli_args *args = malloc(sizeof(cli_args));
	if (args == NULL) {
		perror("Cannot allocate memory.");
		exit(EXIT_FAILURE);
	} else {
		args->nodes_path = NULL;
		args->edges_path = NULL;
		args->repo_dir = NULL;
	}

	static struct option long_opts[] = {
		{"edges-file",   required_argument, 0, 'e' },
		{"nodes-file",   required_argument, 0, 'n' },
		{"help",         no_argument,       0, 'h' },
		{0,              0,                 0,  0  }
	};

	while ((opt = getopt_long(argc, argv, "e:n:h", long_opts,
				  NULL)) != -1) {
		switch (opt) {
		case 'e': args->edges_path = optarg;   break;
		case 'n': args->nodes_path = optarg;   break;
		case 'h':
		default:
			exit_usage(NULL);
		}
	}
	if (argv[optind] == NULL)
		exit_usage(NULL);
	args->repo_dir = argv[optind];

	return args;
}


/* open output stream specified on the command line (if at all) */
FILE *open_out_stream(char *cli_path, char *buf, int bufsiz) {
	FILE *stream;

	if (cli_path == NULL)
		stream = NULL;
	else if (strcmp(cli_path, "-") == 0)
		stream = stdout;
	else if((stream = fopen(cli_path, "w")) == NULL) {
		fprintf(stderr, "can't open file: %s\n", cli_path);
		exit(EXIT_FAILURE);
	}

	// ensure atomic and non-interleaved writes
	if (stream != NULL)
		setvbuf(stream, buf, _IOFBF, bufsiz);

	return stream;
}


int main(int argc, char **argv) {
	git_repository *repo;
	git_odb *odb;
	int rc;
	cli_args *args;
	cb_payload *payload;
	FILE *nodes_out, *edges_out;
	char nodes_buf[NODES_OUTSZ];
	char edges_buf[EDGES_OUTSZ];
	
	args = parse_cli(argc, argv);
	init_allowed_nodes_from_edges(_allowed_edges, _allowed_nodes);

	git_libgit2_init();
	check_lg2(git_repository_open(&repo, args->repo_dir),
		  "cannot open repository", NULL);
	check_lg2(git_repository_odb(&odb, repo),
		  "cannot get object DB", NULL);

	nodes_out = open_out_stream(args->nodes_path, nodes_buf, NODES_OUTSZ);
	edges_out = open_out_stream(args->edges_path, edges_buf, EDGES_OUTSZ);
	assert(NODES_OUTSZ <= PIPE_BUF && (NODES_OUTSZ % NODES_LINELEN == 0));
	assert(EDGES_OUTSZ <= PIPE_BUF && (EDGES_OUTSZ % EDGES_LINELEN == 0));

	payload = malloc(sizeof(cb_payload));
	payload->odb = odb;
	payload->repo = repo;
	payload->nodes_out = nodes_out;
	payload->edges_out = edges_out;

	rc = git_odb_foreach(odb, emit_obj, payload);
	check_lg2(rc, "failure during object iteration", NULL);

	git_odb_free(odb);
	git_repository_free(repo);
	free(payload);
	exit(rc);
}
