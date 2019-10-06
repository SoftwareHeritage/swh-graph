/*
 * Copyright (C) 2019  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

/* Crawl a Git repository and output it as a graph, i.e., as textual file
 * containing a list of graph edges, one per line. Each edge is a <from, to>
 * pair of Software Heritage (SWH) Persistent Identifiers (PIDs).
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
#include <glib.h>


#define SWH_PREFIX  "swh:1"
#define SWH_DIR     "swh:1:dir"
#define SWH_REV     "swh:1:rev"
#define SWH_PIDSZ   (GIT_OID_HEXSZ + 10)  // size of a SWH PID

// length of a textual edge line
#define EDGES_LINELEN  (SWH_PIDSZ * 2 + 2)

// Output buffer size for edges files. To guarantee atomic and non-interleaved
// writes (which matter when used concurrently writing to a shared FIFO), size
// must be <= PIPE_BUF and a multiple of EDGES_LINELEN.
#define EDGES_OUTSZ  ((PIPE_BUF / EDGES_LINELEN) * EDGES_LINELEN)

// GIT_OBJ_* constants extension for non-git objects
#define SWH_OBJ_SNP  5  // snapshots (swh:1:snp:...)
#define SWH_OBJ_ORI  6  // origins (swh:1:ori:...)
#define SWH_OBJ_LOC  7  // lines of code (swh:1:loc:...)

#define OBJ_TYPES  8

#define ELT_SEP   ","  // element separator in lists
#define PAIR_SEP  ":"  // key/value separator in paris

/* map from libgit2's git_otype (+ SWH-specific types above) to SWH PID type
 * qualifiers */
static char *_git_otype2swh[OBJ_TYPES] = {
	"*",   // 0 == GIT_OBJ__EXT1 (unused in libgit2, used as wildcard here)
	"rev", // 1 == GIT_OBJ_COMMIT
	"dir", // 2 == GIT_OBJ_TREE
	"cnt", // 3 == GIT_OBJ_BLOB
	"rel", // 4 == GIT_OBJ_TAG
	"snp", // 5 == SWH_OBJ_SNP
	"ori", // 6 == SWH_OBJ_ORI
	"loc", // 7 == SWH_OBJ_LOC
};

#define GIT_OBJ_ANY  GIT_OBJ__EXT1

/* Convert a git object type (+ SWH-specific types above) to the corresponding
 * SWH PID type. */
#define git_otype2swh(type)  _git_otype2swh[(type)]

/* Parse object type (libgit's + SWH-specific types) from 3-letter type
 * qualifiers. Return either object type, or 0 in case of "*" wildcard, or -1
 * in case of parse error. */
int parse_otype(char *str) {
	for (int i = 0; i < OBJ_TYPES; i++) {
		if (strcmp(str, _git_otype2swh[i]) == 0)
			return i;
	}
	return -1;
}

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

/* Whether a nore type is allowed as *origin* for edges. Derived information
 * from the _allowed_edges matrix. */
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


/* Compute allowed node types based on allowed edge types. */
void init_allowed_nodes_from_edges(
	int allowed_edges[OBJ_TYPES][OBJ_TYPES],
	int allowed_nodes[OBJ_TYPES])
{
	for (int i = 0; i < OBJ_TYPES; i++) {
		allowed_nodes[i] = false;  // disallowed by default
		// allowed if an edge can originate from it...
		for (int src_type = 0; src_type < OBJ_TYPES; src_type++)
			allowed_nodes[i] = allowed_nodes[i]	\
				|| allowed_edges[src_type][i];
		// ...or lead to it
		for (int dst_type = 0; dst_type < OBJ_TYPES; dst_type++)
			allowed_nodes[i] = allowed_nodes[i]	\
				|| allowed_edges[i][dst_type];
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


/* Emit edges for current object. */
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
	FILE *edges_out = ((cb_payload *) payload)->edges_out;

	check_lg2(git_odb_read_header(&len, &obj_type, odb, id),
		  "cannot read object header", NULL);
	if (!is_node_allowed(obj_type))
		return 0;

	// format node PID
	sprintf(swhpid, "swh:1:%s:", git_otype2swh(obj_type));
	git_oid_tostr(swhpid + 10, sizeof(oidstr), id);

	// emit edges
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

	return 0;
}


void exit_usage(char *msg) {
	if (msg != NULL)
		fprintf(stderr, "Error: %s\n\n", msg);

	fprintf(stderr, "Usage: git2graph [OPTION..] GIT_REPO_DIR\n");
	fprintf(stderr, "\n");
	fprintf(stderr, "Options:\n");
	fprintf(stderr, "  -o, --output=PATH              output file, default to stdout\n");
	fprintf(stderr, "  -E, --edges-filter=EDGES_EXPR  only emit selected edges\n");
	fprintf(stderr, "\n");
	fprintf(stderr, "EDGES_EXPR is a comma separate list of src_TYPE:dst_TYPE pairs\n");
	fprintf(stderr, "TYPE is one of: cnt, dir, loc, ori, rel, rev, snp, *\n");
	fprintf(stderr, "\nNote: you can use \"-\" for stdout in file names.\n");


	exit(EXIT_FAILURE);
}


/* command line arguments */
typedef struct {
	char *outfile;
	char *edges_filter;
	char *repo_dir;
} cli_args;


cli_args *parse_cli(int argc, char **argv) {
	int opt;

	cli_args *args = malloc(sizeof(cli_args));
	if (args == NULL) {
		perror("Cannot allocate memory.");
		exit(EXIT_FAILURE);
	} else {
		args->outfile = NULL;
		args->edges_filter = NULL;
		args->repo_dir = NULL;
	}

	static struct option long_opts[] = {
		{"edges-filter", required_argument, 0, 'E' },
		{"output",       required_argument, 0, 'o' },
		{"help",         no_argument,       0, 'h' },
		{0,              0,                 0,  0  }
	};

	while ((opt = getopt_long(argc, argv, "E:o:h", long_opts,
				  NULL)) != -1) {
		switch (opt) {
		case 'E': args->edges_filter = optarg; break;
		case 'o': args->outfile = optarg; break;
		case 'h':
		default:
			exit_usage(NULL);
		}
	}
	if (argv[optind] == NULL)
		exit_usage(NULL);
	args->repo_dir = argv[optind];

	if (args->outfile == NULL)
		args->outfile = "-";

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


void fill_matrix(int matrix[OBJ_TYPES][OBJ_TYPES], int val) {
	for (int i = 0; i < OBJ_TYPES; i++)
		for (int j = 0; j < OBJ_TYPES; j++)
			matrix[i][j] = val;
}


void fill_row(int matrix[OBJ_TYPES][OBJ_TYPES], int row, int val) {
	for (int j = 0; j < OBJ_TYPES; j++)
		matrix[row][j] = val;
}


void fill_column(int matrix[OBJ_TYPES][OBJ_TYPES], int col, int val) {
	for (int i = 0; i < OBJ_TYPES; i++)
		matrix[i][col] = val;
}


void fill_vector(int vector[OBJ_TYPES], int val) {
	for (int i = 0; i < OBJ_TYPES; i++)
		vector[i] = val;
}


/* Dump node/edge filters to a given stream. For debugging purposes. */
void _dump_filters(FILE *out, int matrix[OBJ_TYPES][OBJ_TYPES], int vector[OBJ_TYPES]) {
	fprintf(out, "TO rev dir cnt rel snp ori loc FROM\n");
	for(int i = 0; i < OBJ_TYPES; i++) {
		for(int j = 0; j < OBJ_TYPES; j++)
			fprintf(out, "%d   ", matrix[i][j]);
		fprintf(out, "%s\n", _git_otype2swh[i]);
	}

	fprintf(out, "   rev dir cnt rel snp ori loc\n");
	for (int i = 0; i < OBJ_TYPES; i++)
		fprintf(out, "%d   ", vector[i]);
}


/* set up nodes and edges restrictions, interpreting command line filters */
void init_graph_filters(char *edges_filter) {
	char **filters;
	char **types;
	char **ptr;
	int src_type, dst_type;

	if (edges_filter != NULL) {
		fill_matrix(_allowed_edges, false);  // nothing allowed by default
		filters = g_strsplit(edges_filter, ELT_SEP, -1);  // "typ:typ" pairs
		for (ptr = filters; *ptr; ptr++) {
			types = g_strsplit(*ptr, PAIR_SEP, 2);  // 2 "typ" fragments

			src_type = parse_otype(types[0]);
			dst_type = parse_otype(types[1]);
			if (src_type == GIT_OBJ_ANY && dst_type == GIT_OBJ_ANY) {
				// "*:*" wildcard
				fill_matrix(_allowed_edges, true);
				break;  // all edges allowed already
			} else if (src_type == GIT_OBJ_ANY) {  // "*:typ" wildcard
				fill_column(_allowed_edges, dst_type, true);
			} else if (dst_type == GIT_OBJ_ANY) {  // "typ:*" wildcard
				fill_row(_allowed_edges, src_type, true);
			} else  // "src_type:dst_type"
				_allowed_edges[src_type][dst_type] = true;

			g_strfreev(types);
		}
		g_strfreev(filters);
	}

	init_allowed_nodes_from_edges(_allowed_edges, _allowed_nodes);
}


int main(int argc, char **argv) {
	git_repository *repo;
	git_odb *odb;
	int rc;
	cli_args *args;
	cb_payload *payload;
	FILE *edges_out;
	char edges_buf[EDGES_OUTSZ];
	
	args = parse_cli(argc, argv);
	init_graph_filters(args->edges_filter);
	// _dump_filters(stdout, _allowed_edges, _allowed_nodes);

	git_libgit2_init();
	check_lg2(git_repository_open(&repo, args->repo_dir),
		  "cannot open repository", NULL);
	check_lg2(git_repository_odb(&odb, repo),
		  "cannot get object DB", NULL);

	edges_out = open_out_stream(args->outfile, edges_buf, EDGES_OUTSZ);
	assert(EDGES_OUTSZ <= PIPE_BUF && (EDGES_OUTSZ % EDGES_LINELEN == 0));

	payload = malloc(sizeof(cb_payload));
	payload->odb = odb;
	payload->repo = repo;
	payload->edges_out = edges_out;

	rc = git_odb_foreach(odb, emit_obj, payload);
	check_lg2(rc, "failure during object iteration", NULL);

	git_odb_free(odb);
	git_repository_free(repo);
	free(payload);
	exit(rc);
}
