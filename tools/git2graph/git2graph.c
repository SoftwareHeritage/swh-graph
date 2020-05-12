/*
 * Copyright (C) 2019-2020  The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

/* Crawls a Git repository and outputs it as a graph, i.e., as a pair of
 * textual files <nodes, edges>. The nodes file will contain a list of graph
 * nodes as Software Heritage (SWH) Persistent Identifiers (SWHIDs); the edges
 * file a list of graph edges as <from, to> SWHID pairs.
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


#define SWH_PREFIX   "swh:1"
#define SWH_DIR_PRE  "swh:1:dir"
#define SWH_ORI_PRE  "swh:1:ori"
#define SWH_REV_PRE  "swh:1:rev"
#define SWH_SNP_PRE  "swh:1:snp"

#define SWHID_SZ  (GIT_OID_HEXSZ + 10)  // size of a SWHID

// line-lengths in nodes and edges file
#define NODES_LINELEN  (SWHID_SZ + 1)
#define EDGES_LINELEN  (SWHID_SZ * 2 + 2)

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

#define ELT_SEP   ","  // element separator in lists
#define PAIR_SEP  ":"  // key/value separator in paris

/* map from libgit2's git_otype (+ SWH-specific types above) to SWHID type
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

/* map from libgit2's git_otype (+ SWH-specific types above) to long names of
 * SWHID types. Used most notably to assemble snapshot manifests; see
 * https://docs.softwareheritage.org/devel/apidoc/swh.model.html#swh.model.identifiers.snapshot_identifier */
static char *_git_otype2swhlong[OBJ_TYPES] = {
	"ERROR",      // 0 == GIT_OBJ__EXT1
	"revision",   // 1 == GIT_OBJ_COMMIT
	"directory",  // 2 == GIT_OBJ_TREE
	"content",    // 3 == GIT_OBJ_BLOB
	"release",    // 4 == GIT_OBJ_TAG
	"snapshot",   // 5 == SWH_OBJ_SNP
	"origin",     // 6 == SWH_OBJ_ORI
	"line",       // 7 == SWH_OBJ_LOC
};

#define MY_GIT_OBJ_ANY  GIT_OBJ__EXT1

/* Convert a git object type (+ SWH-specific types above) to the corresponding
 * SWHID type. */
#define git_otype2swh(type)      _git_otype2swh[(type)]
#define git_otype2swhlong(type)  _git_otype2swhlong[(type)]

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

/* Allowed node types vector. */
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


/* runtime configuration, for node/edges emitter functions */
typedef struct {
	git_odb *odb;  // Git object DB
	git_repository *repo;  // Git repository
	FILE *nodes_out;  // stream to write nodes to, or NULL
	FILE *edges_out;  // stream to write edges to, or NULL
} config_t;


/* context for iterating over refs */
typedef struct {
	GByteArray *manifest;  // snapshot manifest, incrementally build
	GSList *pids;          // target SWHIDs, incrementally collected
	char *snapshot_pid;    // snapshot SWHID, initially missing
	config_t *conf;        // runtime configuration
} ref_ctxt_t;


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


/* Emit commit edges. */
void emit_commit_edges(const git_commit *commit, const char *swhpid, FILE *out) {
	unsigned int i, max_i;
	char oidstr[GIT_OID_HEXSZ + 1];  // to SWHID

	// rev -> dir
	if (is_edge_allowed(GIT_OBJ_COMMIT, GIT_OBJ_TREE)) {
		git_oid_tostr(oidstr, sizeof(oidstr),
			      git_commit_tree_id(commit));
		fprintf(out, "%s %s:%s\n", swhpid, SWH_DIR_PRE, oidstr);
	}

	// rev -> rev
	if (is_edge_allowed(GIT_OBJ_COMMIT, GIT_OBJ_COMMIT)) {
		max_i = (unsigned int)git_commit_parentcount(commit);
		for (i = 0; i < max_i; ++i) {
			git_oid_tostr(oidstr, sizeof(oidstr),
				      git_commit_parent_id(commit, i));
			fprintf(out, "%s %s:%s\n", swhpid, SWH_REV_PRE, oidstr);
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
int emit_obj(const git_oid *oid, config_t *conf) {
	char oidstr[GIT_OID_HEXSZ + 1];
	char swhpid[SWHID_SZ + 1];
	size_t len;
	int obj_type;
	git_commit *commit;
	git_tag *tag;
	git_tree *tree;

	git_odb *odb = conf->odb;
	git_repository *repo = conf->repo;
	FILE *nodes_out = conf->nodes_out;
	FILE *edges_out = conf->edges_out;

	check_lg2(git_odb_read_header(&len, &obj_type, odb, oid),
		  "cannot read object header", NULL);

	// emit node
	sprintf(swhpid, "swh:1:%s:", git_otype2swh(obj_type));
	git_oid_tostr(swhpid + 10, sizeof(oidstr), oid);
	if (nodes_out != NULL && is_node_allowed(obj_type))
		fprintf(nodes_out, "%s\n", swhpid);

	if (edges_out != NULL) {
		// emit edges
		switch (obj_type) {
		case GIT_OBJ_BLOB:  // graph leaf: no edges to emit
			break;
		case GIT_OBJ_COMMIT:
			check_lg2(git_commit_lookup(&commit, repo, oid),
				  "cannot find commit", NULL);
			emit_commit_edges(commit, swhpid, edges_out);
			git_commit_free(commit);
			break;
		case GIT_OBJ_TAG:
			check_lg2(git_tag_lookup(&tag, repo, oid),
				  "cannot find tag", NULL);
			emit_tag_edges(tag, swhpid, edges_out);
			git_tag_free(tag);
			break;
		case GIT_OBJ_TREE:
			check_lg2(git_tree_lookup(&tree, repo, oid),
				  "cannot find tree", NULL);
			emit_tree_edges(tree, swhpid, edges_out);
			git_tree_free(tree);
			break;
		default:
			git_oid_tostr(oidstr, sizeof(oidstr), oid);
			fprintf(stderr, "E: ignoring unknown object: %s\n", oidstr);
			break;
		}
	}

	return 0;
}


/* Callback for emit_snapshots. Add a git reference to a snapshot manifest
 * (payload->manifest), according to the snapshot SWHID spec, see:
 * https://docs.softwareheritage.org/devel/apidoc/swh.model.html#swh.model.identifiers.snapshot_identifier .
 * As a side effect collect SWHIDs of references objects in payload->pids for
 * later reuse.
 *
 * Sample manifest entries for the tests/data/sample-repo.tgz repository:
 *
 *   revision  HEAD                    9bf3ce249cf3d74ef57d5a1fb4227e26818553f0
 *   revision  refs/heads/feature/baz  261586c455130b4bf10a5be7ffb0bf4077581b56
 *   revision  refs/heads/feature/qux  20cca959bae94594f60450f339b408581f1b401f
 *   revision  refs/heads/master       9bf3ce249cf3d74ef57d5a1fb4227e26818553f0
 *   release   refs/tags/1.0           1720af781051a8cafdf3cf134c263ec5c5e72412
 *   release   refs/tags/1.1           d48ad9915be780fcfa296985f69df35e144864a5
 */
int _snapshot_add_ref(char *ref_name, ref_ctxt_t *ctxt) {
	const git_oid *oid;
	size_t len;
	int dangling, obj_type, ref_type;
	const char *target_type, *target_name;
	char ascii_len[GIT_OID_HEXSZ];
	char oidstr[GIT_OID_HEXSZ + 1];
	char *swhpid = malloc(SWHID_SZ + 1);
	git_reference *ref, *ref2;

	check_lg2(git_reference_lookup(&ref, ctxt->conf->repo, ref_name),
		  "cannot find reference", NULL);
	ref_type = git_reference_type(ref);
	assert(ref_type == GIT_REF_OID || ref_type == GIT_REF_SYMBOLIC);

	if (ref_type == GIT_REF_OID) {
		oid = git_reference_target(ref);
		dangling = (oid == NULL);
	} else {  // ref_type == GIT_REF_SYMBOLIC
		target_name = git_reference_symbolic_target(ref);
		dangling = (target_name == NULL);
	}

	if (dangling) {  // target type
		target_type = "dangling";
	} else if (ref_type == GIT_REF_OID) {  // non dangling, direct ref
		check_lg2(git_odb_read_header(&len, &obj_type,
					      ctxt->conf->odb, oid),
			  "cannot read object header", NULL);
		target_type = git_otype2swhlong(obj_type);
	} else {  // non dangling, symbolic ref
		target_type = "alias";

		// recurse to lookup OID and type for SWHID generation
		check_lg2(git_reference_resolve(&ref2, ref),
			  "cannot resolve symbolic reference", NULL);
		assert(git_reference_type(ref2) == GIT_REF_OID);
		oid = git_reference_target(ref2);
		check_lg2(git_odb_read_header(&len, &obj_type,
					      ctxt->conf->odb, oid),
			  "cannot read object header", NULL);
	}
	g_byte_array_append(ctxt->manifest, (unsigned char *) target_type,
			    strlen(target_type));
	g_byte_array_append(ctxt->manifest, (unsigned char *) " ", 1);

	// reference name
	g_byte_array_append(ctxt->manifest, (unsigned char *) ref_name,
			    strlen(ref_name));
	g_byte_array_append(ctxt->manifest, (unsigned char *) "\0", 1);

	// (length-encoded) target ID
	if (dangling) {
		g_byte_array_append(ctxt->manifest, (unsigned char *) "0:", 2);
	} else {
		if (ref_type == GIT_REF_OID) {  // direct ref
			g_byte_array_append(ctxt->manifest,
					    (unsigned char *) "20:", 3);
			g_byte_array_append(ctxt->manifest, oid->id, 20);
		} else {  // symbolic ref
			len = snprintf(ascii_len, sizeof(ascii_len),
				       "%zd:", strlen(target_name));
			assert(len <= sizeof(ascii_len));
			g_byte_array_append(ctxt->manifest,
					    (unsigned char *) ascii_len, len);
			g_byte_array_append(ctxt->manifest,
					    (unsigned char *) target_name,
					    strlen(target_name));
		}
	}

	sprintf(swhpid, "swh:1:%s:", git_otype2swh(obj_type));
	git_oid_tostr(swhpid + 10, sizeof(oidstr), oid);
	if (g_slist_find_custom(ctxt->pids, swhpid, (GCompareFunc) strcmp) == NULL)
		// avoid duplicate outbound snp->* edges
		ctxt->pids = g_slist_prepend(ctxt->pids, swhpid);

	git_reference_free(ref);
	// git_reference_free(ref2);  // XXX triggers double-free, WTH
	return 0;
}


/* emit an edge snp->* */
void emit_snapshot_edge(char *target_pid, ref_ctxt_t *ctxt) {
	FILE *edges_out = ctxt->conf->edges_out;
	char **pid_parts, **ptr;

	pid_parts = g_strsplit(target_pid, ":", 4);
	ptr = pid_parts; ptr++; ptr++;  // move ptr to SWHID type component
	int target_type = parse_otype(*ptr);
	g_strfreev(pid_parts);

	if (edges_out != NULL && is_edge_allowed(SWH_OBJ_SNP, target_type))
		fprintf(edges_out, "%s %s\n", ctxt->snapshot_pid, target_pid);
}


int _collect_ref_name(const char *name, GSList **ref_names) {
	*ref_names = g_slist_insert_sorted(*ref_names,
					   (gpointer) strdup(name),
					   (GCompareFunc) strcmp);
	return 0;
}


/* Emit origin nodes and their outbound edges. Return the snapshot SWHID as a
 * freshly allocated string that should be freed by the caller. */
char *emit_snapshot(config_t *conf) {
	gchar *hex_sha1;
	char *snapshot_pid;
	GBytes *manifest;
	char manifest_header[GIT_OID_HEXSZ];
	GSList *ref_names = NULL;
	FILE *nodes_out = conf->nodes_out;
	int len;

	snapshot_pid = malloc(SWHID_SZ + 1);

	ref_ctxt_t *ctxt = malloc(sizeof(ref_ctxt_t));
	ctxt->manifest = g_byte_array_new();
	ctxt->pids = NULL;
	ctxt->snapshot_pid = NULL;
	ctxt->conf = conf;

	// XXX TODO this does not return symbolic refs, making snapshot SWHIDs
	// potentially incompatible with `swh identify` :-( As a partial
	// workaround we explicitly add HEAD here.
	git_reference_foreach_name(conf->repo,
				   (git_reference_foreach_name_cb) _collect_ref_name,
				   &ref_names);  // collect refs, sorted by name
	ref_names = g_slist_insert_sorted(ref_names, (gpointer) "HEAD",
					  (GCompareFunc) strcmp);
	/* iterate over refs to assemble manifest; side-effect: fill ctxt->pids */
	g_slist_foreach(ref_names, (GFunc) _snapshot_add_ref, (gpointer) ctxt);
	ctxt->pids = g_slist_reverse(ctxt->pids);

	/* prepend header for salted git hashes */
	len = snprintf(manifest_header, sizeof(manifest_header),
		       "snapshot %d", ctxt->manifest->len);
	assert(len <= sizeof(manifest_header));
	g_byte_array_prepend(ctxt->manifest, (unsigned char *) "\0", 1);
	g_byte_array_prepend(ctxt->manifest, (unsigned char *) manifest_header, len);

	/* compute snapshot SWHID and emit snapshot node */
	manifest = g_byte_array_free_to_bytes(ctxt->manifest);
	ctxt->manifest = NULL;  // memory has been freed by *_free_to_bytes
	hex_sha1 = g_compute_checksum_for_bytes(G_CHECKSUM_SHA1, manifest);
	sprintf(snapshot_pid, "%s:%s", SWH_SNP_PRE, hex_sha1);
	ctxt->snapshot_pid = snapshot_pid;
	if (nodes_out != NULL && is_node_allowed(SWH_OBJ_SNP))
		fprintf(nodes_out, "%s\n", snapshot_pid);

	/* emit snp->* edges */
	g_slist_foreach(ctxt->pids, (GFunc) emit_snapshot_edge, (void *) ctxt);

	g_slist_free_full(ctxt->pids, (GDestroyNotify) free);
	free(ctxt);
	g_free(hex_sha1);

	return snapshot_pid;
}


/* emit origin node and its outbound edges (to snapshots) */
void emit_origin(char *origin_url, config_t *conf, char *snapshot_pid) {
	gchar *hex_sha1;
	char origin_pid[SWHID_SZ + 1];
	FILE *nodes_out = conf->nodes_out;
	FILE *edges_out = conf->edges_out;

	if (nodes_out != NULL && is_node_allowed(SWH_OBJ_ORI)) {
		hex_sha1 = g_compute_checksum_for_string(
			G_CHECKSUM_SHA1, origin_url, strlen(origin_url));
		sprintf(origin_pid, "%s:%s", SWH_ORI_PRE, hex_sha1);
		fprintf(nodes_out, "%s\n", origin_pid);
		g_free(hex_sha1);
	}

	if (edges_out != NULL && is_edge_allowed(SWH_OBJ_ORI, SWH_OBJ_SNP))
		fprintf(edges_out, "%s %s\n", origin_pid, snapshot_pid);
}


void exit_usage(char *msg) {
	if (msg != NULL)
		fprintf(stderr, "Error: %s\n\n", msg);

	fprintf(stderr, "Usage: git2graph [OPTION..] GIT_REPO_DIR\n");
	fprintf(stderr, "\n");
	fprintf(stderr, "Options:\n");
	fprintf(stderr, "  -e, --edges-output=PATH        edges output file (default: stdout)\n");
	fprintf(stderr, "  -n, --nodes-output=PATH        nodes output file (default: stdout)\n");
	fprintf(stderr, "  -E, --edges-filter=EDGES_EXPR  only emit selected edges\n");
	fprintf(stderr, "  -N, --nodes-filter=NODES_EXPR  only emit selected nodes\n");
	fprintf(stderr, "  -o, --origin=URL               repository origin URL\n");
	fprintf(stderr, "\n");
	fprintf(stderr, "EDGES_EXPR is a comma separate list of src_TYPE:dst_TYPE pairs\n");
	fprintf(stderr, "NODES_EXPR is a comme separate list of node TYPEs\n");
	fprintf(stderr, "{NODES,EDGES}_EXPR can be empty strings to filter *out* all elements.\n");
	fprintf(stderr, "TYPE is one of: cnt, dir, loc, ori, rel, rev, snp, *\n");
	fprintf(stderr, "\nNote: you can use \"-\" for stdout in file names.\n");

	exit(EXIT_FAILURE);
}


/* command line arguments */
typedef struct {
	char *nodes_out;     // path of nodes outputs file
	char *edges_out;     // path of edges outputs file
	char *nodes_filter;  // nodes filter expression
	char *edges_filter;  // edges filter expression
	char *origin_url;    // origin URL
	char *repo_dir;      // repository directory
} cli_args_t;


cli_args_t *parse_cli(int argc, char **argv) {
	int opt;

	cli_args_t *args = malloc(sizeof(cli_args_t));
	if (args == NULL) {
		perror("Cannot allocate memory.");
		exit(EXIT_FAILURE);
	} else {
		args->nodes_out = NULL;
		args->edges_out = NULL;
		args->nodes_filter = NULL;
		args->edges_filter = NULL;
		args->origin_url = NULL;
		args->repo_dir = NULL;
	}

	static struct option long_opts[] = {
		{"edges-output", required_argument, 0, 'e' },
		{"nodes-output", required_argument, 0, 'n' },
		{"edges-filter", required_argument, 0, 'E' },
		{"nodes-filter", required_argument, 0, 'N' },
		{"origin",       required_argument, 0, 'o' },
		{"help",         no_argument,       0, 'h' },
		{0,              0,                 0,  0  }
	};

	while ((opt = getopt_long(argc, argv, "e:n:E:N:o:h", long_opts,
				  NULL)) != -1) {
		switch (opt) {
		case 'e': args->edges_out = optarg; break;
		case 'n': args->nodes_out = optarg; break;
		case 'E': args->edges_filter = optarg; break;
		case 'N': args->nodes_filter = optarg; break;
		case 'o': args->origin_url = optarg; break;
		case 'h':
		default:
			exit_usage(NULL);
		}
	}
	if (argv[optind] == NULL)
		exit_usage(NULL);
	args->repo_dir = argv[optind];

	if (args->edges_out == NULL)
		args->edges_out = "-";
	if (args->nodes_out == NULL)
		args->nodes_out = "-";

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
void init_graph_filters(char *nodes_filter, char *edges_filter) {
	char **filters;
	char **types;
	char **ptr;
	int src_type, dst_type;

	// Note: when either filter is NULL, the parsing loops below will be
	// skipped (due to g_strsplit's semantics on empty strings), which is
	// what we want: all elements will be forbidden.

	if (edges_filter != NULL) {
		fill_matrix(_allowed_edges, false);  // nothing allowed by default
		filters = g_strsplit(edges_filter, ELT_SEP, -1);  // "typ:typ" pairs
		for (ptr = filters; *ptr; ptr++) {
			types = g_strsplit(*ptr, PAIR_SEP, 2);  // 2 "typ" fragments

			src_type = parse_otype(types[0]);
			dst_type = parse_otype(types[1]);
			if (src_type == MY_GIT_OBJ_ANY && dst_type == MY_GIT_OBJ_ANY) {
				// "*:*" wildcard
				fill_matrix(_allowed_edges, true);
				break;  // all edges allowed already
			} else if (src_type == MY_GIT_OBJ_ANY) {  // "*:typ" wildcard
				fill_column(_allowed_edges, dst_type, true);
			} else if (dst_type == MY_GIT_OBJ_ANY) {  // "typ:*" wildcard
				fill_row(_allowed_edges, src_type, true);
			} else  // "src_type:dst_type"
				_allowed_edges[src_type][dst_type] = true;

			g_strfreev(types);
		}
		g_strfreev(filters);
	}

	if (nodes_filter != NULL) {
		fill_vector(_allowed_nodes, false);  // nothing allowed by default
		filters = g_strsplit(nodes_filter, ELT_SEP, -1);  // "typ" fragments
		for (ptr = filters; *ptr; ptr++) {
			src_type = parse_otype(*ptr);
			if (src_type == MY_GIT_OBJ_ANY) {  // "*" wildcard
				fill_vector(_allowed_nodes, true);
				break;  // all nodes allowed already
			} else
				_allowed_nodes[src_type] = true;
		}
		g_strfreev(filters);
	}
}


int main(int argc, char **argv) {
	git_repository *repo;
	git_odb *odb;
	int rc;
	cli_args_t *args;
	config_t *conf;
	FILE *nodes_out, *edges_out;
	char nodes_buf[EDGES_OUTSZ], edges_buf[EDGES_OUTSZ];
	char *snapshot_pid;

	args = parse_cli(argc, argv);
	init_graph_filters(args->nodes_filter, args->edges_filter);
	// _dump_filters(stdout, _allowed_edges, _allowed_nodes);

	git_libgit2_init();
	check_lg2(git_repository_open(&repo, args->repo_dir),
		  "cannot open repository", NULL);
	check_lg2(git_repository_odb(&odb, repo),
		  "cannot get object DB", NULL);

	nodes_out = open_out_stream(args->nodes_out, nodes_buf, NODES_OUTSZ);
	edges_out = open_out_stream(args->edges_out, edges_buf, EDGES_OUTSZ);
	assert(NODES_OUTSZ <= PIPE_BUF && (NODES_OUTSZ % NODES_LINELEN == 0));
	assert(EDGES_OUTSZ <= PIPE_BUF && (EDGES_OUTSZ % EDGES_LINELEN == 0));

	conf = malloc(sizeof(config_t));
	conf->odb = odb;
	conf->repo = repo;
	conf->nodes_out = nodes_out;
	conf->edges_out = edges_out;

	snapshot_pid = emit_snapshot(conf);

	if (args->origin_url != NULL)
		emit_origin(args->origin_url, conf, snapshot_pid);

	rc = git_odb_foreach(odb, (git_odb_foreach_cb) emit_obj, (void *) conf);
	check_lg2(rc, "failure during object iteration", NULL);

	git_odb_free(odb);
	git_repository_free(repo);
	free(conf);
	exit(rc);
}
