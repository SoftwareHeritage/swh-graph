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

#include <git2.h>
#include <stdio.h>
#include <string.h>

#define SWH_PREFIX  "swh:1"
#define SWH_DIR     "swh:1:dir"
#define SWH_REV     "swh:1:rev"
#define SWH_PIDSZ   GIT_OID_HEXSZ + 10  // size of a SWH PID


/* extra payload for callback invoked on Git objects */
typedef struct {
	git_odb *odb;  // Git object DB
	git_repository *repo;  // Git repository
	FILE *nodes;  // stream to write nodes to, format: "PID\n"
	FILE *edges;  // stream to write edges to, format: "PID PID\n"
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


/* Convert a git object type to the corresponding SWH PID type. */
char *git_object_type2swh(int type) {
	switch(type) {
	case GIT_OBJ_BLOB:   return "cnt"; break;
	case GIT_OBJ_COMMIT: return "rev"; break;
	case GIT_OBJ_TAG:    return "rel"; break;
	case GIT_OBJ_TREE:   return "dir"; break;
	default:
		fprintf(stderr, "Unknown object type: %d\n", type);
		exit(EXIT_FAILURE);
	}
}


/* Emit commit edges. */
void emit_commit(const git_commit *commit, const char *swhpid, FILE *out) {
	unsigned int i, max_i;
	char oidstr[GIT_OID_HEXSZ + 1];  // to PID

	// rev -> dir
	git_oid_tostr(oidstr, sizeof(oidstr), git_commit_tree_id(commit));
	fprintf(out, "%s %s:%s\n", swhpid, SWH_DIR, oidstr);

	// rev -> rev
	max_i = (unsigned int)git_commit_parentcount(commit);
	for (i = 0; i < max_i; ++i) {
		git_oid_tostr(oidstr, sizeof(oidstr),
			      git_commit_parent_id(commit, i));
		fprintf(out, "%s %s:%s\n", swhpid, SWH_REV, oidstr);
	}
}

/* Emit tag edges. */
void emit_tag(const git_tag *tag, const char *swhpid, FILE *out) {
	char oidstr[GIT_OID_HEXSZ + 1];

	// rel -> *
	git_oid_tostr(oidstr, sizeof(oidstr), git_tag_target_id(tag));
	fprintf(out, "%s %s:%s:%s\n", swhpid, SWH_PREFIX,
	       git_object_type2swh(git_tag_target_type(tag)), oidstr);
}


/* Emit tree edges. */
void emit_tree(const git_tree *tree, const char *swhpid, FILE *out) {
	size_t i, max_i = (int)git_tree_entrycount(tree);
	char oidstr[GIT_OID_HEXSZ + 1];
	const git_tree_entry *te;

	// dir -> *
	for (i = 0; i < max_i; ++i) {
		te = git_tree_entry_byindex(tree, i);
		git_oid_tostr(oidstr, sizeof(oidstr), git_tree_entry_id(te));
		fprintf(out, "%s %s:%s:%s\n", swhpid, SWH_PREFIX,
			git_object_type2swh(git_tree_entry_type(te)),
			oidstr);
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

	check_lg2(git_odb_read_header(&len, &obj_type, odb, id),
		  "cannot read object header", NULL);

	// emit node
	sprintf(swhpid, "swh:1:%s:", git_object_type2swh(obj_type));
	git_oid_tostr(swhpid + 10, sizeof(oidstr), id);
	fprintf(((cb_payload *) payload)->nodes, "%s\n", swhpid);

	// emit edges
	switch(obj_type) {
	case GIT_OBJ_BLOB:  // graph leaf: no edges to emit
		break;
	case GIT_OBJ_COMMIT:
		check_lg2(git_commit_lookup(&commit, repo, id),
			  "cannot find commit", NULL);
		emit_commit(commit, swhpid, ((cb_payload *) payload)->edges);
		git_commit_free(commit);
		break;
	case GIT_OBJ_TAG:
		check_lg2(git_tag_lookup(&tag, repo, id),
			  "cannot find tag", NULL);
		emit_tag(tag, swhpid, ((cb_payload *) payload)->edges);
		git_tag_free(tag);
		break;
	case GIT_OBJ_TREE:
		check_lg2(git_tree_lookup(&tree, repo, id),
			  "cannot find tree", NULL);
		emit_tree(tree, swhpid, ((cb_payload *) payload)->edges);
		git_tree_free(tree);
		break;
	default:
		git_oid_tostr(oidstr, sizeof(oidstr), id);
		fprintf(stderr, "ignoring unknown object: %s\n", oidstr);
		break;
	}

	return 0;
}


int main(int argc, char **argv) {
	git_repository *repo;
	git_odb *odb;
	int rc;
	cb_payload *payload;
	FILE *nodes, *edges;
	
	if (argc != 4) {
		fprintf(stderr,
			"Usage: git2graph GIT_REPO_DIR NODES_FILE EDGES_FILE\n");
		fprintf(stderr, "Note: you can use \"-\" for stdout.\n");
		exit(EXIT_FAILURE);
	}

	git_libgit2_init();
	check_lg2(git_repository_open(&repo, argv[1]),
		  "cannot open repository", NULL);
	check_lg2(git_repository_odb(&odb, repo),
		  "cannot get object DB", NULL);

	if (strcmp(argv[2], "-") == 0) {
		nodes = stdout;
	} else if((nodes = fopen(argv[2], "w")) == NULL) {
		fprintf(stderr, "can't open nodes file: %s\n", argv[2]);
		exit(EXIT_FAILURE);
	}
	if (strcmp(argv[3], "-") == 0) {
		edges = stdout;
	} else if((edges = fopen(argv[3], "w")) == NULL) {
		fprintf(stderr, "can't open edges file: %s\n", argv[3]);
		exit(EXIT_FAILURE);
	}

	payload = malloc(sizeof(cb_payload));
	payload->odb = odb;
	payload->repo = repo;
	payload->nodes = nodes;
	payload->edges = edges;

	rc = git_odb_foreach(odb, emit_obj, payload);
	check_lg2(rc, "failure during object iteration", NULL);

	git_odb_free(odb);
	git_repository_free(repo);
	free(payload);
	exit(rc);
}
