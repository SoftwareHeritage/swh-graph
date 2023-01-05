/*
 * Copyright (c) 2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.rpc;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.softwareheritage.graph.SWHID;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FindPathToTest extends TraversalServiceTest {
    private FindPathToRequest.Builder getRequestBuilder(SWHID src, String allowedNodes) {
        return FindPathToRequest.newBuilder().addSrc(src.toString())
                .setTarget(NodeFilter.newBuilder().setTypes(allowedNodes).build());
    }

    @Test
    public void testSrcErrors() {
        StatusRuntimeException thrown;
        thrown = assertThrows(StatusRuntimeException.class, () -> client
                .findPathTo(FindPathToRequest.newBuilder().addSrc(fakeSWHID("cnt", 404).toString()).build()));
        assertEquals(Status.INVALID_ARGUMENT.getCode(), thrown.getStatus().getCode());
        thrown = assertThrows(StatusRuntimeException.class, () -> client.findPathTo(
                FindPathToRequest.newBuilder().addSrc("swh:1:lol:0000000000000000000000000000000000000001").build()));
        assertEquals(Status.INVALID_ARGUMENT.getCode(), thrown.getStatus().getCode());
        thrown = assertThrows(StatusRuntimeException.class, () -> client.findPathTo(
                FindPathToRequest.newBuilder().addSrc("swh:1:cnt:000000000000000000000000000000000000000z").build()));
        assertEquals(Status.INVALID_ARGUMENT.getCode(), thrown.getStatus().getCode());
    }

    @Test
    public void testEdgeErrors() {
        StatusRuntimeException thrown;
        thrown = assertThrows(StatusRuntimeException.class, () -> client.findPathTo(
                FindPathToRequest.newBuilder().addSrc(TEST_ORIGIN_ID).setEdges("batracien:reptile").build()));
        assertEquals(Status.INVALID_ARGUMENT.getCode(), thrown.getStatus().getCode());
    }

    @Test
    public void testTargetErrors() {
        StatusRuntimeException thrown;
        thrown = assertThrows(StatusRuntimeException.class,
                () -> client.findPathTo(FindPathToRequest.newBuilder().addSrc(TEST_ORIGIN_ID)
                        .setTarget(NodeFilter.newBuilder().setTypes("argoumante,eglomatique").build()).build()));
        assertEquals(Status.INVALID_ARGUMENT.getCode(), thrown.getStatus().getCode());
    }

    // Test path between ori 1 and any dir (forward graph)
    @Test
    public void forwardOriToFirstDir() {
        ArrayList<SWHID> actual = getSWHIDs(
                client.findPathTo(getRequestBuilder(new SWHID(TEST_ORIGIN_ID), "dir").build()));
        List<SWHID> expected = List.of(new SWHID(TEST_ORIGIN_ID), fakeSWHID("snp", 20), fakeSWHID("rev", 9),
                fakeSWHID("dir", 8));
        Assertions.assertEquals(expected, actual);
    }

    // Test path between rel 19 and any cnt (forward graph)
    @Test
    public void forwardRelToFirstCnt() {
        ArrayList<SWHID> actual = getSWHIDs(client.findPathTo(getRequestBuilder(fakeSWHID("rel", 19), "cnt").build()));
        List<SWHID> expected = List.of(fakeSWHID("rel", 19), fakeSWHID("rev", 18), fakeSWHID("dir", 17),
                fakeSWHID("cnt", 14));
        Assertions.assertEquals(expected, actual);
    }

    // Test path between dir 16 and any rel (backward graph)
    @Test
    public void backwardDirToFirstRel() {
        ArrayList<SWHID> actual = getSWHIDs(client.findPathTo(
                getRequestBuilder(fakeSWHID("dir", 16), "rel").setDirection(GraphDirection.BACKWARD).build()));
        List<SWHID> expected = List.of(fakeSWHID("dir", 16), fakeSWHID("dir", 17), fakeSWHID("rev", 18),
                fakeSWHID("rel", 21)); // FIXME: rel:19 is valid too
        Assertions.assertEquals(expected, actual);
    }

    // Test path between cnt 4 and itself (forward graph)
    @Test
    public void forwardCntToItself() {
        ArrayList<SWHID> actual = getSWHIDs(client.findPathTo(getRequestBuilder(fakeSWHID("cnt", 4), "cnt").build()));
        List<SWHID> expected = List.of(fakeSWHID("cnt", 4));
        Assertions.assertEquals(expected, actual);
    }

    // Start from ori and rel 19 and find any cnt (forward graph)
    @Test
    public void forwardMultipleSources() {
        ArrayList<SWHID> actual = getSWHIDs(
                client.findPathTo(getRequestBuilder(fakeSWHID("rel", 19), "cnt").addSrc(TEST_ORIGIN_ID).build()));
        List<SWHID> expected = List.of(fakeSWHID("rel", 19), fakeSWHID("rev", 18), fakeSWHID("dir", 17),
                fakeSWHID("cnt", 14));
    }

    // Start from cnt 4 and cnt 11 and find any rev (backward graph)
    @Test
    public void backwardMultipleSources() {
        ArrayList<SWHID> actual = getSWHIDs(client.findPathTo(getRequestBuilder(fakeSWHID("cnt", 4), "rev")
                .addSrc(fakeSWHID("cnt", 11).toString()).setDirection(GraphDirection.BACKWARD).build()));
        List<SWHID> expected = List.of(fakeSWHID("cnt", 11), fakeSWHID("dir", 12), fakeSWHID("rev", 13));
        Assertions.assertEquals(expected, actual);
    }

    // Start from all directories and find any origin (backward graph)
    @Test
    public void backwardMultipleSourcesAllDirToOri() {
        ArrayList<SWHID> actual = getSWHIDs(client.findPathTo(getRequestBuilder(fakeSWHID("dir", 2), "ori")
                .addSrc(fakeSWHID("dir", 6).toString()).addSrc(fakeSWHID("dir", 8).toString())
                .addSrc(fakeSWHID("dir", 12).toString()).addSrc(fakeSWHID("dir", 16).toString())
                .addSrc(fakeSWHID("dir", 17).toString()).setDirection(GraphDirection.BACKWARD).build()));
        List<SWHID> expected1 = List.of(fakeSWHID("dir", 8), fakeSWHID("rev", 9), fakeSWHID("snp", 20),
                new SWHID(TEST_ORIGIN_ID));
        List<SWHID> expected2 = List.of(fakeSWHID("dir", 8), fakeSWHID("rev", 9), fakeSWHID("snp", 22),
                new SWHID(TEST_ORIGIN_ID2));
        List<List<SWHID>> expected = List.of(expected1, expected2);
        Assertions.assertTrue(expected.contains(actual), String.format("Expected either %s or %s, got %s",
                expected1.toString(), expected2.toString(), actual.toString()));
    }

    // Impossible path between rev 9 and any release (forward graph)
    @Test
    public void forwardImpossiblePath() {
        // Check that the return is STATUS.NOT_FOUND
        StatusRuntimeException thrown = Assertions.assertThrows(StatusRuntimeException.class, () -> {
            client.findPathTo(getRequestBuilder(fakeSWHID("rev", 9), "rel").build());
        });
        Assertions.assertEquals(thrown.getStatus(), Status.NOT_FOUND);
    }

    // Path from cnt 15 to any rel with various max depths
    @Test
    public void maxDepth() {
        // Works with max_depth = 2
        ArrayList<SWHID> actual = getSWHIDs(client.findPathTo(getRequestBuilder(fakeSWHID("cnt", 15), "rel")
                .setDirection(GraphDirection.BACKWARD).setMaxDepth(4).build()));
        List<SWHID> expected = List.of(fakeSWHID("cnt", 15), fakeSWHID("dir", 16), fakeSWHID("dir", 17),
                fakeSWHID("rev", 18), fakeSWHID("rel", 21)); // FIXME: rel:19 is valid too
        Assertions.assertEquals(expected, actual);

        // Check that it throws NOT_FOUND with max depth = 1
        StatusRuntimeException thrown = Assertions.assertThrows(StatusRuntimeException.class, () -> {
            client.findPathTo(getRequestBuilder(fakeSWHID("cnt", 15), "rel").setDirection(GraphDirection.BACKWARD)
                    .setMaxDepth(3).build());
        });
        Assertions.assertEquals(thrown.getStatus().getCode(), Status.NOT_FOUND.getCode());
    }

    // Path from cnt 15 to any rel with various max edges
    @Test
    public void maxEdges() {
        int backtracked_edges = 2; // FIXME: Number of edges traversed but backtracked from. it changes
                                   // nondeterministically every time the test dataset is changed.

        ArrayList<SWHID> actual = getSWHIDs(client.findPathTo(getRequestBuilder(fakeSWHID("cnt", 15), "rel")
                .setDirection(GraphDirection.BACKWARD).setMaxEdges(4 + backtracked_edges).build()));
        List<SWHID> expected = List.of(fakeSWHID("cnt", 15), fakeSWHID("dir", 16), fakeSWHID("dir", 17),
                fakeSWHID("rev", 18), fakeSWHID("rel", 21)); // FIXME: rel:19 is valid too
        Assertions.assertEquals(expected, actual);

        StatusRuntimeException thrown = Assertions.assertThrows(StatusRuntimeException.class, () -> {
            client.findPathTo(getRequestBuilder(fakeSWHID("cnt", 15), "rel").setDirection(GraphDirection.BACKWARD)
                    .setMaxEdges(3 + backtracked_edges).build());
        });
        Assertions.assertEquals(thrown.getStatus().getCode(), Status.NOT_FOUND.getCode());
    }
}
