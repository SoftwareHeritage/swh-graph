/*
 * Copyright (c) 2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.rpc;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.Test;
import org.softwareheritage.graph.GraphTest;
import org.softwareheritage.graph.SWHID;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TraverseNodesTest extends TraversalServiceTest {
    private TraversalRequest.Builder getTraversalRequestBuilder(SWHID src) {
        return TraversalRequest.newBuilder().addSrc(src.toString());
    }

    @Test
    public void testSrcErrors() {
        StatusRuntimeException thrown;
        thrown = assertThrows(StatusRuntimeException.class,
                () -> client.traverse(TraversalRequest.newBuilder().addSrc(fakeSWHID("cnt", 404).toString()).build())
                        .forEachRemaining((n) -> {
                        }));
        assertEquals(Status.INVALID_ARGUMENT.getCode(), thrown.getStatus().getCode());
        thrown = assertThrows(StatusRuntimeException.class,
                () -> client
                        .traverse(TraversalRequest.newBuilder()
                                .addSrc("swh:1:lol:0000000000000000000000000000000000000001").build())
                        .forEachRemaining((n) -> {
                        }));
        assertEquals(Status.INVALID_ARGUMENT.getCode(), thrown.getStatus().getCode());
        thrown = assertThrows(StatusRuntimeException.class,
                () -> client
                        .traverse(TraversalRequest.newBuilder()
                                .addSrc("swh:1:cnt:000000000000000000000000000000000000000z").build())
                        .forEachRemaining((n) -> {
                        }));
        assertEquals(Status.INVALID_ARGUMENT.getCode(), thrown.getStatus().getCode());
    }

    @Test
    public void forwardFromRoot() {
        ArrayList<SWHID> actual = getSWHIDs(
                client.traverse(getTraversalRequestBuilder(new SWHID(TEST_ORIGIN_ID)).build()));
        List<SWHID> expected = List.of(fakeSWHID("cnt", 1), fakeSWHID("cnt", 4), fakeSWHID("cnt", 5),
                fakeSWHID("cnt", 7), fakeSWHID("dir", 2), fakeSWHID("dir", 6), fakeSWHID("dir", 8),
                fakeSWHID("rel", 10), fakeSWHID("rev", 3), fakeSWHID("rev", 9), fakeSWHID("snp", 20),
                new SWHID(TEST_ORIGIN_ID));
        GraphTest.assertEqualsAnyOrder(expected, actual);
    }

    @Test
    public void forwardFromMiddle() {
        ArrayList<SWHID> actual = getSWHIDs(client.traverse(getTraversalRequestBuilder(fakeSWHID("dir", 12)).build()));
        List<SWHID> expected = List.of(fakeSWHID("cnt", 1), fakeSWHID("cnt", 4), fakeSWHID("cnt", 5),
                fakeSWHID("cnt", 7), fakeSWHID("cnt", 11), fakeSWHID("dir", 6), fakeSWHID("dir", 8),
                fakeSWHID("dir", 12));
        GraphTest.assertEqualsAnyOrder(expected, actual);
    }

    @Test
    public void forwardRelRev() {
        ArrayList<SWHID> actual = getSWHIDs(
                client.traverse(getTraversalRequestBuilder(fakeSWHID("rel", 10)).setEdges("rel:rev,rev:rev").build()));
        List<SWHID> expected = List.of(fakeSWHID("rel", 10), fakeSWHID("rev", 9), fakeSWHID("rev", 3));
        GraphTest.assertEqualsAnyOrder(expected, actual);
    }

    @Test
    public void forwardFilterReturnedNodesDir() {
        ArrayList<SWHID> actual = getSWHIDs(client.traverse(getTraversalRequestBuilder(fakeSWHID("rel", 10))
                .setReturnNodes(NodeFilter.newBuilder().setTypes("dir").build()).build()));
        List<SWHID> expected = List.of(fakeSWHID("dir", 2), fakeSWHID("dir", 8), fakeSWHID("dir", 6));
        GraphTest.assertEqualsAnyOrder(expected, actual);
    }

    @Test
    public void backwardFromRoot() {
        ArrayList<SWHID> actual = getSWHIDs(client.traverse(
                getTraversalRequestBuilder(new SWHID(TEST_ORIGIN_ID)).setDirection(GraphDirection.BACKWARD).build()));
        List<SWHID> expected = List.of(new SWHID(TEST_ORIGIN_ID));
        GraphTest.assertEqualsAnyOrder(expected, actual);
    }

    @Test
    public void backwardFromMiddle() {
        ArrayList<SWHID> actual = getSWHIDs(client.traverse(
                getTraversalRequestBuilder(fakeSWHID("dir", 12)).setDirection(GraphDirection.BACKWARD).build()));
        List<SWHID> expected = List.of(fakeSWHID("dir", 12), fakeSWHID("rel", 19), fakeSWHID("rev", 13),
                fakeSWHID("rev", 18), fakeSWHID("rel", 21), fakeSWHID("snp", 22), new SWHID(TEST_ORIGIN_ID2));
        GraphTest.assertEqualsAnyOrder(expected, actual);
    }

    @Test
    public void backwardFromLeaf() {
        ArrayList<SWHID> actual = getSWHIDs(client.traverse(
                getTraversalRequestBuilder(fakeSWHID("cnt", 4)).setDirection(GraphDirection.BACKWARD).build()));
        List<SWHID> expected = List.of(new SWHID(TEST_ORIGIN_ID), new SWHID(TEST_ORIGIN_ID2), fakeSWHID("cnt", 4),
                fakeSWHID("dir", 6), fakeSWHID("dir", 8), fakeSWHID("dir", 12), fakeSWHID("rel", 10),
                fakeSWHID("rel", 19), fakeSWHID("rev", 9), fakeSWHID("rev", 13), fakeSWHID("rev", 18),
                fakeSWHID("snp", 20), fakeSWHID("rel", 21), fakeSWHID("snp", 22));
        GraphTest.assertEqualsAnyOrder(expected, actual);
    }

    @Test
    public void forwardSnpToRev() {
        ArrayList<SWHID> actual = getSWHIDs(
                client.traverse(getTraversalRequestBuilder(fakeSWHID("snp", 20)).setEdges("snp:rev").build()));
        List<SWHID> expected = List.of(fakeSWHID("rev", 9), fakeSWHID("snp", 20));
        GraphTest.assertEqualsAnyOrder(expected, actual);
    }

    @Test
    public void forwardRelToRevRevToRev() {
        ArrayList<SWHID> actual = getSWHIDs(
                client.traverse(getTraversalRequestBuilder(fakeSWHID("rel", 10)).setEdges("rel:rev,rev:rev").build()));
        List<SWHID> expected = List.of(fakeSWHID("rel", 10), fakeSWHID("rev", 3), fakeSWHID("rev", 9));
        GraphTest.assertEqualsAnyOrder(expected, actual);
    }

    @Test
    public void forwardRevToAllDirToAll() {
        ArrayList<SWHID> actual = getSWHIDs(
                client.traverse(getTraversalRequestBuilder(fakeSWHID("rev", 13)).setEdges("rev:*,dir:*").build()));
        List<SWHID> expected = List.of(fakeSWHID("cnt", 1), fakeSWHID("cnt", 4), fakeSWHID("cnt", 5),
                fakeSWHID("cnt", 7), fakeSWHID("cnt", 11), fakeSWHID("dir", 2), fakeSWHID("dir", 6),
                fakeSWHID("dir", 8), fakeSWHID("dir", 12), fakeSWHID("rev", 3), fakeSWHID("rev", 9),
                fakeSWHID("rev", 13));
        GraphTest.assertEqualsAnyOrder(expected, actual);
    }

    @Test
    public void forwardSnpToAllRevToAll() {
        ArrayList<SWHID> actual = getSWHIDs(
                client.traverse(getTraversalRequestBuilder(fakeSWHID("snp", 20)).setEdges("snp:*,rev:*").build()));
        List<SWHID> expected = List.of(fakeSWHID("dir", 2), fakeSWHID("dir", 8), fakeSWHID("rel", 10),
                fakeSWHID("rev", 3), fakeSWHID("rev", 9), fakeSWHID("snp", 20));
        GraphTest.assertEqualsAnyOrder(expected, actual);
    }

    @Test
    public void forwardNoEdges() {
        ArrayList<SWHID> actual = getSWHIDs(
                client.traverse(getTraversalRequestBuilder(fakeSWHID("snp", 20)).setEdges("").build()));
        List<SWHID> expected = List.of(fakeSWHID("snp", 20));
        GraphTest.assertEqualsAnyOrder(expected, actual);
    }

    @Test
    public void backwardRevToRevRevToRel() {
        ArrayList<SWHID> actual = getSWHIDs(client.traverse(getTraversalRequestBuilder(fakeSWHID("rev", 3))
                .setEdges("rev:rev,rev:rel").setDirection(GraphDirection.BACKWARD).build()));
        List<SWHID> expected = List.of(fakeSWHID("rel", 10), fakeSWHID("rel", 19), fakeSWHID("rev", 3),
                fakeSWHID("rev", 9), fakeSWHID("rev", 13), fakeSWHID("rev", 18), fakeSWHID("rel", 21));
        GraphTest.assertEqualsAnyOrder(expected, actual);
    }

    @Test
    public void forwardFromRootNodesOnly() {
        ArrayList<SWHID> actual = getSWHIDs(
                client.traverse(getTraversalRequestBuilder(new SWHID(TEST_ORIGIN_ID)).build()));
        List<SWHID> expected = List.of(new SWHID(TEST_ORIGIN_ID), fakeSWHID("cnt", 1), fakeSWHID("cnt", 4),
                fakeSWHID("cnt", 5), fakeSWHID("cnt", 7), fakeSWHID("dir", 2), fakeSWHID("dir", 6), fakeSWHID("dir", 8),
                fakeSWHID("rel", 10), fakeSWHID("rev", 3), fakeSWHID("rev", 9), fakeSWHID("snp", 20));
        GraphTest.assertEqualsAnyOrder(expected, actual);
    }

    @Test
    public void backwardRevToAllNodesOnly() {
        ArrayList<SWHID> actual = getSWHIDs(client.traverse(getTraversalRequestBuilder(fakeSWHID("rev", 3))
                .setDirection(GraphDirection.BACKWARD).setEdges("rev:*").build()));
        List<SWHID> expected = List.of(fakeSWHID("rel", 10), fakeSWHID("rel", 19), fakeSWHID("rev", 3),
                fakeSWHID("rev", 9), fakeSWHID("rev", 13), fakeSWHID("rev", 18), fakeSWHID("snp", 20),
                fakeSWHID("rel", 21), fakeSWHID("snp", 22));
        GraphTest.assertEqualsAnyOrder(expected, actual);
    }

    @Test
    public void forwardMultipleSources() {
        ArrayList<SWHID> actual = getSWHIDs(client.traverse(getTraversalRequestBuilder(fakeSWHID("snp", 20))
                .addSrc(fakeSWHID("rel", 19).toString()).setMaxDepth(1).build()));
        List<SWHID> expected = List.of(fakeSWHID("snp", 20), fakeSWHID("rel", 19), fakeSWHID("rel", 10),
                fakeSWHID("rev", 9), fakeSWHID("rev", 18));
        GraphTest.assertEqualsAnyOrder(expected, actual);
    }

    @Test
    public void backwardMultipleSources() {
        ArrayList<SWHID> actual = getSWHIDs(client.traverse(getTraversalRequestBuilder(fakeSWHID("cnt", 5))
                .addSrc(fakeSWHID("dir", 16).toString()).setMaxDepth(2).setDirection(GraphDirection.BACKWARD).build()));
        List<SWHID> expected = List.of(fakeSWHID("cnt", 5), fakeSWHID("dir", 16), fakeSWHID("dir", 6),
                fakeSWHID("dir", 8), fakeSWHID("dir", 17), fakeSWHID("rev", 18));
        GraphTest.assertEqualsAnyOrder(expected, actual);
    }

    // Go from rel 19 with various max depths
    @Test
    public void maxDepth() {
        TraversalRequest.Builder builder = getTraversalRequestBuilder(fakeSWHID("rel", 19));

        ArrayList<SWHID> actual;
        List<SWHID> expected;

        actual = getSWHIDs(client.traverse(builder.setMaxDepth(0).build()));
        expected = List.of(fakeSWHID("rel", 19));
        GraphTest.assertEqualsAnyOrder(expected, actual);

        actual = getSWHIDs(client.traverse(builder.setMaxDepth(1).build()));
        expected = List.of(fakeSWHID("rel", 19), fakeSWHID("rev", 18));
        GraphTest.assertEqualsAnyOrder(expected, actual);

        actual = getSWHIDs(client.traverse(builder.setMaxDepth(2).build()));
        expected = List.of(fakeSWHID("rel", 19), fakeSWHID("rev", 18), fakeSWHID("rev", 13), fakeSWHID("dir", 17));
        GraphTest.assertEqualsAnyOrder(expected, actual);

        actual = getSWHIDs(client.traverse(builder.setMaxDepth(3).build()));
        expected = List.of(fakeSWHID("rel", 19), fakeSWHID("rev", 18), fakeSWHID("rev", 13), fakeSWHID("dir", 17),
                fakeSWHID("rev", 9), fakeSWHID("dir", 12), fakeSWHID("dir", 16), fakeSWHID("cnt", 14));
        GraphTest.assertEqualsAnyOrder(expected, actual);
    }

    // Go from rel 19 with various max edges
    @Test
    public void maxEdges() {
        TraversalRequest.Builder builder = getTraversalRequestBuilder(fakeSWHID("rel", 19));

        ArrayList<SWHID> actual;
        List<SWHID> expected;

        actual = getSWHIDs(client.traverse(builder.setMaxEdges(1).build()));
        expected = List.of(fakeSWHID("rel", 19));
        GraphTest.assertEqualsAnyOrder(expected, actual);

        actual = getSWHIDs(client.traverse(builder.setMaxEdges(3).build()));
        expected = List.of(fakeSWHID("rel", 19), fakeSWHID("rev", 18));
        GraphTest.assertEqualsAnyOrder(expected, actual);

        actual = getSWHIDs(client.traverse(builder.setMaxEdges(7).build()));
        expected = List.of(fakeSWHID("rel", 19), fakeSWHID("rev", 18), fakeSWHID("rev", 13), fakeSWHID("dir", 17));
        GraphTest.assertEqualsAnyOrder(expected, actual);

        actual = getSWHIDs(client.traverse(builder.setMaxEdges(12).build()));
        expected = List.of(fakeSWHID("rel", 19), fakeSWHID("rev", 18), fakeSWHID("rev", 13), fakeSWHID("dir", 17),
                fakeSWHID("rev", 9), fakeSWHID("dir", 12), fakeSWHID("dir", 16), fakeSWHID("cnt", 14));
        GraphTest.assertEqualsAnyOrder(expected, actual);
    }
}
