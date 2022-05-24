package org.softwareheritage.graph.rpc;

import org.junit.jupiter.api.Test;
import org.softwareheritage.graph.GraphTest;
import org.softwareheritage.graph.SWHID;

import java.util.ArrayList;
import java.util.List;

public class TraverseNodesTest extends TraversalServiceTest {
    private TraversalRequest.Builder getTraversalRequestBuilder(SWHID src) {
        return TraversalRequest.newBuilder().addSrc(src.toString());
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
                fakeSWHID("rev", 18));
        GraphTest.assertEqualsAnyOrder(expected, actual);
    }

    @Test
    public void backwardFromLeaf() {
        ArrayList<SWHID> actual = getSWHIDs(client.traverse(
                getTraversalRequestBuilder(fakeSWHID("cnt", 4)).setDirection(GraphDirection.BACKWARD).build()));
        List<SWHID> expected = List.of(new SWHID(TEST_ORIGIN_ID), fakeSWHID("cnt", 4), fakeSWHID("dir", 6),
                fakeSWHID("dir", 8), fakeSWHID("dir", 12), fakeSWHID("rel", 10), fakeSWHID("rel", 19),
                fakeSWHID("rev", 9), fakeSWHID("rev", 13), fakeSWHID("rev", 18), fakeSWHID("snp", 20));
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
                fakeSWHID("rev", 9), fakeSWHID("rev", 13), fakeSWHID("rev", 18));
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
                fakeSWHID("rev", 9), fakeSWHID("rev", 13), fakeSWHID("rev", 18), fakeSWHID("snp", 20));
        GraphTest.assertEqualsAnyOrder(expected, actual);
    }
}
