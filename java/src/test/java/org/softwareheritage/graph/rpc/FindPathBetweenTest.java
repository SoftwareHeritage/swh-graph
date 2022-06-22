package org.softwareheritage.graph.rpc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.softwareheritage.graph.SWHID;

import java.util.ArrayList;
import java.util.List;

public class FindPathBetweenTest extends TraversalServiceTest {
    private FindPathBetweenRequest.Builder getRequestBuilder(SWHID src, SWHID dst) {
        return FindPathBetweenRequest.newBuilder().addSrc(src.toString()).addDst(dst.toString());
    }

    // Test path between ori 1 and cnt 4 (forward graph)
    @Test
    public void forwardRootToLeaf() {
        ArrayList<SWHID> actual = getSWHIDs(
                client.findPathBetween(getRequestBuilder(new SWHID(TEST_ORIGIN_ID), fakeSWHID("cnt", 4)).build()));
        List<SWHID> expected = List.of(new SWHID(TEST_ORIGIN_ID), fakeSWHID("snp", 20), fakeSWHID("rev", 9),
                fakeSWHID("dir", 8), fakeSWHID("dir", 6), fakeSWHID("cnt", 4));
        Assertions.assertEquals(expected, actual);
    }

    // Test path between rev 18 and rev 3 (forward graph)
    @Test
    public void forwardRevToRev() {
        ArrayList<SWHID> actual = getSWHIDs(
                client.findPathBetween(getRequestBuilder(fakeSWHID("rev", 18), fakeSWHID("rev", 3)).build()));
        List<SWHID> expected = List.of(fakeSWHID("rev", 18), fakeSWHID("rev", 13), fakeSWHID("rev", 9),
                fakeSWHID("rev", 3));
        Assertions.assertEquals(expected, actual);
    }

    // Test path between rev 3 and rev 18 (backward graph)
    @Test
    public void backwardRevToRev() {
        ArrayList<SWHID> actual = getSWHIDs(
                client.findPathBetween(getRequestBuilder(fakeSWHID("rev", 3), fakeSWHID("rev", 18))
                        .setDirection(GraphDirection.BACKWARD).build()));
        List<SWHID> expected = List.of(fakeSWHID("rev", 3), fakeSWHID("rev", 9), fakeSWHID("rev", 13),
                fakeSWHID("rev", 18));
        Assertions.assertEquals(expected, actual);
    }

    // Test path between cnt 4 and itself (forward graph)
    @Test
    public void forwardCntToItself() {
        ArrayList<SWHID> actual = getSWHIDs(
                client.findPathBetween(getRequestBuilder(fakeSWHID("cnt", 4), fakeSWHID("cnt", 4)).build()));
        List<SWHID> expected = List.of(fakeSWHID("cnt", 4));
        Assertions.assertEquals(expected, actual);
    }

    // Start from ori and rel 19 and find cnt 14 or cnt 7 (forward graph)
    @Test
    public void forwardMultipleSourcesDest() {
        ArrayList<SWHID> actual = getSWHIDs(
                client.findPathBetween(getRequestBuilder(fakeSWHID("rel", 19), fakeSWHID("cnt", 14))
                        .addSrc(TEST_ORIGIN_ID).addDst(fakeSWHID("cnt", 7).toString()).build()));
        List<SWHID> expected = List.of(fakeSWHID("rel", 19), fakeSWHID("rev", 18), fakeSWHID("dir", 17),
                fakeSWHID("cnt", 14));
    }

    // Start from cnt 4 and cnt 11 and find rev 13 or rev 9 (backward graph)
    @Test
    public void backwardMultipleSourcesDest() {
        ArrayList<SWHID> actual = getSWHIDs(client.findPathBetween(
                getRequestBuilder(fakeSWHID("cnt", 4), fakeSWHID("rev", 13)).setDirection(GraphDirection.BACKWARD)
                        .addSrc(fakeSWHID("cnt", 11).toString()).addDst(fakeSWHID("rev", 9).toString()).build()));
        List<SWHID> expected = List.of(fakeSWHID("cnt", 11), fakeSWHID("dir", 12), fakeSWHID("rev", 13));
        Assertions.assertEquals(expected, actual);
    }

    // Start from all directories and find the origin (backward graph)
    @Test
    public void backwardMultipleSourcesAllDirToOri() {
        ArrayList<SWHID> actual = getSWHIDs(
                client.findPathBetween(getRequestBuilder(fakeSWHID("dir", 2), new SWHID(TEST_ORIGIN_ID))
                        .addSrc(fakeSWHID("dir", 6).toString()).addSrc(fakeSWHID("dir", 8).toString())
                        .addSrc(fakeSWHID("dir", 12).toString()).addSrc(fakeSWHID("dir", 16).toString())
                        .addSrc(fakeSWHID("dir", 17).toString()).setDirection(GraphDirection.BACKWARD).build()));
        List<SWHID> expected = List.of(fakeSWHID("dir", 8), fakeSWHID("rev", 9), fakeSWHID("snp", 20),
                new SWHID(TEST_ORIGIN_ID));
        Assertions.assertEquals(expected, actual);
    }

    // Start from cnt 4 and find any rev (backward graph)
    @Test
    public void backwardCntToAnyRev() {
        ArrayList<SWHID> actual = getSWHIDs(
                client.findPathBetween(getRequestBuilder(fakeSWHID("cnt", 4), fakeSWHID("rev", 3))
                        .addDst(fakeSWHID("rev", 9).toString()).addDst(fakeSWHID("rev", 13).toString())
                        .addDst(fakeSWHID("rev", 18).toString()).setDirection(GraphDirection.BACKWARD).build()));
        List<SWHID> expected = List.of(fakeSWHID("cnt", 4), fakeSWHID("dir", 6), fakeSWHID("dir", 8),
                fakeSWHID("rev", 9));
        Assertions.assertEquals(expected, actual);
    }
}
