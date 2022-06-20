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

    @Test
    public void forwardRootToLeaf() {
        ArrayList<SWHID> actual = getSWHIDs(
                client.findPathBetween(getRequestBuilder(new SWHID(TEST_ORIGIN_ID), fakeSWHID("cnt", 4)).build()));
        List<SWHID> expected = List.of(new SWHID(TEST_ORIGIN_ID), fakeSWHID("snp", 20), fakeSWHID("rev", 9),
                fakeSWHID("dir", 8), fakeSWHID("dir", 6), fakeSWHID("cnt", 4));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void forwardRevToRev() {
        ArrayList<SWHID> actual = getSWHIDs(
                client.findPathBetween(getRequestBuilder(fakeSWHID("rev", 18), fakeSWHID("rev", 3)).build()));
        List<SWHID> expected = List.of(fakeSWHID("rev", 18), fakeSWHID("rev", 13), fakeSWHID("rev", 9),
                fakeSWHID("rev", 3));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void backwardRevToRev() {
        ArrayList<SWHID> actual = getSWHIDs(
                client.findPathBetween(getRequestBuilder(fakeSWHID("rev", 3), fakeSWHID("rev", 18))
                        .setDirection(GraphDirection.BACKWARD).build()));
        List<SWHID> expected = List.of(fakeSWHID("rev", 3), fakeSWHID("rev", 9), fakeSWHID("rev", 13),
                fakeSWHID("rev", 18));
        Assertions.assertEquals(expected, actual);
    }
}
