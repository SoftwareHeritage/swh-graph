package org.softwareheritage.graph.rpc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.softwareheritage.graph.SWHID;

import java.util.ArrayList;
import java.util.List;

public class FindPathToTest extends TraversalServiceTest {
    private FindPathToRequest.Builder getRequestBuilder(SWHID src, String allowedNodes) {
        return FindPathToRequest.newBuilder().addSrc(src.toString())
                .setTarget(NodeFilter.newBuilder().setTypes(allowedNodes).build());
    }

    @Test
    public void forwardOriToFirstDir() {
        ArrayList<SWHID> actual = getSWHIDs(
                client.findPathTo(getRequestBuilder(new SWHID(TEST_ORIGIN_ID), "dir").build()));
        List<SWHID> expected = List.of(new SWHID(TEST_ORIGIN_ID), fakeSWHID("snp", 20), fakeSWHID("rev", 9),
                fakeSWHID("dir", 8));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void forwardRelToFirstCnt() {
        ArrayList<SWHID> actual = getSWHIDs(client.findPathTo(getRequestBuilder(fakeSWHID("rel", 19), "cnt").build()));
        List<SWHID> expected = List.of(fakeSWHID("rel", 19), fakeSWHID("rev", 18), fakeSWHID("dir", 17),
                fakeSWHID("cnt", 14));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void backwardDirToFirstRel() {
        ArrayList<SWHID> actual = getSWHIDs(client.findPathTo(
                getRequestBuilder(fakeSWHID("dir", 16), "rel").setDirection(GraphDirection.BACKWARD).build()));
        List<SWHID> expected = List.of(fakeSWHID("dir", 16), fakeSWHID("dir", 17), fakeSWHID("rev", 18),
                fakeSWHID("rel", 19));
        Assertions.assertEquals(expected, actual);
    }

    @Test
    public void forwardCntToItself() {
        ArrayList<SWHID> actual = getSWHIDs(client.findPathTo(getRequestBuilder(fakeSWHID("cnt", 4), "cnt").build()));
        List<SWHID> expected = List.of(fakeSWHID("cnt", 4));
        Assertions.assertEquals(expected, actual);
    }
}
