package org.softwareheritage.graph.rpc;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
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
                fakeSWHID("rel", 19));
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
        List<SWHID> expected = List.of(fakeSWHID("dir", 8), fakeSWHID("rev", 9), fakeSWHID("snp", 20),
                new SWHID(TEST_ORIGIN_ID));
        Assertions.assertEquals(expected, actual);
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
}
