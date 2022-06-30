/*
 * Copyright (c) 2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.rpc;

import org.junit.jupiter.api.Test;
import org.softwareheritage.graph.GraphTest;
import org.softwareheritage.graph.SWHID;

import java.util.ArrayList;

public class TraverseLeavesTest extends TraversalServiceTest {
    private TraversalRequest.Builder getLeavesRequestBuilder(SWHID src) {
        return TraversalRequest.newBuilder().addSrc(src.toString())
                .setReturnNodes(NodeFilter.newBuilder().setMaxTraversalSuccessors(0).build());
    }

    @Test
    public void forwardFromSnp() {
        TraversalRequest request = getLeavesRequestBuilder(fakeSWHID("snp", 20)).build();

        ArrayList<SWHID> expectedLeaves = new ArrayList<>();
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000001"));
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000004"));
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000005"));
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000007"));

        ArrayList<SWHID> actualLeaves = getSWHIDs(client.traverse(request));
        GraphTest.assertEqualsAnyOrder(expectedLeaves, actualLeaves);
    }

    @Test
    public void forwardFromRel() {
        TraversalRequest request = getLeavesRequestBuilder(fakeSWHID("rel", 19)).build();
        ArrayList<SWHID> actualLeaves = getSWHIDs(client.traverse(request));
        ArrayList<SWHID> expectedLeaves = new ArrayList<>();
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000015"));
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000014"));
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000001"));
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000004"));
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000005"));
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000007"));
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000011"));

        GraphTest.assertEqualsAnyOrder(expectedLeaves, actualLeaves);
    }

    @Test
    public void backwardFromLeaf() {
        TraversalRequest request1 = getLeavesRequestBuilder(fakeSWHID("cnt", 15)).setDirection(GraphDirection.BACKWARD)
                .build();
        ArrayList<SWHID> actualLeaves1 = getSWHIDs(client.traverse(request1));
        ArrayList<SWHID> expectedLeaves1 = new ArrayList<>();
        expectedLeaves1.add(new SWHID("swh:1:rel:0000000000000000000000000000000000000019"));
        GraphTest.assertEqualsAnyOrder(expectedLeaves1, actualLeaves1);

        TraversalRequest request2 = getLeavesRequestBuilder(fakeSWHID("cnt", 4)).setDirection(GraphDirection.BACKWARD)
                .build();
        ArrayList<SWHID> actualLeaves2 = getSWHIDs(client.traverse(request2));
        ArrayList<SWHID> expectedLeaves2 = new ArrayList<>();
        expectedLeaves2.add(new SWHID(TEST_ORIGIN_ID));
        expectedLeaves2.add(new SWHID("swh:1:rel:0000000000000000000000000000000000000019"));
        GraphTest.assertEqualsAnyOrder(expectedLeaves2, actualLeaves2);
    }

    @Test
    public void forwardRevToRevOnly() {
        TraversalRequest request = getLeavesRequestBuilder(fakeSWHID("rev", 18)).setEdges("rev:rev").build();
        ArrayList<SWHID> actualLeaves = getSWHIDs(client.traverse(request));
        ArrayList<SWHID> expectedLeaves = new ArrayList<>();
        expectedLeaves.add(new SWHID("swh:1:rev:0000000000000000000000000000000000000003"));
        GraphTest.assertEqualsAnyOrder(expectedLeaves, actualLeaves);
    }

    @Test
    public void forwardDirToAll() {
        TraversalRequest request = getLeavesRequestBuilder(fakeSWHID("dir", 8)).setEdges("dir:*").build();
        ArrayList<SWHID> actualLeaves = getSWHIDs(client.traverse(request));
        ArrayList<SWHID> expectedLeaves = new ArrayList<>();
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000004"));
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000005"));
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000001"));
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000007"));
        GraphTest.assertEqualsAnyOrder(expectedLeaves, actualLeaves);
    }

    @Test
    public void backwardCntToDirDirToDir() {
        TraversalRequest request = getLeavesRequestBuilder(fakeSWHID("cnt", 5)).setEdges("cnt:dir,dir:dir")
                .setDirection(GraphDirection.BACKWARD).build();
        ArrayList<SWHID> actualLeaves = getSWHIDs(client.traverse(request));
        ArrayList<SWHID> expectedLeaves = new ArrayList<>();
        expectedLeaves.add(new SWHID("swh:1:dir:0000000000000000000000000000000000000012"));
        GraphTest.assertEqualsAnyOrder(expectedLeaves, actualLeaves);
    }
}
