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

public class TraverseNeighborsTest extends TraversalServiceTest {
    private TraversalRequest.Builder getNeighborsRequestBuilder(SWHID src) {
        return TraversalRequest.newBuilder().addSrc(src.toString()).setMinDepth(1).setMaxDepth(1);
    }

    @Test
    public void zeroNeighbor() {
        ArrayList<SWHID> expectedNodes = new ArrayList<>();

        TraversalRequest request1 = getNeighborsRequestBuilder(new SWHID(TEST_ORIGIN_ID))
                .setDirection(GraphDirection.BACKWARD).build();
        ArrayList<SWHID> actuals1 = getSWHIDs(client.traverse(request1));
        GraphTest.assertEqualsAnyOrder(expectedNodes, actuals1);

        TraversalRequest request2 = getNeighborsRequestBuilder(fakeSWHID("cnt", 4)).build();
        ArrayList<SWHID> actuals2 = getSWHIDs(client.traverse(request2));
        GraphTest.assertEqualsAnyOrder(expectedNodes, actuals2);

        TraversalRequest request3 = getNeighborsRequestBuilder(fakeSWHID("cnt", 15)).build();
        ArrayList<SWHID> actuals3 = getSWHIDs(client.traverse(request3));
        GraphTest.assertEqualsAnyOrder(expectedNodes, actuals3);

        TraversalRequest request4 = getNeighborsRequestBuilder(fakeSWHID("rel", 19))
                .setDirection(GraphDirection.BACKWARD).build();
        ArrayList<SWHID> actuals4 = getSWHIDs(client.traverse(request4));
        GraphTest.assertEqualsAnyOrder(expectedNodes, actuals4);

        TraversalRequest request5 = getNeighborsRequestBuilder(fakeSWHID("dir", 8)).setEdges("snp:*,rev:*,rel:*")
                .build();
        ArrayList<SWHID> actuals5 = getSWHIDs(client.traverse(request5));
        GraphTest.assertEqualsAnyOrder(expectedNodes, actuals5);
    }

    @Test
    public void oneNeighbor() {
        TraversalRequest request1 = getNeighborsRequestBuilder(fakeSWHID("rev", 3)).build();
        ArrayList<SWHID> actuals1 = getSWHIDs(client.traverse(request1));
        ArrayList<SWHID> expectedNodes1 = new ArrayList<>();
        expectedNodes1.add(new SWHID("swh:1:dir:0000000000000000000000000000000000000002"));
        GraphTest.assertEqualsAnyOrder(expectedNodes1, actuals1);

        TraversalRequest request2 = getNeighborsRequestBuilder(fakeSWHID("dir", 17)).setEdges("dir:cnt").build();
        ArrayList<SWHID> actuals2 = getSWHIDs(client.traverse(request2));
        ArrayList<SWHID> expectedNodes2 = new ArrayList<>();
        expectedNodes2.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000014"));
        GraphTest.assertEqualsAnyOrder(expectedNodes2, actuals2);

        TraversalRequest request3 = getNeighborsRequestBuilder(fakeSWHID("dir", 12))
                .setDirection(GraphDirection.BACKWARD).build();
        ArrayList<SWHID> actuals3 = getSWHIDs(client.traverse(request3));
        ArrayList<SWHID> expectedNodes3 = new ArrayList<>();
        expectedNodes3.add(new SWHID("swh:1:rev:0000000000000000000000000000000000000013"));
        GraphTest.assertEqualsAnyOrder(expectedNodes3, actuals3);

        TraversalRequest request4 = getNeighborsRequestBuilder(fakeSWHID("rev", 9))
                .setDirection(GraphDirection.BACKWARD).setEdges("rev:rev").build();
        ArrayList<SWHID> actuals4 = getSWHIDs(client.traverse(request4));
        ArrayList<SWHID> expectedNodes4 = new ArrayList<>();
        expectedNodes4.add(new SWHID("swh:1:rev:0000000000000000000000000000000000000013"));
        GraphTest.assertEqualsAnyOrder(expectedNodes4, actuals4);

        TraversalRequest request5 = getNeighborsRequestBuilder(fakeSWHID("snp", 20))
                .setDirection(GraphDirection.BACKWARD).build();
        ArrayList<SWHID> actuals5 = getSWHIDs(client.traverse(request5));
        ArrayList<SWHID> expectedNodes5 = new ArrayList<>();
        expectedNodes5.add(new SWHID(TEST_ORIGIN_ID));
        GraphTest.assertEqualsAnyOrder(expectedNodes5, actuals5);
    }

    @Test
    public void twoNeighbors() {
        TraversalRequest request1 = getNeighborsRequestBuilder(fakeSWHID("snp", 20)).build();
        ArrayList<SWHID> actuals1 = getSWHIDs(client.traverse(request1));
        ArrayList<SWHID> expectedNodes1 = new ArrayList<>();
        expectedNodes1.add(new SWHID("swh:1:rel:0000000000000000000000000000000000000010"));
        expectedNodes1.add(new SWHID("swh:1:rev:0000000000000000000000000000000000000009"));
        GraphTest.assertEqualsAnyOrder(expectedNodes1, actuals1);

        TraversalRequest request2 = getNeighborsRequestBuilder(fakeSWHID("dir", 8)).setEdges("dir:cnt").build();
        ArrayList<SWHID> actuals2 = getSWHIDs(client.traverse(request2));
        ArrayList<SWHID> expectedNodes2 = new ArrayList<>();
        expectedNodes2.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000001"));
        expectedNodes2.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000007"));
        GraphTest.assertEqualsAnyOrder(expectedNodes2, actuals2);

        TraversalRequest request3 = getNeighborsRequestBuilder(fakeSWHID("cnt", 1))
                .setDirection(GraphDirection.BACKWARD).build();
        ArrayList<SWHID> actuals3 = getSWHIDs(client.traverse(request3));
        ArrayList<SWHID> expectedNodes3 = new ArrayList<>();
        expectedNodes3.add(new SWHID("swh:1:dir:0000000000000000000000000000000000000008"));
        expectedNodes3.add(new SWHID("swh:1:dir:0000000000000000000000000000000000000002"));
        GraphTest.assertEqualsAnyOrder(expectedNodes3, actuals3);

        TraversalRequest request4 = getNeighborsRequestBuilder(fakeSWHID("rev", 9))
                .setDirection(GraphDirection.BACKWARD).setEdges("rev:snp,rev:rel").build();
        ArrayList<SWHID> actuals4 = getSWHIDs(client.traverse(request4));
        ArrayList<SWHID> expectedNodes4 = new ArrayList<>();
        expectedNodes4.add(new SWHID("swh:1:snp:0000000000000000000000000000000000000020"));
        expectedNodes4.add(new SWHID("swh:1:rel:0000000000000000000000000000000000000010"));
        expectedNodes4.add(new SWHID("swh:1:snp:0000000000000000000000000000000000000022"));
        GraphTest.assertEqualsAnyOrder(expectedNodes4, actuals4);
    }

    @Test
    public void threeNeighbors() {
        TraversalRequest request1 = getNeighborsRequestBuilder(fakeSWHID("dir", 8)).build();
        ArrayList<SWHID> actuals1 = getSWHIDs(client.traverse(request1));
        ArrayList<SWHID> expectedNodes1 = new ArrayList<>();
        expectedNodes1.add(new SWHID("swh:1:dir:0000000000000000000000000000000000000006"));
        expectedNodes1.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000001"));
        expectedNodes1.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000007"));
        GraphTest.assertEqualsAnyOrder(expectedNodes1, actuals1);

        TraversalRequest request2 = getNeighborsRequestBuilder(fakeSWHID("rev", 9))
                .setDirection(GraphDirection.BACKWARD).build();
        ArrayList<SWHID> actuals2 = getSWHIDs(client.traverse(request2));
        ArrayList<SWHID> expectedNodes2 = new ArrayList<>();
        expectedNodes2.add(new SWHID("swh:1:snp:0000000000000000000000000000000000000020"));
        expectedNodes2.add(new SWHID("swh:1:rel:0000000000000000000000000000000000000010"));
        expectedNodes2.add(new SWHID("swh:1:rev:0000000000000000000000000000000000000013"));
        expectedNodes2.add(new SWHID("swh:1:snp:0000000000000000000000000000000000000022"));
        GraphTest.assertEqualsAnyOrder(expectedNodes2, actuals2);
    }
}
