/*
 * Copyright (c) 2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph;

import java.util.*;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class SubgraphTest extends GraphTest {
    @Test
    public void noFilter() {
        SwhBidirectionalGraph g = getGraph();
        Subgraph sg = new Subgraph(g, new AllowedNodes("*"));

        for (long i = 0; i < g.numNodes(); ++i) {
            Assertions.assertEquals(g.outdegree(i), sg.outdegree(i));
        }
    }

    @Test
    public void missingNode() {
        SwhBidirectionalGraph g = getGraph();
        Subgraph sg = new Subgraph(g, new AllowedNodes("dir,ori"));

        SWHID rev1 = fakeSWHID("rev", 18);
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            sg.outdegree(sg.getNodeId(rev1));
        });
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            sg.successors(sg.getNodeId(rev1));
        });
    }

    @Test
    public void outdegreeOnlyDirOri() {
        SwhBidirectionalGraph g = getGraph();
        Subgraph sg = new Subgraph(g, new AllowedNodes("dir,ori"));

        SWHID dir1 = fakeSWHID("dir", 17);
        Assertions.assertEquals(2, g.outdegree(g.getNodeId(dir1)));
        Assertions.assertEquals(1, sg.outdegree(sg.getNodeId(dir1)));

        SWHID dir2 = fakeSWHID("dir", 6);
        Assertions.assertEquals(2, g.outdegree(g.getNodeId(dir2)));
        Assertions.assertEquals(0, sg.outdegree(sg.getNodeId(dir2)));

        SWHID ori1 = new SWHID(TEST_ORIGIN_ID);
        Assertions.assertEquals(1, g.outdegree(g.getNodeId(ori1)));
        Assertions.assertEquals(0, sg.outdegree(sg.getNodeId(ori1)));
    }

    @Test
    public void successorsOnlyDirOri() {
        SwhBidirectionalGraph g = getGraph();
        Subgraph sg = new Subgraph(g, new AllowedNodes("dir,ori"));

        SWHID dir1 = fakeSWHID("dir", 17);
        assertEqualsAnyOrder(Collections.singletonList(sg.getNodeId(fakeSWHID("dir", 16))),
                lazyLongIteratorToList(sg.successors(sg.getNodeId(dir1))));

        SWHID dir2 = fakeSWHID("dir", 6);
        assertEqualsAnyOrder(Collections.emptyList(), lazyLongIteratorToList(sg.successors(sg.getNodeId(dir2))));

        SWHID ori1 = new SWHID(TEST_ORIGIN_ID);
        assertEqualsAnyOrder(Collections.emptyList(), lazyLongIteratorToList(sg.successors(sg.getNodeId(ori1))));
    }

    @Test
    public void nodeIteratorOnlyOriDir() {
        SwhBidirectionalGraph g = getGraph();
        Subgraph sg = new Subgraph(g, new AllowedNodes("dir,ori"));
        ArrayList<Long> nodeList = new ArrayList<>();
        Iterator<Long> nodeIt = sg.nodeIterator();
        nodeIt.forEachRemaining(nodeList::add);
        assertEqualsAnyOrder(
                Arrays.asList(sg.getNodeId(new SWHID(TEST_ORIGIN_ID)), sg.getNodeId(new SWHID(TEST_ORIGIN_ID2)),
                        sg.getNodeId(fakeSWHID("dir", 2)), sg.getNodeId(fakeSWHID("dir", 6)),
                        sg.getNodeId(fakeSWHID("dir", 8)), sg.getNodeId(fakeSWHID("dir", 12)),
                        sg.getNodeId(fakeSWHID("dir", 16)), sg.getNodeId(fakeSWHID("dir", 17))),
                nodeList);
        sg = new Subgraph(g, new AllowedNodes("snp,rel"));
        nodeList = new ArrayList<>();
        nodeIt = sg.nodeIterator();
        nodeIt.forEachRemaining(nodeList::add);
        assertEqualsAnyOrder(Arrays.asList(sg.getNodeId(fakeSWHID("snp", 20)), sg.getNodeId(fakeSWHID("rel", 10)),
                sg.getNodeId(fakeSWHID("rel", 19)), sg.getNodeId(fakeSWHID("rel", 21)),
                sg.getNodeId(fakeSWHID("snp", 22))), nodeList);
    }
}
