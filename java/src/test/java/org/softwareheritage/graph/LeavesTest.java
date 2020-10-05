package org.softwareheritage.graph;

import java.util.ArrayList;

import org.junit.jupiter.api.Test;
import org.softwareheritage.graph.server.Endpoint;

// Avoid warnings concerning Endpoint.Output.result manual cast
@SuppressWarnings("unchecked")
public class LeavesTest extends GraphTest {
    @Test
    public void forwardFromSnp() {
        Graph graph = getGraph();
        SWHID src = new SWHID("swh:1:snp:0000000000000000000000000000000000000020");
        Endpoint endpoint = new Endpoint(graph, "forward", "*");

        ArrayList<SWHID> expectedLeaves = new ArrayList<>();
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000001"));
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000004"));
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000005"));
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000007"));

        ArrayList<SWHID> actualLeaves = (ArrayList) endpoint.leaves(new Endpoint.Input(src)).result;
        GraphTest.assertEqualsAnyOrder(expectedLeaves, actualLeaves);
    }

    @Test
    public void forwardFromRel() {
        Graph graph = getGraph();
        SWHID src = new SWHID("swh:1:rel:0000000000000000000000000000000000000019");
        Endpoint endpoint = new Endpoint(graph, "forward", "*");

        ArrayList<SWHID> expectedLeaves = new ArrayList<>();
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000015"));
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000014"));
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000001"));
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000004"));
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000005"));
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000007"));
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000011"));

        ArrayList<SWHID> actualLeaves = (ArrayList) endpoint.leaves(new Endpoint.Input(src)).result;
        GraphTest.assertEqualsAnyOrder(expectedLeaves, actualLeaves);
    }

    @Test
    public void backwardFromLeaf() {
        Graph graph = getGraph();

        Endpoint endpoint1 = new Endpoint(graph, "backward", "*");
        SWHID src1 = new SWHID("swh:1:cnt:0000000000000000000000000000000000000015");
        ArrayList<SWHID> expectedLeaves1 = new ArrayList<>();
        expectedLeaves1.add(new SWHID("swh:1:rel:0000000000000000000000000000000000000019"));
        ArrayList<SWHID> actualLeaves1 = (ArrayList) endpoint1.leaves(new Endpoint.Input(src1)).result;
        GraphTest.assertEqualsAnyOrder(expectedLeaves1, actualLeaves1);

        Endpoint endpoint2 = new Endpoint(graph, "backward", "*");
        SWHID src2 = new SWHID("swh:1:cnt:0000000000000000000000000000000000000004");
        ArrayList<SWHID> expectedLeaves2 = new ArrayList<>();
        expectedLeaves2.add(new SWHID("swh:1:ori:0000000000000000000000000000000000000021"));
        expectedLeaves2.add(new SWHID("swh:1:rel:0000000000000000000000000000000000000019"));
        ArrayList<SWHID> actualLeaves2 = (ArrayList) endpoint2.leaves(new Endpoint.Input(src2)).result;
        GraphTest.assertEqualsAnyOrder(expectedLeaves2, actualLeaves2);
    }

    @Test
    public void forwardRevToRevOnly() {
        Graph graph = getGraph();
        SWHID src = new SWHID("swh:1:rev:0000000000000000000000000000000000000018");
        Endpoint endpoint = new Endpoint(graph, "forward", "rev:rev");

        ArrayList<SWHID> expectedLeaves = new ArrayList<>();
        expectedLeaves.add(new SWHID("swh:1:rev:0000000000000000000000000000000000000003"));

        ArrayList<SWHID> actualLeaves = (ArrayList) endpoint.leaves(new Endpoint.Input(src)).result;
        GraphTest.assertEqualsAnyOrder(expectedLeaves, actualLeaves);
    }

    @Test
    public void forwardDirToAll() {
        Graph graph = getGraph();
        SWHID src = new SWHID("swh:1:dir:0000000000000000000000000000000000000008");
        Endpoint endpoint = new Endpoint(graph, "forward", "dir:*");

        ArrayList<SWHID> expectedLeaves = new ArrayList<>();
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000004"));
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000005"));
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000001"));
        expectedLeaves.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000007"));

        ArrayList<SWHID> actualLeaves = (ArrayList) endpoint.leaves(new Endpoint.Input(src)).result;
        GraphTest.assertEqualsAnyOrder(expectedLeaves, actualLeaves);
    }

    @Test
    public void backwardCntToDirDirToDir() {
        Graph graph = getGraph();
        SWHID src = new SWHID("swh:1:cnt:0000000000000000000000000000000000000005");
        Endpoint endpoint = new Endpoint(graph, "backward", "cnt:dir,dir:dir");

        ArrayList<SWHID> expectedLeaves = new ArrayList<>();
        expectedLeaves.add(new SWHID("swh:1:dir:0000000000000000000000000000000000000012"));

        ArrayList<SWHID> actualLeaves = (ArrayList) endpoint.leaves(new Endpoint.Input(src)).result;
        GraphTest.assertEqualsAnyOrder(expectedLeaves, actualLeaves);
    }
}
