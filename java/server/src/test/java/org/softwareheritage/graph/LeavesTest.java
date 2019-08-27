package org.softwareheritage.graph;

import java.util.ArrayList;

import org.junit.Test;

import org.softwareheritage.graph.Endpoint;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.GraphTest;
import org.softwareheritage.graph.SwhPID;

// Avoid warnings concerning Endpoint.Output.result manual cast
@SuppressWarnings("unchecked")
public class LeavesTest extends GraphTest {
    @Test
    public void forwardFromSnp() {
        Graph graph = getGraph();
        SwhPID src = new SwhPID("swh:1:snp:0000000000000000000000000000000000000020");
        Endpoint endpoint = new Endpoint(graph, "forward", "*");

        ArrayList<SwhPID> expectedLeaves = new ArrayList<>();
        expectedLeaves.add(new SwhPID("swh:1:cnt:0000000000000000000000000000000000000001"));
        expectedLeaves.add(new SwhPID("swh:1:cnt:0000000000000000000000000000000000000004"));
        expectedLeaves.add(new SwhPID("swh:1:cnt:0000000000000000000000000000000000000005"));
        expectedLeaves.add(new SwhPID("swh:1:cnt:0000000000000000000000000000000000000007"));

        ArrayList<SwhPID> actualLeaves = (ArrayList) endpoint.leaves(new Endpoint.Input(src)).result;
        GraphTest.assertEqualsAnyOrder(expectedLeaves, actualLeaves);
    }

    @Test
    public void forwardFromRel() {
        Graph graph = getGraph();
        SwhPID src = new SwhPID("swh:1:rel:0000000000000000000000000000000000000019");
        Endpoint endpoint = new Endpoint(graph, "forward", "*");

        ArrayList<SwhPID> expectedLeaves = new ArrayList<>();
        expectedLeaves.add(new SwhPID("swh:1:cnt:0000000000000000000000000000000000000015"));
        expectedLeaves.add(new SwhPID("swh:1:cnt:0000000000000000000000000000000000000014"));
        expectedLeaves.add(new SwhPID("swh:1:cnt:0000000000000000000000000000000000000001"));
        expectedLeaves.add(new SwhPID("swh:1:cnt:0000000000000000000000000000000000000004"));
        expectedLeaves.add(new SwhPID("swh:1:cnt:0000000000000000000000000000000000000005"));
        expectedLeaves.add(new SwhPID("swh:1:cnt:0000000000000000000000000000000000000007"));
        expectedLeaves.add(new SwhPID("swh:1:cnt:0000000000000000000000000000000000000011"));

        ArrayList<SwhPID> actualLeaves = (ArrayList) endpoint.leaves(new Endpoint.Input(src)).result;
        GraphTest.assertEqualsAnyOrder(expectedLeaves, actualLeaves);
    }

    @Test
    public void backwardFromLeaf() {
        Graph graph = getGraph();

        Endpoint endpoint1 = new Endpoint(graph, "backward", "*");
        SwhPID src1 = new SwhPID("swh:1:cnt:0000000000000000000000000000000000000015");
        ArrayList<SwhPID> expectedLeaves1 = new ArrayList<>();
        expectedLeaves1.add(new SwhPID("swh:1:rel:0000000000000000000000000000000000000019"));
        ArrayList<SwhPID> actualLeaves1 = (ArrayList) endpoint1.leaves(new Endpoint.Input(src1)).result;
        GraphTest.assertEqualsAnyOrder(expectedLeaves1, actualLeaves1);

        Endpoint endpoint2 = new Endpoint(graph, "backward", "*");
        SwhPID src2 = new SwhPID("swh:1:cnt:0000000000000000000000000000000000000004");
        ArrayList<SwhPID> expectedLeaves2 = new ArrayList<>();
        expectedLeaves2.add(new SwhPID("swh:1:ori:0000000000000000000000000000000000000021"));
        expectedLeaves2.add(new SwhPID("swh:1:rel:0000000000000000000000000000000000000019"));
        ArrayList<SwhPID> actualLeaves2 = (ArrayList) endpoint2.leaves(new Endpoint.Input(src2)).result;
        GraphTest.assertEqualsAnyOrder(expectedLeaves2, actualLeaves2);
    }

    @Test
    public void forwardRevToRevOnly() {
        Graph graph = getGraph();
        SwhPID src = new SwhPID("swh:1:rev:0000000000000000000000000000000000000018");
        Endpoint endpoint = new Endpoint(graph, "forward", "rev:rev");

        ArrayList<SwhPID> expectedLeaves = new ArrayList<>();
        expectedLeaves.add(new SwhPID("swh:1:rev:0000000000000000000000000000000000000003"));

        ArrayList<SwhPID> actualLeaves = (ArrayList) endpoint.leaves(new Endpoint.Input(src)).result;
        GraphTest.assertEqualsAnyOrder(expectedLeaves, actualLeaves);
    }

    @Test
    public void forwardDirToAll() {
        Graph graph = getGraph();
        SwhPID src = new SwhPID("swh:1:dir:0000000000000000000000000000000000000008");
        Endpoint endpoint = new Endpoint(graph, "forward", "dir:*");

        ArrayList<SwhPID> expectedLeaves = new ArrayList<>();
        expectedLeaves.add(new SwhPID("swh:1:cnt:0000000000000000000000000000000000000004"));
        expectedLeaves.add(new SwhPID("swh:1:cnt:0000000000000000000000000000000000000005"));
        expectedLeaves.add(new SwhPID("swh:1:cnt:0000000000000000000000000000000000000001"));
        expectedLeaves.add(new SwhPID("swh:1:cnt:0000000000000000000000000000000000000007"));

        ArrayList<SwhPID> actualLeaves = (ArrayList) endpoint.leaves(new Endpoint.Input(src)).result;
        GraphTest.assertEqualsAnyOrder(expectedLeaves, actualLeaves);
    }

    @Test
    public void backwardCntToDirDirToDir() {
        Graph graph = getGraph();
        SwhPID src = new SwhPID("swh:1:cnt:0000000000000000000000000000000000000005");
        Endpoint endpoint = new Endpoint(graph, "backward", "cnt:dir,dir:dir");

        ArrayList<SwhPID> expectedLeaves = new ArrayList<>();
        expectedLeaves.add(new SwhPID("swh:1:dir:0000000000000000000000000000000000000012"));

        ArrayList<SwhPID> actualLeaves = (ArrayList) endpoint.leaves(new Endpoint.Input(src)).result;
        GraphTest.assertEqualsAnyOrder(expectedLeaves, actualLeaves);
    }
}
