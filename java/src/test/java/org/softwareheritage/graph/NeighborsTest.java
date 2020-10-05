package org.softwareheritage.graph;

import java.util.ArrayList;

import org.junit.jupiter.api.Test;
import org.softwareheritage.graph.server.Endpoint;

// Avoid warnings concerning Endpoint.Output.result manual cast
@SuppressWarnings("unchecked")
public class NeighborsTest extends GraphTest {
    @Test
    public void zeroNeighbor() {
        Graph graph = getGraph();
        ArrayList<SWHID> expectedNodes = new ArrayList<>();

        SWHID src1 = new SWHID("swh:1:ori:0000000000000000000000000000000000000021");
        Endpoint endpoint1 = new Endpoint(graph, "backward", "*");
        ArrayList<SWHID> actuals1 = (ArrayList) endpoint1.neighbors(new Endpoint.Input(src1)).result;
        GraphTest.assertEqualsAnyOrder(expectedNodes, actuals1);

        SWHID src2 = new SWHID("swh:1:cnt:0000000000000000000000000000000000000004");
        Endpoint endpoint2 = new Endpoint(graph, "forward", "*");
        ArrayList<SWHID> actuals2 = (ArrayList) endpoint2.neighbors(new Endpoint.Input(src2)).result;
        GraphTest.assertEqualsAnyOrder(expectedNodes, actuals2);

        SWHID src3 = new SWHID("swh:1:cnt:0000000000000000000000000000000000000015");
        Endpoint endpoint3 = new Endpoint(graph, "forward", "*");
        ArrayList<SWHID> actuals3 = (ArrayList) endpoint3.neighbors(new Endpoint.Input(src3)).result;
        GraphTest.assertEqualsAnyOrder(expectedNodes, actuals3);

        SWHID src4 = new SWHID("swh:1:rel:0000000000000000000000000000000000000019");
        Endpoint endpoint4 = new Endpoint(graph, "backward", "*");
        ArrayList<SWHID> actuals4 = (ArrayList) endpoint4.neighbors(new Endpoint.Input(src4)).result;
        GraphTest.assertEqualsAnyOrder(expectedNodes, actuals4);

        SWHID src5 = new SWHID("swh:1:dir:0000000000000000000000000000000000000008");
        Endpoint endpoint5 = new Endpoint(graph, "forward", "snp:*,rev:*,rel:*");
        ArrayList<SWHID> actuals5 = (ArrayList) endpoint5.neighbors(new Endpoint.Input(src5)).result;
        GraphTest.assertEqualsAnyOrder(expectedNodes, actuals5);
    }

    @Test
    public void oneNeighbor() {
        Graph graph = getGraph();

        SWHID src1 = new SWHID("swh:1:rev:0000000000000000000000000000000000000003");
        Endpoint endpoint1 = new Endpoint(graph, "forward", "*");
        ArrayList<SWHID> expectedNodes1 = new ArrayList<>();
        expectedNodes1.add(new SWHID("swh:1:dir:0000000000000000000000000000000000000002"));
        ArrayList<SWHID> actuals1 = (ArrayList) endpoint1.neighbors(new Endpoint.Input(src1)).result;
        GraphTest.assertEqualsAnyOrder(expectedNodes1, actuals1);

        SWHID src2 = new SWHID("swh:1:dir:0000000000000000000000000000000000000017");
        Endpoint endpoint2 = new Endpoint(graph, "forward", "dir:cnt");
        ArrayList<SWHID> expectedNodes2 = new ArrayList<>();
        expectedNodes2.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000014"));
        ArrayList<SWHID> actuals2 = (ArrayList) endpoint2.neighbors(new Endpoint.Input(src2)).result;
        GraphTest.assertEqualsAnyOrder(expectedNodes2, actuals2);

        SWHID src3 = new SWHID("swh:1:dir:0000000000000000000000000000000000000012");
        Endpoint endpoint3 = new Endpoint(graph, "backward", "*");
        ArrayList<SWHID> expectedNodes3 = new ArrayList<>();
        expectedNodes3.add(new SWHID("swh:1:rev:0000000000000000000000000000000000000013"));
        ArrayList<SWHID> actuals3 = (ArrayList) endpoint3.neighbors(new Endpoint.Input(src3)).result;
        GraphTest.assertEqualsAnyOrder(expectedNodes3, actuals3);

        SWHID src4 = new SWHID("swh:1:rev:0000000000000000000000000000000000000009");
        Endpoint endpoint4 = new Endpoint(graph, "backward", "rev:rev");
        ArrayList<SWHID> expectedNodes4 = new ArrayList<>();
        expectedNodes4.add(new SWHID("swh:1:rev:0000000000000000000000000000000000000013"));
        ArrayList<SWHID> actuals4 = (ArrayList) endpoint4.neighbors(new Endpoint.Input(src4)).result;
        GraphTest.assertEqualsAnyOrder(expectedNodes4, actuals4);

        SWHID src5 = new SWHID("swh:1:snp:0000000000000000000000000000000000000020");
        Endpoint endpoint5 = new Endpoint(graph, "backward", "*");
        ArrayList<SWHID> expectedNodes5 = new ArrayList<>();
        expectedNodes5.add(new SWHID("swh:1:ori:0000000000000000000000000000000000000021"));
        ArrayList<SWHID> actuals5 = (ArrayList) endpoint5.neighbors(new Endpoint.Input(src5)).result;
        GraphTest.assertEqualsAnyOrder(expectedNodes5, actuals5);
    }

    @Test
    public void twoNeighbors() {
        Graph graph = getGraph();

        SWHID src1 = new SWHID("swh:1:snp:0000000000000000000000000000000000000020");
        Endpoint endpoint1 = new Endpoint(graph, "forward", "*");
        ArrayList<SWHID> expectedNodes1 = new ArrayList<>();
        expectedNodes1.add(new SWHID("swh:1:rel:0000000000000000000000000000000000000010"));
        expectedNodes1.add(new SWHID("swh:1:rev:0000000000000000000000000000000000000009"));
        ArrayList<SWHID> actuals1 = (ArrayList) endpoint1.neighbors(new Endpoint.Input(src1)).result;
        GraphTest.assertEqualsAnyOrder(expectedNodes1, actuals1);

        SWHID src2 = new SWHID("swh:1:dir:0000000000000000000000000000000000000008");
        Endpoint endpoint2 = new Endpoint(graph, "forward", "dir:cnt");
        ArrayList<SWHID> expectedNodes2 = new ArrayList<>();
        expectedNodes2.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000001"));
        expectedNodes2.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000007"));
        ArrayList<SWHID> actuals2 = (ArrayList) endpoint2.neighbors(new Endpoint.Input(src2)).result;
        GraphTest.assertEqualsAnyOrder(expectedNodes2, actuals2);

        SWHID src3 = new SWHID("swh:1:cnt:0000000000000000000000000000000000000001");
        Endpoint endpoint3 = new Endpoint(graph, "backward", "*");
        ArrayList<SWHID> expectedNodes3 = new ArrayList<>();
        expectedNodes3.add(new SWHID("swh:1:dir:0000000000000000000000000000000000000008"));
        expectedNodes3.add(new SWHID("swh:1:dir:0000000000000000000000000000000000000002"));
        ArrayList<SWHID> actuals3 = (ArrayList) endpoint3.neighbors(new Endpoint.Input(src3)).result;
        GraphTest.assertEqualsAnyOrder(expectedNodes3, actuals3);

        SWHID src4 = new SWHID("swh:1:rev:0000000000000000000000000000000000000009");
        Endpoint endpoint4 = new Endpoint(graph, "backward", "rev:snp,rev:rel");
        ArrayList<SWHID> expectedNodes4 = new ArrayList<>();
        expectedNodes4.add(new SWHID("swh:1:snp:0000000000000000000000000000000000000020"));
        expectedNodes4.add(new SWHID("swh:1:rel:0000000000000000000000000000000000000010"));
        ArrayList<SWHID> actuals4 = (ArrayList) endpoint4.neighbors(new Endpoint.Input(src4)).result;
        GraphTest.assertEqualsAnyOrder(expectedNodes4, actuals4);
    }

    @Test
    public void threeNeighbors() {
        Graph graph = getGraph();

        SWHID src1 = new SWHID("swh:1:dir:0000000000000000000000000000000000000008");
        Endpoint endpoint1 = new Endpoint(graph, "forward", "*");
        ArrayList<SWHID> expectedNodes1 = new ArrayList<>();
        expectedNodes1.add(new SWHID("swh:1:dir:0000000000000000000000000000000000000006"));
        expectedNodes1.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000001"));
        expectedNodes1.add(new SWHID("swh:1:cnt:0000000000000000000000000000000000000007"));
        ArrayList<SWHID> actuals1 = (ArrayList) endpoint1.neighbors(new Endpoint.Input(src1)).result;
        GraphTest.assertEqualsAnyOrder(expectedNodes1, actuals1);

        SWHID src2 = new SWHID("swh:1:rev:0000000000000000000000000000000000000009");
        Endpoint endpoint2 = new Endpoint(graph, "backward", "*");
        ArrayList<SWHID> expectedNodes2 = new ArrayList<>();
        expectedNodes2.add(new SWHID("swh:1:snp:0000000000000000000000000000000000000020"));
        expectedNodes2.add(new SWHID("swh:1:rel:0000000000000000000000000000000000000010"));
        expectedNodes2.add(new SWHID("swh:1:rev:0000000000000000000000000000000000000013"));
        ArrayList<SWHID> actuals2 = (ArrayList) endpoint2.neighbors(new Endpoint.Input(src2)).result;
        GraphTest.assertEqualsAnyOrder(expectedNodes2, actuals2);
    }
}
