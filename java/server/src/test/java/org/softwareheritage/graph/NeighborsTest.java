package org.softwareheritage.graph;

import java.util.ArrayList;

import org.junit.Test;

import org.softwareheritage.graph.Endpoint;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.GraphTest;
import org.softwareheritage.graph.SwhPID;

// Avoid warnings concerning Endpoint.Output.result manual cast
@SuppressWarnings("unchecked")
public class NeighborsTest extends GraphTest {
  @Test
  public void zeroNeighbor() {
    Graph graph = getGraph();
    ArrayList<SwhPID> expectedNodes = new ArrayList<>();

    SwhPID src1 = new SwhPID("swh:1:ori:0000000000000000000000000000000000000021");
    Endpoint endpoint1 = new Endpoint(graph, "backward", "*");
    GraphTest.assertEqualsAnyOrder(expectedNodes, (ArrayList) endpoint1.neighbors(src1).result);

    SwhPID src2 = new SwhPID("swh:1:cnt:0000000000000000000000000000000000000004");
    Endpoint endpoint2 = new Endpoint(graph, "forward", "*");
    GraphTest.assertEqualsAnyOrder(expectedNodes, (ArrayList) endpoint2.neighbors(src2).result);

    SwhPID src3 = new SwhPID("swh:1:cnt:0000000000000000000000000000000000000015");
    Endpoint endpoint3 = new Endpoint(graph, "forward", "*");
    GraphTest.assertEqualsAnyOrder(expectedNodes, (ArrayList) endpoint3.neighbors(src3).result);

    SwhPID src4 = new SwhPID("swh:1:rel:0000000000000000000000000000000000000019");
    Endpoint endpoint4 = new Endpoint(graph, "backward", "*");
    GraphTest.assertEqualsAnyOrder(expectedNodes, (ArrayList) endpoint4.neighbors(src4).result);

    SwhPID src5 = new SwhPID("swh:1:dir:0000000000000000000000000000000000000008");
    Endpoint endpoint5 = new Endpoint(graph, "forward", "snp:*,rev:*,rel:*");
    GraphTest.assertEqualsAnyOrder(expectedNodes, (ArrayList) endpoint5.neighbors(src5).result);
  }

  @Test
  public void oneNeighbor() {
    Graph graph = getGraph();

    SwhPID src1 = new SwhPID("swh:1:rev:0000000000000000000000000000000000000003");
    Endpoint endpoint1 = new Endpoint(graph, "forward", "*");
    ArrayList<SwhPID> expectedNodes1 = new ArrayList<>();
    expectedNodes1.add(new SwhPID("swh:1:dir:0000000000000000000000000000000000000002"));
    GraphTest.assertEqualsAnyOrder(expectedNodes1, (ArrayList) endpoint1.neighbors(src1).result);

    SwhPID src2 = new SwhPID("swh:1:dir:0000000000000000000000000000000000000017");
    Endpoint endpoint2 = new Endpoint(graph, "forward", "dir:cnt");
    ArrayList<SwhPID> expectedNodes2 = new ArrayList<>();
    expectedNodes2.add(new SwhPID("swh:1:cnt:0000000000000000000000000000000000000014"));
    GraphTest.assertEqualsAnyOrder(expectedNodes2, (ArrayList) endpoint2.neighbors(src2).result);

    SwhPID src3 = new SwhPID("swh:1:dir:0000000000000000000000000000000000000012");
    Endpoint endpoint3 = new Endpoint(graph, "backward", "*");
    ArrayList<SwhPID> expectedNodes3 = new ArrayList<>();
    expectedNodes3.add(new SwhPID("swh:1:rev:0000000000000000000000000000000000000013"));
    GraphTest.assertEqualsAnyOrder(expectedNodes3, (ArrayList) endpoint3.neighbors(src3).result);

    SwhPID src4 = new SwhPID("swh:1:rev:0000000000000000000000000000000000000009");
    Endpoint endpoint4 = new Endpoint(graph, "backward", "rev:rev");
    ArrayList<SwhPID> expectedNodes4 = new ArrayList<>();
    expectedNodes4.add(new SwhPID("swh:1:rev:0000000000000000000000000000000000000013"));
    GraphTest.assertEqualsAnyOrder(expectedNodes4, (ArrayList) endpoint4.neighbors(src4).result);

    SwhPID src5 = new SwhPID("swh:1:snp:0000000000000000000000000000000000000020");
    Endpoint endpoint5 = new Endpoint(graph, "backward", "*");
    ArrayList<SwhPID> expectedNodes5 = new ArrayList<>();
    expectedNodes5.add(new SwhPID("swh:1:ori:0000000000000000000000000000000000000021"));
    GraphTest.assertEqualsAnyOrder(expectedNodes5, (ArrayList) endpoint5.neighbors(src5).result);
  }

  @Test
  public void twoNeighbors() {
    Graph graph = getGraph();

    SwhPID src1 = new SwhPID("swh:1:snp:0000000000000000000000000000000000000020");
    Endpoint endpoint1 = new Endpoint(graph, "forward", "*");
    ArrayList<SwhPID> expectedNodes1 = new ArrayList<>();
    expectedNodes1.add(new SwhPID("swh:1:rel:0000000000000000000000000000000000000010"));
    expectedNodes1.add(new SwhPID("swh:1:rev:0000000000000000000000000000000000000009"));
    GraphTest.assertEqualsAnyOrder(expectedNodes1, (ArrayList) endpoint1.neighbors(src1).result);

    SwhPID src2 = new SwhPID("swh:1:dir:0000000000000000000000000000000000000008");
    Endpoint endpoint2 = new Endpoint(graph, "forward", "dir:cnt");
    ArrayList<SwhPID> expectedNodes2 = new ArrayList<>();
    expectedNodes2.add(new SwhPID("swh:1:cnt:0000000000000000000000000000000000000001"));
    expectedNodes2.add(new SwhPID("swh:1:cnt:0000000000000000000000000000000000000007"));
    GraphTest.assertEqualsAnyOrder(expectedNodes2, (ArrayList) endpoint2.neighbors(src2).result);

    SwhPID src3 = new SwhPID("swh:1:cnt:0000000000000000000000000000000000000001");
    Endpoint endpoint3 = new Endpoint(graph, "backward", "*");
    ArrayList<SwhPID> expectedNodes3 = new ArrayList<>();
    expectedNodes3.add(new SwhPID("swh:1:dir:0000000000000000000000000000000000000008"));
    expectedNodes3.add(new SwhPID("swh:1:dir:0000000000000000000000000000000000000002"));
    GraphTest.assertEqualsAnyOrder(expectedNodes3, (ArrayList) endpoint3.neighbors(src3).result);

    SwhPID src4 = new SwhPID("swh:1:rev:0000000000000000000000000000000000000009");
    Endpoint endpoint4 = new Endpoint(graph, "backward", "rev:snp,rev:rel");
    ArrayList<SwhPID> expectedNodes4 = new ArrayList<>();
    expectedNodes4.add(new SwhPID("swh:1:snp:0000000000000000000000000000000000000020"));
    expectedNodes4.add(new SwhPID("swh:1:rel:0000000000000000000000000000000000000010"));
    GraphTest.assertEqualsAnyOrder(expectedNodes4, (ArrayList) endpoint4.neighbors(src4).result);
  }

  @Test
  public void threeNeighbors() {
    Graph graph = getGraph();

    SwhPID src1 = new SwhPID("swh:1:dir:0000000000000000000000000000000000000008");
    Endpoint endpoint1 = new Endpoint(graph, "forward", "*");
    ArrayList<SwhPID> expectedNodes1 = new ArrayList<>();
    expectedNodes1.add(new SwhPID("swh:1:dir:0000000000000000000000000000000000000006"));
    expectedNodes1.add(new SwhPID("swh:1:cnt:0000000000000000000000000000000000000001"));
    expectedNodes1.add(new SwhPID("swh:1:cnt:0000000000000000000000000000000000000007"));
    GraphTest.assertEqualsAnyOrder(expectedNodes1, (ArrayList) endpoint1.neighbors(src1).result);

    SwhPID src2 = new SwhPID("swh:1:rev:0000000000000000000000000000000000000009");
    Endpoint endpoint2 = new Endpoint(graph, "backward", "*");
    ArrayList<SwhPID> expectedNodes2 = new ArrayList<>();
    expectedNodes2.add(new SwhPID("swh:1:snp:0000000000000000000000000000000000000020"));
    expectedNodes2.add(new SwhPID("swh:1:rel:0000000000000000000000000000000000000010"));
    expectedNodes2.add(new SwhPID("swh:1:rev:0000000000000000000000000000000000000013"));
    GraphTest.assertEqualsAnyOrder(expectedNodes2, (ArrayList) endpoint2.neighbors(src2).result);
  }
}
