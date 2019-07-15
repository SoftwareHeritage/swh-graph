package org.softwareheritage.graph;

import java.util.ArrayList;

import org.junit.Test;

import org.softwareheritage.graph.Endpoint;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.GraphTest;
import org.softwareheritage.graph.SwhId;

public class NeighborsTest extends GraphTest {
  @Test
  public void zeroNeighbor() {
    Graph graph = getGraph();
    ArrayList<SwhId> expectedNodes = new ArrayList<>();

    SwhId src1 = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Endpoint endpoint1 = new Endpoint(graph, "backward", "*");
    GraphTest.assertEqualsAnyOrder(expectedNodes, endpoint1.neighbors(src1));

    SwhId src2 = new SwhId("swh:1:cnt:0000000000000000000000000000000000000004");
    Endpoint endpoint2 = new Endpoint(graph, "forward", "*");
    GraphTest.assertEqualsAnyOrder(expectedNodes, endpoint2.neighbors(src2));

    SwhId src3 = new SwhId("swh:1:cnt:0000000000000000000000000000000000000015");
    Endpoint endpoint3 = new Endpoint(graph, "forward", "*");
    GraphTest.assertEqualsAnyOrder(expectedNodes, endpoint3.neighbors(src3));

    SwhId src4 = new SwhId("swh:1:rel:0000000000000000000000000000000000000019");
    Endpoint endpoint4 = new Endpoint(graph, "backward", "*");
    GraphTest.assertEqualsAnyOrder(expectedNodes, endpoint4.neighbors(src4));

    SwhId src5 = new SwhId("swh:1:dir:0000000000000000000000000000000000000008");
    Endpoint endpoint5 = new Endpoint(graph, "forward", "snp:*,rev:*,rel:*");
    GraphTest.assertEqualsAnyOrder(expectedNodes, endpoint5.neighbors(src5));
  }

  @Test
  public void oneNeighbor() {
    Graph graph = getGraph();

    SwhId src1 = new SwhId("swh:1:rev:0000000000000000000000000000000000000003");
    Endpoint endpoint1 = new Endpoint(graph, "forward", "*");
    ArrayList<SwhId> expectedNodes1 = new ArrayList<>();
    expectedNodes1.add(new SwhId("swh:1:dir:0000000000000000000000000000000000000002"));
    GraphTest.assertEqualsAnyOrder(expectedNodes1, endpoint1.neighbors(src1));

    SwhId src2 = new SwhId("swh:1:dir:0000000000000000000000000000000000000017");
    Endpoint endpoint2 = new Endpoint(graph, "forward", "dir:cnt");
    ArrayList<SwhId> expectedNodes2 = new ArrayList<>();
    expectedNodes2.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000014"));
    GraphTest.assertEqualsAnyOrder(expectedNodes2, endpoint2.neighbors(src2));

    SwhId src3 = new SwhId("swh:1:dir:0000000000000000000000000000000000000012");
    Endpoint endpoint3 = new Endpoint(graph, "backward", "*");
    ArrayList<SwhId> expectedNodes3 = new ArrayList<>();
    expectedNodes3.add(new SwhId("swh:1:rev:0000000000000000000000000000000000000013"));
    GraphTest.assertEqualsAnyOrder(expectedNodes3, endpoint3.neighbors(src3));

    SwhId src4 = new SwhId("swh:1:rev:0000000000000000000000000000000000000009");
    Endpoint endpoint4 = new Endpoint(graph, "backward", "rev:rev");
    ArrayList<SwhId> expectedNodes4 = new ArrayList<>();
    expectedNodes4.add(new SwhId("swh:1:rev:0000000000000000000000000000000000000013"));
    GraphTest.assertEqualsAnyOrder(expectedNodes4, endpoint4.neighbors(src4));
  }

  @Test
  public void twoNeighbors() {
    Graph graph = getGraph();

    SwhId src1 = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Endpoint endpoint1 = new Endpoint(graph, "forward", "*");
    ArrayList<SwhId> expectedNodes1 = new ArrayList<>();
    expectedNodes1.add(new SwhId("swh:1:rel:0000000000000000000000000000000000000010"));
    expectedNodes1.add(new SwhId("swh:1:rev:0000000000000000000000000000000000000009"));
    GraphTest.assertEqualsAnyOrder(expectedNodes1, endpoint1.neighbors(src1));

    SwhId src2 = new SwhId("swh:1:dir:0000000000000000000000000000000000000008");
    Endpoint endpoint2 = new Endpoint(graph, "forward", "dir:cnt");
    ArrayList<SwhId> expectedNodes2 = new ArrayList<>();
    expectedNodes2.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000001"));
    expectedNodes2.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000007"));
    GraphTest.assertEqualsAnyOrder(expectedNodes2, endpoint2.neighbors(src2));

    SwhId src3 = new SwhId("swh:1:cnt:0000000000000000000000000000000000000001");
    Endpoint endpoint3 = new Endpoint(graph, "backward", "*");
    ArrayList<SwhId> expectedNodes3 = new ArrayList<>();
    expectedNodes3.add(new SwhId("swh:1:dir:0000000000000000000000000000000000000008"));
    expectedNodes3.add(new SwhId("swh:1:dir:0000000000000000000000000000000000000002"));
    GraphTest.assertEqualsAnyOrder(expectedNodes3, endpoint3.neighbors(src3));

    SwhId src4 = new SwhId("swh:1:rev:0000000000000000000000000000000000000009");
    Endpoint endpoint4 = new Endpoint(graph, "backward", "rev:snp,rev:rel");
    ArrayList<SwhId> expectedNodes4 = new ArrayList<>();
    expectedNodes4.add(new SwhId("swh:1:snp:0000000000000000000000000000000000000020"));
    expectedNodes4.add(new SwhId("swh:1:rel:0000000000000000000000000000000000000010"));
    GraphTest.assertEqualsAnyOrder(expectedNodes4, endpoint4.neighbors(src4));
  }

  @Test
  public void threeNeighbors() {
    Graph graph = getGraph();

    SwhId src1 = new SwhId("swh:1:dir:0000000000000000000000000000000000000008");
    Endpoint endpoint1 = new Endpoint(graph, "forward", "*");
    ArrayList<SwhId> expectedNodes1 = new ArrayList<>();
    expectedNodes1.add(new SwhId("swh:1:dir:0000000000000000000000000000000000000006"));
    expectedNodes1.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000001"));
    expectedNodes1.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000007"));
    GraphTest.assertEqualsAnyOrder(expectedNodes1, endpoint1.neighbors(src1));

    SwhId src2 = new SwhId("swh:1:rev:0000000000000000000000000000000000000009");
    Endpoint endpoint2 = new Endpoint(graph, "backward", "*");
    ArrayList<SwhId> expectedNodes2 = new ArrayList<>();
    expectedNodes2.add(new SwhId("swh:1:snp:0000000000000000000000000000000000000020"));
    expectedNodes2.add(new SwhId("swh:1:rel:0000000000000000000000000000000000000010"));
    expectedNodes2.add(new SwhId("swh:1:rev:0000000000000000000000000000000000000013"));
    GraphTest.assertEqualsAnyOrder(expectedNodes2, endpoint2.neighbors(src2));
  }
}
