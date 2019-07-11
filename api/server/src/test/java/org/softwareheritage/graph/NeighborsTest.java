package org.softwareheritage.graph;

import java.util.ArrayList;

import org.junit.Test;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.GraphTest;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.algo.Neighbors;

public class NeighborsTest extends GraphTest {
  @Test
  public void zeroNeighbor() {
    Graph graph = getGraph();
    ArrayList<SwhId> expectedNodes = new ArrayList<>();

    SwhId src1 = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Neighbors neighbors1 = new Neighbors(graph, src1, "*", "backward");
    GraphTest.assertEqualsAnyOrder(expectedNodes, neighbors1.getNeighbors());

    SwhId src2 = new SwhId("swh:1:cnt:0000000000000000000000000000000000000004");
    Neighbors neighbors2 = new Neighbors(graph, src2, "*", "forward");
    GraphTest.assertEqualsAnyOrder(expectedNodes, neighbors2.getNeighbors());

    SwhId src3 = new SwhId("swh:1:cnt:0000000000000000000000000000000000000015");
    Neighbors neighbors3 = new Neighbors(graph, src3, "*", "forward");
    GraphTest.assertEqualsAnyOrder(expectedNodes, neighbors3.getNeighbors());

    SwhId src4 = new SwhId("swh:1:rel:0000000000000000000000000000000000000019");
    Neighbors neighbors4 = new Neighbors(graph, src4, "*", "backward");
    GraphTest.assertEqualsAnyOrder(expectedNodes, neighbors4.getNeighbors());

    SwhId src5 = new SwhId("swh:1:dir:0000000000000000000000000000000000000008");
    Neighbors neighbors5 = new Neighbors(graph, src5, "snp:*,rev:*,rel:*", "forward");
    GraphTest.assertEqualsAnyOrder(expectedNodes, neighbors5.getNeighbors());
  }

  @Test
  public void oneNeighbor() {
    Graph graph = getGraph();

    SwhId src1 = new SwhId("swh:1:rev:0000000000000000000000000000000000000003");
    Neighbors neighbors1 = new Neighbors(graph, src1, "*", "forward");
    ArrayList<SwhId> expectedNodes1 = new ArrayList<>();
    expectedNodes1.add(new SwhId("swh:1:dir:0000000000000000000000000000000000000002"));
    GraphTest.assertEqualsAnyOrder(expectedNodes1, neighbors1.getNeighbors());

    SwhId src2 = new SwhId("swh:1:dir:0000000000000000000000000000000000000017");
    Neighbors neighbors2 = new Neighbors(graph, src2, "dir:cnt", "forward");
    ArrayList<SwhId> expectedNodes2 = new ArrayList<>();
    expectedNodes2.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000014"));
    GraphTest.assertEqualsAnyOrder(expectedNodes2, neighbors2.getNeighbors());

    SwhId src3 = new SwhId("swh:1:dir:0000000000000000000000000000000000000012");
    Neighbors neighbors3 = new Neighbors(graph, src3, "*", "backward");
    ArrayList<SwhId> expectedNodes3 = new ArrayList<>();
    expectedNodes3.add(new SwhId("swh:1:rev:0000000000000000000000000000000000000013"));
    GraphTest.assertEqualsAnyOrder(expectedNodes3, neighbors3.getNeighbors());

    SwhId src4 = new SwhId("swh:1:rev:0000000000000000000000000000000000000009");
    Neighbors neighbors4 = new Neighbors(graph, src4, "rev:rev", "backward");
    ArrayList<SwhId> expectedNodes4 = new ArrayList<>();
    expectedNodes4.add(new SwhId("swh:1:rev:0000000000000000000000000000000000000013"));
    GraphTest.assertEqualsAnyOrder(expectedNodes4, neighbors4.getNeighbors());
  }

  @Test
  public void twoNeighbors() {
    Graph graph = getGraph();

    SwhId src1 = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Neighbors neighbors1 = new Neighbors(graph, src1, "*", "forward");
    ArrayList<SwhId> expectedNodes1 = new ArrayList<>();
    expectedNodes1.add(new SwhId("swh:1:rel:0000000000000000000000000000000000000010"));
    expectedNodes1.add(new SwhId("swh:1:rev:0000000000000000000000000000000000000009"));
    GraphTest.assertEqualsAnyOrder(expectedNodes1, neighbors1.getNeighbors());

    SwhId src2 = new SwhId("swh:1:dir:0000000000000000000000000000000000000008");
    Neighbors neighbors2 = new Neighbors(graph, src2, "dir:cnt", "forward");
    ArrayList<SwhId> expectedNodes2 = new ArrayList<>();
    expectedNodes2.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000001"));
    expectedNodes2.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000007"));
    GraphTest.assertEqualsAnyOrder(expectedNodes2, neighbors2.getNeighbors());

    SwhId src3 = new SwhId("swh:1:cnt:0000000000000000000000000000000000000001");
    Neighbors neighbors3 = new Neighbors(graph, src3, "*", "backward");
    ArrayList<SwhId> expectedNodes3 = new ArrayList<>();
    expectedNodes3.add(new SwhId("swh:1:dir:0000000000000000000000000000000000000008"));
    expectedNodes3.add(new SwhId("swh:1:dir:0000000000000000000000000000000000000002"));
    GraphTest.assertEqualsAnyOrder(expectedNodes3, neighbors3.getNeighbors());

    SwhId src4 = new SwhId("swh:1:rev:0000000000000000000000000000000000000009");
    Neighbors neighbors4 = new Neighbors(graph, src4, "rev:snp,rev:rel", "backward");
    ArrayList<SwhId> expectedNodes4 = new ArrayList<>();
    expectedNodes4.add(new SwhId("swh:1:snp:0000000000000000000000000000000000000020"));
    expectedNodes4.add(new SwhId("swh:1:rel:0000000000000000000000000000000000000010"));
    GraphTest.assertEqualsAnyOrder(expectedNodes4, neighbors4.getNeighbors());
  }

  @Test
  public void threeNeighbors() {
    Graph graph = getGraph();

    SwhId src1 = new SwhId("swh:1:dir:0000000000000000000000000000000000000008");
    Neighbors neighbors1 = new Neighbors(graph, src1, "*", "forward");
    ArrayList<SwhId> expectedNodes1 = new ArrayList<>();
    expectedNodes1.add(new SwhId("swh:1:dir:0000000000000000000000000000000000000006"));
    expectedNodes1.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000001"));
    expectedNodes1.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000007"));
    GraphTest.assertEqualsAnyOrder(expectedNodes1, neighbors1.getNeighbors());

    SwhId src2 = new SwhId("swh:1:rev:0000000000000000000000000000000000000009");
    Neighbors neighbors2 = new Neighbors(graph, src2, "*", "backward");
    ArrayList<SwhId> expectedNodes2 = new ArrayList<>();
    expectedNodes2.add(new SwhId("swh:1:snp:0000000000000000000000000000000000000020"));
    expectedNodes2.add(new SwhId("swh:1:rel:0000000000000000000000000000000000000010"));
    expectedNodes2.add(new SwhId("swh:1:rev:0000000000000000000000000000000000000013"));
    GraphTest.assertEqualsAnyOrder(expectedNodes2, neighbors2.getNeighbors());
  }
}
