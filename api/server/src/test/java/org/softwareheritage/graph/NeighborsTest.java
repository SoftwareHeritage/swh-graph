package org.softwareheritage.graph;

import java.util.ArrayList;

import org.junit.Test;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.GraphTest;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.algo.Traversal;

public class NeighborsTest extends GraphTest {
  @Test
  public void zeroNeighbor() {
    Graph graph = getGraph();
    ArrayList<SwhId> expectedNodes = new ArrayList<>();

    SwhId src1 = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Traversal traversal1 = new Traversal(graph, "backward", "*");
    GraphTest.assertEqualsAnyOrder(expectedNodes, traversal1.neighborsEndpoint(src1));

    SwhId src2 = new SwhId("swh:1:cnt:0000000000000000000000000000000000000004");
    Traversal traversal2 = new Traversal(graph, "forward", "*");
    GraphTest.assertEqualsAnyOrder(expectedNodes, traversal2.neighborsEndpoint(src2));

    SwhId src3 = new SwhId("swh:1:cnt:0000000000000000000000000000000000000015");
    Traversal traversal3 = new Traversal(graph, "forward", "*");
    GraphTest.assertEqualsAnyOrder(expectedNodes, traversal3.neighborsEndpoint(src3));

    SwhId src4 = new SwhId("swh:1:rel:0000000000000000000000000000000000000019");
    Traversal traversal4 = new Traversal(graph, "backward", "*");
    GraphTest.assertEqualsAnyOrder(expectedNodes, traversal4.neighborsEndpoint(src4));

    SwhId src5 = new SwhId("swh:1:dir:0000000000000000000000000000000000000008");
    Traversal traversal5 = new Traversal(graph, "forward", "snp:*,rev:*,rel:*");
    GraphTest.assertEqualsAnyOrder(expectedNodes, traversal5.neighborsEndpoint(src5));
  }

  @Test
  public void oneNeighbor() {
    Graph graph = getGraph();

    SwhId src1 = new SwhId("swh:1:rev:0000000000000000000000000000000000000003");
    Traversal traversal1 = new Traversal(graph, "forward", "*");
    ArrayList<SwhId> expectedNodes1 = new ArrayList<>();
    expectedNodes1.add(new SwhId("swh:1:dir:0000000000000000000000000000000000000002"));
    GraphTest.assertEqualsAnyOrder(expectedNodes1, traversal1.neighborsEndpoint(src1));

    SwhId src2 = new SwhId("swh:1:dir:0000000000000000000000000000000000000017");
    Traversal traversal2 = new Traversal(graph, "forward", "dir:cnt");
    ArrayList<SwhId> expectedNodes2 = new ArrayList<>();
    expectedNodes2.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000014"));
    GraphTest.assertEqualsAnyOrder(expectedNodes2, traversal2.neighborsEndpoint(src2));

    SwhId src3 = new SwhId("swh:1:dir:0000000000000000000000000000000000000012");
    Traversal traversal3 = new Traversal(graph, "backward", "*");
    ArrayList<SwhId> expectedNodes3 = new ArrayList<>();
    expectedNodes3.add(new SwhId("swh:1:rev:0000000000000000000000000000000000000013"));
    GraphTest.assertEqualsAnyOrder(expectedNodes3, traversal3.neighborsEndpoint(src3));

    SwhId src4 = new SwhId("swh:1:rev:0000000000000000000000000000000000000009");
    Traversal traversal4 = new Traversal(graph, "backward", "rev:rev");
    ArrayList<SwhId> expectedNodes4 = new ArrayList<>();
    expectedNodes4.add(new SwhId("swh:1:rev:0000000000000000000000000000000000000013"));
    GraphTest.assertEqualsAnyOrder(expectedNodes4, traversal4.neighborsEndpoint(src4));
  }

  @Test
  public void twoNeighbors() {
    Graph graph = getGraph();

    SwhId src1 = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Traversal traversal1 = new Traversal(graph, "forward", "*");
    ArrayList<SwhId> expectedNodes1 = new ArrayList<>();
    expectedNodes1.add(new SwhId("swh:1:rel:0000000000000000000000000000000000000010"));
    expectedNodes1.add(new SwhId("swh:1:rev:0000000000000000000000000000000000000009"));
    GraphTest.assertEqualsAnyOrder(expectedNodes1, traversal1.neighborsEndpoint(src1));

    SwhId src2 = new SwhId("swh:1:dir:0000000000000000000000000000000000000008");
    Traversal traversal2 = new Traversal(graph, "forward", "dir:cnt");
    ArrayList<SwhId> expectedNodes2 = new ArrayList<>();
    expectedNodes2.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000001"));
    expectedNodes2.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000007"));
    GraphTest.assertEqualsAnyOrder(expectedNodes2, traversal2.neighborsEndpoint(src2));

    SwhId src3 = new SwhId("swh:1:cnt:0000000000000000000000000000000000000001");
    Traversal traversal3 = new Traversal(graph, "backward", "*");
    ArrayList<SwhId> expectedNodes3 = new ArrayList<>();
    expectedNodes3.add(new SwhId("swh:1:dir:0000000000000000000000000000000000000008"));
    expectedNodes3.add(new SwhId("swh:1:dir:0000000000000000000000000000000000000002"));
    GraphTest.assertEqualsAnyOrder(expectedNodes3, traversal3.neighborsEndpoint(src3));

    SwhId src4 = new SwhId("swh:1:rev:0000000000000000000000000000000000000009");
    Traversal traversal4 = new Traversal(graph, "backward", "rev:snp,rev:rel");
    ArrayList<SwhId> expectedNodes4 = new ArrayList<>();
    expectedNodes4.add(new SwhId("swh:1:snp:0000000000000000000000000000000000000020"));
    expectedNodes4.add(new SwhId("swh:1:rel:0000000000000000000000000000000000000010"));
    GraphTest.assertEqualsAnyOrder(expectedNodes4, traversal4.neighborsEndpoint(src4));
  }

  @Test
  public void threeNeighbors() {
    Graph graph = getGraph();

    SwhId src1 = new SwhId("swh:1:dir:0000000000000000000000000000000000000008");
    Traversal traversal1 = new Traversal(graph, "forward", "*");
    ArrayList<SwhId> expectedNodes1 = new ArrayList<>();
    expectedNodes1.add(new SwhId("swh:1:dir:0000000000000000000000000000000000000006"));
    expectedNodes1.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000001"));
    expectedNodes1.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000007"));
    GraphTest.assertEqualsAnyOrder(expectedNodes1, traversal1.neighborsEndpoint(src1));

    SwhId src2 = new SwhId("swh:1:rev:0000000000000000000000000000000000000009");
    Traversal traversal2 = new Traversal(graph, "backward", "*");
    ArrayList<SwhId> expectedNodes2 = new ArrayList<>();
    expectedNodes2.add(new SwhId("swh:1:snp:0000000000000000000000000000000000000020"));
    expectedNodes2.add(new SwhId("swh:1:rel:0000000000000000000000000000000000000010"));
    expectedNodes2.add(new SwhId("swh:1:rev:0000000000000000000000000000000000000013"));
    GraphTest.assertEqualsAnyOrder(expectedNodes2, traversal2.neighborsEndpoint(src2));
  }
}
