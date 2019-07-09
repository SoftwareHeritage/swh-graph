package org.softwareheritage.graph;

import java.util.ArrayList;

import org.junit.Test;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.GraphTest;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.algo.Traversal;

public class LeavesTest extends GraphTest {
  @Test
  public void forwardFromSnp() {
    Graph graph = getGraph();
    SwhId src = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Traversal traversal = new Traversal(graph, "forward", "*");

    ArrayList<SwhId> expectedLeaves = new ArrayList<>();
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000001"));
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000004"));
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000005"));
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000007"));

    assertEquals(expectedLeaves, traversal.leavesEndpoint(src));
  }

  @Test
  public void forwardFromRel() {
    Graph graph = getGraph();
    SwhId src = new SwhId("swh:1:rel:0000000000000000000000000000000000000019");
    Traversal traversal = new Traversal(graph, "forward", "*");

    ArrayList<SwhId> expectedLeaves = new ArrayList<>();
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000015"));
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000014"));
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000001"));
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000004"));
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000005"));
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000007"));
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000011"));

    assertEquals(expectedLeaves, traversal.leavesEndpoint(src));
  }

  @Test
  public void backwardFromLeaf() {
    Graph graph = getGraph();
    Traversal traversal = new Traversal(graph, "backward", "*");

    SwhId src1 = new SwhId("swh:1:cnt:0000000000000000000000000000000000000015");
    ArrayList<SwhId> expectedLeaves1 = new ArrayList<>();
    expectedLeaves1.add(new SwhId("swh:1:rel:0000000000000000000000000000000000000019"));
    assertEquals(expectedLeaves1, traversal.leavesEndpoint(src1));

    SwhId src2 = new SwhId("swh:1:cnt:0000000000000000000000000000000000000004");
    ArrayList<SwhId> expectedLeaves2 = new ArrayList<>();
    expectedLeaves2.add(new SwhId("swh:1:snp:0000000000000000000000000000000000000020"));
    expectedLeaves2.add(new SwhId("swh:1:rel:0000000000000000000000000000000000000019"));
    assertEquals(expectedLeaves2, traversal.leavesEndpoint(src2));
  }

  @Test
  public void forwardRevToRevOnly() {
    Graph graph = getGraph();
    SwhId src = new SwhId("swh:1:rev:0000000000000000000000000000000000000018");
    Traversal traversal = new Traversal(graph, "forward", "rev:rev");

    ArrayList<SwhId> expectedLeaves = new ArrayList<>();
    expectedLeaves.add(new SwhId("swh:1:rev:0000000000000000000000000000000000000003"));

    assertEquals(expectedLeaves, traversal.leavesEndpoint(src));
  }

  @Test
  public void forwardDirToAll() {
    Graph graph = getGraph();
    SwhId src = new SwhId("swh:1:dir:0000000000000000000000000000000000000008");
    Traversal traversal = new Traversal(graph, "forward", "dir:*");

    ArrayList<SwhId> expectedLeaves = new ArrayList<>();
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000004"));
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000005"));
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000001"));
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000007"));

    assertEquals(expectedLeaves, traversal.leavesEndpoint(src));
  }

  @Test
  public void backwardCntToDirDirToDir() {
    Graph graph = getGraph();
    SwhId src = new SwhId("swh:1:cnt:0000000000000000000000000000000000000005");
    Traversal traversal = new Traversal(graph, "backward", "cnt:dir,dir:dir");

    ArrayList<SwhId> expectedLeaves = new ArrayList<>();
    expectedLeaves.add(new SwhId("swh:1:dir:0000000000000000000000000000000000000012"));

    assertEquals(expectedLeaves, traversal.leavesEndpoint(src));
  }
}
