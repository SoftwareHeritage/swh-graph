package org.softwareheritage.graph;

import java.util.ArrayList;

import org.junit.Test;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.GraphTest;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.algo.Leaves;

public class LeavesTest extends GraphTest {
  @Test
  public void forwardFromSnp() {
    Graph graph = getGraph();
    SwhId src = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Leaves leaves = new Leaves(graph, src, "*", "forward");

    ArrayList<SwhId> expectedLeaves = new ArrayList<>();
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000001"));
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000004"));
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000005"));
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000007"));

    GraphTest.assertEqualsAnyOrder(expectedLeaves, leaves.getLeaves());
  }

  @Test
  public void forwardFromRel() {
    Graph graph = getGraph();
    SwhId src = new SwhId("swh:1:rel:0000000000000000000000000000000000000019");
    Leaves leaves = new Leaves(graph, src, "*", "forward");

    ArrayList<SwhId> expectedLeaves = new ArrayList<>();
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000015"));
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000014"));
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000001"));
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000004"));
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000005"));
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000007"));
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000011"));

    GraphTest.assertEqualsAnyOrder(expectedLeaves, leaves.getLeaves());
  }

  @Test
  public void backwardFromLeaf() {
    Graph graph = getGraph();

    SwhId src1 = new SwhId("swh:1:cnt:0000000000000000000000000000000000000015");
    Leaves leaves1 = new Leaves(graph, src1, "*", "backward");
    ArrayList<SwhId> expectedLeaves1 = new ArrayList<>();
    expectedLeaves1.add(new SwhId("swh:1:rel:0000000000000000000000000000000000000019"));
    GraphTest.assertEqualsAnyOrder(expectedLeaves1, leaves1.getLeaves());

    SwhId src2 = new SwhId("swh:1:cnt:0000000000000000000000000000000000000004");
    Leaves leaves2 = new Leaves(graph, src2, "*", "backward");
    ArrayList<SwhId> expectedLeaves2 = new ArrayList<>();
    expectedLeaves2.add(new SwhId("swh:1:snp:0000000000000000000000000000000000000020"));
    expectedLeaves2.add(new SwhId("swh:1:rel:0000000000000000000000000000000000000019"));
    GraphTest.assertEqualsAnyOrder(expectedLeaves2, leaves2.getLeaves());
  }

  @Test
  public void forwardRevToRevOnly() {
    Graph graph = getGraph();
    SwhId src = new SwhId("swh:1:rev:0000000000000000000000000000000000000018");
    Leaves leaves = new Leaves(graph, src, "rev:rev", "forward");

    ArrayList<SwhId> expectedLeaves = new ArrayList<>();
    expectedLeaves.add(new SwhId("swh:1:rev:0000000000000000000000000000000000000003"));

    GraphTest.assertEqualsAnyOrder(expectedLeaves, leaves.getLeaves());
  }

  @Test
  public void forwardDirToAll() {
    Graph graph = getGraph();
    SwhId src = new SwhId("swh:1:dir:0000000000000000000000000000000000000008");
    Leaves leaves = new Leaves(graph, src, "dir:*", "forward");

    ArrayList<SwhId> expectedLeaves = new ArrayList<>();
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000004"));
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000005"));
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000001"));
    expectedLeaves.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000007"));

    GraphTest.assertEqualsAnyOrder(expectedLeaves, leaves.getLeaves());
  }

  @Test
  public void backwardCntToDirDirToDir() {
    Graph graph = getGraph();
    SwhId src = new SwhId("swh:1:cnt:0000000000000000000000000000000000000005");
    Leaves leaves = new Leaves(graph, src, "cnt:dir,dir:dir", "backward");

    ArrayList<SwhId> expectedLeaves = new ArrayList<>();
    expectedLeaves.add(new SwhId("swh:1:dir:0000000000000000000000000000000000000012"));

    GraphTest.assertEqualsAnyOrder(expectedLeaves, leaves.getLeaves());
  }
}
