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

    GraphTest.assertEqualsAnyOrder(expectedLeaves, (ArrayList) endpoint.leaves(src).result);
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

    GraphTest.assertEqualsAnyOrder(expectedLeaves, (ArrayList) endpoint.leaves(src).result);
  }

  @Test
  public void backwardFromLeaf() {
    Graph graph = getGraph();
    Endpoint endpoint = new Endpoint(graph, "backward", "*");

    SwhPID src1 = new SwhPID("swh:1:cnt:0000000000000000000000000000000000000015");
    ArrayList<SwhPID> expectedLeaves1 = new ArrayList<>();
    expectedLeaves1.add(new SwhPID("swh:1:rel:0000000000000000000000000000000000000019"));
    GraphTest.assertEqualsAnyOrder(expectedLeaves1, (ArrayList) endpoint.leaves(src1).result);

    SwhPID src2 = new SwhPID("swh:1:cnt:0000000000000000000000000000000000000004");
    ArrayList<SwhPID> expectedLeaves2 = new ArrayList<>();
    expectedLeaves2.add(new SwhPID("swh:1:ori:0000000000000000000000000000000000000021"));
    expectedLeaves2.add(new SwhPID("swh:1:rel:0000000000000000000000000000000000000019"));
    GraphTest.assertEqualsAnyOrder(expectedLeaves2, (ArrayList) endpoint.leaves(src2).result);
  }

  @Test
  public void forwardRevToRevOnly() {
    Graph graph = getGraph();
    SwhPID src = new SwhPID("swh:1:rev:0000000000000000000000000000000000000018");
    Endpoint endpoint = new Endpoint(graph, "forward", "rev:rev");

    ArrayList<SwhPID> expectedLeaves = new ArrayList<>();
    expectedLeaves.add(new SwhPID("swh:1:rev:0000000000000000000000000000000000000003"));

    GraphTest.assertEqualsAnyOrder(expectedLeaves, (ArrayList) endpoint.leaves(src).result);
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

    GraphTest.assertEqualsAnyOrder(expectedLeaves, (ArrayList) endpoint.leaves(src).result);
  }

  @Test
  public void backwardCntToDirDirToDir() {
    Graph graph = getGraph();
    SwhPID src = new SwhPID("swh:1:cnt:0000000000000000000000000000000000000005");
    Endpoint endpoint = new Endpoint(graph, "backward", "cnt:dir,dir:dir");

    ArrayList<SwhPID> expectedLeaves = new ArrayList<>();
    expectedLeaves.add(new SwhPID("swh:1:dir:0000000000000000000000000000000000000012"));

    GraphTest.assertEqualsAnyOrder(expectedLeaves, (ArrayList) endpoint.leaves(src).result);
  }
}
