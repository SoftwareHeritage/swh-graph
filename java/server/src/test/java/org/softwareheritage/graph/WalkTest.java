package org.softwareheritage.graph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import org.softwareheritage.graph.Endpoint;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.GraphTest;
import org.softwareheritage.graph.SwhPID;
import org.softwareheritage.graph.SwhPath;

public class WalkTest extends GraphTest {
  @Test
  public void forwardRootToLeaf() {
    Graph graph = getGraph();
    Endpoint endpoint = new Endpoint(graph, "forward", "*");
    SwhPID src = new SwhPID("swh:1:snp:0000000000000000000000000000000000000020");
    String dstFmt = "swh:1:cnt:0000000000000000000000000000000000000005";

    SwhPath solution1 =
      new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:dir:0000000000000000000000000000000000000006",
          "swh:1:cnt:0000000000000000000000000000000000000005"
      );
    SwhPath solution2 =
      new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rel:0000000000000000000000000000000000000010",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:dir:0000000000000000000000000000000000000006",
          "swh:1:cnt:0000000000000000000000000000000000000005"
      );

    SwhPath dfsPath = (SwhPath) endpoint.walk(new Endpoint.Input(src, dstFmt, "dfs")).result;
    SwhPath bfsPath = (SwhPath) endpoint.walk(new Endpoint.Input(src, dstFmt, "bfs")).result;

    List<SwhPath> possibleSolutions = Arrays.asList(solution1, solution2);
    Assert.assertTrue(possibleSolutions.contains(dfsPath));
    Assert.assertTrue(possibleSolutions.contains(bfsPath));
  }

  @Test
  public void forwardLeafToLeaf() {
    Graph graph = getGraph();
    Endpoint endpoint = new Endpoint(graph, "forward", "*");
    SwhPID src = new SwhPID("swh:1:cnt:0000000000000000000000000000000000000007");
    String dstFmt = "cnt";

    SwhPath expectedPath =
      new SwhPath(
          "swh:1:cnt:0000000000000000000000000000000000000007"
      );

    SwhPath dfsPath = (SwhPath) endpoint.walk(new Endpoint.Input(src, dstFmt, "dfs")).result;
    SwhPath bfsPath = (SwhPath) endpoint.walk(new Endpoint.Input(src, dstFmt, "bfs")).result;

    Assert.assertEquals(dfsPath, expectedPath);
    Assert.assertEquals(bfsPath, expectedPath);
  }

  @Test
  public void forwardRevToRev() {
    Graph graph = getGraph();
    Endpoint endpoint = new Endpoint(graph, "forward", "rev:rev");
    SwhPID src = new SwhPID("swh:1:rev:0000000000000000000000000000000000000018");
    String dstFmt = "swh:1:rev:0000000000000000000000000000000000000003";

    SwhPath expectedPath =
      new SwhPath(
          "swh:1:rev:0000000000000000000000000000000000000018",
          "swh:1:rev:0000000000000000000000000000000000000013",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:rev:0000000000000000000000000000000000000003"
      );

    SwhPath dfsPath = (SwhPath) endpoint.walk(new Endpoint.Input(src, dstFmt, "dfs")).result;
    SwhPath bfsPath = (SwhPath) endpoint.walk(new Endpoint.Input(src, dstFmt, "bfs")).result;

    Assert.assertEquals(dfsPath, expectedPath);
    Assert.assertEquals(bfsPath, expectedPath);
  }

  @Test
  public void backwardRevToRev() {
    Graph graph = getGraph();
    Endpoint endpoint = new Endpoint(graph, "backward", "rev:rev");
    SwhPID src = new SwhPID("swh:1:rev:0000000000000000000000000000000000000003");
    String dstFmt = "swh:1:rev:0000000000000000000000000000000000000018";

    SwhPath expectedPath =
      new SwhPath(
          "swh:1:rev:0000000000000000000000000000000000000003",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:rev:0000000000000000000000000000000000000013",
          "swh:1:rev:0000000000000000000000000000000000000018"
      );

    SwhPath dfsPath = (SwhPath) endpoint.walk(new Endpoint.Input(src, dstFmt, "dfs")).result;
    SwhPath bfsPath = (SwhPath) endpoint.walk(new Endpoint.Input(src, dstFmt, "bfs")).result;

    Assert.assertEquals(dfsPath, expectedPath);
    Assert.assertEquals(bfsPath, expectedPath);
  }

  @Test
  public void backwardCntToFirstSnp() {
    Graph graph = getGraph();
    Endpoint endpoint = new Endpoint(graph, "backward", "*");
    SwhPID src = new SwhPID("swh:1:cnt:0000000000000000000000000000000000000001");
    String dstFmt = "snp";

    SwhPath solution1 =
      new SwhPath(
          "swh:1:cnt:0000000000000000000000000000000000000001",
          "swh:1:dir:0000000000000000000000000000000000000002",
          "swh:1:rev:0000000000000000000000000000000000000003",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:snp:0000000000000000000000000000000000000020"
      );
    SwhPath solution2 =
      new SwhPath(
          "swh:1:cnt:0000000000000000000000000000000000000001",
          "swh:1:dir:0000000000000000000000000000000000000002",
          "swh:1:rev:0000000000000000000000000000000000000003",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:rel:0000000000000000000000000000000000000010",
          "swh:1:snp:0000000000000000000000000000000000000020"
      );
    SwhPath solution3 =
      new SwhPath(
          "swh:1:cnt:0000000000000000000000000000000000000001",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:snp:0000000000000000000000000000000000000020"
      );
    SwhPath solution4 =
      new SwhPath(
          "swh:1:cnt:0000000000000000000000000000000000000001",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:rel:0000000000000000000000000000000000000010",
          "swh:1:snp:0000000000000000000000000000000000000020"
      );

    SwhPath dfsPath = (SwhPath) endpoint.walk(new Endpoint.Input(src, dstFmt, "dfs")).result;
    SwhPath bfsPath = (SwhPath) endpoint.walk(new Endpoint.Input(src, dstFmt, "bfs")).result;

    List<SwhPath> possibleSolutions = Arrays.asList(solution1, solution2, solution3, solution4);
    Assert.assertTrue(possibleSolutions.contains(dfsPath));
    Assert.assertTrue(possibleSolutions.contains(bfsPath));
  }

  @Test
  public void forwardRevToFirstCnt() {
    Graph graph = getGraph();
    Endpoint endpoint = new Endpoint(graph, "forward", "rev:*,dir:*");
    SwhPID src = new SwhPID("swh:1:rev:0000000000000000000000000000000000000009");
    String dstFmt = "cnt";

    SwhPath solution1 =
      new SwhPath(
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:cnt:0000000000000000000000000000000000000007"
      );
    SwhPath solution2 =
      new SwhPath(
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:dir:0000000000000000000000000000000000000006",
          "swh:1:cnt:0000000000000000000000000000000000000005"
      );
    SwhPath solution3 =
      new SwhPath(
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:dir:0000000000000000000000000000000000000006",
          "swh:1:cnt:0000000000000000000000000000000000000004"
      );
    SwhPath solution4 =
      new SwhPath(
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:cnt:0000000000000000000000000000000000000001"
      );
    SwhPath solution5 =
      new SwhPath(
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:rev:0000000000000000000000000000000000000003",
          "swh:1:dir:0000000000000000000000000000000000000002",
          "swh:1:cnt:0000000000000000000000000000000000000001"
      );

    SwhPath dfsPath = (SwhPath) endpoint.walk(new Endpoint.Input(src, dstFmt, "dfs")).result;
    SwhPath bfsPath = (SwhPath) endpoint.walk(new Endpoint.Input(src, dstFmt, "bfs")).result;

    List<SwhPath> possibleSolutions =
      Arrays.asList(solution1, solution2, solution3, solution4, solution5);
    Assert.assertTrue(possibleSolutions.contains(dfsPath));
    Assert.assertTrue(possibleSolutions.contains(bfsPath));
  }

  @Test
  public void backwardDirToFirstRel() {
    Graph graph = getGraph();
    Endpoint endpoint = new Endpoint(graph, "backward", "dir:dir,dir:rev,rev:*");
    SwhPID src = new SwhPID("swh:1:dir:0000000000000000000000000000000000000016");
    String dstFmt = "rel";

    SwhPath expectedPath =
      new SwhPath(
          "swh:1:dir:0000000000000000000000000000000000000016",
          "swh:1:dir:0000000000000000000000000000000000000017",
          "swh:1:rev:0000000000000000000000000000000000000018",
          "swh:1:rel:0000000000000000000000000000000000000019"
      );

    SwhPath dfsPath = (SwhPath) endpoint.walk(new Endpoint.Input(src, dstFmt, "dfs")).result;
    SwhPath bfsPath = (SwhPath) endpoint.walk(new Endpoint.Input(src, dstFmt, "bfs")).result;

    Assert.assertEquals(dfsPath, expectedPath);
    Assert.assertEquals(bfsPath, expectedPath);
  }
}
