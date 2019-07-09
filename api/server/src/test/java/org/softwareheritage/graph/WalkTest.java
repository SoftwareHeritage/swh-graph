package org.softwareheritage.graph;

import java.util.ArrayList;
import java.util.LinkedHashSet;

import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.GraphTest;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.SwhPath;
import org.softwareheritage.graph.algo.Traversal;

public class WalkTest extends GraphTest {
  @Test
  public void forwardRootToLeaf() {
    Graph graph = getGraph();
    Traversal traversal = new Traversal(graph, "forward", "*");
    SwhId src = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    String dstFmt = "swh:1:cnt:0000000000000000000000000000000000000005";

    SwhPath dfsPath = traversal.walkEndpoint(src, dstFmt, "dfs");
    SwhPath dfsExpectedPath =
      new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:dir:0000000000000000000000000000000000000006",
          "swh:1:cnt:0000000000000000000000000000000000000005"
      );
    Assert.assertEquals(dfsExpectedPath, dfsPath);

    // Most of DFS and BFS traversal outputs are the same because it is a small test graph
    SwhPath bfsPath = traversal.walkEndpoint(src, dstFmt, "bfs");
    Assert.assertEquals(dfsExpectedPath, bfsPath);
  }

  @Test
  public void forwardLeafToLeaf() {
    Graph graph = getGraph();
    Traversal traversal = new Traversal(graph, "forward", "*");
    SwhId src = new SwhId("swh:1:cnt:0000000000000000000000000000000000000007");
    String dstFmt = "cnt";

    SwhPath dfsPath = traversal.walkEndpoint(src, dstFmt, "dfs");
    SwhPath dfsExpectedPath =
      new SwhPath(
          "swh:1:cnt:0000000000000000000000000000000000000007"
      );
    Assert.assertEquals(dfsExpectedPath, dfsPath);

    SwhPath bfsPath = traversal.walkEndpoint(src, dstFmt, "bfs");
    Assert.assertEquals(dfsExpectedPath, bfsPath);
  }

  @Test
  public void forwardRevToRev() {
    Graph graph = getGraph();
    Traversal traversal = new Traversal(graph, "forward", "rev:rev");
    SwhId src = new SwhId("swh:1:rev:0000000000000000000000000000000000000018");
    String dstFmt = "swh:1:rev:0000000000000000000000000000000000000003";

    SwhPath dfsPath = traversal.walkEndpoint(src, dstFmt, "dfs");
    SwhPath dfsExpectedPath =
      new SwhPath(
          "swh:1:rev:0000000000000000000000000000000000000018",
          "swh:1:rev:0000000000000000000000000000000000000013",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:rev:0000000000000000000000000000000000000003"
      );
    Assert.assertEquals(dfsExpectedPath, dfsPath);

    SwhPath bfsPath = traversal.walkEndpoint(src, dstFmt, "bfs");
    Assert.assertEquals(dfsExpectedPath, bfsPath);
  }

  @Test
  public void backwardRevToRev() {
    Graph graph = getGraph();
    Traversal traversal = new Traversal(graph, "backward", "rev:rev");
    SwhId src = new SwhId("swh:1:rev:0000000000000000000000000000000000000003");
    String dstFmt = "swh:1:rev:0000000000000000000000000000000000000018";

    SwhPath dfsPath = traversal.walkEndpoint(src, dstFmt, "dfs");
    SwhPath dfsExpectedPath =
      new SwhPath(
          "swh:1:rev:0000000000000000000000000000000000000003",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:rev:0000000000000000000000000000000000000013",
          "swh:1:rev:0000000000000000000000000000000000000018"
      );
    Assert.assertEquals(dfsExpectedPath, dfsPath);

    SwhPath bfsPath = traversal.walkEndpoint(src, dstFmt, "bfs");
    Assert.assertEquals(dfsExpectedPath, bfsPath);
  }

  @Test
  public void backwardCntToFirstSnp() {
    Graph graph = getGraph();
    Traversal traversal = new Traversal(graph, "backward", "*");
    SwhId src = new SwhId("swh:1:cnt:0000000000000000000000000000000000000001");
    String dstFmt = "snp";

    SwhPath dfsPath = traversal.walkEndpoint(src, dstFmt, "dfs");
    SwhPath dfsExpectedPath =
      new SwhPath(
          "swh:1:cnt:0000000000000000000000000000000000000001",
          "swh:1:dir:0000000000000000000000000000000000000002",
          "swh:1:rev:0000000000000000000000000000000000000003",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:snp:0000000000000000000000000000000000000020"
      );
    Assert.assertEquals(dfsExpectedPath, dfsPath);

    SwhPath bfsPath = traversal.walkEndpoint(src, dstFmt, "bfs");
    SwhPath bfsExpectedPath =
      new SwhPath(
          "swh:1:cnt:0000000000000000000000000000000000000001",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:snp:0000000000000000000000000000000000000020"
      );
    Assert.assertEquals(bfsExpectedPath, bfsPath);
  }

  @Test
  public void forwardRevToFirstCnt() {
    Graph graph = getGraph();
    Traversal traversal = new Traversal(graph, "forward", "rev:*,dir:*");
    SwhId src = new SwhId("swh:1:rev:0000000000000000000000000000000000000013");
    String dstFmt = "cnt";

    SwhPath dfsPath = traversal.walkEndpoint(src, dstFmt, "dfs");
    SwhPath dfsExpectedPath =
      new SwhPath(
          "swh:1:rev:0000000000000000000000000000000000000013",
          "swh:1:dir:0000000000000000000000000000000000000012",
          "swh:1:cnt:0000000000000000000000000000000000000011"
      );
    Assert.assertEquals(dfsExpectedPath, dfsPath);

    SwhPath bfsPath = traversal.walkEndpoint(src, dstFmt, "bfs");
    Assert.assertEquals(dfsExpectedPath, bfsPath);
  }

  @Test
  public void backwardDirToFirstRel() {
    Graph graph = getGraph();
    Traversal traversal = new Traversal(graph, "backward", "dir:dir,dir:rev,rev:*");
    SwhId src = new SwhId("swh:1:dir:0000000000000000000000000000000000000016");
    String dstFmt = "rel";

    SwhPath dfsPath = traversal.walkEndpoint(src, dstFmt, "dfs");
    SwhPath dfsExpectedPath =
      new SwhPath(
          "swh:1:dir:0000000000000000000000000000000000000016",
          "swh:1:dir:0000000000000000000000000000000000000017",
          "swh:1:rev:0000000000000000000000000000000000000018",
          "swh:1:rel:0000000000000000000000000000000000000019"
      );
    Assert.assertEquals(dfsExpectedPath, dfsPath);

    SwhPath bfsPath = traversal.walkEndpoint(src, dstFmt, "bfs");
    Assert.assertEquals(dfsExpectedPath, bfsPath);
  }
}
