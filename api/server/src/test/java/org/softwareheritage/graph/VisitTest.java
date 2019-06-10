package org.softwareheritage.graph;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.GraphTest;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.SwhPath;
import org.softwareheritage.graph.algo.Visit;

public class VisitTest extends GraphTest {
  void assertEqualPaths(ArrayList<SwhPath> expecteds, ArrayList<SwhPath> actuals) {
    Assert.assertThat(expecteds, containsInAnyOrder(actuals.toArray()));
  }

  @Test
  public void dfsForwardFromRoot() {
    Graph graph = getGraph();
    SwhId start = new SwhId("swh:1:dir:0000000000000000000000000000000000000001");
    Visit visit = new Visit(graph, start, "all", "dfs", "forward");
    ArrayList<SwhPath> paths = visit.getPaths();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:dir:0000000000000000000000000000000000000001",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:cnt:0000000000000000000000000000000000000009"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:dir:0000000000000000000000000000000000000001",
          "swh:1:cnt:0000000000000000000000000000000000000010"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:dir:0000000000000000000000000000000000000001",
          "swh:1:dir:0000000000000000000000000000000000000002",
          "swh:1:dir:0000000000000000000000000000000000000005",
          "swh:1:cnt:0000000000000000000000000000000000000006"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:dir:0000000000000000000000000000000000000001",
          "swh:1:dir:0000000000000000000000000000000000000002",
          "swh:1:cnt:0000000000000000000000000000000000000007"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:dir:0000000000000000000000000000000000000001",
          "swh:1:dir:0000000000000000000000000000000000000002",
          "swh:1:dir:0000000000000000000000000000000000000003",
          "swh:1:cnt:0000000000000000000000000000000000000004"
        ));

    assertEqualPaths(expectedPaths, paths);
  }

  @Test
  public void dfsForwardFromMiddle() {
    Graph graph = getGraph();
    SwhId start = new SwhId("swh:1:dir:0000000000000000000000000000000000000002");
    Visit visit = new Visit(graph, start, "all", "dfs", "forward");
    ArrayList<SwhPath> paths = visit.getPaths();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:dir:0000000000000000000000000000000000000002",
          "swh:1:dir:0000000000000000000000000000000000000005",
          "swh:1:cnt:0000000000000000000000000000000000000006"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:dir:0000000000000000000000000000000000000002",
          "swh:1:cnt:0000000000000000000000000000000000000007"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:dir:0000000000000000000000000000000000000002",
          "swh:1:dir:0000000000000000000000000000000000000003",
          "swh:1:cnt:0000000000000000000000000000000000000004"
        ));

    assertEqualPaths(expectedPaths, paths);
  }

  @Test
  public void dfsForwardFromLeaf() {
    Graph graph = getGraph();
    SwhId start = new SwhId("swh:1:cnt:0000000000000000000000000000000000000006");
    Visit visit = new Visit(graph, start, "all", "dfs", "forward");
    ArrayList<SwhPath> paths = visit.getPaths();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:cnt:0000000000000000000000000000000000000006"
        ));

    assertEqualPaths(expectedPaths, paths);
  }

  @Test
  public void dfsBackwardFromRoot() {
    Graph graph = getGraph();
    SwhId start = new SwhId("swh:1:dir:0000000000000000000000000000000000000001");
    Visit visit = new Visit(graph, start, "all", "dfs", "backward");
    ArrayList<SwhPath> paths = visit.getPaths();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:dir:0000000000000000000000000000000000000001"
        ));

    assertEqualPaths(expectedPaths, paths);
  }

  @Test
  public void dfsBackwardFromMiddle() {
    Graph graph = getGraph();
    SwhId start = new SwhId("swh:1:dir:0000000000000000000000000000000000000008");
    Visit visit = new Visit(graph, start, "all", "dfs", "backward");
    ArrayList<SwhPath> paths = visit.getPaths();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:dir:0000000000000000000000000000000000000001"
        ));

    assertEqualPaths(expectedPaths, paths);
  }

  @Test
  public void dfsBackwardFromLeaf() {
    Graph graph = getGraph();
    SwhId start = new SwhId("swh:1:cnt:0000000000000000000000000000000000000006");
    Visit visit = new Visit(graph, start, "all", "dfs", "backward");
    ArrayList<SwhPath> paths = visit.getPaths();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:cnt:0000000000000000000000000000000000000006",
          "swh:1:dir:0000000000000000000000000000000000000005",
          "swh:1:dir:0000000000000000000000000000000000000002",
          "swh:1:dir:0000000000000000000000000000000000000001"
        ));

    assertEqualPaths(expectedPaths, paths);
  }
}
