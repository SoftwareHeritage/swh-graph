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
  void assertEqualSwhPaths(ArrayList<SwhPath> expecteds, ArrayList<SwhPath> actuals) {
    Assert.assertThat(expecteds, containsInAnyOrder(actuals.toArray()));
  }

  @Test
  public void dfsForwardFromRoot() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Visit visit = new Visit(graph, swhId, "all", "dfs", "forward");
    ArrayList<SwhPath> paths = visit.getPaths();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:cnt:0000000000000000000000000000000000000007"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:cnt:0000000000000000000000000000000000000001"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:dir:0000000000000000000000000000000000000006",
          "swh:1:cnt:0000000000000000000000000000000000000004"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:dir:0000000000000000000000000000000000000006",
          "swh:1:cnt:0000000000000000000000000000000000000005"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:rev:0000000000000000000000000000000000000003",
          "swh:1:dir:0000000000000000000000000000000000000002",
          "swh:1:cnt:0000000000000000000000000000000000000001"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rel:0000000000000000000000000000000000000010",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:cnt:0000000000000000000000000000000000000007"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rel:0000000000000000000000000000000000000010",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:cnt:0000000000000000000000000000000000000001"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rel:0000000000000000000000000000000000000010",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:dir:0000000000000000000000000000000000000006",
          "swh:1:cnt:0000000000000000000000000000000000000004"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rel:0000000000000000000000000000000000000010",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:dir:0000000000000000000000000000000000000006",
          "swh:1:cnt:0000000000000000000000000000000000000005"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rel:0000000000000000000000000000000000000010",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:rev:0000000000000000000000000000000000000003",
          "swh:1:dir:0000000000000000000000000000000000000002",
          "swh:1:cnt:0000000000000000000000000000000000000001"
        ));

    assertEqualSwhPaths(expectedPaths, paths);
  }

  @Test
  public void dfsForwardFromMiddle() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:dir:0000000000000000000000000000000000000012");
    Visit visit = new Visit(graph, swhId, "all", "dfs", "forward");
    ArrayList<SwhPath> paths = visit.getPaths();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:dir:0000000000000000000000000000000000000012",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:cnt:0000000000000000000000000000000000000007"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:dir:0000000000000000000000000000000000000012",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:cnt:0000000000000000000000000000000000000001"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:dir:0000000000000000000000000000000000000012",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:dir:0000000000000000000000000000000000000006",
          "swh:1:cnt:0000000000000000000000000000000000000004"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:dir:0000000000000000000000000000000000000012",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:dir:0000000000000000000000000000000000000006",
          "swh:1:cnt:0000000000000000000000000000000000000005"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:dir:0000000000000000000000000000000000000012",
          "swh:1:cnt:0000000000000000000000000000000000000011"
        ));

    assertEqualSwhPaths(expectedPaths, paths);
  }

  @Test
  public void dfsForwardFromLeaf() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:cnt:0000000000000000000000000000000000000004");
    Visit visit = new Visit(graph, swhId, "all", "dfs", "forward");
    ArrayList<SwhPath> paths = visit.getPaths();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:cnt:0000000000000000000000000000000000000004"
        ));

    assertEqualSwhPaths(expectedPaths, paths);
  }

  @Test
  public void dfsBackwardFromRoot() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Visit visit = new Visit(graph, swhId, "all", "dfs", "backward");
    ArrayList<SwhPath> paths = visit.getPaths();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020"
        ));

    assertEqualSwhPaths(expectedPaths, paths);
  }

  @Test
  public void dfsBackwardFromMiddle() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:dir:0000000000000000000000000000000000000012");
    Visit visit = new Visit(graph, swhId, "all", "dfs", "backward");
    ArrayList<SwhPath> paths = visit.getPaths();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:dir:0000000000000000000000000000000000000012",
          "swh:1:rev:0000000000000000000000000000000000000013",
          "swh:1:rev:0000000000000000000000000000000000000018",
          "swh:1:rel:0000000000000000000000000000000000000019"
        ));

    assertEqualSwhPaths(expectedPaths, paths);
  }

  @Test
  public void dfsBackwardFromLeaf() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:cnt:0000000000000000000000000000000000000004");
    Visit visit = new Visit(graph, swhId, "all", "dfs", "backward");
    ArrayList<SwhPath> paths = visit.getPaths();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:cnt:0000000000000000000000000000000000000004",
          "swh:1:dir:0000000000000000000000000000000000000006",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:dir:0000000000000000000000000000000000000012",
          "swh:1:rev:0000000000000000000000000000000000000013",
          "swh:1:rev:0000000000000000000000000000000000000018",
          "swh:1:rel:0000000000000000000000000000000000000019"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:cnt:0000000000000000000000000000000000000004",
          "swh:1:dir:0000000000000000000000000000000000000006",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:rev:0000000000000000000000000000000000000013",
          "swh:1:rev:0000000000000000000000000000000000000018",
          "swh:1:rel:0000000000000000000000000000000000000019"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:cnt:0000000000000000000000000000000000000004",
          "swh:1:dir:0000000000000000000000000000000000000006",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:snp:0000000000000000000000000000000000000020"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:cnt:0000000000000000000000000000000000000004",
          "swh:1:dir:0000000000000000000000000000000000000006",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:rel:0000000000000000000000000000000000000010",
          "swh:1:snp:0000000000000000000000000000000000000020"
        ));

    assertEqualSwhPaths(expectedPaths, paths);
  }

  @Test
  public void dfsForwardOnlySnpRevAllowed() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Visit visit = new Visit(graph, swhId, "snp:rev", "dfs", "forward");
    ArrayList<SwhPath> paths = visit.getPaths();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rev:0000000000000000000000000000000000000009"
        ));

    assertEqualSwhPaths(expectedPaths, paths);
  }

  @Test
  public void dfsForwardOnlyRevRelAllowed() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:rel:0000000000000000000000000000000000000010");
    Visit visit = new Visit(graph, swhId, "rel:rev,rev:rev", "dfs", "forward");
    ArrayList<SwhPath> paths = visit.getPaths();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:rel:0000000000000000000000000000000000000010",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:rev:0000000000000000000000000000000000000003"
        ));

    assertEqualSwhPaths(expectedPaths, paths);
  }

  @Test
  public void dfsForwardOnlyRevDirCntAllowed() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:rev:0000000000000000000000000000000000000013");
    Visit visit = new Visit(graph, swhId, "rev:rev,rev:dir,dir:dir,dir:cnt", "dfs", "forward");
    ArrayList<SwhPath> paths = visit.getPaths();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:rev:0000000000000000000000000000000000000013",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:dir:0000000000000000000000000000000000000006",
          "swh:1:cnt:0000000000000000000000000000000000000005"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:rev:0000000000000000000000000000000000000013",
          "swh:1:dir:0000000000000000000000000000000000000012",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:dir:0000000000000000000000000000000000000006",
          "swh:1:cnt:0000000000000000000000000000000000000005"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:rev:0000000000000000000000000000000000000013",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:dir:0000000000000000000000000000000000000006",
          "swh:1:cnt:0000000000000000000000000000000000000004"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:rev:0000000000000000000000000000000000000013",
          "swh:1:dir:0000000000000000000000000000000000000012",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:dir:0000000000000000000000000000000000000006",
          "swh:1:cnt:0000000000000000000000000000000000000004"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:rev:0000000000000000000000000000000000000013",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:cnt:0000000000000000000000000000000000000007"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:rev:0000000000000000000000000000000000000013",
          "swh:1:dir:0000000000000000000000000000000000000012",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:cnt:0000000000000000000000000000000000000007"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:rev:0000000000000000000000000000000000000013",
          "swh:1:dir:0000000000000000000000000000000000000012",
          "swh:1:cnt:0000000000000000000000000000000000000011"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:rev:0000000000000000000000000000000000000013",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:rev:0000000000000000000000000000000000000003",
          "swh:1:dir:0000000000000000000000000000000000000002",
          "swh:1:cnt:0000000000000000000000000000000000000001"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:rev:0000000000000000000000000000000000000013",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:cnt:0000000000000000000000000000000000000001"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:rev:0000000000000000000000000000000000000013",
          "swh:1:dir:0000000000000000000000000000000000000012",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:cnt:0000000000000000000000000000000000000001"
        ));

    assertEqualSwhPaths(expectedPaths, paths);
  }

  @Test
  public void dfsForwardNoEdgesAllowed() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Visit visit = new Visit(graph, swhId, "", "dfs", "forward");
    ArrayList<SwhPath> paths = visit.getPaths();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020"
        ));

    assertEqualSwhPaths(expectedPaths, paths);
  }

  @Test
  public void dfsBackwardOnlyRevRelAllowed() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:rev:0000000000000000000000000000000000000003");
    Visit visit = new Visit(graph, swhId, "rev:rev,rev:rel", "dfs", "backward");
    ArrayList<SwhPath> paths = visit.getPaths();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:rev:0000000000000000000000000000000000000003",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:rev:0000000000000000000000000000000000000013",
          "swh:1:rev:0000000000000000000000000000000000000018",
          "swh:1:rel:0000000000000000000000000000000000000019"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:rev:0000000000000000000000000000000000000003",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:rel:0000000000000000000000000000000000000010"
        ));

    assertEqualSwhPaths(expectedPaths, paths);
  }

  @Test
  public void dfsBackwardAllowedEdgesNotOrderSensitive() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:cnt:0000000000000000000000000000000000000004");
    Visit visit1 = new Visit(graph, swhId, "cnt:dir", "dfs", "backward");
    Visit visit2 = new Visit(graph, swhId, "dir:cnt", "dfs", "backward");
    ArrayList<SwhPath> paths1 = visit1.getPaths();
    ArrayList<SwhPath> paths2 = visit2.getPaths();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:cnt:0000000000000000000000000000000000000004",
          "swh:1:dir:0000000000000000000000000000000000000006"
        ));

    assertEqualSwhPaths(expectedPaths, paths1);
    assertEqualSwhPaths(expectedPaths, paths2);
  }
}
