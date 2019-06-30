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

  Visit.OutputFmt outputFmt = Visit.OutputFmt.NODES_AND_PATHS;

  @Test
  public void dfsForwardFromRoot() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Visit visit = new Visit(graph, swhId, "*", "forward", outputFmt);
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
    Visit visit = new Visit(graph, swhId, "*", "forward", outputFmt);
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
    Visit visit = new Visit(graph, swhId, "*", "forward", outputFmt);
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
    Visit visit = new Visit(graph, swhId, "*", "backward", outputFmt);
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
    Visit visit = new Visit(graph, swhId, "*", "backward", outputFmt);
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
    Visit visit = new Visit(graph, swhId, "*", "backward", outputFmt);
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
  public void dfsForwardSnpToRev() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Visit visit = new Visit(graph, swhId, "snp:rev", "forward", outputFmt);
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
  public void dfsForwardRelToRevRevToRev() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:rel:0000000000000000000000000000000000000010");
    Visit visit = new Visit(graph, swhId, "rel:rev,rev:rev", "forward", outputFmt);
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
  public void dfsForwardRevToAllDirToAll() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:rev:0000000000000000000000000000000000000013");
    Visit visit = new Visit(graph, swhId, "rev:*,dir:*", "forward", outputFmt);
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
  public void dfsForwardSnpToAllRevToAll() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Visit visit = new Visit(graph, swhId, "snp:*,rev:*", "forward", outputFmt);
    ArrayList<SwhPath> paths = visit.getPaths();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:rev:0000000000000000000000000000000000000003",
          "swh:1:dir:0000000000000000000000000000000000000002"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rel:0000000000000000000000000000000000000010"
        ));

    assertEqualSwhPaths(expectedPaths, paths);
  }

  @Test
  public void dfsForwardNoEdges() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Visit visit = new Visit(graph, swhId, "", "forward", outputFmt);
    ArrayList<SwhPath> paths = visit.getPaths();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020"
        ));

    assertEqualSwhPaths(expectedPaths, paths);
  }

  @Test
  public void dfsBackwardRevToRevRevToRel() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:rev:0000000000000000000000000000000000000003");
    Visit visit = new Visit(graph, swhId, "rev:rev,rev:rel", "backward", outputFmt);
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
}
