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
import org.softwareheritage.graph.algo.Visit;

public class VisitTest extends GraphTest {
  Visit.OutputFmt outputFmt = Visit.OutputFmt.NODES_AND_PATHS;

  private void assertEqualPaths(ArrayList<SwhPath> expecteds, ArrayList<SwhPath> actuals) {
    Assert.assertThat(expecteds, containsInAnyOrder(actuals.toArray()));
  }

  private void assertEqualNodes(LinkedHashSet<SwhId> expecteds, LinkedHashSet<SwhId> actuals) {
    Assert.assertThat(expecteds, containsInAnyOrder(actuals.toArray()));
  }

  private void assertSameNodesFromPaths(ArrayList<SwhPath> paths, LinkedHashSet<SwhId> nodes) {
    LinkedHashSet<SwhId> expectedNodes = new LinkedHashSet<SwhId>();
    for (SwhPath path : paths) {
      for (SwhId node : path.getPath()) {
        expectedNodes.add(node);
      }
    }
    assertEqualNodes(expectedNodes, nodes);
  }

  @Test
  public void forwardFromRoot() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Visit visit = new Visit(graph, swhId, "*", "forward", outputFmt);
    ArrayList<SwhPath> paths = visit.getPaths();
    LinkedHashSet<SwhId> nodes = visit.getNodes();

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

    assertEqualPaths(expectedPaths, paths);
    assertSameNodesFromPaths(expectedPaths, nodes);
  }

  @Test
  public void forwardFromMiddle() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:dir:0000000000000000000000000000000000000012");
    Visit visit = new Visit(graph, swhId, "*", "forward", outputFmt);
    ArrayList<SwhPath> paths = visit.getPaths();
    LinkedHashSet<SwhId> nodes = visit.getNodes();

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

    assertEqualPaths(expectedPaths, paths);
    assertSameNodesFromPaths(expectedPaths, nodes);
  }

  @Test
  public void forwardFromLeaf() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:cnt:0000000000000000000000000000000000000004");
    Visit visit = new Visit(graph, swhId, "*", "forward", outputFmt);
    ArrayList<SwhPath> paths = visit.getPaths();
    LinkedHashSet<SwhId> nodes = visit.getNodes();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:cnt:0000000000000000000000000000000000000004"
        ));

    assertEqualPaths(expectedPaths, paths);
    assertSameNodesFromPaths(expectedPaths, nodes);
  }

  @Test
  public void backwardFromRoot() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Visit visit = new Visit(graph, swhId, "*", "backward", outputFmt);
    ArrayList<SwhPath> paths = visit.getPaths();
    LinkedHashSet<SwhId> nodes = visit.getNodes();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020"
        ));

    assertEqualPaths(expectedPaths, paths);
    assertSameNodesFromPaths(expectedPaths, nodes);
  }

  @Test
  public void backwardFromMiddle() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:dir:0000000000000000000000000000000000000012");
    Visit visit = new Visit(graph, swhId, "*", "backward", outputFmt);
    ArrayList<SwhPath> paths = visit.getPaths();
    LinkedHashSet<SwhId> nodes = visit.getNodes();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:dir:0000000000000000000000000000000000000012",
          "swh:1:rev:0000000000000000000000000000000000000013",
          "swh:1:rev:0000000000000000000000000000000000000018",
          "swh:1:rel:0000000000000000000000000000000000000019"
        ));

    assertEqualPaths(expectedPaths, paths);
    assertSameNodesFromPaths(expectedPaths, nodes);
  }

  @Test
  public void backwardFromLeaf() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:cnt:0000000000000000000000000000000000000004");
    Visit visit = new Visit(graph, swhId, "*", "backward", outputFmt);
    ArrayList<SwhPath> paths = visit.getPaths();
    LinkedHashSet<SwhId> nodes = visit.getNodes();

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

    assertEqualPaths(expectedPaths, paths);
    assertSameNodesFromPaths(expectedPaths, nodes);
  }

  @Test
  public void forwardSnpToRev() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Visit visit = new Visit(graph, swhId, "snp:rev", "forward", outputFmt);
    ArrayList<SwhPath> paths = visit.getPaths();
    LinkedHashSet<SwhId> nodes = visit.getNodes();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rev:0000000000000000000000000000000000000009"
        ));

    assertEqualPaths(expectedPaths, paths);
    assertSameNodesFromPaths(expectedPaths, nodes);
  }

  @Test
  public void forwardRelToRevRevToRev() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:rel:0000000000000000000000000000000000000010");
    Visit visit = new Visit(graph, swhId, "rel:rev,rev:rev", "forward", outputFmt);
    ArrayList<SwhPath> paths = visit.getPaths();
    LinkedHashSet<SwhId> nodes = visit.getNodes();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:rel:0000000000000000000000000000000000000010",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:rev:0000000000000000000000000000000000000003"
        ));

    assertEqualPaths(expectedPaths, paths);
    assertSameNodesFromPaths(expectedPaths, nodes);
  }

  @Test
  public void forwardRevToAllDirToAll() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:rev:0000000000000000000000000000000000000013");
    Visit visit = new Visit(graph, swhId, "rev:*,dir:*", "forward", outputFmt);
    ArrayList<SwhPath> paths = visit.getPaths();
    LinkedHashSet<SwhId> nodes = visit.getNodes();

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

    assertEqualPaths(expectedPaths, paths);
    assertSameNodesFromPaths(expectedPaths, nodes);
  }

  @Test
  public void forwardSnpToAllRevToAll() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Visit visit = new Visit(graph, swhId, "snp:*,rev:*", "forward", outputFmt);
    ArrayList<SwhPath> paths = visit.getPaths();
    LinkedHashSet<SwhId> nodes = visit.getNodes();

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

    assertEqualPaths(expectedPaths, paths);
    assertSameNodesFromPaths(expectedPaths, nodes);
  }

  @Test
  public void forwardNoEdges() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Visit visit = new Visit(graph, swhId, "", "forward", outputFmt);
    ArrayList<SwhPath> paths = visit.getPaths();
    LinkedHashSet<SwhId> nodes = visit.getNodes();

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020"
        ));

    assertEqualPaths(expectedPaths, paths);
    assertSameNodesFromPaths(expectedPaths, nodes);
  }

  @Test
  public void backwardRevToRevRevToRel() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:rev:0000000000000000000000000000000000000003");
    Visit visit = new Visit(graph, swhId, "rev:rev,rev:rel", "backward", outputFmt);
    ArrayList<SwhPath> paths = visit.getPaths();
    LinkedHashSet<SwhId> nodes = visit.getNodes();

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

    assertEqualPaths(expectedPaths, paths);
    assertSameNodesFromPaths(expectedPaths, nodes);
  }

  @Test
  public void forwardFromRootNodesOnly() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Visit visit = new Visit(graph, swhId, "*", "forward", Visit.OutputFmt.ONLY_NODES);
    LinkedHashSet<SwhId> nodes = visit.getNodes();

    LinkedHashSet<SwhId> expectedNodes = new LinkedHashSet<SwhId>();
    expectedNodes.add(new SwhId("swh:1:snp:0000000000000000000000000000000000000020"));
    expectedNodes.add(new SwhId("swh:1:rel:0000000000000000000000000000000000000010"));
    expectedNodes.add(new SwhId("swh:1:rev:0000000000000000000000000000000000000009"));
    expectedNodes.add(new SwhId("swh:1:rev:0000000000000000000000000000000000000003"));
    expectedNodes.add(new SwhId("swh:1:dir:0000000000000000000000000000000000000002"));
    expectedNodes.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000001"));
    expectedNodes.add(new SwhId("swh:1:dir:0000000000000000000000000000000000000008"));
    expectedNodes.add(new SwhId("swh:1:dir:0000000000000000000000000000000000000006"));
    expectedNodes.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000004"));
    expectedNodes.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000005"));
    expectedNodes.add(new SwhId("swh:1:cnt:0000000000000000000000000000000000000007"));

    assertEqualNodes(expectedNodes, nodes);
  }

  @Test
  public void backwardRevToAllNodesOnly() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:rev:0000000000000000000000000000000000000003");
    Visit visit = new Visit(graph, swhId, "rev:*", "backward", Visit.OutputFmt.ONLY_NODES);
    LinkedHashSet<SwhId> nodes = visit.getNodes();

    LinkedHashSet<SwhId> expectedNodes = new LinkedHashSet<SwhId>();
    expectedNodes.add(new SwhId("swh:1:rev:0000000000000000000000000000000000000003"));
    expectedNodes.add(new SwhId("swh:1:rev:0000000000000000000000000000000000000009"));
    expectedNodes.add(new SwhId("swh:1:snp:0000000000000000000000000000000000000020"));
    expectedNodes.add(new SwhId("swh:1:rel:0000000000000000000000000000000000000010"));
    expectedNodes.add(new SwhId("swh:1:rev:0000000000000000000000000000000000000013"));
    expectedNodes.add(new SwhId("swh:1:rev:0000000000000000000000000000000000000018"));
    expectedNodes.add(new SwhId("swh:1:rel:0000000000000000000000000000000000000019"));

    assertEqualNodes(expectedNodes, nodes);
  }
}
