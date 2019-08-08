package org.softwareheritage.graph;

import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;

import org.junit.Test;

import org.softwareheritage.graph.Endpoint;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.GraphTest;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.SwhPath;

// Avoid warnings concerning Endpoint.Output.result manual cast
@SuppressWarnings("unchecked")
public class VisitTest extends GraphTest {
  private void assertSameNodesFromPaths(ArrayList<SwhPath> paths, ArrayList<SwhId> nodes) {
    Set<SwhId> expectedNodes = new HashSet<SwhId>();
    for (SwhPath path : paths) {
      for (SwhId node : path.getPath()) {
        expectedNodes.add(node);
      }
    }
    GraphTest.assertEqualsAnyOrder(expectedNodes, nodes);
  }

  @Test
  public void forwardFromRoot() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:ori:0000000000000000000000000000000000000021");
    Endpoint endpoint = new Endpoint(graph, "forward", "*");
    ArrayList<SwhPath> paths = (ArrayList) endpoint.visitPaths(swhId).result;
    ArrayList<SwhId> nodes = (ArrayList) endpoint.visitNodes(swhId).result;

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:ori:0000000000000000000000000000000000000021",
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:cnt:0000000000000000000000000000000000000007"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:ori:0000000000000000000000000000000000000021",
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:cnt:0000000000000000000000000000000000000001"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:ori:0000000000000000000000000000000000000021",
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:dir:0000000000000000000000000000000000000006",
          "swh:1:cnt:0000000000000000000000000000000000000004"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:ori:0000000000000000000000000000000000000021",
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:dir:0000000000000000000000000000000000000006",
          "swh:1:cnt:0000000000000000000000000000000000000005"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:ori:0000000000000000000000000000000000000021",
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:rev:0000000000000000000000000000000000000003",
          "swh:1:dir:0000000000000000000000000000000000000002",
          "swh:1:cnt:0000000000000000000000000000000000000001"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:ori:0000000000000000000000000000000000000021",
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rel:0000000000000000000000000000000000000010",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:cnt:0000000000000000000000000000000000000007"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:ori:0000000000000000000000000000000000000021",
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rel:0000000000000000000000000000000000000010",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:cnt:0000000000000000000000000000000000000001"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:ori:0000000000000000000000000000000000000021",
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rel:0000000000000000000000000000000000000010",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:dir:0000000000000000000000000000000000000006",
          "swh:1:cnt:0000000000000000000000000000000000000004"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:ori:0000000000000000000000000000000000000021",
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rel:0000000000000000000000000000000000000010",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:dir:0000000000000000000000000000000000000006",
          "swh:1:cnt:0000000000000000000000000000000000000005"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:ori:0000000000000000000000000000000000000021",
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rel:0000000000000000000000000000000000000010",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:rev:0000000000000000000000000000000000000003",
          "swh:1:dir:0000000000000000000000000000000000000002",
          "swh:1:cnt:0000000000000000000000000000000000000001"
        ));

    GraphTest.assertEqualsAnyOrder(expectedPaths, paths);
    assertSameNodesFromPaths(expectedPaths, nodes);
  }

  @Test
  public void forwardFromMiddle() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:dir:0000000000000000000000000000000000000012");
    Endpoint endpoint = new Endpoint(graph, "forward", "*");
    ArrayList<SwhPath> paths = (ArrayList) endpoint.visitPaths(swhId).result;
    ArrayList<SwhId> nodes = (ArrayList) endpoint.visitNodes(swhId).result;

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

    GraphTest.assertEqualsAnyOrder(expectedPaths, paths);
    assertSameNodesFromPaths(expectedPaths, nodes);
  }

  @Test
  public void forwardFromLeaf() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:cnt:0000000000000000000000000000000000000004");
    Endpoint endpoint = new Endpoint(graph, "forward", "*");
    ArrayList<SwhPath> paths = (ArrayList) endpoint.visitPaths(swhId).result;
    ArrayList<SwhId> nodes = (ArrayList) endpoint.visitNodes(swhId).result;

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:cnt:0000000000000000000000000000000000000004"
        ));

    GraphTest.assertEqualsAnyOrder(expectedPaths, paths);
    assertSameNodesFromPaths(expectedPaths, nodes);
  }

  @Test
  public void backwardFromRoot() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:ori:0000000000000000000000000000000000000021");
    Endpoint endpoint = new Endpoint(graph, "backward", "*");
    ArrayList<SwhPath> paths = (ArrayList) endpoint.visitPaths(swhId).result;
    ArrayList<SwhId> nodes = (ArrayList) endpoint.visitNodes(swhId).result;

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:ori:0000000000000000000000000000000000000021"
        ));

    GraphTest.assertEqualsAnyOrder(expectedPaths, paths);
    assertSameNodesFromPaths(expectedPaths, nodes);
  }

  @Test
  public void backwardFromMiddle() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:dir:0000000000000000000000000000000000000012");
    Endpoint endpoint = new Endpoint(graph, "backward", "*");
    ArrayList<SwhPath> paths = (ArrayList) endpoint.visitPaths(swhId).result;
    ArrayList<SwhId> nodes = (ArrayList) endpoint.visitNodes(swhId).result;

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:dir:0000000000000000000000000000000000000012",
          "swh:1:rev:0000000000000000000000000000000000000013",
          "swh:1:rev:0000000000000000000000000000000000000018",
          "swh:1:rel:0000000000000000000000000000000000000019"
        ));

    GraphTest.assertEqualsAnyOrder(expectedPaths, paths);
    assertSameNodesFromPaths(expectedPaths, nodes);
  }

  @Test
  public void backwardFromLeaf() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:cnt:0000000000000000000000000000000000000004");
    Endpoint endpoint = new Endpoint(graph, "backward", "*");
    ArrayList<SwhPath> paths = (ArrayList) endpoint.visitPaths(swhId).result;
    ArrayList<SwhId> nodes = (ArrayList) endpoint.visitNodes(swhId).result;

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
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:ori:0000000000000000000000000000000000000021"
        ));
    expectedPaths.add(
        new SwhPath(
          "swh:1:cnt:0000000000000000000000000000000000000004",
          "swh:1:dir:0000000000000000000000000000000000000006",
          "swh:1:dir:0000000000000000000000000000000000000008",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:rel:0000000000000000000000000000000000000010",
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:ori:0000000000000000000000000000000000000021"
        ));

    GraphTest.assertEqualsAnyOrder(expectedPaths, paths);
    assertSameNodesFromPaths(expectedPaths, nodes);
  }

  @Test
  public void forwardSnpToRev() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Endpoint endpoint = new Endpoint(graph, "forward", "snp:rev");
    ArrayList<SwhPath> paths = (ArrayList) endpoint.visitPaths(swhId).result;
    ArrayList<SwhId> nodes = (ArrayList) endpoint.visitNodes(swhId).result;

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020",
          "swh:1:rev:0000000000000000000000000000000000000009"
        ));

    GraphTest.assertEqualsAnyOrder(expectedPaths, paths);
    assertSameNodesFromPaths(expectedPaths, nodes);
  }

  @Test
  public void forwardRelToRevRevToRev() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:rel:0000000000000000000000000000000000000010");
    Endpoint endpoint = new Endpoint(graph, "forward", "rel:rev,rev:rev");
    ArrayList<SwhPath> paths = (ArrayList) endpoint.visitPaths(swhId).result;
    ArrayList<SwhId> nodes = (ArrayList) endpoint.visitNodes(swhId).result;

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:rel:0000000000000000000000000000000000000010",
          "swh:1:rev:0000000000000000000000000000000000000009",
          "swh:1:rev:0000000000000000000000000000000000000003"
        ));

    GraphTest.assertEqualsAnyOrder(expectedPaths, paths);
    assertSameNodesFromPaths(expectedPaths, nodes);
  }

  @Test
  public void forwardRevToAllDirToAll() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:rev:0000000000000000000000000000000000000013");
    Endpoint endpoint = new Endpoint(graph, "forward", "rev:*,dir:*");
    ArrayList<SwhPath> paths = (ArrayList) endpoint.visitPaths(swhId).result;
    ArrayList<SwhId> nodes = (ArrayList) endpoint.visitNodes(swhId).result;

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

    GraphTest.assertEqualsAnyOrder(expectedPaths, paths);
    assertSameNodesFromPaths(expectedPaths, nodes);
  }

  @Test
  public void forwardSnpToAllRevToAll() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Endpoint endpoint = new Endpoint(graph, "forward", "snp:*,rev:*");
    ArrayList<SwhPath> paths = (ArrayList) endpoint.visitPaths(swhId).result;
    ArrayList<SwhId> nodes = (ArrayList) endpoint.visitNodes(swhId).result;

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

    GraphTest.assertEqualsAnyOrder(expectedPaths, paths);
    assertSameNodesFromPaths(expectedPaths, nodes);
  }

  @Test
  public void forwardNoEdges() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:snp:0000000000000000000000000000000000000020");
    Endpoint endpoint = new Endpoint(graph, "forward", "");
    ArrayList<SwhPath> paths = (ArrayList) endpoint.visitPaths(swhId).result;
    ArrayList<SwhId> nodes = (ArrayList) endpoint.visitNodes(swhId).result;

    ArrayList<SwhPath> expectedPaths = new ArrayList<SwhPath>();
    expectedPaths.add(
        new SwhPath(
          "swh:1:snp:0000000000000000000000000000000000000020"
        ));

    GraphTest.assertEqualsAnyOrder(expectedPaths, paths);
    assertSameNodesFromPaths(expectedPaths, nodes);
  }

  @Test
  public void backwardRevToRevRevToRel() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:rev:0000000000000000000000000000000000000003");
    Endpoint endpoint = new Endpoint(graph, "backward", "rev:rev,rev:rel");
    ArrayList<SwhPath> paths = (ArrayList) endpoint.visitPaths(swhId).result;
    ArrayList<SwhId> nodes = (ArrayList) endpoint.visitNodes(swhId).result;

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

    GraphTest.assertEqualsAnyOrder(expectedPaths, paths);
    assertSameNodesFromPaths(expectedPaths, nodes);
  }

  @Test
  public void forwardFromRootNodesOnly() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:ori:0000000000000000000000000000000000000021");
    Endpoint endpoint = new Endpoint(graph, "forward", "*");
    ArrayList<SwhId> nodes = (ArrayList) endpoint.visitNodes(swhId).result;

    ArrayList<SwhId> expectedNodes = new ArrayList<SwhId>();
    expectedNodes.add(new SwhId("swh:1:ori:0000000000000000000000000000000000000021"));
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

    GraphTest.assertEqualsAnyOrder(expectedNodes, nodes);
  }

  @Test
  public void backwardRevToAllNodesOnly() {
    Graph graph = getGraph();
    SwhId swhId = new SwhId("swh:1:rev:0000000000000000000000000000000000000003");
    Endpoint endpoint = new Endpoint(graph, "backward", "rev:*");
    ArrayList<SwhId> nodes = (ArrayList) endpoint.visitNodes(swhId).result;

    ArrayList<SwhId> expectedNodes = new ArrayList<SwhId>();
    expectedNodes.add(new SwhId("swh:1:rev:0000000000000000000000000000000000000003"));
    expectedNodes.add(new SwhId("swh:1:rev:0000000000000000000000000000000000000009"));
    expectedNodes.add(new SwhId("swh:1:snp:0000000000000000000000000000000000000020"));
    expectedNodes.add(new SwhId("swh:1:rel:0000000000000000000000000000000000000010"));
    expectedNodes.add(new SwhId("swh:1:rev:0000000000000000000000000000000000000013"));
    expectedNodes.add(new SwhId("swh:1:rev:0000000000000000000000000000000000000018"));
    expectedNodes.add(new SwhId("swh:1:rel:0000000000000000000000000000000000000019"));

    GraphTest.assertEqualsAnyOrder(expectedNodes, nodes);
  }
}
