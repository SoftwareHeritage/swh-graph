package org.softwareheritage.graph;

import java.util.ArrayList;

import org.junit.Test;
import static org.junit.Assert.assertTrue;

import org.softwareheritage.graph.Edges;
import org.softwareheritage.graph.Node;

public class EdgesTest {
  class EdgeType {
    Node.Type src;
    Node.Type dst;

    public EdgeType(Node.Type src, Node.Type dst) {
      this.src = src;
      this.dst = dst;
    }

    @Override
    public boolean equals(Object otherObj) {
      if (otherObj == this) return true;
      if (!(otherObj instanceof EdgeType)) return false;

      EdgeType other = (EdgeType) otherObj;
      return src == other.src && dst == other.dst;
    }
  }

  void assertEdgeRestriction(Edges edges, ArrayList<EdgeType> expectedAllowed) {
    Node.Type[] nodeTypes = Node.Type.values();
    for (Node.Type src : nodeTypes) {
      for (Node.Type dst : nodeTypes) {
        EdgeType edge = new EdgeType(src, dst);
        boolean isAllowed = edges.isAllowed(src, dst);
        boolean isExpected = false;
        for (EdgeType expected : expectedAllowed) {
          if (expected.equals(edge)) {
            isExpected = true;
            break;
          }
        }

        assertTrue("Edge type: " + src + " -> " + dst, isAllowed == isExpected);
      }
    }
  }

  @Test
  public void dirToDirDirToCntEdges() {
    Edges edges = new Edges("dir:dir,dir:cnt");
    ArrayList<EdgeType> expected = new ArrayList<>();
    expected.add(new EdgeType(Node.Type.DIR, Node.Type.DIR));
    expected.add(new EdgeType(Node.Type.DIR, Node.Type.CNT));
    assertEdgeRestriction(edges, expected);
  }

  @Test
  public void relToRevRevToRevRevToDirEdges() {
    Edges edges = new Edges("rel:rev,rev:rev,rev:dir");
    ArrayList<EdgeType> expected = new ArrayList<>();
    expected.add(new EdgeType(Node.Type.REL, Node.Type.REV));
    expected.add(new EdgeType(Node.Type.REV, Node.Type.REV));
    expected.add(new EdgeType(Node.Type.REV, Node.Type.DIR));
    assertEdgeRestriction(edges, expected);
  }

  @Test
  public void revToAllDirToDirEdges() {
    Edges edges = new Edges("rev:*,dir:dir");
    ArrayList<EdgeType> expected = new ArrayList<>();
    for (Node.Type dst : Node.Type.values()) {
      expected.add(new EdgeType(Node.Type.REV, dst));
    }
    expected.add(new EdgeType(Node.Type.DIR, Node.Type.DIR));
    assertEdgeRestriction(edges, expected);
  }

  @Test
  public void allToCntEdges() {
    Edges edges = new Edges("*:cnt");
    ArrayList<EdgeType> expected = new ArrayList<>();
    for (Node.Type src : Node.Type.values()) {
      expected.add(new EdgeType(src, Node.Type.CNT));
    }
    assertEdgeRestriction(edges, expected);
  }

  @Test
  public void allEdges() {
    Edges edges = new Edges("*:*");
    Edges edges2 = new Edges("*");
    ArrayList<EdgeType> expected = new ArrayList<>();
    for (Node.Type src : Node.Type.values()) {
      for (Node.Type dst : Node.Type.values()) {
        expected.add(new EdgeType(src, dst));
      }
    }
    assertEdgeRestriction(edges, expected);
    assertEdgeRestriction(edges2, expected);
  }

  @Test
  public void noEdges() {
    Edges edges = new Edges("");
    Edges edges2 = new Edges(null);
    ArrayList<EdgeType> expected = new ArrayList<>();
    assertEdgeRestriction(edges, expected);
    assertEdgeRestriction(edges2, expected);
  }
}
