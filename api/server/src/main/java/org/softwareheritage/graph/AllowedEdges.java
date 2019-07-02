package org.softwareheritage.graph;

import java.util.ArrayList;

import org.softwareheritage.graph.Node;

public class AllowedEdges {
  // First dimension is source node type, second dimension is destination node type
  boolean[][] allowed;

  public AllowedEdges(String edgesFmt) {
    int nbNodeTypes = Node.Type.values().length;
    this.allowed = new boolean[nbNodeTypes][nbNodeTypes];
    // Special values (null, empty, "*")
    if (edgesFmt == null || edgesFmt.isEmpty()) {
      return;
    }
    if (edgesFmt.equals("*")) {
      for (int i = 0; i < nbNodeTypes; i++) {
        for (int j = 0; j < nbNodeTypes; j++) {
          allowed[i][j] = true;
        }
      }
      return;
    }

    // Format: "src1:dst1,src2:dst2,[...]"
    String[] edgeTypes = edgesFmt.split(",");
    for (String edgeType : edgeTypes) {
      String[] nodeTypes = edgeType.split(":");
      if (nodeTypes.length != 2) {
        throw new IllegalArgumentException("Cannot parse edge type: " + edgeType);
      }

      ArrayList<Node.Type> srcTypes = Node.Type.parse(nodeTypes[0]);
      ArrayList<Node.Type> dstTypes = Node.Type.parse(nodeTypes[1]);
      for (Node.Type srcType : srcTypes) {
        for (Node.Type dstType : dstTypes) {
          allowed[srcType.ordinal()][dstType.ordinal()] = true;
        }
      }
    }
  }

  public boolean isAllowed(Node.Type currentType, Node.Type neighborType) {
    return allowed[currentType.ordinal()][neighborType.ordinal()];
  }
}
