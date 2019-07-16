package org.softwareheritage.graph;

import java.util.ArrayList;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Node;

/**
 * Edge restriction based on node types, used when visiting the graph.
 *
 * @author Thibault Allançon
 * @version 1.0
 * @since 1.0
 */

public class AllowedEdges {
  /** Graph on which edge restriction is performed */
  Graph graph;
  /**
   * 2D boolean matrix storing access rights for all combination of src/dst node types (first
   * dimension is source, second dimension is destination)
   */
  boolean[][] allowed;

  /**
   * Constructor.
   *
   * @param graph the graph on which to perform edge restriction
   * @param edgesFmt a formatted string describing allowed edges (TODO: link API doc)
   */
  public AllowedEdges(Graph graph, String edgesFmt) {
    this.graph = graph;

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
    // TODO: link API doc
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

  /**
   * Checks if a given edge can be followed during graph traversal.
   *
   * @param srcNodeId edge source node
   * @param dstNodeId edge destination node
   * @return true if allowed and false otherwise
   */
  public boolean isAllowed(long srcNodeId, long dstNodeId) {
    Node.Type srcType = graph.getNodeType(srcNodeId);
    Node.Type dstType = graph.getNodeType(dstNodeId);
    return isAllowed(srcType, dstType);
  }

  /**
   * Works like {@link AllowedEdges#isAllowed(long, long)} but with node types directly instead of
   * node ids.
   *
   * @see AllowedEdges#isAllowed(long, long)
   */
  public boolean isAllowed(Node.Type srcType, Node.Type dstType) {
    return allowed[srcType.ordinal()][dstType.ordinal()];
  }
}
