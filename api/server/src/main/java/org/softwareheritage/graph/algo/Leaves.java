package org.softwareheritage.graph.algo;

import java.util.ArrayList;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.bits.LongArrayBitVector;

import org.softwareheritage.graph.AllowedEdges;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.SwhId;

public class Leaves {
  Graph graph;
  boolean useTransposed;
  AllowedEdges edges;

  ArrayList<SwhId> leaves;
  LongArrayBitVector visited;

  public Leaves(Graph graph, SwhId src, String edgesFmt, String direction) {
    if (!direction.matches("forward|backward")) {
      throw new IllegalArgumentException("Unknown traversal direction: " + direction);
    }

    this.graph = graph;
    this.useTransposed = (direction.equals("backward"));
    this.edges = new AllowedEdges(edgesFmt);
    this.leaves = new ArrayList<SwhId>();
    this.visited = LongArrayBitVector.ofLength(graph.getNbNodes());

    long nodeId = graph.getNodeId(src);
    dfs(nodeId);
  }

  public ArrayList<SwhId> getLeaves() {
    return leaves;
  }

  private void dfs(long currentNodeId) {
    visited.set(currentNodeId);
    SwhId currentSwhId = graph.getSwhId(currentNodeId);

    long degree = graph.degree(currentNodeId, useTransposed);
    LazyLongIterator neighbors = graph.neighbors(currentNodeId, useTransposed);
    long neighborsCnt = 0;

    while (degree-- > 0) {
      long neighborNodeId = neighbors.nextLong();
      Node.Type currentNodeType = currentSwhId.getType();
      Node.Type neighborNodeType = graph.getSwhId(neighborNodeId).getType();
      if (edges.isAllowed(currentNodeType, neighborNodeType)) {
        neighborsCnt++;
        if (!visited.getBoolean(neighborNodeId)) {
          dfs(neighborNodeId);
        }
      }
    }

    if (neighborsCnt == 0) {
      leaves.add(currentSwhId);
    }
  }
}
