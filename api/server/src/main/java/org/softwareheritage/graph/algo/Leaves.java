package org.softwareheritage.graph.algo;

import java.util.ArrayList;

import it.unimi.dsi.bits.LongArrayBitVector;

import org.softwareheritage.graph.AllowedEdges;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Neighbors;
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

    long neighborsCnt = 0;
    for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, currentSwhId)) {
      neighborsCnt++;
      if (!visited.getBoolean(neighborNodeId)) {
        dfs(neighborNodeId);
      }
    }

    if (neighborsCnt == 0) {
      leaves.add(currentSwhId);
    }
  }
}
