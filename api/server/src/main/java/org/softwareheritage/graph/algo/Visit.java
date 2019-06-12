package org.softwareheritage.graph.algo;

import java.util.ArrayList;
import java.util.Stack;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.bits.LongArrayBitVector;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.SwhPath;

public class Visit {
  Graph graph;
  boolean useTransposed;
  String allowedEdges;
  Stack<Long> currentPath;
  ArrayList<SwhPath> paths;
  LongArrayBitVector visited;

  public Visit(Graph graph, SwhId swhId, String allowedEdges, String algorithm, String direction) {
    if (!algorithm.matches("dfs|bfs")) {
      throw new IllegalArgumentException("Unknown traversal algorithm: " + algorithm);
    }
    if (!direction.matches("forward|backward")) {
      throw new IllegalArgumentException("Unknown traversal direction: " + direction);
    }

    this.graph = graph;
    this.useTransposed = (direction.equals("backward"));
    this.allowedEdges = allowedEdges;
    this.paths = new ArrayList<SwhPath>();
    this.currentPath = new Stack<Long>();
    this.visited = LongArrayBitVector.ofLength(graph.getNbNodes());

    if (algorithm.equals("dfs")) {
      dfs(graph.getNodeId(swhId));
    }
  }

  // Allow Jackson JSON to only serialize the 'paths' field
  public ArrayList<SwhPath> getPaths() {
    return paths;
  }

  private void dfs(long currentNodeId) {
    visited.set(currentNodeId);
    currentPath.push(currentNodeId);

    long degree = graph.degree(currentNodeId, useTransposed);
    LazyLongIterator neighbors = graph.neighbors(currentNodeId, useTransposed);

    if (degree == 0) {
      SwhPath path = new SwhPath();
      for (long nodeId : currentPath) {
        path.add(graph.getSwhId(nodeId));
      }
      paths.add(path);
    }

    while (degree-- > 0) {
      long nextNodeId = neighbors.nextLong();
      if (isEdgeAllowed(currentNodeId, nextNodeId) && !visited.getBoolean(nextNodeId)) {
        dfs(nextNodeId);
      }
    }

    currentPath.pop();
  }

  private boolean isEdgeAllowed(long currentNodeId, long nextNodeId) {
    // TODO
    return true;
  }
}
