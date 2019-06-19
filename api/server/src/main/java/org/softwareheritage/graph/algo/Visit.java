package org.softwareheritage.graph.algo;

import java.util.ArrayList;
import java.util.Stack;

import it.unimi.dsi.big.webgraph.LazyLongIterator;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.SwhPath;

public class Visit {
  Graph graph;
  boolean useTransposed;
  String allowedEdges;
  Stack<Long> currentPath;
  ArrayList<SwhPath> paths;

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

    if (algorithm.equals("dfs")) {
      dfs(graph.getNodeId(swhId));
    }
  }

  // Allow Jackson JSON to only serialize the 'paths' field
  public ArrayList<SwhPath> getPaths() {
    return paths;
  }

  private void dfs(long currentNodeId) {
    currentPath.push(currentNodeId);

    long degree = graph.degree(currentNodeId, useTransposed);
    LazyLongIterator neighbors = graph.neighbors(currentNodeId, useTransposed);
    long visitedNeighbors = 0;

    while (degree-- > 0) {
      long neighborNodeId = neighbors.nextLong();
      if (isEdgeAllowed(currentNodeId, neighborNodeId)) {
        dfs(neighborNodeId);
        visitedNeighbors++;
      }
    }

    if (visitedNeighbors == 0) {
      SwhPath path = new SwhPath();
      for (long nodeId : currentPath) {
        path.add(graph.getSwhId(nodeId));
      }
      paths.add(path);
    }

    currentPath.pop();
  }

  private boolean isEdgeAllowed(long currentNodeId, long neighborNodeId) {
    if (allowedEdges.equals("all")) {
      return true;
    }

    String currentType = graph.getSwhId(currentNodeId).getType();
    String neighborType = graph.getSwhId(neighborNodeId).getType();
    String edgeType = currentType + ":" + neighborType;
    String edgeTypeRev = neighborType + ":" + currentType;
    return allowedEdges.contains(edgeType) || allowedEdges.contains(edgeTypeRev);
  }
}
