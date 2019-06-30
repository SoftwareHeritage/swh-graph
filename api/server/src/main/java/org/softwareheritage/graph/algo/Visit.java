package org.softwareheritage.graph.algo;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Stack;

import it.unimi.dsi.big.webgraph.LazyLongIterator;

import org.softwareheritage.graph.Edges;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.SwhPath;

public class Visit {
  public enum OutputFmt { ONLY_NODES, ONLY_PATHS, NODES_AND_PATHS }

  Graph graph;
  boolean useTransposed;
  Edges edges;
  // LinkedHashSet is necessary to preserve insertion order
  LinkedHashSet<SwhId> nodes;
  ArrayList<SwhPath> paths;
  Stack<Long> currentPath;

  public Visit(Graph graph, SwhId src, String edges, String direction, OutputFmt output) {
    if (!direction.matches("forward|backward")) {
      throw new IllegalArgumentException("Unknown traversal direction: " + direction);
    }

    this.graph = graph;
    this.useTransposed = (direction.equals("backward"));
    this.edges = new Edges(edges);
    this.nodes = new LinkedHashSet<SwhId>();
    this.paths = new ArrayList<SwhPath>();
    this.currentPath = new Stack<Long>();

    long nodeId = graph.getNodeId(src);
    if (output == OutputFmt.ONLY_NODES) {
      dfsOutputOnlyNodes(nodeId);
    } else {
      dfs(nodeId);
    }
  }

  public LinkedHashSet<SwhId> getNodes() {
    return nodes;
  }

  public ArrayList<SwhPath> getPaths() {
    return paths;
  }

  private void dfs(long currentNodeId) {
    nodes.add(graph.getSwhId(currentNodeId));
    currentPath.push(currentNodeId);

    long degree = graph.degree(currentNodeId, useTransposed);
    LazyLongIterator neighbors = graph.neighbors(currentNodeId, useTransposed);
    long visitedNeighbors = 0;

    while (degree-- > 0) {
      long neighborNodeId = neighbors.nextLong();
      Node.Type currentNodeType = graph.getSwhId(currentNodeId).getType();
      Node.Type neighborNodeType = graph.getSwhId(neighborNodeId).getType();
      if (edges.isAllowed(currentNodeType, neighborNodeType)) {
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

  private void dfsOutputOnlyNodes(long currentNodeId) {
    nodes.add(graph.getSwhId(currentNodeId));
    long degree = graph.degree(currentNodeId, useTransposed);
    LazyLongIterator neighbors = graph.neighbors(currentNodeId, useTransposed);

    while (degree-- > 0) {
      long neighborNodeId = neighbors.nextLong();
      Node.Type currentNodeType = graph.getSwhId(currentNodeId).getType();
      Node.Type neighborNodeType = graph.getSwhId(neighborNodeId).getType();
      if (edges.isAllowed(currentNodeType, neighborNodeType)) {
        dfsOutputOnlyNodes(neighborNodeId);
      }
    }
  }
}
