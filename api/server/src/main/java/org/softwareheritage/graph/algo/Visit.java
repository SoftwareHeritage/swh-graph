package org.softwareheritage.graph.algo;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Stack;

import it.unimi.dsi.bits.LongArrayBitVector;

import org.softwareheritage.graph.AllowedEdges;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Neighbors;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.SwhPath;

public class Visit {
  public enum OutputFmt { ONLY_NODES, ONLY_PATHS, NODES_AND_PATHS }

  Graph graph;
  boolean useTransposed;
  AllowedEdges edges;

  // LinkedHashSet is necessary to preserve insertion order
  LinkedHashSet<SwhId> nodes;
  ArrayList<SwhPath> paths;
  Stack<Long> currentPath;
  LongArrayBitVector visited;

  public Visit(Graph graph, SwhId src, String edgesFmt, String direction, OutputFmt output) {
    if (!direction.matches("forward|backward")) {
      throw new IllegalArgumentException("Unknown traversal direction: " + direction);
    }

    this.graph = graph;
    this.useTransposed = (direction.equals("backward"));
    this.edges = new AllowedEdges(edgesFmt);
    this.nodes = new LinkedHashSet<SwhId>();
    this.paths = new ArrayList<SwhPath>();
    this.currentPath = new Stack<Long>();
    this.visited = LongArrayBitVector.ofLength(graph.getNbNodes());

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
    SwhId currentSwhId = graph.getSwhId(currentNodeId);
    nodes.add(currentSwhId);
    currentPath.push(currentNodeId);

    long visitedNeighbors = 0;
    for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, currentSwhId)) {
        dfs(neighborNodeId);
        visitedNeighbors++;
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
    SwhId currentSwhId = graph.getSwhId(currentNodeId);
    nodes.add(currentSwhId);
    visited.set(currentNodeId);

    for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, currentSwhId)) {
      if (!visited.getBoolean(neighborNodeId)) {
        dfsOutputOnlyNodes(neighborNodeId);
      }
    }
  }
}
