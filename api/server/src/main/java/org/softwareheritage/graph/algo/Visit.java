package org.softwareheritage.graph.algo;

import java.util.ArrayList;
import java.util.Stack;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.bits.LongArrayBitVector;

import org.softwareheritage.graph.Graph;

public class Visit {
  public class Path extends ArrayList<String> {}

  Graph graph;
  String allowedEdges;
  Stack<Long> currentPath;
  ArrayList<Path> paths;
  LongArrayBitVector visited;

  public Visit(Graph graph, String start, String allowedEdges, String algorithm) {
    this.graph = graph;
    this.allowedEdges = allowedEdges;
    this.paths = new ArrayList<Path>();
    this.currentPath = new Stack<Long>();
    this.visited = LongArrayBitVector.ofLength(graph.getNbNodes());

    if (algorithm == "dfs") {
      dfs(graph.getNode(start));
    }
  }

  // Allow Jackson JSON to only serialize the 'paths' field
  public ArrayList<Path> getPaths() {
    return paths;
  }

  private void dfs(long current) {
    visited.set(current);
    currentPath.push(current);

    long degree = graph.outdegree(current);
    if (degree == 0) {
      Path path = new Path();
      for (long node : currentPath) {
        path.add(graph.getHash(node));
      }
      paths.add(path);
    }

    LazyLongIterator successors = graph.successors(current);
    while (degree-- > 0) {
      long next = successors.nextLong();
      if (isEdgeAllowed(current, next) && !visited.getBoolean(next)) {
        dfs(next);
      }
    }

    currentPath.pop();
  }

  private boolean isEdgeAllowed(long current, long next) {
    // TODO
    return true;
  }
}
