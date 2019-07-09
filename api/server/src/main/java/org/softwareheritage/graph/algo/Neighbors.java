package org.softwareheritage.graph.algo;

import java.util.ArrayList;

import org.softwareheritage.graph.AllowedEdges;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.SwhId;

public class Neighbors {
  Graph graph;
  boolean useTransposed;
  AllowedEdges edges;

  ArrayList<SwhId> neighbors;

  public Neighbors(Graph graph, SwhId src, String edgesFmt, String direction) {
    if (!direction.matches("forward|backward")) {
      throw new IllegalArgumentException("Unknown traversal direction: " + direction);
    }

    this.graph = graph;
    this.useTransposed = (direction.equals("backward"));
    this.edges = new AllowedEdges(edgesFmt);
    this.neighbors = new ArrayList<SwhId>();

    iterateNeighbors(src);
  }

  public ArrayList<SwhId> getNeighbors() {
    return neighbors;
  }

  private void iterateNeighbors(SwhId swhId) {
    // TEMPORARY FIX: Avoid import naming problem with Neighbors
    for (long neighborNodeId : new org.softwareheritage.graph.Neighbors(graph, useTransposed, edges, swhId)) {
      neighbors.add(graph.getSwhId(neighborNodeId));
    }
  }
}
