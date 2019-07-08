package org.softwareheritage.graph.algo;

import java.util.ArrayList;

import it.unimi.dsi.big.webgraph.LazyLongIterator;

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
    long nodeId = graph.getNodeId(swhId);
    long degree = graph.degree(nodeId, useTransposed);
    LazyLongIterator neighborsNodeIds = graph.neighbors(nodeId, useTransposed);

    while (degree-- > 0) {
      long neighborNodeId = neighborsNodeIds.nextLong();
      SwhId neighborSwhId = graph.getSwhId(neighborNodeId);
      if (edges.isAllowed(swhId.getType(), neighborSwhId.getType())) {
        neighbors.add(neighborSwhId);
      }
    }
  }
}

