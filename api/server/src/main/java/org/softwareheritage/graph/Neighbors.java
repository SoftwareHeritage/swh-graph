package org.softwareheritage.graph;

import java.util.Iterator;

import it.unimi.dsi.fastutil.longs.LongBigArrays;

import org.softwareheritage.graph.AllowedEdges;
import org.softwareheritage.graph.Graph;

public class Neighbors implements Iterable<Long> {
  Graph graph;
  boolean useTransposed;
  AllowedEdges edges;
  long srcNodeId;

  public Neighbors(Graph graph, boolean useTransposed, AllowedEdges edges, long srcNodeId) {
    this.graph = graph;
    this.useTransposed = useTransposed;
    this.edges = edges;
    this.srcNodeId = srcNodeId;
  }

  @Override
  public Iterator<Long> iterator() {
    return new NeighborsIterator();
  }

  public class NeighborsIterator implements Iterator<Long> {
    long nextNeighborIdx;
    long nbNeighbors;
    long[][] neighbors;

    public NeighborsIterator() {
      this.nextNeighborIdx = -1;
      this.nbNeighbors = graph.degree(srcNodeId, useTransposed);
      this.neighbors = graph.neighbors(srcNodeId, useTransposed);
    }

    // Look ahead because with edge restriction not all neighbors are considered
    public boolean hasNext() {
      for (long lookAheadIdx = nextNeighborIdx + 1; lookAheadIdx < nbNeighbors; lookAheadIdx++) {
        long nextNodeId = LongBigArrays.get(neighbors, lookAheadIdx);
        if (edges.isAllowed(srcNodeId, nextNodeId)) {
          nextNeighborIdx = lookAheadIdx;
          return true;
        }
      }
      return false;
    }

    public Long next() {
      long nextNodeId = LongBigArrays.get(neighbors, nextNeighborIdx);
      return nextNodeId;
    }
  }
}
