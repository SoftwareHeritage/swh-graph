package org.softwareheritage.graph;

import java.util.Iterator;

import it.unimi.dsi.fastutil.longs.LongBigArrays;

import org.softwareheritage.graph.AllowedEdges;
import org.softwareheritage.graph.Graph;

/**
 * Iterator class to go over a node neighbors in the graph.
 * <p>
 * Wrapper iterator class to easily deal with {@link AllowedEdges} during traversals.
 *
 * @author Thibault Allançon
 * @version 0.0.1
 * @since 0.0.1
 * @see org.softwareheritage.graph.AllowedEdges
 */

public class Neighbors implements Iterable<Long> {
  /** Graph used to explore neighbors */
  Graph graph;
  /** Boolean to specify the use of the transposed graph */
  boolean useTransposed;
  /** Graph edge restriction */
  AllowedEdges edges;
  /** Source node from which neighbors will be listed */
  long srcNodeId;

  /**
   * Constructor.
   *
   * @param graph graph used to explore neighbors
   * @param useTransposed boolean value to use transposed graph
   * @param edges edges allowed to be used in the graph
   * @param srcNodeId source node from where to list neighbors
   */
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

  /**
  * Inner class for {@link Neighbors} iterator.
  *
  * @author Thibault Allançon
  * @version 0.0.1
  * @since 0.0.1
  */

  public class NeighborsIterator implements Iterator<Long> {
    long nextNeighborIdx;
    long nbNeighbors;
    // LongBigArrays to support 64-bit indexing.
    long[][] neighbors;

    public NeighborsIterator() {
      this.nextNeighborIdx = -1;
      this.nbNeighbors = graph.degree(srcNodeId, useTransposed);
      this.neighbors = graph.neighbors(srcNodeId, useTransposed);
    }

    public boolean hasNext() {
      // Case 1: no edge restriction, bypass type checks and skip to next neighbor
      if (edges.restrictedTo == null) {
        if (nextNeighborIdx + 1 < nbNeighbors) {
          nextNeighborIdx++;
          return true;
        } else {
          return false;
        }
      }

      // Case 2: edge restriction, look ahead for next neighbor
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
