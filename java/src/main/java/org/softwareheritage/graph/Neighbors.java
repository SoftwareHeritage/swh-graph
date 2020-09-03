package org.softwareheritage.graph;

import java.util.Iterator;

import it.unimi.dsi.big.webgraph.LazyLongIterator;

import org.softwareheritage.graph.AllowedEdges;
import org.softwareheritage.graph.Graph;

/**
 * Iterator class to go over a node neighbors in the graph.
 * <p>
 * Wrapper iterator class to easily deal with {@link AllowedEdges} during traversals.
 *
 * @author The Software Heritage developers
 * @see org.softwareheritage.graph.AllowedEdges
 */

public class Neighbors implements Iterable<Long> {
    /** Graph used to explore neighbors */
    Graph graph;
    /** Graph edge restriction */
    AllowedEdges edges;
    /** Source node from which neighbors will be listed */
    long srcNodeId;

    /**
     * Constructor.
     *
     * @param graph graph used to explore neighbors
     * @param edges edges allowed to be used in the graph
     * @param srcNodeId source node from where to list neighbors
     */
    public Neighbors(Graph graph, AllowedEdges edges, long srcNodeId) {
        this.graph = graph;
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
     * @author The Software Heritage developers
     */

    public class NeighborsIterator implements Iterator<Long> {
        LazyLongIterator neighbors;
        long nextNeighborId;

        public NeighborsIterator() {
            this.neighbors = graph.successors(srcNodeId);
            this.nextNeighborId = -1;
        }

        public boolean hasNext() {
            // Case 1: no edge restriction, bypass type checks and skip to next neighbor
            if (edges.restrictedTo == null) {
                nextNeighborId = neighbors.nextLong();
                return (nextNeighborId != -1);
            }

            // Case 2: edge restriction, look ahead for next neighbor
            while ((nextNeighborId = neighbors.nextLong()) != -1) {
                if (edges.isAllowed(srcNodeId, nextNeighborId)) {
                    return true;
                }
            }
            return false;
        }

        public Long next() {
            return nextNeighborId;
        }
    }
}
