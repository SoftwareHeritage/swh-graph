/*
 * Copyright (c) 2020 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;

import java.util.NoSuchElementException;

public class Subgraph extends ImmutableGraph {
    private final SwhBidirectionalGraph underlyingGraph;
    public final AllowedNodes allowedNodeTypes;

    private long nodeCount = -1;

    /**
     * Constructor.
     *
     */
    public Subgraph(SwhBidirectionalGraph underlyingGraph, AllowedNodes allowedNodeTypes) {
        this.underlyingGraph = underlyingGraph.copy();
        this.allowedNodeTypes = allowedNodeTypes;
    }

    /**
     * Return a flyweight copy of the graph.
     */
    @Override
    public Subgraph copy() {
        return new Subgraph(this.underlyingGraph.copy(), allowedNodeTypes);
    }

    @Override
    public boolean randomAccess() {
        return underlyingGraph.randomAccess();
    }

    /**
     * Return a transposed version of the graph.
     */
    public Subgraph transpose() {
        return new Subgraph(underlyingGraph.transpose(), allowedNodeTypes);
    }

    /**
     * Return a symmetric version of the graph.
     */
    public Subgraph symmetrize() {
        return new Subgraph(underlyingGraph.symmetrize(), allowedNodeTypes);
    }

    /**
     * Returns number of nodes in the graph.
     *
     * @return number of nodes in the graph
     */
    @Override
    public long numNodes() {
        if (nodeCount == -1) {
            for (long i = 0; i < underlyingGraph.numNodes(); ++i) {
                if (nodeExists(i))
                    ++nodeCount;
            }
        }
        return nodeCount;
    }

    /**
     * Returns number of edges in the graph.
     *
     * @return number of edges in the graph
     */
    @Override
    public long numArcs() {
        throw new UnsupportedOperationException("Cannot determine the number of arcs in a subgraph");
    }

    public long maxNodeNumber() {
        return underlyingGraph.numNodes();
    }

    public boolean nodeExists(long node) {
        return allowedNodeTypes.isAllowed(underlyingGraph.getNodeType(node));
    }

    /**
     * Returns lazy iterator of successors of a node.
     *
     * @param nodeId node specified as a long id
     * @return lazy iterator of successors of the node, specified as a
     *         <a href="http://webgraph.di.unimi.it/">WebGraph</a> LazyLongIterator
     */
    @Override
    public LazyLongIterator successors(long nodeId) {
        if (!nodeExists(nodeId)) {
            throw new IllegalArgumentException("Node " + nodeId + " not in subgraph");
        }
        LazyLongIterator allSuccessors = underlyingGraph.successors(nodeId);
        return new LazyLongIterator() {
            @Override
            public long nextLong() {
                long neighbor;
                while ((neighbor = allSuccessors.nextLong()) != -1) {
                    if (nodeExists(neighbor)) {
                        return neighbor;
                    }
                }
                return -1;
            }

            @Override
            public long skip(final long n) {
                long i;
                for (i = 0; i < n && nextLong() != -1; i++)
                    ;
                return i;
            }
        };
    }

    /**
     * Returns the outdegree of a node.
     *
     * @param nodeId node specified as a long id
     * @return outdegree of a node
     */
    @Override
    public long outdegree(long nodeId) {
        long deg = 0;
        for (LazyLongIterator allSuccessors = successors(nodeId); allSuccessors.nextLong() != -1; ++deg)
            ;
        return deg;
    }

    @Override
    public NodeIterator nodeIterator() {
        return new NodeIterator() {
            final long n = numNodes();
            long i = -1;
            long done = 0;

            @Override
            public boolean hasNext() {
                return done <= n;
            }

            @Override
            public long nextLong() {
                if (!hasNext())
                    throw new NoSuchElementException();
                do {
                    ++i;
                    if (i >= underlyingGraph.numNodes())
                        throw new NoSuchElementException();
                } while (!nodeExists(i));
                ++done;
                return i;
            }

            @Override
            public long outdegree() {
                return Subgraph.this.outdegree(i);
            }

            @Override
            public LazyLongIterator successors() {
                return Subgraph.this.successors(i);
            }
        };
    }

    /**
     * Returns lazy iterator of predecessors of a node.
     *
     * @param nodeId node specified as a long id
     * @return lazy iterator of predecessors of the node, specified as a
     *         <a href="http://webgraph.di.unimi.it/">WebGraph</a> LazyLongIterator
     */
    public LazyLongIterator predecessors(long nodeId) {
        return this.transpose().successors(nodeId);
    }

    /**
     * Returns the indegree of a node.
     *
     * @param nodeId node specified as a long id
     * @return indegree of a node
     */
    public long indegree(long nodeId) {
        return this.transpose().outdegree(nodeId);
    }

    /**
     * Converts SWHID node to long.
     *
     * @param swhid node specified as a <code>byte[]</code>
     * @return internal long node id
     * @see SWHID
     */
    public long getNodeId(byte[] swhid) {
        return underlyingGraph.getNodeId(swhid);
    }

    /**
     * Converts SWHID node to long.
     *
     * @param swhid node specified as a <code>String</code>
     * @return internal long node id
     * @see SWHID
     */
    public long getNodeId(String swhid) {
        return underlyingGraph.getNodeId(swhid);
    }

    /**
     * Converts {@link SWHID} node to long.
     *
     * @param swhid node specified as a {@link SWHID}
     * @return internal long node id
     * @see SWHID
     */
    public long getNodeId(SWHID swhid) {
        return underlyingGraph.getNodeId(swhid);
    }

    /**
     * Converts long id node to {@link SWHID}.
     *
     * @param nodeId node specified as a long id
     * @return external SWHID
     * @see SWHID
     */
    public SWHID getSWHID(long nodeId) {
        return underlyingGraph.getSWHID(nodeId);
    }

    /**
     * Returns node type.
     *
     * @param nodeId node specified as a long id
     * @return corresponding node type
     * @see SwhType
     */
    public SwhType getNodeType(long nodeId) {
        return underlyingGraph.getNodeType(nodeId);
    }
}
