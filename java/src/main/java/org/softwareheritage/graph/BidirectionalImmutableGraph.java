package org.softwareheritage.graph;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.Transform;

public class BidirectionalImmutableGraph extends ImmutableGraph {
    private final ImmutableGraph forwardGraph;
    private final ImmutableGraph backwardGraph;

    protected BidirectionalImmutableGraph(ImmutableGraph forwardGraph, ImmutableGraph backwardGraph) {
        this.forwardGraph = forwardGraph;
        this.backwardGraph = backwardGraph;
    }

    @Override
    public long numNodes() {
        assert forwardGraph.numNodes() == backwardGraph.numNodes();
        return this.forwardGraph.numNodes();
    }

    @Override
    public long numArcs() {
        assert forwardGraph.numArcs() == backwardGraph.numArcs();
        return this.forwardGraph.numArcs();
    }

    @Override
    public boolean randomAccess() {
        return this.forwardGraph.randomAccess() && this.backwardGraph.randomAccess();
    }

    @Override
    public boolean hasCopiableIterators() {
        return forwardGraph.hasCopiableIterators() && backwardGraph.hasCopiableIterators();
    }

    @Override
    public BidirectionalImmutableGraph copy() {
        return new BidirectionalImmutableGraph(this.forwardGraph.copy(), this.backwardGraph.copy());
    }

    public BidirectionalImmutableGraph transpose() {
        return new BidirectionalImmutableGraph(backwardGraph, forwardGraph);
    }

    public BidirectionalImmutableGraph symmetrize() {
        ImmutableGraph symmetric = Transform.union(forwardGraph, backwardGraph);
        return new BidirectionalImmutableGraph(symmetric, symmetric);
    }

    @Override
    public long outdegree(long l) {
        return forwardGraph.outdegree(l);
    }

    public long indegree(long l) {
        return backwardGraph.outdegree(l);
    }

    @Override
    public LazyLongIterator successors(long nodeId) {
        return forwardGraph.successors(nodeId);
    }

    public LazyLongIterator predecessors(long nodeId) {
        return backwardGraph.successors(nodeId);
    }

    public ImmutableGraph getForwardGraph() {
        return forwardGraph;
    }

    public ImmutableGraph getBackwardGraph() {
        return backwardGraph;
    }
}
