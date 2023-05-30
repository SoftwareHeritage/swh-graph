/*
 * Copyright (c) 2021-2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph;

import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.big.webgraph.BidirectionalImmutableGraph;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.IOException;
import java.io.InputStream;

/**
 * Class representing the compressed Software Heritage graph in both directions (forward and
 * backward).
 *
 * This class uses the {@link BidirectionalImmutableGraph} class internally to implement the
 * backward equivalent of graph operations ({@link SwhBidirectionalGraph#indegree(long)},
 * {@link SwhBidirectionalGraph#predecessors(long)}, etc.) by holding a reference to two
 * {@link SwhUnidirectionalGraph} (a forward graph and a backward graph).
 *
 * Both graphs share their graph properties in memory by storing references to the same
 * {@link SwhGraphProperties} object.
 *
 * <pre>
 *                 ┌──────────────┐
 *                 │ImmutableGraph◄────────┐
 *                 └────▲─────────┘        │extends
 *                      │                  │
 *                      │       ┌──────────┴────────────────┐
 *               extends│       │BidirectionalImmutableGraph│
 *                      │       └────────────▲──────────────┘
 *                      │                    │extends
 *       ┌──────────────┴───────┐     ┌──────┴──────────────┐
 *       │SwhUnidirectionalGraph│◄────┤SwhBidirectionalGraph│
 *       └──┬──────────────┬────┘     └────────┬───────────┬┘
 *          │              │    contains x2    │           │
 *          │              │                   │           │
 *          │    implements│                   │implements │
 *          │             ┌▼──────────┐        │           │
 *          │             │SwhGraph(I)◄────────┘           │
 * contains │             └───────────┘                    │contains
 *          │                                              │
 *          │            ┌──────────────────┐              │
 *          └────────────►SwhGraphProperties◄──────────────┘
 *                       └──────────────────┘
 * </pre>
 *
 * @author The Software Heritage developers
 * @see SwhUnidirectionalGraph
 */

public class SwhBidirectionalGraph extends BidirectionalImmutableGraph implements SwhGraph {
    /** Property data of the graph (id/type mappings etc.) */
    public final SwhGraphProperties properties;

    private final SwhUnidirectionalGraph forwardGraph;
    private final SwhUnidirectionalGraph backwardGraph;

    public SwhBidirectionalGraph(SwhUnidirectionalGraph forwardGraph, SwhUnidirectionalGraph backwardGraph,
            SwhGraphProperties properties) {
        super(forwardGraph, backwardGraph);
        this.forwardGraph = forwardGraph;
        this.backwardGraph = backwardGraph;
        this.properties = properties;
    }

    private SwhBidirectionalGraph(BidirectionalImmutableGraph graph, SwhGraphProperties properties) {
        super(graph.forward, graph.backward);
        this.forwardGraph = new SwhUnidirectionalGraph(graph.forward, properties);
        this.backwardGraph = new SwhUnidirectionalGraph(graph.backward, properties);
        this.properties = properties;
    }

    public static SwhBidirectionalGraph load(LoadMethod method, String path, InputStream is, ProgressLogger pl)
            throws IOException {
        SwhUnidirectionalGraph forward = SwhUnidirectionalGraph.loadGraphOnly(method, path, is, pl);
        SwhUnidirectionalGraph backward = SwhUnidirectionalGraph.loadGraphOnly(method, path + "-transposed", is, pl);
        SwhGraphProperties properties = SwhGraphProperties.load(path);
        forward.setProperties(properties);
        backward.setProperties(properties);
        return new SwhBidirectionalGraph(forward, backward, properties);
    }

    public static SwhBidirectionalGraph loadLabelled(LoadMethod method, String path, InputStream is, ProgressLogger pl)
            throws IOException {
        SwhUnidirectionalGraph forward = SwhUnidirectionalGraph.loadLabelledGraphOnly(method, path, is, pl);
        SwhUnidirectionalGraph backward = SwhUnidirectionalGraph.loadLabelledGraphOnly(method, path + "-transposed", is,
                pl);
        SwhGraphProperties properties = SwhGraphProperties.load(path);
        forward.setProperties(properties);
        backward.setProperties(properties);
        return new SwhBidirectionalGraph(forward, backward, properties);
    }

    // loadXXX methods from ImmutableGraph
    public static SwhBidirectionalGraph load(String path, ProgressLogger pl) throws IOException {
        return load(LoadMethod.STANDARD, path, null, pl);
    }
    public static SwhBidirectionalGraph load(String path) throws IOException {
        return load(LoadMethod.STANDARD, path, null, null);
    }
    public static SwhBidirectionalGraph loadMapped(String path, ProgressLogger pl) throws IOException {
        return load(LoadMethod.MAPPED, path, null, pl);
    }
    public static SwhBidirectionalGraph loadMapped(String path) throws IOException {
        return load(LoadMethod.MAPPED, path, null, null);
    }
    public static SwhBidirectionalGraph loadOffline(String path, ProgressLogger pl) throws IOException {
        return load(LoadMethod.OFFLINE, path, null, pl);
    }
    public static SwhBidirectionalGraph loadOffline(String path) throws IOException {
        return load(LoadMethod.OFFLINE, path, null, null);
    }

    // Labelled versions of the loadXXX methods from ImmutableGraph
    public static SwhBidirectionalGraph loadLabelled(String path, ProgressLogger pl) throws IOException {
        return loadLabelled(LoadMethod.STANDARD, path, null, pl);
    }
    public static SwhBidirectionalGraph loadLabelled(String path) throws IOException {
        return loadLabelled(LoadMethod.STANDARD, path, null, null);
    }
    public static SwhBidirectionalGraph loadLabelledMapped(String path, ProgressLogger pl) throws IOException {
        return loadLabelled(LoadMethod.MAPPED, path, null, pl);
    }
    public static SwhBidirectionalGraph loadLabelledMapped(String path) throws IOException {
        return loadLabelled(LoadMethod.MAPPED, path, null, null);
    }
    public static SwhBidirectionalGraph loadLabelledOffline(String path, ProgressLogger pl) throws IOException {
        return loadLabelled(LoadMethod.OFFLINE, path, null, pl);
    }
    public static SwhBidirectionalGraph loadLabelledOffline(String path) throws IOException {
        return loadLabelled(LoadMethod.OFFLINE, path, null, null);
    }

    @Override
    public SwhBidirectionalGraph copy() {
        return new SwhBidirectionalGraph(forwardGraph.copy(), backwardGraph.copy(), this.properties.copy());
    }

    @Override
    public SwhBidirectionalGraph transpose() {
        return new SwhBidirectionalGraph(super.transpose(), this.properties);
    }

    @Override
    public SwhBidirectionalGraph symmetrize() {
        return new SwhBidirectionalGraph(super.symmetrize(), this.properties);
    }

    public SwhUnidirectionalGraph getForwardGraph() {
        return this.forwardGraph;
    }

    public SwhUnidirectionalGraph getBackwardGraph() {
        return this.backwardGraph;
    }

    /**
     * Returns a *labelled* lazy iterator over the successors of a given node. The iteration terminates
     * when -1 is returned.
     */
    public ArcLabelledNodeIterator.LabelledArcIterator labelledSuccessors(long x) {
        return forwardGraph.labelledSuccessors(x);
    }

    /**
     * Returns a *labelled* lazy iterator over the predecessors of a given node. The iteration
     * terminates when -1 is returned.
     */
    public ArcLabelledNodeIterator.LabelledArcIterator labelledPredecessors(long x) {
        return backwardGraph.labelledSuccessors(x);
    }

    public void close() throws IOException {
        this.properties.close();
    }

    @Override
    public SwhGraphProperties getProperties() {
        return properties;
    }
}
