/*
 * Copyright (c) 2019-2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.big.webgraph.labelling.BitStreamArcLabelledImmutableGraph;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledImmutableGraph;

import java.io.IOException;
import java.io.InputStream;

/**
 * Class representing the compressed Software Heritage graph in a single direction.
 * <p>
 * The compressed graph is stored using the <a href="http://webgraph.di.unimi.it/">WebGraph</a>
 * framework. This class contains an {@link ImmutableGraph} representing the graph itself, as well
 * as a reference to the object containing the graph properties (e.g. node labels). Optionally,
 * <em>arc labels</em> (properties stored on the graph edges) can also be loaded with the
 * <code>loadLabelled...()</code> function family.
 *
 * @author The Software Heritage developers
 * @see SwhGraphProperties
 * @see SwhUnidirectionalGraph
 */

public class SwhUnidirectionalGraph extends ImmutableGraph implements SwhGraph {
    /** Underlying ImmutableGraph */
    private final ImmutableGraph graph;

    /** Labelled ImmutableGraph, null if labels are not loaded */
    private ArcLabelledImmutableGraph labelledGraph;

    /** Property data of the graph (id/type mappings etc.) */
    public SwhGraphProperties properties;

    public SwhUnidirectionalGraph(ImmutableGraph graph, SwhGraphProperties properties) {
        this.graph = graph;
        this.properties = properties;
    }

    protected SwhUnidirectionalGraph(ImmutableGraph graph, ArcLabelledImmutableGraph labelledGraph,
            SwhGraphProperties properties) {
        this.graph = graph;
        this.labelledGraph = labelledGraph;
        this.properties = properties;
    }

    /**
     * Load the (unlabelled) graph only, without the SWH properties.
     */
    public static SwhUnidirectionalGraph loadGraphOnly(LoadMethod method, String path, InputStream is,
            ProgressLogger pl) throws IOException {
        return new SwhUnidirectionalGraph(ImmutableGraph.load(method, path, is, pl), null);
    }

    /**
     * Load the <em>labelled</em> graph only, without the SWH properties.
     */
    public static SwhUnidirectionalGraph loadLabelledGraphOnly(LoadMethod method, String path, InputStream is,
            ProgressLogger pl) throws IOException {
        BitStreamArcLabelledImmutableGraph g = (BitStreamArcLabelledImmutableGraph) BitStreamArcLabelledImmutableGraph
                .load(method, path + "-labelled", is, pl);
        return new SwhUnidirectionalGraph(g.g, g, null);
    }

    /**
     * Load the SWH properties of the graph from a given path.
     */
    public void loadProperties(String path) throws IOException {
        properties = SwhGraphProperties.load(path);
    }

    /**
     * Setter for the SWH graph properties.
     *
     * @param properties The {@link SwhGraphProperties} object containing the graph properties
     */
    public void setProperties(SwhGraphProperties properties) {
        this.properties = properties;
    }

    /**
     * Load the unlabelled graph and its SWH properties.
     */
    public static SwhUnidirectionalGraph load(LoadMethod method, String path, InputStream is, ProgressLogger pl)
            throws IOException {
        SwhUnidirectionalGraph g = loadGraphOnly(method, path, is, pl);
        g.loadProperties(path);
        return g;
    }

    /**
     * Load the labelled graph and its SWH properties.
     */
    public static SwhUnidirectionalGraph loadLabelled(LoadMethod method, String path, InputStream is, ProgressLogger pl)
            throws IOException {
        SwhUnidirectionalGraph g = loadLabelledGraphOnly(method, path, is, pl);
        g.loadProperties(path);
        return g;
    }

    // loadXXX methods of ImmutableGraph
    public static SwhUnidirectionalGraph load(String path, ProgressLogger pl) throws IOException {
        return load(LoadMethod.STANDARD, path, null, pl);
    }
    public static SwhUnidirectionalGraph load(String path) throws IOException {
        return load(LoadMethod.STANDARD, path, null, null);
    }
    public static SwhUnidirectionalGraph loadMapped(String path, ProgressLogger pl) throws IOException {
        return load(LoadMethod.MAPPED, path, null, pl);
    }
    public static SwhUnidirectionalGraph loadMapped(String path) throws IOException {
        return load(LoadMethod.MAPPED, path, null, null);
    }
    public static SwhUnidirectionalGraph loadOffline(String path, ProgressLogger pl) throws IOException {
        return load(LoadMethod.OFFLINE, path, null, pl);
    }
    public static SwhUnidirectionalGraph loadOffline(String path) throws IOException {
        return load(LoadMethod.OFFLINE, path, null, null);
    }

    // Labelled versions of the loadXXX methods from ImmutableGraph
    public static SwhUnidirectionalGraph loadLabelled(String path, ProgressLogger pl) throws IOException {
        return loadLabelled(LoadMethod.STANDARD, path, null, pl);
    }
    public static SwhUnidirectionalGraph loadLabelled(String path) throws IOException {
        return loadLabelled(LoadMethod.STANDARD, path, null, null);
    }
    public static SwhUnidirectionalGraph loadLabelledMapped(String path, ProgressLogger pl) throws IOException {
        return loadLabelled(LoadMethod.MAPPED, path, null, pl);
    }
    public static SwhUnidirectionalGraph loadLabelledMapped(String path) throws IOException {
        return loadLabelled(LoadMethod.MAPPED, path, null, null);
    }
    public static SwhUnidirectionalGraph loadLabelledOffline(String path, ProgressLogger pl) throws IOException {
        return loadLabelled(LoadMethod.OFFLINE, path, null, pl);
    }
    public static SwhUnidirectionalGraph loadLabelledOffline(String path) throws IOException {
        return loadLabelled(LoadMethod.OFFLINE, path, null, null);
    }

    @Override
    public SwhUnidirectionalGraph copy() {
        return new SwhUnidirectionalGraph(this.graph.copy(),
                this.labelledGraph != null ? this.labelledGraph.copy() : null, this.properties.copy());
    }

    @Override
    public boolean randomAccess() {
        return graph.randomAccess();
    }

    public void close() throws IOException {
        this.properties.close();
    }

    @Override
    public long numNodes() {
        return graph.numNodes();
    }

    @Override
    public long numArcs() {
        return graph.numArcs();
    }

    @Override
    public LazyLongIterator successors(long nodeId) {
        return graph.successors(nodeId);
    }

    /**
     * Returns a <em>labelled</em> node iterator for scanning the graph sequentially, starting from the
     * first node.
     */
    public ArcLabelledNodeIterator labelledNodeIterator() {
        if (labelledGraph == null) {
            throw new RuntimeException("Calling labelledNodeIterator() but labels were not loaded.");
        }
        return labelledGraph.nodeIterator();
    }

    /**
     * Returns a <em>labelled</em> node iterator for scanning the graph sequentially, starting from a
     * given node.
     */
    public ArcLabelledNodeIterator labelledNodeIterator(long from) {
        if (labelledGraph == null) {
            throw new RuntimeException("Calling labelledNodeIterator() but labels were not loaded.");
        }
        return labelledGraph.nodeIterator(from);
    }

    /**
     * Returns a <em>labelled</em> lazy iterator over the successors of a given node. The iteration
     * terminates when -1 is returned.
     */
    public ArcLabelledNodeIterator.LabelledArcIterator labelledSuccessors(long x) {
        if (labelledGraph == null) {
            throw new RuntimeException("Calling labelledNodeIterator() but labels were not loaded.");
        }
        return labelledGraph.successors(x);
    }

    @Override
    public long outdegree(long nodeId) {
        return graph.outdegree(nodeId);
    }

    @Override
    public SwhGraphProperties getProperties() {
        return properties;
    }

    public ImmutableGraph underlyingGraph() {
        return graph;
    }

    public ArcLabelledImmutableGraph underlyingLabelledGraph() {
        return labelledGraph;
    }
}
