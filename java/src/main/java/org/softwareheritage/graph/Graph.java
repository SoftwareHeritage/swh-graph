package org.softwareheritage.graph;

import java.io.IOException;

import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.Transform;
import it.unimi.dsi.lang.FlyweightPrototype;
import it.unimi.dsi.big.webgraph.LazyLongIterator;

import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.SwhPID;
import org.softwareheritage.graph.backend.NodeIdMap;
import org.softwareheritage.graph.backend.NodeTypesMap;

/**
 * Main class storing the compressed graph and node id mappings.
 * <p>
 * The compressed graph is stored using the <a href="http://webgraph.di.unimi.it/">WebGraph</a>
 * ecosystem. Additional mappings are necessary because Software Heritage uses string based <a
 * href="https://docs.softwareheritage.org/devel/swh-model/persistent-identifiers.html">persistent
 * identifiers</a> (PID) while WebGraph uses integers internally. These two mappings (long id &harr;
 * PID) are used for the input (users refer to the graph using PID) and the output (convert back to
 * PID for users results). However, since graph traversal can be restricted depending on the node
 * type (see {@link AllowedEdges}), a long id &rarr; node type map is stored as well to avoid a full
 * PID lookup.
 *
 * @author The Software Heritage developers
 * @see org.softwareheritage.graph.AllowedEdges
 * @see org.softwareheritage.graph.backend.NodeIdMap
 * @see org.softwareheritage.graph.backend.NodeTypesMap
 */

public class Graph extends ImmutableGraph {
    /** File extension for the SWH PID to long node id map */
    public static final String PID_TO_NODE = ".pid2node.bin";
    /** File extension for the long node id to SWH PID map */
    public static final String NODE_TO_PID = ".node2pid.bin";
    /** File extension for the long node id to node type map */
    public static final String NODE_TO_TYPE = ".node2type.map";

    /** Compressed graph stored as a {@link it.unimi.dsi.big.webgraph.BVGraph} */
    ImmutableGraph graph;
    /** Transposed compressed graph (used for backward traversals) */
    ImmutableGraph graphTransposed;
    /** Path and basename of the compressed graph */
    String path;
    /** Mapping long id &harr; SWH PIDs */
    NodeIdMap nodeIdMap;
    /** Mapping long id &rarr; node types */
    NodeTypesMap nodeTypesMap;

    /**
     * Constructor.
     *
     * @param path path and basename of the compressed graph to load
     */
    public Graph(String path) throws IOException {
        this.graph = BVGraph.loadMapped(path);
        this.graphTransposed = BVGraph.loadMapped(path + "-transposed");
        this.path = path;
        this.nodeTypesMap = new NodeTypesMap(path);

        // TODO: we don't need to load the nodeIdMap now that all the
        // translation between ints and PIDs is happening on the Python side.
        // However, some parts of the code still depend on this, so it's
        // commented out while we decide on what to do with it.
        this.nodeIdMap = null; // new NodeIdMap(path, numNodes());
    }

    // Protected empty constructor to implement copy()
    protected Graph() { }

    protected Graph(ImmutableGraph graph, ImmutableGraph graphTransposed, String path, NodeIdMap nodeIdMap, NodeTypesMap nodeTypesMap) {
        this.graph = graph;
        this.graphTransposed = graphTransposed;
        this.path = path;
        this.nodeIdMap = nodeIdMap;
        this.nodeTypesMap = nodeTypesMap;
    }

    /**
     * Return a flyweight copy of the graph.
     */
    @Override
    public Graph copy() {
        return new Graph(this.graph.copy(), this.graphTransposed.copy(), this.path, this.nodeIdMap, this.nodeTypesMap);
    }

    public Graph transpose() {
        return new Graph(this.graphTransposed.copy(), this.graph.copy(), this.path, this.nodeIdMap, this.nodeTypesMap);
    }

    public Graph symmetrize() {
        ImmutableGraph symmetric = Transform.union(graph, graphTransposed);
        return new Graph(symmetric, symmetric, this.path, this.nodeIdMap, this.nodeTypesMap);
    }

    /**
     * Cleans up graph resources after use.
     */
    public void cleanUp() throws IOException {
        nodeIdMap.close();
    }

    /**
     * Returns the graph full path.
     *
     * @return graph full path
     */
    public String getPath() {
        return path;
    }

    /**
     * Converts {@link SwhPID} node to long.
     *
     * @param swhPID node specified as a {@link SwhPID}
     * @return internal long node id
     * @see org.softwareheritage.graph.SwhPID
     */
    public long getNodeId(SwhPID swhPID) {
        return nodeIdMap.getNodeId(swhPID);
    }

    /**
     * Converts long id node to {@link SwhPID}.
     *
     * @param nodeId node specified as a long id
     * @return external SWH PID
     * @see org.softwareheritage.graph.SwhPID
     */
    public SwhPID getSwhPID(long nodeId) {
        return nodeIdMap.getSwhPID(nodeId);
    }

    /**
     * Returns node type.
     *
     * @param nodeId node specified as a long id
     * @return corresponding node type
     * @see org.softwareheritage.graph.Node.Type
     */
    public Node.Type getNodeType(long nodeId) {
        return nodeTypesMap.getType(nodeId);
    }

    public boolean randomAccess() { return graph.randomAccess() && graphTransposed.randomAccess(); }

    /**
     * Returns number of nodes in the graph.
     *
     * @return number of nodes in the graph
     */
    public long numNodes() {
        return graph.numNodes();
    }

    /**
     * Returns number of edges in the graph.
     *
     * @return number of edges in the graph
     */
    public long numArcs() {
        return graph.numArcs();
    }

    /**
     * Returns lazy iterator of successors of a node.
     *
     * @param nodeId node specified as a long id
     * @return lazy iterator of successors of the node, specified as a <a
     * href="http://webgraph.di.unimi.it/">WebGraph</a> LazyLongIterator
     */
    public LazyLongIterator successors(long nodeId) {
        return graph.successors(nodeId);
    }

    /**
     * Returns the outdegree of a node.
     *
     * @param nodeId node specified as a long id
     * @return outdegree of a node
     */
    public long outdegree(long nodeId) {
        return graph.outdegree(nodeId);
    }

    /**
     * Returns lazy iterator of predecessors of a node.
     *
     * @param nodeId node specified as a long id
     * @return lazy iterator of predecessors of the node, specified as a <a
     * href="http://webgraph.di.unimi.it/">WebGraph</a> LazyLongIterator
     */
    public LazyLongIterator predecessors(long nodeId) {
        return graphTransposed.successors(nodeId);
    }

    /**
     * Returns the indegree of a node.
     *
     * @param nodeId node specified as a long id
     * @return indegree of a node
     */
    public long indegree(long nodeId) {
        return graphTransposed.outdegree(nodeId);
    }

    /**
     * Returns the underlying BVGraph.
     *
     * @return WebGraph BVGraph
     */
    public ImmutableGraph getGraph() {
        return this.graph;
    }
}
