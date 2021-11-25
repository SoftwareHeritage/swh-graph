package org.softwareheritage.graph;

import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.logging.ProgressLogger;
import org.softwareheritage.graph.maps.NodeIdMap;
import org.softwareheritage.graph.maps.NodeTypesMap;

import java.io.IOException;

/**
 * Main class storing the compressed graph and node id mappings.
 * <p>
 * The compressed graph is stored using the <a href="http://webgraph.di.unimi.it/">WebGraph</a>
 * ecosystem. Additional mappings are necessary because Software Heritage uses string based <a href=
 * "https://docs.softwareheritage.org/devel/swh-model/persistent-identifiers.html">persistent
 * identifiers</a> (SWHID) while WebGraph uses integers internally. These two mappings (long id
 * &harr; SWHID) are used for the input (users refer to the graph using SWHID) and the output
 * (convert back to SWHID for users results). However, since graph traversal can be restricted
 * depending on the node type (see {@link AllowedEdges}), a long id &rarr; node type map is stored
 * as well to avoid a full SWHID lookup.
 *
 * @author The Software Heritage developers
 * @see org.softwareheritage.graph.AllowedEdges
 * @see org.softwareheritage.graph.maps.NodeIdMap
 * @see org.softwareheritage.graph.maps.NodeTypesMap
 */

public class Graph extends ImmutableGraph {
    /**
     * Bidirectional graph containing two compressed {@link it.unimi.dsi.big.webgraph.BVGraph} one for
     * each direction
     */
    BidirectionalImmutableGraph graph;

    /** Path and basename of the compressed graph */
    String path;
    /** Mapping long id &harr; SWHIDs */
    NodeIdMap nodeIdMap;
    /** Mapping long id &rarr; node types */
    NodeTypesMap nodeTypesMap;

    /**
     * Constructor.
     *
     * @param path path and basename of the compressed graph to load
     */

    private Graph(String path) throws IOException {
        loadInternal(path, null, LoadMethod.MAPPED);
    }

    /**
     * Loading mechanisms
     */

    enum LoadMethod {
        MEMORY, MAPPED, OFFLINE,
    }

    protected Graph loadInternal(String path, ProgressLogger pl, LoadMethod method) throws IOException {
        this.path = path;
        ImmutableGraph direct = null;
        ImmutableGraph transposed = null;
        if (method == LoadMethod.MEMORY) {
            direct = ImmutableGraph.load(path, pl);
            transposed = ImmutableGraph.load(path + "-transposed", pl);
        } else if (method == LoadMethod.MAPPED) {
            direct = ImmutableGraph.load(path, pl);
            transposed = ImmutableGraph.loadMapped(path + "-transposed", pl);
        } else if (method == LoadMethod.OFFLINE) {
            direct = ImmutableGraph.loadOffline(path, pl);
            transposed = ImmutableGraph.loadOffline(path + "-transposed", pl);
        }
        this.graph = new BidirectionalImmutableGraph(direct, transposed);
        this.nodeTypesMap = new NodeTypesMap(path);
        this.nodeIdMap = new NodeIdMap(path, numNodes());
        return this;
    }

    protected Graph() {
    }

    public static Graph load(String path, ProgressLogger pl) throws IOException {
        return new Graph().loadInternal(path, pl, LoadMethod.MEMORY);
    }

    public static Graph loadMapped(String path, ProgressLogger pl) throws IOException {
        return new Graph().loadInternal(path, pl, LoadMethod.MAPPED);
    }

    public static Graph loadOffline(String path, ProgressLogger pl) throws IOException {
        return new Graph().loadInternal(path, null, LoadMethod.OFFLINE);
    }

    public static Graph load(String path) throws IOException {
        return new Graph().loadInternal(path, null, LoadMethod.MEMORY);
    }

    public static Graph loadMapped(String path) throws IOException {
        return new Graph().loadInternal(path, null, LoadMethod.MAPPED);
    }

    public static Graph loadOffline(String path) throws IOException {
        return new Graph().loadInternal(path, null, LoadMethod.OFFLINE);
    }

    /**
     * Constructor used for copy()
     */
    protected Graph(BidirectionalImmutableGraph graph, String path, NodeIdMap nodeIdMap, NodeTypesMap nodeTypesMap) {
        this.graph = graph;
        this.path = path;
        this.nodeIdMap = nodeIdMap;
        this.nodeTypesMap = nodeTypesMap;
    }

    /**
     * Return a flyweight copy of the graph.
     */
    @Override
    public Graph copy() {
        return new Graph(this.graph.copy(), this.path, this.nodeIdMap, this.nodeTypesMap);
    }

    @Override
    public boolean randomAccess() {
        return graph.randomAccess();
    }

    /**
     * Return a transposed version of the graph.
     */
    public Graph transpose() {
        return new Graph(this.graph.transpose(), this.path, this.nodeIdMap, this.nodeTypesMap);
    }

    /**
     * Return a symmetric version of the graph.
     */
    public Graph symmetrize() {
        return new Graph(this.graph.symmetrize(), this.path, this.nodeIdMap, this.nodeTypesMap);
    }

    /**
     * Cleans up graph resources after use.
     */
    public void cleanUp() throws IOException {
        nodeIdMap.close();
    }

    /**
     * Returns number of nodes in the graph.
     *
     * @return number of nodes in the graph
     */
    @Override
    public long numNodes() {
        return graph.numNodes();
    }

    /**
     * Returns number of edges in the graph.
     *
     * @return number of edges in the graph
     */
    @Override
    public long numArcs() {
        return graph.numArcs();
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
        return graph.successors(nodeId);
    }

    /**
     * Returns lazy iterator of successors of a node while following a specific set of edge types.
     *
     * @param nodeId node specified as a long id
     * @param allowedEdges the specification of which edges can be traversed
     * @return lazy iterator of successors of the node, specified as a
     *         <a href="http://webgraph.di.unimi.it/">WebGraph</a> LazyLongIterator
     */
    public LazyLongIterator successors(long nodeId, AllowedEdges allowedEdges) {
        if (allowedEdges.restrictedTo == null) {
            // All edges are allowed, bypass edge check
            return this.successors(nodeId);
        } else {
            LazyLongIterator allSuccessors = this.successors(nodeId);
            Graph thisGraph = this;
            return new LazyLongIterator() {
                @Override
                public long nextLong() {
                    long neighbor;
                    while ((neighbor = allSuccessors.nextLong()) != -1) {
                        if (allowedEdges.isAllowed(thisGraph.getNodeType(nodeId), thisGraph.getNodeType(neighbor))) {
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
    }

    /**
     * Returns the outdegree of a node.
     *
     * @param nodeId node specified as a long id
     * @return outdegree of a node
     */
    @Override
    public long outdegree(long nodeId) {
        return graph.outdegree(nodeId);
    }

    /**
     * Returns lazy iterator of predecessors of a node.
     *
     * @param nodeId node specified as a long id
     * @return lazy iterator of predecessors of the node, specified as a
     *         <a href="http://webgraph.di.unimi.it/">WebGraph</a> LazyLongIterator
     */
    public LazyLongIterator predecessors(long nodeId) {
        return graph.predecessors(nodeId);
    }

    /**
     * Returns the indegree of a node.
     *
     * @param nodeId node specified as a long id
     * @return indegree of a node
     */
    public long indegree(long nodeId) {
        return graph.indegree(nodeId);
    }

    /**
     * Returns the underlying BidirectionalImmutableGraph.
     *
     * @return WebGraph ImmutableGraph
     */
    public ImmutableGraph getGraph() {
        return this.graph;
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
     * Converts {@link SWHID} node to long.
     *
     * @param swhid node specified as a {@link SWHID}
     * @return internal long node id
     * @see SWHID
     */
    public long getNodeId(SWHID swhid) {
        return nodeIdMap.getNodeId(swhid);
    }

    /**
     * Converts long id node to {@link SWHID}.
     *
     * @param nodeId node specified as a long id
     * @return external SWHID
     * @see SWHID
     */
    public SWHID getSWHID(long nodeId) {
        return nodeIdMap.getSWHID(nodeId);
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
}
