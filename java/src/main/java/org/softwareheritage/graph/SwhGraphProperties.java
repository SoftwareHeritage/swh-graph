package org.softwareheritage.graph;

import org.softwareheritage.graph.maps.NodeIdMap;
import org.softwareheritage.graph.maps.NodeTypesMap;

import java.io.IOException;

/**
 * This objects contains SWH graph properties such as node labels.
 *
 * Some property mappings are necessary because Software Heritage uses string based <a href=
 * "https://docs.softwareheritage.org/devel/swh-model/persistent-identifiers.html">persistent
 * identifiers</a> (SWHID) while WebGraph uses integers internally.
 *
 * The two node ID mappings (long id &harr; SWHID) are used for the input (users refer to the graph
 * using SWHID) and the output (convert back to SWHID for users results).
 *
 * Since graph traversal can be restricted depending on the node type (see {@link AllowedEdges}), a
 * long id &rarr; node type map is stored as well to avoid a full SWHID lookup.
 *
 * @see NodeIdMap
 * @see NodeTypesMap
 */
public class SwhGraphProperties {
    /** Path and basename of the compressed graph */
    String path;
    /** Mapping long id &harr; SWHIDs */
    NodeIdMap nodeIdMap;
    /** Mapping long id &rarr; node types */
    NodeTypesMap nodeTypesMap;

    protected SwhGraphProperties(String path, NodeIdMap nodeIdMap, NodeTypesMap nodeTypesMap) {
        this.path = path;
        this.nodeIdMap = nodeIdMap;
        this.nodeTypesMap = nodeTypesMap;
    }

    public static SwhGraphProperties load(String path) throws IOException {
        return new SwhGraphProperties(path, new NodeIdMap(path), new NodeTypesMap(path));
    }

    /**
     * Cleans up graph resources after use.
     */
    public void close() throws IOException {
        this.nodeIdMap.close();
    }

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
     * @see Node.Type
     */
    public Node.Type getNodeType(long nodeId) {
        return nodeTypesMap.getType(nodeId);
    }
}
