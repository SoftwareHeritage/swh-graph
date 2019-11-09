package org.softwareheritage.graph.backend;

import java.io.IOException;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigList;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Node;

/**
 * Mapping between long node id and SWH node type as described in the <a
 * href="https://docs.softwareheritage.org/devel/swh-model/data-model.html">data
 * model</a>.
 *
 * The type mapping is pre-computed and dumped on disk in the {@link MapBuilder}
 * class, then it is loaded in-memory here using
 * <a href="http://fastutil.di.unimi.it/">fastutil</a> LongBigList. To be
 * space-efficient, the mapping is stored as a bitmap using minimum number of
 * bits per {@link Node.Type}.
 *
 * @author The Software Heritage developers
 */

public class NodeTypesMap {
    /**
     * Array storing for each node its type
     */
    public LongBigList nodeTypesMap;

    /**
     * Constructor.
     *
     * @param graphPath path and basename of the compressed graph
     */
    public NodeTypesMap(String graphPath) throws IOException {
        try {
            nodeTypesMap = (LongBigList) BinIO.loadObject(graphPath + Graph.NODE_TO_TYPE);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Unknown class object: " + e);
        }
    }

    /**
     * Returns node type from a node long id.
     *
     * @param nodeId node as a long id
     * @return corresponding {@link Node.Type} value
     * @see org.softwareheritage.graph.Node.Type
     */
    public Node.Type getType(long nodeId) {
        long type = nodeTypesMap.getLong(nodeId);
        return Node.Type.fromInt((int) type);
    }
}
