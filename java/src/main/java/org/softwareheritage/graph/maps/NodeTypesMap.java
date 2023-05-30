/*
 * Copyright (c) 2019-2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.maps;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.fastutil.longs.LongMappedBigList;
import it.unimi.dsi.lang.FlyweightPrototype;
import org.softwareheritage.graph.SwhType;

import java.io.IOException;

/**
 * Mapping between long node id and SWH node type as described in the
 * <a href="https://docs.softwareheritage.org/devel/swh-model/data-model.html">data model</a>.
 * <p>
 * The type mapping is pre-computed and dumped on disk in the
 * {@link org.softwareheritage.graph.compress.NodeMapBuilder} class, then it is loaded in-memory
 * here using <a href="http://fastutil.di.unimi.it/">fastutil</a> LongBigList. To be
 * space-efficient, the mapping is stored as a bitmap using minimum number of bits per
 * {@link SwhType}.
 *
 * @author The Software Heritage developers
 */

public class NodeTypesMap implements FlyweightPrototype<NodeTypesMap> {
    /** File extension for the long node id to node type map */
    public static final String NODE_TO_TYPE = ".node2type.map";

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
            nodeTypesMap = (LongBigList) BinIO.loadObject(graphPath + NODE_TO_TYPE);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Unknown class object: " + e);
        }
    }

    public NodeTypesMap(LongBigList nodeTypesMap) {
        this.nodeTypesMap = nodeTypesMap;
    }

    @Override
    public NodeTypesMap copy() {
        return new NodeTypesMap(
                (nodeTypesMap instanceof LongMappedBigList) ? ((LongMappedBigList) nodeTypesMap).copy() : nodeTypesMap);
    }

    /**
     * Returns node type from a node long id.
     *
     * @param nodeId node as a long id
     * @return corresponding {@link SwhType} value
     * @see SwhType
     */
    public SwhType getType(long nodeId) {
        long type = nodeTypesMap.getLong(nodeId);
        return SwhType.fromInt((int) type);
    }
}
