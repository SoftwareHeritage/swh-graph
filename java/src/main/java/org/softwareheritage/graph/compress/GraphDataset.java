/*
 * Copyright (c) 2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.compress;

import java.io.IOException;

/**
 * GraphDataset is a common interface to represent on-disk graph datasets in various formats,
 * usually extracted from the SWH archive with the swh-dataset tool.
 */
public interface GraphDataset {
    interface NodeCallback {
        void onNode(byte[] node) throws IOException;
    }

    interface EdgeCallback {
        void onEdge(byte[] src, byte[] dst, byte[] label, int permission) throws IOException;
    }

    /**
     * Read the graph dataset and call the callback methods for each node and edge encountered.
     *
     * <ul>
     * <li>The node callback is called for each object stored in the graph.</li>
     * <li>The edge callback is called for each relationship (between two nodes) stored in the
     * graph.</li>
     * </ul>
     *
     * <p>
     * Note that because the graph can contain holes, loose objects and dangling objects, the edge
     * callback may be called with parameters representing nodes that are not stored in the graph. This
     * is because some nodes that are referred to as destinations in the dataset might not be present in
     * the archive (e.g., a revision entry in a directory pointing to a revision that we have not
     * crawled yet).
     * </p>
     *
     * <p>
     * In order to generate a complete set of all the nodes that are <em>referred</em> to in the graph
     * dataset, see the {@link ExtractNodes} class.
     * </p>
     *
     * @param nodeCb callback for each node
     * @param edgeCb callback for each edge
     */
    void readEdges(NodeCallback nodeCb, EdgeCallback edgeCb) throws IOException;

    interface TimestampCallback {
        void onTimestamp(byte[] swhid, long timestamp, short offset) throws IOException;
    }

    interface LongCallback {
        void onLong(byte[] swhid, long value) throws IOException;
    }

    interface BytesCallback {
        void onBytes(byte[] swhid, byte[] value) throws IOException;
    }

    interface HashedEdgeCallback {
        void onHashedEdge(long src, long dst, long label, int permission) throws IOException;
    }
}
