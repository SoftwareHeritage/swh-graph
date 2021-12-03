package org.softwareheritage.graph;

import java.io.IOException;

/**
 * Common interface for SWH graph classes.
 */
public interface SwhGraph {
    /**
     * Cleans up graph resources after use.
     */
    void close() throws IOException;

    /**
     * Converts {@link SWHID} node to long.
     *
     * @param swhid node specified as a {@link SWHID}
     * @return internal long node id
     * @see SWHID
     */
    long getNodeId(SWHID swhid);

    /**
     * Converts long id node to {@link SWHID}.
     *
     * @param nodeId node specified as a long id
     * @return external SWHID
     * @see SWHID
     */
    SWHID getSWHID(long nodeId);

    /**
     * Returns node type.
     *
     * @param nodeId node specified as a long id
     * @return corresponding node type
     * @see Node.Type
     */
    Node.Type getNodeType(long nodeId);

    /**
     * Returns the graph full path.
     *
     * @return graph full path
     */
    String getPath();
}
