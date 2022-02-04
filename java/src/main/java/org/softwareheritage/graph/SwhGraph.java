package org.softwareheritage.graph;

import java.io.IOException;

/**
 * Common interface for SWH graph classes.
 *
 * This interface forwards all property loading/access methods to the SwhGraphProperties object
 * returned by the getProperties() method of the implementing class. This allows API users to write
 * graph.getNodeType() instead of graph.getProperties().getNodeType().
 */
public interface SwhGraph {
    /**
     * Cleans up graph resources after use.
     */
    void close() throws IOException;

    /**
     * Returns the SWH graph properties object of this graph.
     *
     * @return graph properties
     */
    SwhGraphProperties getProperties();

    /** @see SwhGraphProperties#getPath() */
    default String getPath() {
        return getProperties().getPath();
    }

    /** @see SwhGraphProperties#getNodeId(SWHID) */
    default long getNodeId(SWHID swhid) {
        return getProperties().getNodeId(swhid);
    }

    /** @see SwhGraphProperties#getSWHID(long) */
    default SWHID getSWHID(long nodeId) {
        return getProperties().getSWHID(nodeId);
    }

    /** @see SwhGraphProperties#getNodeType(long) */
    default Node.Type getNodeType(long nodeId) {
        return getProperties().getNodeType(nodeId);
    }

    /** @see SwhGraphProperties#loadContentLength() */
    default void loadContentLength() throws IOException {
        getProperties().loadContentLength();
    }

    /** @see SwhGraphProperties#getContentLength(long) */
    default long getContentLength(long nodeId) {
        return getProperties().getContentLength(nodeId);
    }

    /** @see SwhGraphProperties#loadPersonIds() */
    default void loadPersonIds() throws IOException {
        getProperties().loadPersonIds();
    }

    /** @see SwhGraphProperties#getAuthorId(long) */
    default long getAuthorId(long nodeId) {
        return getProperties().getAuthorId(nodeId);
    }

    /** @see SwhGraphProperties#getCommitterId(long) */
    default long getCommitterId(long nodeId) {
        return getProperties().getCommitterId(nodeId);
    }

    /** @see SwhGraphProperties#loadContentIsSkipped() */
    default void loadContentIsSkipped() throws IOException {
        getProperties().loadContentIsSkipped();
    }

    /** @see SwhGraphProperties#isContentSkipped(long) */
    default boolean isContentSkipped(long nodeId) {
        return getProperties().isContentSkipped(nodeId);
    }

    /** @see SwhGraphProperties#loadAuthorTimestamps() */
    default void loadAuthorTimestamps() throws IOException {
        getProperties().loadAuthorTimestamps();
    }

    /** @see SwhGraphProperties#getAuthorTimestamp(long) */
    default long getAuthorTimestamp(long nodeId) {
        return getProperties().getAuthorTimestamp(nodeId);
    }

    /** @see SwhGraphProperties#getAuthorTimestampOffset(long) */
    default short getAuthorTimestampOffset(long nodeId) {
        return getProperties().getAuthorTimestampOffset(nodeId);
    }

    /** @see SwhGraphProperties#loadCommitterTimestamps() */
    default void loadCommitterTimestamps() throws IOException {
        getProperties().loadCommitterTimestamps();
    }

    /** @see SwhGraphProperties#getCommitterTimestamp(long) */
    default long getCommitterTimestamp(long nodeId) {
        return getProperties().getCommitterTimestamp(nodeId);
    }

    /** @see SwhGraphProperties#getCommitterTimestampOffset(long) */
    default short getCommitterTimestampOffset(long nodeId) {
        return getProperties().getCommitterTimestampOffset(nodeId);
    }

    /** @see SwhGraphProperties#loadMessages() */
    default void loadMessages() throws IOException {
        getProperties().loadMessages();
    }

    /** @see SwhGraphProperties#getMessage(long) */
    default byte[] getMessage(long nodeId) throws IOException {
        return getProperties().getMessage(nodeId);
    }

    /** @see SwhGraphProperties#getUrl(long) */
    default String getUrl(long nodeId) throws IOException {
        return getProperties().getUrl(nodeId);
    }

    /** @see SwhGraphProperties#loadTagNames() */
    default void loadTagNames() throws IOException {
        getProperties().loadTagNames();
    }

    /** @see SwhGraphProperties#getTagName(long) */
    default byte[] getTagName(long nodeId) throws IOException {
        return getProperties().getTagName(nodeId);
    }

    /** @see SwhGraphProperties#loadLabelNames() */
    default void loadLabelNames() throws IOException {
        getProperties().loadLabelNames();
    }

    /** @see SwhGraphProperties#getLabelName(long) */
    default byte[] getLabelName(long labelId) {
        return getProperties().getLabelName(labelId);
    }
}
