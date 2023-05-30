/*
 * Copyright (c) 2021-2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

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

    /** @see SwhGraphProperties#getNodeId(byte[]) */
    default long getNodeId(byte[] swhid) {
        return getProperties().getNodeId(swhid);
    }

    /** @see SwhGraphProperties#getNodeId(String) */
    default long getNodeId(String swhid) {
        return getProperties().getNodeId(swhid);
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
    default SwhType getNodeType(long nodeId) {
        return getProperties().getNodeType(nodeId);
    }

    /** @see SwhGraphProperties#loadContentLength() */
    default void loadContentLength() throws IOException {
        getProperties().loadContentLength();
    }

    /** @see SwhGraphProperties#getContentLength(long) */
    default Long getContentLength(long nodeId) {
        return getProperties().getContentLength(nodeId);
    }

    /** @see SwhGraphProperties#loadPersonIds() */
    default void loadPersonIds() throws IOException {
        getProperties().loadPersonIds();
    }

    /** @see SwhGraphProperties#getAuthorId(long) */
    default Long getAuthorId(long nodeId) {
        return getProperties().getAuthorId(nodeId);
    }

    /** @see SwhGraphProperties#getCommitterId(long) */
    default Long getCommitterId(long nodeId) {
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
    default Long getAuthorTimestamp(long nodeId) {
        return getProperties().getAuthorTimestamp(nodeId);
    }

    /** @see SwhGraphProperties#getAuthorTimestampOffset(long) */
    default Short getAuthorTimestampOffset(long nodeId) {
        return getProperties().getAuthorTimestampOffset(nodeId);
    }

    /** @see SwhGraphProperties#loadCommitterTimestamps() */
    default void loadCommitterTimestamps() throws IOException {
        getProperties().loadCommitterTimestamps();
    }

    /** @see SwhGraphProperties#getCommitterTimestamp(long) */
    default Long getCommitterTimestamp(long nodeId) {
        return getProperties().getCommitterTimestamp(nodeId);
    }

    /** @see SwhGraphProperties#getCommitterTimestampOffset(long) */
    default Short getCommitterTimestampOffset(long nodeId) {
        return getProperties().getCommitterTimestampOffset(nodeId);
    }

    /** @see SwhGraphProperties#loadMessages() */
    default void loadMessages() throws IOException {
        getProperties().loadMessages();
    }

    /** @see SwhGraphProperties#getMessage(long) */
    default byte[] getMessage(long nodeId) {
        return getProperties().getMessage(nodeId);
    }

    /** @see SwhGraphProperties#getMessageBase64(long) */
    default byte[] getMessageBase64(long nodeId) {
        return getProperties().getMessageBase64(nodeId);
    }

    /** @see SwhGraphProperties#getUrl(long) */
    default String getUrl(long nodeId) {
        return getProperties().getUrl(nodeId);
    }

    /** @see SwhGraphProperties#loadTagNames() */
    default void loadTagNames() throws IOException {
        getProperties().loadTagNames();
    }

    /** @see SwhGraphProperties#getTagName(long) */
    default byte[] getTagName(long nodeId) {
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
