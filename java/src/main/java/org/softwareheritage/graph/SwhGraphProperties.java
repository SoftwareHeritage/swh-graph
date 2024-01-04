/*
 * Copyright (c) 2021-2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph;

import it.unimi.dsi.big.util.MappedFrontCodedStringBigList;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.bytes.ByteBigList;
import it.unimi.dsi.fastutil.bytes.ByteMappedBigList;
import it.unimi.dsi.fastutil.ints.IntBigList;
import it.unimi.dsi.fastutil.ints.IntMappedBigList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.fastutil.longs.LongMappedBigList;
import it.unimi.dsi.fastutil.shorts.ShortBigList;
import it.unimi.dsi.fastutil.shorts.ShortMappedBigList;
import it.unimi.dsi.lang.FlyweightPrototype;
import it.unimi.dsi.sux4j.util.EliasFanoLongBigList;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.softwareheritage.graph.maps.NodeIdMap;
import org.softwareheritage.graph.maps.NodeTypesMap;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Base64;

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
public class SwhGraphProperties implements FlyweightPrototype<SwhGraphProperties> {
    private final String path;

    private final NodeIdMap nodeIdMap;
    private final NodeTypesMap nodeTypesMap;
    private LongBigList authorTimestamp;
    private ShortBigList authorTimestampOffset;
    private LongBigList committerTimestamp;
    private ShortBigList committerTimestampOffset;
    private LongBigList contentLength;
    private LongArrayBitVector contentIsSkipped;
    private IntBigList authorId;
    private IntBigList committerId;
    private ByteBigList messageBuffer;
    private LongBigList messageOffsets;
    private ByteBigList tagNameBuffer;
    private LongBigList tagNameOffsets;
    private MappedFrontCodedStringBigList edgeLabelNames;

    protected SwhGraphProperties(String path, NodeIdMap nodeIdMap, NodeTypesMap nodeTypesMap) {
        this.path = path;
        this.nodeIdMap = nodeIdMap;
        this.nodeTypesMap = nodeTypesMap;
    }

    protected SwhGraphProperties(String path, NodeIdMap nodeIdMap, NodeTypesMap nodeTypesMap,
            LongBigList authorTimestamp, ShortBigList authorTimestampOffset, LongBigList committerTimestamp,
            ShortBigList committerTimestampOffset, LongBigList contentLength, LongArrayBitVector contentIsSkipped,
            IntBigList authorId, IntBigList committerId, ByteBigList messageBuffer, LongBigList messageOffsets,
            ByteBigList tagNameBuffer, LongBigList tagNameOffsets, MappedFrontCodedStringBigList edgeLabelNames) {
        this.path = path;
        this.nodeIdMap = nodeIdMap;
        this.nodeTypesMap = nodeTypesMap;
        this.authorTimestamp = authorTimestamp;
        this.authorTimestampOffset = authorTimestampOffset;
        this.committerTimestamp = committerTimestamp;
        this.committerTimestampOffset = committerTimestampOffset;
        this.contentLength = contentLength;
        this.contentIsSkipped = contentIsSkipped;
        this.authorId = authorId;
        this.committerId = committerId;
        this.messageBuffer = messageBuffer;
        this.messageOffsets = messageOffsets;
        this.tagNameBuffer = tagNameBuffer;
        this.tagNameOffsets = tagNameOffsets;
        this.edgeLabelNames = edgeLabelNames;
    }

    public SwhGraphProperties copy() {
        return new SwhGraphProperties(path, nodeIdMap.copy(), nodeTypesMap.copy(),
                ((authorTimestamp instanceof LongMappedBigList)
                        ? ((LongMappedBigList) authorTimestamp).copy()
                        : authorTimestamp),
                ((authorTimestampOffset instanceof ShortMappedBigList)
                        ? ((ShortMappedBigList) authorTimestampOffset).copy()
                        : authorTimestampOffset),
                ((committerTimestamp instanceof LongMappedBigList)
                        ? ((LongMappedBigList) committerTimestamp).copy()
                        : committerTimestamp),
                ((committerTimestampOffset instanceof ShortMappedBigList)
                        ? ((ShortMappedBigList) committerTimestampOffset).copy()
                        : committerTimestampOffset),
                ((contentLength instanceof LongMappedBigList)
                        ? ((LongMappedBigList) contentLength).copy()
                        : contentLength),
                contentIsSkipped, // Don't need to copy because it is eagerly loaded in RAM, not mmapped
                ((authorId instanceof IntMappedBigList) ? ((IntMappedBigList) authorId).copy() : authorId),
                ((committerId instanceof IntMappedBigList) ? ((IntMappedBigList) committerId).copy() : committerId),
                ((messageBuffer instanceof ByteMappedBigList)
                        ? ((ByteMappedBigList) messageBuffer).copy()
                        : messageBuffer),
                ((messageOffsets instanceof LongMappedBigList)
                        ? ((LongMappedBigList) messageOffsets).copy()
                        : messageOffsets),
                ((tagNameBuffer instanceof ByteMappedBigList)
                        ? ((ByteMappedBigList) tagNameBuffer).copy()
                        : tagNameBuffer),
                ((tagNameOffsets instanceof LongMappedBigList)
                        ? ((LongMappedBigList) tagNameOffsets).copy()
                        : tagNameOffsets),
                (edgeLabelNames != null) ? edgeLabelNames.copy() : null);
    }

    public static SwhGraphProperties load(String path) throws IOException {
        return new SwhGraphProperties(path, new NodeIdMap(path), new NodeTypesMap(path));
    }

    /**
     * Cleans up resources after use.
     */
    public void close() throws IOException {
        edgeLabelNames.close();
    }

    /** Return the basename of the compressed graph */
    public String getPath() {
        return path;
    }

    /**
     * Converts SWHID node to long.
     *
     * @param swhid node specified as a <code>byte[]</code>
     * @return internal long node id
     * @see SWHID
     */
    public long getNodeId(byte[] swhid) {
        return nodeIdMap.getNodeId(swhid);
    }

    /**
     * Converts SWHID node to long.
     *
     * @param swhid node specified as a <code>String</code>
     * @return internal long node id
     * @see SWHID
     */
    public long getNodeId(String swhid) {
        return nodeIdMap.getNodeId(swhid);
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
     * @see SwhType
     */
    public SwhType getNodeType(long nodeId) {
        return nodeTypesMap.getType(nodeId);
    }

    private static LongBigList loadMappedLongs(String path) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(path, "r")) {
            return LongMappedBigList.map(raf.getChannel());
        }
    }

    private static IntBigList loadMappedInts(String path) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(path, "r")) {
            return IntMappedBigList.map(raf.getChannel());
        }
    }

    private static ShortBigList loadMappedShorts(String path) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(path, "r")) {
            return ShortMappedBigList.map(raf.getChannel());
        }
    }

    private static ByteBigList loadMappedBytes(String path) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(path, "r")) {
            return ByteMappedBigList.map(raf.getChannel());
        }
    }

    private static LongBigList loadEFLongs(String path) throws IOException {
        try {
            return (EliasFanoLongBigList) BinIO.loadObject(path);
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    private static byte[] getLine(ByteBigList byteArray, long start) {
        long end = start;
        while (end < byteArray.size64() && byteArray.getByte(end) != '\n') {
            end++;
        }
        int length = (int) (end - start);
        byte[] buffer = new byte[length];
        byteArray.getElements(start, buffer, 0, length);
        return buffer;
    }

    /** Load the sizes of the content nodes */
    public void loadContentLength() throws IOException {
        contentLength = loadMappedLongs(path + ".property.content.length.bin");
    }

    /** Get the size (in bytes) of the given content node */
    public Long getContentLength(long nodeId) {
        if (contentLength == null) {
            throw new IllegalStateException("Content lengths not loaded");
        }
        long res = contentLength.getLong(nodeId);
        return (res >= 0) ? res : null;
    }

    /** Load the IDs of the persons (authors and committers) */
    public void loadPersonIds() throws IOException {
        authorId = loadMappedInts(path + ".property.author_id.bin");
        committerId = loadMappedInts(path + ".property.committer_id.bin");
    }

    /** Get a unique integer ID representing the author of the given revision or release node */
    public Long getAuthorId(long nodeId) {
        if (authorId == null) {
            throw new IllegalStateException("Author IDs not loaded");
        }
        long res = authorId.getInt(nodeId);
        return (res >= 0) ? res : null;
    }

    /** Get a unique integer ID representing the committer of the given revision node */
    public Long getCommitterId(long nodeId) {
        if (committerId == null) {
            throw new IllegalStateException("Committer IDs not loaded");
        }
        long res = committerId.getInt(nodeId);
        return (res >= 0) ? res : null;
    }

    /**
     * Loads a boolean array indicating whether the given content node was skipped during archive
     * ingestion
     */
    public void loadContentIsSkipped() throws IOException {
        try {
            contentIsSkipped = (LongArrayBitVector) BinIO.loadObject(path + ".property.content.is_skipped.bin");
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    /** Returns whether the given content node was skipped during archive ingestion */
    public boolean isContentSkipped(long nodeId) {
        if (contentIsSkipped == null) {
            throw new IllegalStateException("Skipped content array not loaded");
        }
        return contentIsSkipped.getBoolean(nodeId);
    }

    /** Load the timestamps at which the releases and revisions were authored */
    public void loadAuthorTimestamps() throws IOException {
        authorTimestamp = loadMappedLongs(path + ".property.author_timestamp.bin");
        authorTimestampOffset = loadMappedShorts(path + ".property.author_timestamp_offset.bin");
    }

    /** Return the timestamp at which the given revision or release was authored */
    public Long getAuthorTimestamp(long nodeId) {
        if (authorTimestamp == null) {
            throw new IllegalStateException("Author timestamps not loaded");
        }
        long res = authorTimestamp.getLong(nodeId);
        return (res > Long.MIN_VALUE) ? res : null;
    }

    /** Return the timestamp offset at which the given revision or release was authored */
    public Short getAuthorTimestampOffset(long nodeId) {
        if (authorTimestampOffset == null) {
            throw new IllegalStateException("Author timestamp offsets not loaded");
        }
        short res = authorTimestampOffset.getShort(nodeId);
        return (res > Short.MIN_VALUE) ? res : null;
    }

    /** Load the timestamps at which the releases and revisions were committed */
    public void loadCommitterTimestamps() throws IOException {
        committerTimestamp = loadMappedLongs(path + ".property.committer_timestamp.bin");
        committerTimestampOffset = loadMappedShorts(path + ".property.committer_timestamp_offset.bin");
    }

    /** Return the timestamp at which the given revision was committed */
    public Long getCommitterTimestamp(long nodeId) {
        if (committerTimestamp == null) {
            throw new IllegalStateException("Committer timestamps not loaded");
        }
        long res = committerTimestamp.getLong(nodeId);
        return (res > Long.MIN_VALUE) ? res : null;
    }

    /** Return the timestamp offset at which the given revision was committed */
    public Short getCommitterTimestampOffset(long nodeId) {
        if (committerTimestampOffset == null) {
            throw new IllegalStateException("Committer timestamp offsets not loaded");
        }
        short res = committerTimestampOffset.getShort(nodeId);
        return (res > Short.MIN_VALUE) ? res : null;
    }

    /** Load the revision messages, the release messages and the origin URLs */
    public void loadMessages() throws IOException {
        messageBuffer = loadMappedBytes(path + ".property.message.bin");
        messageOffsets = loadMappedLongs(path + ".property.message.offset.bin");
    }

    /** Get the message of the given revision or release node */
    public byte[] getMessage(long nodeId) {
        byte[] messageBase64 = getMessageBase64(nodeId);
        if (messageBase64 == null) {
            return null;
        }
        return Base64.getDecoder().decode(messageBase64);
    }

    /** Get the message of the given revision or release node, encoded as a base64 byte array */
    public byte[] getMessageBase64(long nodeId) {
        if (messageBuffer == null || messageOffsets == null) {
            throw new IllegalStateException("Messages not loaded");
        }
        long startOffset = messageOffsets.getLong(nodeId);
        if (startOffset == -1) {
            return null;
        }
        return getLine(messageBuffer, startOffset);
    }

    /** Get the URL of the given origin node */
    public String getUrl(long nodeId) {
        byte[] url = getMessage(nodeId);
        return (url != null) ? new String(url) : null;
    }

    /** Load the release names */
    public void loadTagNames() throws IOException {
        tagNameBuffer = loadMappedBytes(path + ".property.tag_name.bin");
        tagNameOffsets = loadMappedLongs(path + ".property.tag_name.offset.bin");
    }

    /** Get the name of the given release node */
    public byte[] getTagName(long nodeId) {
        if (tagNameBuffer == null || tagNameOffsets == null) {
            throw new IllegalStateException("Tag names not loaded");
        }
        long startOffset = tagNameOffsets.getLong(nodeId);
        if (startOffset == -1) {
            return null;
        }
        return Base64.getDecoder().decode(getLine(tagNameBuffer, startOffset));
    }

    /** Load the arc label names (directory entry names and snapshot branch names) */
    public void loadLabelNames() throws IOException {
        try {
            edgeLabelNames = MappedFrontCodedStringBigList.load(path + ".labels.fcl");
        } catch (ConfigurationException e) {
            throw new IOException(e);
        }
    }

    /**
     * Get the arc label name (either a directory entry name or snapshot branch name) associated with
     * the given label ID
     */
    public byte[] getLabelName(long labelId) {
        if (edgeLabelNames == null) {
            throw new IllegalStateException("Label names not loaded");
        }
        return Base64.getDecoder().decode(edgeLabelNames.getArray(labelId));
    }
}
