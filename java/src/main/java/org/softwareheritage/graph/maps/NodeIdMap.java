/*
 * Copyright (c) 2019-2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.maps;

import it.unimi.dsi.fastutil.Size64;
import it.unimi.dsi.fastutil.bytes.ByteBigList;
import it.unimi.dsi.fastutil.bytes.ByteMappedBigList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.fastutil.longs.LongMappedBigList;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.lang.FlyweightPrototype;
import org.softwareheritage.graph.SWHID;
import org.softwareheritage.graph.compress.NodeMapBuilder;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;

/**
 * Mapping between internal long node id and external SWHID.
 * <p>
 * The SWHID -> node mapping is obtained from hashing the SWHID with a MPH, then permuting it using
 * an mmap()-ed .order file containing the graph permutation.
 *
 * The node -> SWHID reverse mapping is pre-computed and dumped on disk in the
 * {@link NodeMapBuilder} class, then it is loaded here using mmap().
 *
 * @author The Software Heritage developers
 * @see NodeMapBuilder
 */

public class NodeIdMap implements Size64, FlyweightPrototype<NodeIdMap> {
    /** Fixed length of binary SWHID buffer */
    public static final int SWHID_BIN_SIZE = 22;

    /** File extension for the long node id to SWHID map */
    public static final String NODE_TO_SWHID = ".node2swhid.bin";

    /** Graph path and basename */
    String graphPath;

    /** mmap()-ed NODE_TO_SWHID file */
    ByteBigList nodeToSwhMap;

    /** Minimal perfect hash (MPH) function SWHID -> initial order */
    Object2LongFunction<byte[]> mph;
    /** mmap()-ed long list with the permutation initial order -> graph order */
    LongBigList orderMap;

    /**
     * Constructor.
     *
     * @param graphPath full graph path
     */
    public NodeIdMap(String graphPath) throws IOException {
        this.graphPath = graphPath;

        // node -> SWHID
        try (RandomAccessFile raf = new RandomAccessFile(graphPath + NODE_TO_SWHID, "r")) {
            this.nodeToSwhMap = ByteMappedBigList.map(raf.getChannel());
        }

        long byte_size = this.nodeToSwhMap.size64();
        if (byte_size % SWHID_BIN_SIZE != 0) {
            throw new RuntimeException(
                    String.format("%s%s has size %d bytes, which is not a multiple of SWHID_BIN_SIZE (%d)", graphPath,
                            NODE_TO_SWHID, byte_size, SWHID_BIN_SIZE));
        }

        // SWHID -> node
        this.mph = loadMph(graphPath + ".mph");
        try (RandomAccessFile mapFile = new RandomAccessFile(new File(graphPath + ".order"), "r")) {
            this.orderMap = LongMappedBigList.map(mapFile.getChannel());
        }
    }

    protected NodeIdMap(String graphPath, ByteBigList nodeToSwhMap, Object2LongFunction<byte[]> mph,
            LongBigList orderMap) {
        this.graphPath = graphPath;
        this.nodeToSwhMap = nodeToSwhMap;
        this.mph = mph;
        this.orderMap = orderMap;
    }

    @Override
    public NodeIdMap copy() {
        return new NodeIdMap(graphPath,
                ((nodeToSwhMap instanceof ByteMappedBigList)
                        ? ((ByteMappedBigList) nodeToSwhMap).copy()
                        : nodeToSwhMap),
                mph, ((orderMap instanceof LongMappedBigList) ? ((LongMappedBigList) orderMap).copy() : orderMap));
    }

    @SuppressWarnings("unchecked")
    public static Object2LongFunction<byte[]> loadMph(String path) throws IOException {
        Object obj;
        try {
            obj = BinIO.loadObject(path);
        } catch (ClassNotFoundException e) {
            throw new IOException(e.getMessage());
        }

        Object2LongFunction<byte[]> res = (Object2LongFunction<byte[]>) obj;

        // Backward-compatibility for old maps parametrized with <String>.
        // New maps should be parametrized with <byte[]>, which is faster.
        try {
            // Try to call it with bytes, will fail if it's a O2LF<String>.
            res.getLong("42".getBytes(StandardCharsets.UTF_8));
        } catch (ClassCastException e) {
            class StringCompatibleByteFunction implements Object2LongFunction<byte[]>, Size64 {
                private final Object2LongFunction<String> legacyFunction;

                public StringCompatibleByteFunction(Object2LongFunction<String> legacyFunction) {
                    this.legacyFunction = legacyFunction;
                }

                @Override
                public long getLong(Object o) {
                    byte[] bi = (byte[]) o;
                    return legacyFunction.getLong(new String(bi, StandardCharsets.UTF_8));
                }

                @SuppressWarnings("deprecation")
                @Override
                public int size() {
                    return legacyFunction.size();
                }

                @Override
                public long size64() {
                    return (legacyFunction instanceof Size64)
                            ? ((Size64) legacyFunction).size64()
                            : legacyFunction.size();
                }
            }

            Object2LongFunction<String> mphLegacy = (Object2LongFunction<String>) obj;
            return new StringCompatibleByteFunction(mphLegacy);
        }
        // End of backward-compatibility block

        return res;
    }

    /**
     * Converts byte-form SWHID to corresponding long node id. Low-level function, does not check if the
     * SWHID is valid.
     *
     * @param swhid node represented as bytes
     * @return corresponding node as a long id
     */
    public long getNodeId(byte[] swhid) {
        // 1. Hash the SWHID with the MPH to get its original ID
        long origNodeId = mph.getLong(swhid);

        // 2. Use the order permutation to get the position in the permuted graph
        return this.orderMap.getLong(origNodeId);
    }

    /**
     * Converts String-form SWHID to corresponding long node id. Low-level function, does not check if
     * the SWHID is valid.
     *
     * @param swhid node represented as String
     * @return corresponding node as a long id
     */
    public long getNodeId(String swhid) {
        return getNodeId(swhid.getBytes(StandardCharsets.US_ASCII));
    }

    /**
     * Converts SWHID to corresponding long node id.
     *
     * @param swhid node represented as a {@link SWHID}
     * @param checkExists if true, error if the SWHID is not present in the graph, if false the check
     *            will be skipped and invalid data will be returned for non-existing SWHIDs.
     * @return corresponding node as a long id
     * @see SWHID
     */
    public long getNodeId(SWHID swhid, boolean checkExists) {
        // Convert the SWHID to bytes and call getNodeId()
        long nodeId = getNodeId(swhid.toString().getBytes(StandardCharsets.US_ASCII));

        // Check that the position effectively corresponds to a real node using the reverse map.
        // This is necessary because the MPH makes no guarantees on whether the input SWHID is valid.
        if (!checkExists || getSWHID(nodeId).equals(swhid)) {
            return nodeId;
        } else {
            throw new IllegalArgumentException("Unknown SWHID: " + swhid);
        }
    }

    public long getNodeId(SWHID swhid) {
        return getNodeId(swhid, true);
    }

    /**
     * Converts a node long id to corresponding SWHID.
     *
     * @param nodeId node as a long id
     * @return corresponding node as a {@link SWHID}
     * @see SWHID
     */
    public SWHID getSWHID(long nodeId) {
        /*
         * Each line in NODE_TO_SWHID is formatted as: swhid The file is ordered by nodeId, meaning node0's
         * swhid is at line 0, hence we can read the nodeId-th line to get corresponding swhid
         */
        if (nodeId < 0 || nodeId >= size64()) {
            throw new IllegalArgumentException("Node id " + nodeId + " should be between 0 and " + size64());
        }

        byte[] swhid = new byte[SWHID_BIN_SIZE];
        nodeToSwhMap.getElements(nodeId * SWHID_BIN_SIZE, swhid, 0, SWHID_BIN_SIZE);
        return SWHID.fromBytes(swhid);
    }

    /** Return the number of nodes in the map. */
    @Override
    public long size64() {
        return nodeToSwhMap.size64() / SWHID_BIN_SIZE;
    }
}
