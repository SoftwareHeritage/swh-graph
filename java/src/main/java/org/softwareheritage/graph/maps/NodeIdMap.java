package org.softwareheritage.graph.maps;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.SWHID;

import java.io.IOException;

/**
 * Mapping between internal long node id and external SWHID.
 * <p>
 * Mappings in both directions are pre-computed and dumped on disk in the {@link NodeMapBuilder}
 * class, then they are loaded here using mmap().
 *
 * @author The Software Heritage developers
 * @see NodeMapBuilder
 */

public class NodeIdMap {
    /** Fixed length of full SWHID */
    public static final int SWHID_LENGTH = 50;
    /** Fixed length of long node id */
    public static final int NODE_ID_LENGTH = 20;

    /** Fixed length of binary SWHID buffer */
    public static final int SWHID_BIN_SIZE = 22;
    /** Fixed length of binary node id buffer */
    public static final int NODE_ID_BIN_SIZE = 8;

    /** Graph path and basename */
    String graphPath;
    /** Number of ids to map */
    long nbIds;
    /** mmap()-ed SWHID_TO_NODE file */
    MapFile swhToNodeMap;
    /** mmap()-ed NODE_TO_SWHID file */
    MapFile nodeToSwhMap;

    /**
     * Constructor.
     *
     * @param graphPath full graph path
     * @param nbNodes number of nodes in the graph
     */
    public NodeIdMap(String graphPath, long nbNodes) throws IOException {
        this.graphPath = graphPath;
        this.nbIds = nbNodes;

        this.swhToNodeMap = new MapFile(graphPath + Graph.SWHID_TO_NODE, SWHID_BIN_SIZE + NODE_ID_BIN_SIZE);
        this.nodeToSwhMap = new MapFile(graphPath + Graph.NODE_TO_SWHID, SWHID_BIN_SIZE);
    }

    /**
     * Converts SWHID to corresponding long node id.
     *
     * @param swhid node represented as a {@link SWHID}
     * @return corresponding node as a long id
     * @see SWHID
     */
    public long getNodeId(SWHID swhid) {
        /*
         * The file is sorted by swhid, hence we can binary search on swhid to get corresponding nodeId
         */
        long start = 0;
        long end = nbIds - 1;

        while (start <= end) {
            long lineNumber = (start + end) / 2L;
            byte[] buffer = swhToNodeMap.readAtLine(lineNumber);
            byte[] swhidBuffer = new byte[SWHID_BIN_SIZE];
            byte[] nodeBuffer = new byte[NODE_ID_BIN_SIZE];
            System.arraycopy(buffer, 0, swhidBuffer, 0, SWHID_BIN_SIZE);
            System.arraycopy(buffer, SWHID_BIN_SIZE, nodeBuffer, 0, NODE_ID_BIN_SIZE);

            String currentSWHID = SWHID.fromBytes(swhidBuffer).getSWHID();
            long currentNodeId = java.nio.ByteBuffer.wrap(nodeBuffer).getLong();

            int cmp = currentSWHID.compareTo(swhid.toString());
            if (cmp == 0) {
                return currentNodeId;
            } else if (cmp < 0) {
                start = lineNumber + 1;
            } else {
                end = lineNumber - 1;
            }
        }

        throw new IllegalArgumentException("Unknown SWHID: " + swhid);
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
        if (nodeId < 0 || nodeId >= nbIds) {
            throw new IllegalArgumentException("Node id " + nodeId + " should be between 0 and " + nbIds);
        }

        return SWHID.fromBytes(nodeToSwhMap.readAtLine(nodeId));
    }

    /**
     * Closes the mapping files.
     */
    public void close() throws IOException {
        swhToNodeMap.close();
        nodeToSwhMap.close();
    }
}
