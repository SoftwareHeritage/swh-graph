package org.softwareheritage.graph.backend;

import java.io.IOException;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.SWHID;
import org.softwareheritage.graph.backend.MapFile;

/**
 * Mapping between internal long node id and external SWHID.
 *
 * Mappings in both directions are pre-computed and dumped on disk in the
 * {@link MapBuilder} class, then they are loaded here using mmap().
 *
 * @author The Software Heritage developers
 * @see org.softwareheritage.graph.backend.MapBuilder
 */

public class NodeIdMap {
    /** Fixed length of full SWHID */
    public static final int SWH_ID_LENGTH = 50;
    /** Fixed length of long node id */
    public static final int NODE_ID_LENGTH = 20;

    /** Graph path and basename */
    String graphPath;
    /** Number of ids to map */
    long nbIds;
    /** mmap()-ed SWHID_TO_NODE file */
    MapFile swhidToNodeMap;
    /** mmap()-ed NODE_TO_SWHID file */
    MapFile nodeToSwhidMap;

    /**
     * Constructor.
     *
     * @param graphPath full graph path
     * @param nbNodes number of nodes in the graph
     */
    public NodeIdMap(String graphPath, long nbNodes) throws IOException {
        this.graphPath = graphPath;
        this.nbIds = nbNodes;

        // +1 are for spaces and end of lines
        int swhidToNodeLineLength = SWH_ID_LENGTH + 1 + NODE_ID_LENGTH + 1;
        int nodeToSwhidLineLength = SWH_ID_LENGTH + 1;
        this.swhidToNodeMap = new MapFile(graphPath + Graph.SWHID_TO_NODE, swhidToNodeLineLength);
        this.nodeToSwhidMap = new MapFile(graphPath + Graph.NODE_TO_SWHID, nodeToSwhidLineLength);
    }

    /**
     * Converts SWHID to corresponding long node id.
     *
     * @param swhid node represented as a {@link SWHID}
     * @return corresponding node as a long id
     * @see org.softwareheritage.graph.SWHID
     */
    public long getNodeId(SWHID swhid) {
        // Each line in SWHID_TO_NODE is formatted as: swhid nodeId
        // The file is sorted by swhid, hence we can binary search on swhid to get corresponding
        // nodeId
        long start = 0;
        long end = nbIds - 1;

        while (start <= end) {
            long lineNumber = (start + end) / 2L;
            String[] parts = swhidToNodeMap.readAtLine(lineNumber).split(" ");
            if (parts.length != 2) {
                break;
            }

            String currentSWHID = parts[0];
            long currentNodeId = Long.parseLong(parts[1]);

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
     * @see org.softwareheritage.graph.SWHID
     */
    public SWHID getSWHID(long nodeId) {
        // Each line in NODE_TO_SWHID is formatted as: SWHID
        // The file is ordered by nodeId, meaning node0's swhid is at line 0, hence we can read the
        // nodeId-th line to get corresponding swhid
        if (nodeId < 0 || nodeId >= nbIds) {
            throw new IllegalArgumentException("Node id " + nodeId + " should be between 0 and " + nbIds);
        }

        String swhid = nodeToSwhidMap.readAtLine(nodeId);
        return new SWHID(swhid);
    }

    /**
     * Closes the mapping files.
     */
    public void close() throws IOException {
        swhidToNodeMap.close();
        nodeToSwhidMap.close();
    }
}
