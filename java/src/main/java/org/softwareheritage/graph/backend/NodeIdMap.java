package org.softwareheritage.graph.backend;

import java.io.IOException;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.SwhPID;
import org.softwareheritage.graph.backend.MapFile;

/**
 * Mapping between internal long node id and external SWH PID.
 *
 * Mappings in both directions are pre-computed and dumped on disk in the
 * {@link MapBuilder} class, then they are loaded here using mmap().
 *
 * @author The Software Heritage developers
 * @see org.softwareheritage.graph.backend.MapBuilder
 */

public class NodeIdMap {
    /** Fixed length of full SWH PID */
    public static final int SWH_ID_LENGTH = 50;
    /** Fixed length of long node id */
    public static final int NODE_ID_LENGTH = 20;

    /** Graph path and basename */
    String graphPath;
    /** Number of ids to map */
    long nbIds;
    /** mmap()-ed PID_TO_NODE file */
    MapFile swhToNodeMap;
    /** mmap()-ed NODE_TO_PID file */
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

        // +1 are for spaces and end of lines
        int swhToNodeLineLength = SWH_ID_LENGTH + 1 + NODE_ID_LENGTH + 1;
        int nodeToSwhLineLength = SWH_ID_LENGTH + 1;
        this.swhToNodeMap = new MapFile(graphPath + Graph.PID_TO_NODE, swhToNodeLineLength);
        this.nodeToSwhMap = new MapFile(graphPath + Graph.NODE_TO_PID, nodeToSwhLineLength);
    }

    /**
     * Converts SWH PID to corresponding long node id.
     *
     * @param swhPID node represented as a {@link SwhPID}
     * @return corresponding node as a long id
     * @see org.softwareheritage.graph.SwhPID
     */
    public long getNodeId(SwhPID swhPID) {
        // Each line in PID_TO_NODE is formatted as: swhPID nodeId
        // The file is sorted by swhPID, hence we can binary search on swhPID to get corresponding
        // nodeId
        long start = 0;
        long end = nbIds - 1;

        while (start <= end) {
            long lineNumber = (start + end) / 2L;
            String[] parts = swhToNodeMap.readAtLine(lineNumber).split(" ");
            if (parts.length != 2) {
                break;
            }

            String currentSwhPID = parts[0];
            long currentNodeId = Long.parseLong(parts[1]);

            int cmp = currentSwhPID.compareTo(swhPID.toString());
            if (cmp == 0) {
                return currentNodeId;
            } else if (cmp < 0) {
                start = lineNumber + 1;
            } else {
                end = lineNumber - 1;
            }
        }

        throw new IllegalArgumentException("Unknown SWH PID: " + swhPID);
    }

    /**
     * Converts a node long id to corresponding SWH PID.
     *
     * @param nodeId node as a long id
     * @return corresponding node as a {@link SwhPID}
     * @see org.softwareheritage.graph.SwhPID
     */
    public SwhPID getSwhPID(long nodeId) {
        // Each line in NODE_TO_PID is formatted as: swhPID
        // The file is ordered by nodeId, meaning node0's swhPID is at line 0, hence we can read the
        // nodeId-th line to get corresponding swhPID
        if (nodeId < 0 || nodeId >= nbIds) {
            throw new IllegalArgumentException("Node id " + nodeId + " should be between 0 and " + nbIds);
        }

        String swhPID = nodeToSwhMap.readAtLine(nodeId);
        return new SwhPID(swhPID);
    }

    /**
     * Closes the mapping files.
     */
    public void close() throws IOException {
        swhToNodeMap.close();
        nodeToSwhMap.close();
    }
}
