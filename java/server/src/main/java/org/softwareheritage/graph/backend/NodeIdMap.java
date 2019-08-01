package org.softwareheritage.graph.backend;

import java.io.IOException;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.backend.MapFile;

/**
 * Mapping between internal long node id and external SWH PID.
 *
 * @author Thibault Allançon
 * @version 0.0.1
 * @since 0.0.1
 */

public class NodeIdMap {
  /** Fixed length of full SWH PID */
  public static final int SWH_ID_LENGTH = 50;
  /** Fixed length of long node id */
  public static final int NODE_ID_LENGTH = 20;

  /** Full graph path */
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
   * @param swhId node represented as a {@link SwhId}
   * @return corresponding node as a long id
   * @see org.softwareheritage.graph.SwhId
   */
  public long getNodeId(SwhId swhId) {
    // Each line in PID_TO_NODE is formatted as: swhId nodeId
    // The file is sorted by swhId, hence we can binary search on swhId to get corresponding nodeId
    long start = 0;
    long end = nbIds - 1;

    while (start <= end) {
      long lineNumber = (start + end) / 2L;
      String[] parts = swhToNodeMap.readAtLine(lineNumber).split(" ");
      if (parts.length != 2) {
        break;
      }

      String currentSwhId = parts[0];
      long currentNodeId = Long.parseLong(parts[1]);

      int cmp = currentSwhId.compareTo(swhId.toString());
      if (cmp == 0) {
        return currentNodeId;
      } else if (cmp < 0) {
        start = lineNumber + 1;
      } else {
        end = lineNumber - 1;
      }
    }

    throw new IllegalArgumentException("Unknown SWH id: " + swhId);
  }

  /**
   * Converts a node long id to corresponding SWH PID.
   *
   * @param nodeId node as a long id
   * @return corresponding node as a {@link SwhId}
   * @see org.softwareheritage.graph.SwhId
   */
  public SwhId getSwhId(long nodeId) {
    // Each line in NODE_TO_PID is formatted as: swhId
    // The file is ordered by nodeId, meaning node0's swhId is at line 0, hence we can read the
    // nodeId-th line to get corresponding swhId
    if (nodeId < 0 || nodeId >= nbIds) {
      throw new IllegalArgumentException("Node id " + nodeId + " should be between 0 and " + nbIds);
    }

    String swhId = nodeToSwhMap.readAtLine(nodeId);
    return new SwhId(swhId);
  }

  /**
   * Closes the mapping files.
   */
  public void close() throws IOException {
    swhToNodeMap.close();
    nodeToSwhMap.close();
  }
}
