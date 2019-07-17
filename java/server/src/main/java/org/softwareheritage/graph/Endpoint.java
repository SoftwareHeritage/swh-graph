package org.softwareheritage.graph;

import java.util.ArrayList;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.SwhPath;
import org.softwareheritage.graph.algo.Traversal;

/**
 * REST API endpoints wrapper functions.
 *
 * @author Thibault Allançon
 * @version 1.0
 * @since 1.0
 */

public class Endpoint {
  /** Graph where traversal endpoint is performed */
  Graph graph;
  /** Internal traversal API */
  Traversal traversal;

  /**
   * Constructor.
   *
   * @param graph the graph used for traversal endpoint
   * @param direction a string (either "forward" or "backward") specifying edge orientation
   * @param edgesFmt a formatted string describing allowed edges (TODO: link API doc)
   */
  public Endpoint(Graph graph, String direction, String edgesFmt) {
    this.graph = graph;
    this.traversal = new Traversal(graph, direction, edgesFmt);
  }

  /**
   * Converts a list of (internal) long node ids to a list of corresponding (external) SWH PIDs.
   *
   * @param nodeIds the list of long node ids
   * @return a list of corresponding SWH PIDs
   */
  private ArrayList<SwhId> convertNodesToSwhIds(ArrayList<Long> nodeIds) {
    ArrayList<SwhId> swhIds = new ArrayList<>();
    for (long nodeId : nodeIds) {
      swhIds.add(graph.getSwhId(nodeId));
    }
    return swhIds;
  }

  /**
   * Converts a list of (internal) long node ids to the corresponding {@link SwhPath}.
   *
   * @param nodeIds the list of long node ids
   * @return the corresponding {@link SwhPath}
   * @see org.softwareheritage.graph.SwhPath
   */
  private SwhPath convertNodesToSwhPath(ArrayList<Long> nodeIds) {
    SwhPath path = new SwhPath();
    for (long nodeId : nodeIds) {
      path.add(graph.getSwhId(nodeId));
    }
    return path;
  }

  /**
   * Converts a list of paths made of (internal) long node ids to one made of {@link SwhPath}-s.
   *
   * @param pathsNodeId the list of paths with long node ids
   * @return a list of corresponding {@link SwhPath}
   * @see org.softwareheritage.graph.SwhPath
   */
  private ArrayList<SwhPath> convertPathsToSwhIds(ArrayList<ArrayList<Long>> pathsNodeId) {
    ArrayList<SwhPath> paths = new ArrayList<>();
    for (ArrayList<Long> path : pathsNodeId) {
      paths.add(convertNodesToSwhPath(path));
    }
    return paths;
  }

  /**
   * Leaves endpoint wrapper (performs input/output node ids conversions).
   *
   * @param src source node of endpoint call specified as a SwhId
   * @return the resulting list of SwhId from endpoint call
   * @see org.softwareheritage.graph.SwhId
   * @see org.softwareheritage.graph.algo.Traversal#leaves(long)
   */
  public ArrayList<SwhId> leaves(SwhId src) {
    long srcNodeId = graph.getNodeId(src);
    ArrayList<Long> nodeIds = traversal.leaves(srcNodeId);
    return convertNodesToSwhIds(nodeIds);
  }

  /**
   * Neighbors endpoint wrapper (performs input/output node ids conversions).
   *
   * @param src source node of endpoint call specified as a SwhId
   * @return the resulting list of SwhId from endpoint call
   * @see org.softwareheritage.graph.SwhId
   * @see org.softwareheritage.graph.algo.Traversal#neighbors(long)
   */
  public ArrayList<SwhId> neighbors(SwhId src) {
    long srcNodeId = graph.getNodeId(src);
    ArrayList<Long> nodeIds = traversal.neighbors(srcNodeId);
    return convertNodesToSwhIds(nodeIds);
  }

  /**
   * Walk endpoint wrapper (performs input/output node ids conversions).
   *
   * @param src source node of endpoint call specified as a SwhId
   * @param dstFmt destination used in endpoint call (TODO: link to API doc)
   * @param algorithm traversal algorithm used in endpoint call (either "dfs" or "bfs")
   * @return the resulting {@link SwhPath} from endpoint call
   * @see org.softwareheritage.graph.SwhId
   * @see org.softwareheritage.graph.SwhPath
   * @see org.softwareheritage.graph.algo.Traversal#walk
   */
  public SwhPath walk(SwhId src, String dstFmt, String algorithm) {
    long srcNodeId = graph.getNodeId(src);
    ArrayList<Long> nodeIds = null;

    // Destination is either a SWH ID or a node type
    try {
      SwhId dstSwhId = new SwhId(dstFmt);
      long dstNodeId = graph.getNodeId(dstSwhId);
      nodeIds = traversal.walk(srcNodeId, dstNodeId, algorithm);
    } catch (IllegalArgumentException ignored1) {
      try {
        Node.Type dstType = Node.Type.fromStr(dstFmt);
        nodeIds = traversal.walk(srcNodeId, dstType, algorithm);
      } catch (IllegalArgumentException ignored2) { }
    }

    return convertNodesToSwhPath(nodeIds);
  }

  /**
   * VisitNodes endpoint wrapper (performs input/output node ids conversions).
   *
   * @param src source node of endpoint call specified as a SwhId
   * @return the resulting list of SwhId from endpoint call
   * @see org.softwareheritage.graph.SwhId
   * @see org.softwareheritage.graph.algo.Traversal#visitNodes(long)
   */
  public ArrayList<SwhId> visitNodes(SwhId src) {
    long srcNodeId = graph.getNodeId(src);
    ArrayList<Long> nodeIds = traversal.visitNodes(srcNodeId);
    return convertNodesToSwhIds(nodeIds);
  }

  /**
   * VisitPaths endpoint wrapper (performs input/output node ids conversions).
   *
   * @param src source node of endpoint call specified as a SwhId
   * @return the resulting list of {@link SwhPath} from endpoint call
   * @see org.softwareheritage.graph.SwhId
   * @see org.softwareheritage.graph.SwhPath
   * @see org.softwareheritage.graph.algo.Traversal#visitPaths(long)
   */
  public ArrayList<SwhPath> visitPaths(SwhId src) {
    long srcNodeId = graph.getNodeId(src);
    ArrayList<ArrayList<Long>> paths = traversal.visitPaths(srcNodeId);
    return convertPathsToSwhIds(paths);
  }
}
