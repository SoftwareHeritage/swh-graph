package org.softwareheritage.graph;

import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.SwhPath;
import org.softwareheritage.graph.algo.Traversal;
import org.softwareheritage.graph.utils.Timing;

/**
 * REST API endpoints wrapper functions.
 * <p>
 * Graph operations are segmented between high-level class (this one) and the low-level class
 * ({@link Traversal}). The {@link Endpoint} class creates wrappers for each endpoints by performing
 * all the input/output node ids conversions and logging timings.
 *
 * @author Thibault Allançon
 * @version 0.0.1
 * @since 0.0.1
 * @see org.softwareheritage.graph.algo.Traversal
 */

public class Endpoint {
  /** Graph where traversal endpoint is performed */
  Graph graph;
  /** Internal traversal API */
  Traversal traversal;

  /** Timings logger */
  private static final Logger logger = LoggerFactory.getLogger(Endpoint.class);

  /**
   * Constructor.
   *
   * @param graph the graph used for traversal endpoint
   * @param direction a string (either "forward" or "backward") specifying edge orientation
   * @param edgesFmt a formatted string describing <a
   * href="https://docs.softwareheritage.org/devel/swh-graph/api.html#terminology">allowed edges</a>
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
    long startTime = Timing.start();
    ArrayList<SwhId> swhIds = new ArrayList<>();
    for (long nodeId : nodeIds) {
      swhIds.add(graph.getSwhId(nodeId));
    }
    float duration = Timing.stop(startTime);
    logger.debug("convertNodesToSwhIds() took {} s.", duration);
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
    long startTime = Timing.start();
    SwhPath path = new SwhPath();
    for (long nodeId : nodeIds) {
      path.add(graph.getSwhId(nodeId));
    }
    float duration = Timing.stop(startTime);
    logger.debug("convertNodesToSwhPath() took {} s.", duration);
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
    long startTime = Timing.start();
    ArrayList<SwhPath> paths = new ArrayList<>();
    for (ArrayList<Long> path : pathsNodeId) {
      paths.add(convertNodesToSwhPath(path));
    }
    float duration = Timing.stop(startTime);
    logger.debug("convertPathsToSwhIds() took {} s.", duration);
    return paths;
  }

  /**
   * Leaves endpoint wrapper.
   *
   * @param src source node of endpoint call specified as a {@link SwhId}
   * @return the resulting list of {@link SwhId} from endpoint call
   * @see org.softwareheritage.graph.SwhId
   * @see org.softwareheritage.graph.algo.Traversal#leaves(long)
   */
  public ArrayList<SwhId> leaves(SwhId src) {
    long srcNodeId = graph.getNodeId(src);

    long startTime = Timing.start();
    ArrayList<Long> nodeIds = traversal.leaves(srcNodeId);
    float duration = Timing.stop(startTime);
    logger.debug("leaves({}) took {} s.", src, duration);

    return convertNodesToSwhIds(nodeIds);
  }

  /**
   * Neighbors endpoint wrapper.
   *
   * @param src source node of endpoint call specified as a {@link SwhId}
   * @return the resulting list of {@link SwhId} from endpoint call
   * @see org.softwareheritage.graph.SwhId
   * @see org.softwareheritage.graph.algo.Traversal#neighbors(long)
   */
  public ArrayList<SwhId> neighbors(SwhId src) {
    long srcNodeId = graph.getNodeId(src);

    long startTime = Timing.start();
    ArrayList<Long> nodeIds = traversal.neighbors(srcNodeId);
    float duration = Timing.stop(startTime);
    logger.debug("neighbors({}) took {} s.", src, duration);

    return convertNodesToSwhIds(nodeIds);
  }

  /**
   * Walk endpoint wrapper.
   *
   * @param src source node of endpoint call specified as a {@link SwhId}
   * @param dstFmt destination formatted string as described in the <a
   * href="https://docs.softwareheritage.org/devel/swh-graph/api.html#walk">API</a>
   * @param algorithm traversal algorithm used in endpoint call (either "dfs" or "bfs")
   * @return the resulting {@link SwhPath} from endpoint call
   * @see org.softwareheritage.graph.SwhId
   * @see org.softwareheritage.graph.SwhPath
   * @see org.softwareheritage.graph.algo.Traversal#walk
   */
  public SwhPath walk(SwhId src, String dstFmt, String algorithm) {
    long srcNodeId = graph.getNodeId(src);
    ArrayList<Long> nodeIds = new ArrayList<Long>();

    // Destination is either a SWH ID or a node type
    try {
      SwhId dstSwhId = new SwhId(dstFmt);
      long dstNodeId = graph.getNodeId(dstSwhId);

      long startTime = Timing.start();
      nodeIds = traversal.walk(srcNodeId, dstNodeId, algorithm);
      float duration = Timing.stop(startTime);
      logger.debug("walk({}) took {} s.", src, duration);
    } catch (IllegalArgumentException ignored1) {
      try {
        Node.Type dstType = Node.Type.fromStr(dstFmt);

        long startTime = Timing.start();
        nodeIds = traversal.walk(srcNodeId, dstType, algorithm);
        float duration = Timing.stop(startTime);
        logger.debug("walk({}) took {} s.", src, duration);
      } catch (IllegalArgumentException ignored2) { }
    }

    return convertNodesToSwhPath(nodeIds);
  }

  /**
   * VisitNodes endpoint wrapper.
   *
   * @param src source node of endpoint call specified as a {@link SwhId}
   * @return the resulting list of {@link SwhId} from endpoint call
   * @see org.softwareheritage.graph.SwhId
   * @see org.softwareheritage.graph.algo.Traversal#visitNodes(long)
   */
  public ArrayList<SwhId> visitNodes(SwhId src) {
    long srcNodeId = graph.getNodeId(src);

    long startTime = Timing.start();
    ArrayList<Long> nodeIds = traversal.visitNodes(srcNodeId);
    float duration = Timing.stop(startTime);
    logger.debug("visitNodes({}) took {} s.", src, duration);

    return convertNodesToSwhIds(nodeIds);
  }

  /**
   * VisitPaths endpoint wrapper.
   *
   * @param src source node of endpoint call specified as a {@link SwhId}
   * @return the resulting list of {@link SwhPath} from endpoint call
   * @see org.softwareheritage.graph.SwhId
   * @see org.softwareheritage.graph.SwhPath
   * @see org.softwareheritage.graph.algo.Traversal#visitPaths(long)
   */
  public ArrayList<SwhPath> visitPaths(SwhId src) {
    long srcNodeId = graph.getNodeId(src);

    long startTime = Timing.start();
    ArrayList<ArrayList<Long>> paths = traversal.visitPaths(srcNodeId);
    float duration = Timing.stop(startTime);
    logger.debug("visitPaths({}) took {} s.", src, duration);

    return convertPathsToSwhIds(paths);
  }
}
