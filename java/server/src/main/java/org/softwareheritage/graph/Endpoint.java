package org.softwareheritage.graph;

import java.util.ArrayList;

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
  /**
   * Wrapper class to return both the endpoint result and metadata (such as timings).
   */
  public class Output<T> {
    /** The result content itself */
    public T result;
    /** Various metadata about the result */
    public Meta meta;

    public Output() {
      this.result = null;
      this.meta = new Meta();
    }

    /**
    * Endpoint result metadata.
    */
    public class Meta {
      /** Operations timings */
      public Timings timings;

      public Meta() {
        this.timings = new Timings();
      }

      /**
      * Wrapper class for JSON output format.
      */
      public class Timings {
        /** Time in seconds to do the traversal */
        public float traversal;
        /** Time in seconds to convert input SWH PID to node id */
        public float pid2node;
        /** Time in seconds to convert output node ids to SWH PIDs */
        public float node2pid;
      }
    }
  }

  /** Graph where traversal endpoint is performed */
  Graph graph;
  /** Internal traversal API */
  Traversal traversal;

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
   * Leaves endpoint wrapper.
   *
   * @param src source node of endpoint call specified as a {@link SwhId}
   * @return the resulting list of {@link SwhId} from endpoint call and operation metadata
   * @see org.softwareheritage.graph.SwhId
   * @see org.softwareheritage.graph.algo.Traversal#leaves(long)
   */
  public Output leaves(SwhId src) {
    Output<ArrayList<SwhId>> output = new Output<>();
    long startTime;

    startTime = Timing.start();
    long srcNodeId = graph.getNodeId(src);
    output.meta.timings.pid2node = Timing.stop(startTime);

    startTime = Timing.start();
    ArrayList<Long> nodeIds = traversal.leaves(srcNodeId);
    output.meta.timings.traversal = Timing.stop(startTime);

    startTime = Timing.start();
    output.result = convertNodesToSwhIds(nodeIds);
    output.meta.timings.node2pid = Timing.stop(startTime);

    return output;
  }

  /**
   * Neighbors endpoint wrapper.
   *
   * @param src source node of endpoint call specified as a {@link SwhId}
   * @return the resulting list of {@link SwhId} from endpoint call and operation metadata
   * @see org.softwareheritage.graph.SwhId
   * @see org.softwareheritage.graph.algo.Traversal#neighbors(long)
   */
  public Output neighbors(SwhId src) {
    Output<ArrayList<SwhId>> output = new Output<>();
    long startTime;

    startTime = Timing.start();
    long srcNodeId = graph.getNodeId(src);
    output.meta.timings.pid2node = Timing.stop(startTime);

    startTime = Timing.start();
    ArrayList<Long> nodeIds = traversal.neighbors(srcNodeId);
    output.meta.timings.traversal = Timing.stop(startTime);

    startTime = Timing.start();
    output.result = convertNodesToSwhIds(nodeIds);
    output.meta.timings.node2pid = Timing.stop(startTime);

    return output;
  }

  /**
   * Walk endpoint wrapper.
   *
   * @param src source node of endpoint call specified as a {@link SwhId}
   * @param dstFmt destination formatted string as described in the <a
   * href="https://docs.softwareheritage.org/devel/swh-graph/api.html#walk">API</a>
   * @param algorithm traversal algorithm used in endpoint call (either "dfs" or "bfs")
   * @return the resulting {@link SwhPath} from endpoint call and operation metadata
   * @see org.softwareheritage.graph.SwhId
   * @see org.softwareheritage.graph.SwhPath
   * @see org.softwareheritage.graph.algo.Traversal#walk
   */
  public Output walk(SwhId src, String dstFmt, String algorithm) {
    Output<SwhPath> output = new Output<>();
    long startTime;

    startTime = Timing.start();
    long srcNodeId = graph.getNodeId(src);
    output.meta.timings.pid2node = Timing.stop(startTime);

    ArrayList<Long> nodeIds = new ArrayList<Long>();

    // Destination is either a SWH ID or a node type
    try {
      SwhId dstSwhId = new SwhId(dstFmt);
      long dstNodeId = graph.getNodeId(dstSwhId);

      startTime = Timing.start();
      nodeIds = traversal.walk(srcNodeId, dstNodeId, algorithm);
      output.meta.timings.traversal = Timing.stop(startTime);
    } catch (IllegalArgumentException ignored1) {
      try {
        Node.Type dstType = Node.Type.fromStr(dstFmt);

        startTime = Timing.start();
        nodeIds = traversal.walk(srcNodeId, dstType, algorithm);
        output.meta.timings.traversal = Timing.stop(startTime);
      } catch (IllegalArgumentException ignored2) { }
    }

    startTime = Timing.start();
    output.result = convertNodesToSwhPath(nodeIds);
    output.meta.timings.node2pid = Timing.stop(startTime);

    return output;
  }

  /**
   * VisitNodes endpoint wrapper.
   *
   * @param src source node of endpoint call specified as a {@link SwhId}
   * @return the resulting list of {@link SwhId} from endpoint call and operation metadata
   * @see org.softwareheritage.graph.SwhId
   * @see org.softwareheritage.graph.algo.Traversal#visitNodes(long)
   */
  public Output visitNodes(SwhId src) {
    Output<ArrayList<SwhId>> output = new Output<>();
    long startTime;

    startTime = Timing.start();
    long srcNodeId = graph.getNodeId(src);
    output.meta.timings.pid2node = Timing.stop(startTime);

    startTime = Timing.start();
    ArrayList<Long> nodeIds = traversal.visitNodes(srcNodeId);
    output.meta.timings.traversal = Timing.stop(startTime);

    startTime = Timing.start();
    output.result = convertNodesToSwhIds(nodeIds);
    output.meta.timings.node2pid = Timing.stop(startTime);

    return output;
  }

  /**
   * VisitPaths endpoint wrapper.
   *
   * @param src source node of endpoint call specified as a {@link SwhId}
   * @return the resulting list of {@link SwhPath} from endpoint call and operation metadata
   * @see org.softwareheritage.graph.SwhId
   * @see org.softwareheritage.graph.SwhPath
   * @see org.softwareheritage.graph.algo.Traversal#visitPaths(long)
   */
  public Output visitPaths(SwhId src) {
    Output<ArrayList<SwhPath>> output = new Output<>();
    long startTime;

    startTime = Timing.start();
    long srcNodeId = graph.getNodeId(src);
    output.meta.timings.pid2node = Timing.stop(startTime);

    startTime = Timing.start();
    ArrayList<ArrayList<Long>> paths = traversal.visitPaths(srcNodeId);
    output.meta.timings.traversal = Timing.stop(startTime);

    startTime = Timing.start();
    output.result = convertPathsToSwhIds(paths);
    output.meta.timings.node2pid = Timing.stop(startTime);

    return output;
  }
}
