package org.softwareheritage.graph.algo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;

import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.longs.LongBigArrays;

import org.softwareheritage.graph.AllowedEdges;
import org.softwareheritage.graph.Endpoint;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Neighbors;
import org.softwareheritage.graph.Node;

/**
 * Traversal algorithms on the compressed graph.
 * <p>
 * Internal implementation of the traversal API endpoints. These methods only input/output internal
 * long ids, which are converted in the {@link Endpoint} higher-level class to Software Heritage
 * PID.
 *
 * @author Thibault Allançon
 * @version 0.0.1
 * @since 0.0.1
 * @see org.softwareheritage.graph.Endpoint
 */

public class Traversal {
  /** Graph used in the traversal */
  Graph graph;
  /** Boolean to specify the use of the transposed graph */
  boolean useTransposed;
  /** Graph edge restriction */
  AllowedEdges edges;

  /** Bit array storing if we have visited a node */
  LongArrayBitVector visited;
  /** LongBigArray storing parent node id for each nodes during a traversal */
  long[][] nodeParent;
  /** Number of edges accessed during traversal */
  long nbEdgesAccessed;

  /**
   * Constructor.
   *
   * @param graph graph used in the traversal
   * @param direction a string (either "forward" or "backward") specifying edge orientation
   * @param edgesFmt a formatted string describing <a
   * href="https://docs.softwareheritage.org/devel/swh-graph/api.html#terminology">allowed edges</a>
   */
  public Traversal(Graph graph, String direction, String edgesFmt) {
    if (!direction.matches("forward|backward")) {
      throw new IllegalArgumentException("Unknown traversal direction: " + direction);
    }

    this.graph = graph;
    this.useTransposed = (direction.equals("backward"));
    this.edges = new AllowedEdges(graph, edgesFmt);

    long nbNodes = graph.getNbNodes();
    this.visited = LongArrayBitVector.ofLength(nbNodes);
    this.nodeParent = LongBigArrays.newBigArray(nbNodes);
    this.nbEdgesAccessed = 0;
  }

  /**
   * Returns number of accessed edges during traversal.
   *
   * @return number of edges accessed in last traversal
   */
  public long getnbEdgesAccessed() {
    return nbEdgesAccessed;
  }

  /**
   * Returns the leaves of a subgraph rooted at the specified source node.
   *
   * @param srcNodeId source node
   * @return list of node ids corresponding to the leaves
   */
  public ArrayList<Long> leaves(long srcNodeId) {
    ArrayList<Long> nodeIds = new ArrayList<Long>();
    Stack<Long> stack = new Stack<Long>();
    this.visited.fill(false);
    this.nbEdgesAccessed = 0;

    stack.push(srcNodeId);
    visited.set(srcNodeId);

    while (!stack.isEmpty()) {
      long currentNodeId = stack.pop();

      long neighborsCnt = 0;
      for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, currentNodeId)) {
        neighborsCnt++;
        nbEdgesAccessed++;
        if (!visited.getBoolean(neighborNodeId)) {
          stack.push(neighborNodeId);
          visited.set(neighborNodeId);
        }
      }

      if (neighborsCnt == 0) {
        nodeIds.add(currentNodeId);
      }
    }

    return nodeIds;
  }

  /**
   * Returns node direct neighbors (linked with exactly one edge).
   *
   * @param srcNodeId source node
   * @return list of node ids corresponding to the neighbors
   */
  public ArrayList<Long> neighbors(long srcNodeId) {
    ArrayList<Long> nodeIds = new ArrayList<Long>();
    this.nbEdgesAccessed = 0;
    for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, srcNodeId)) {
      nodeIds.add(neighborNodeId);
      nbEdgesAccessed++;
    }
    return nodeIds;
  }

  /**
   * Performs a graph traversal and returns explored nodes.
   *
   * @param srcNodeId source node
   * @return list of explored node ids
   */
  public ArrayList<Long> visitNodes(long srcNodeId) {
    ArrayList<Long> nodeIds = new ArrayList<Long>();
    Stack<Long> stack = new Stack<Long>();
    this.visited.fill(false);
    this.nbEdgesAccessed = 0;

    stack.push(srcNodeId);
    visited.set(srcNodeId);

    while (!stack.isEmpty()) {
      long currentNodeId = stack.pop();
      nodeIds.add(currentNodeId);

      for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, currentNodeId)) {
        nbEdgesAccessed++;
        if (!visited.getBoolean(neighborNodeId)) {
          stack.push(neighborNodeId);
          visited.set(neighborNodeId);
        }
      }
    }

    return nodeIds;
  }

  /**
   * Performs a graph traversal and returns explored paths.
   *
   * @param srcNodeId source node
   * @return list of explored paths (represented as a list of node ids)
   */
  public ArrayList<ArrayList<Long>> visitPaths(long srcNodeId) {
    ArrayList<ArrayList<Long>> paths = new ArrayList<>();
    Stack<Long> currentPath = new Stack<Long>();
    this.nbEdgesAccessed = 0;
    visitPathsInternal(srcNodeId, paths, currentPath);
    return paths;
  }

  /**
   * Internal recursive function of {@link #visitPaths}.
   *
   * @param currentNodeId current node
   * @param paths list of currently stored paths
   * @param currentPath current path as node ids
   */
  private void visitPathsInternal(
      long currentNodeId, ArrayList<ArrayList<Long>> paths, Stack<Long> currentPath) {
    currentPath.push(currentNodeId);

    long visitedNeighbors = 0;
    for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, currentNodeId)) {
      visitPathsInternal(neighborNodeId, paths, currentPath);
      visitedNeighbors++;
      this.nbEdgesAccessed++;
    }

    if (visitedNeighbors == 0) {
      ArrayList<Long> path = new ArrayList<Long>();
      for (long nodeId : currentPath) {
        path.add(nodeId);
      }
      paths.add(path);
    }

    currentPath.pop();
  }

  /**
   * Performs a graph traversal and returns the first found path from source to destination.
   *
   * @param srcNodeId source node
   * @param dst destination (either a node or a node type)
   * @return found path as a list of node ids
   */
  public <T> ArrayList<Long> walk(long srcNodeId, T dst, String algorithm) {
    long dstNodeId = -1;
    if (algorithm.equals("dfs")) {
      dstNodeId = walkInternalDfs(srcNodeId, dst);
    } else if (algorithm.equals("bfs")) {
      dstNodeId = walkInternalBfs(srcNodeId, dst);
    } else {
      throw new IllegalArgumentException("Unknown traversal algorithm: " + algorithm);
    }

    if (dstNodeId == -1) {
      throw new IllegalArgumentException("Unable to find destination point: " + dst);
    }

    ArrayList<Long> nodeIds = backtracking(srcNodeId, dstNodeId);
    return nodeIds;
  }

  /**
   * Internal DFS function of {@link #walk}.
   *
   * @param srcNodeId source node
   * @param dst destination (either a node or a node type)
   * @return final destination node or -1 if no path found
   */
  private <T> long walkInternalDfs(long srcNodeId, T dst) {
    Stack<Long> stack = new Stack<Long>();
    this.visited.fill(false);
    this.nbEdgesAccessed = 0;

    stack.push(srcNodeId);
    visited.set(srcNodeId);

    while (!stack.isEmpty()) {
      long currentNodeId = stack.pop();
      if (isDstNode(currentNodeId, dst)) {
        return currentNodeId;
      }

      for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, currentNodeId)) {
        nbEdgesAccessed++;
        if (!visited.getBoolean(neighborNodeId)) {
          stack.push(neighborNodeId);
          visited.set(neighborNodeId);
          LongBigArrays.set(nodeParent, neighborNodeId, currentNodeId);
        }
      }
    }

    return -1;
  }

  /**
   * Internal BFS function of {@link #walk}.
   *
   * @param srcNodeId source node
   * @param dst destination (either a node or a node type)
   * @return final destination node or -1 if no path found
   */
  private <T> long walkInternalBfs(long srcNodeId, T dst) {
    Queue<Long> queue = new LinkedList<Long>();
    this.visited.fill(false);
    this.nbEdgesAccessed = 0;

    queue.add(srcNodeId);
    visited.set(srcNodeId);

    while (!queue.isEmpty()) {
      long currentNodeId = queue.poll();
      if (isDstNode(currentNodeId, dst)) {
        return currentNodeId;
      }

      for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, currentNodeId)) {
        nbEdgesAccessed++;
        if (!visited.getBoolean(neighborNodeId)) {
          queue.add(neighborNodeId);
          visited.set(neighborNodeId);
          LongBigArrays.set(nodeParent, neighborNodeId, currentNodeId);
        }
      }
    }

    return -1;
  }

  /**
   * Internal function of {@link #walk} to check if a node corresponds to the destination.
   *
   * @param nodeId current node
   * @param dst destination (either a node or a node type)
   * @return true if the node is a destination, or false otherwise
   */
  private <T> boolean isDstNode(long nodeId, T dst) {
    if (dst instanceof Long) {
      long dstNodeId = (Long) dst;
      return nodeId == dstNodeId;
    } else if (dst instanceof Node.Type) {
      Node.Type dstType = (Node.Type) dst;
      return graph.getNodeType(nodeId) == dstType;
    } else {
      return false;
    }
  }

  /**
   * Internal backtracking function of {@link #walk}.
   *
   * @param srcNodeId source node
   * @param dstNodeId destination node
   * @return the found path, as a list of node ids
   */
  private ArrayList<Long> backtracking(long srcNodeId, long dstNodeId) {
    ArrayList<Long> path = new ArrayList<Long>();
    long currentNodeId = dstNodeId;
    while (currentNodeId != srcNodeId) {
      path.add(currentNodeId);
      currentNodeId = LongBigArrays.get(nodeParent, currentNodeId);
    }
    path.add(srcNodeId);
    Collections.reverse(path);
    return path;
  }
}
