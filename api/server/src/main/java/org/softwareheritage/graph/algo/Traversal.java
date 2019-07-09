package org.softwareheritage.graph.algo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;

import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.longs.LongBigArrays;

import org.softwareheritage.graph.AllowedEdges;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Neighbors;
import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.SwhId;

public class Traversal {
  Graph graph;
  boolean useTransposed;
  AllowedEdges edges;

  // Big array storing if we have visited a node
  LongArrayBitVector visited;
  // Big array storing parents to retrieve the path when backtracking
  long[][] nodeParent;

  public Traversal(Graph graph, String direction, String edgesFmt) {
    if (!direction.matches("forward|backward")) {
      throw new IllegalArgumentException("Unknown traversal direction: " + direction);
    }

    this.graph = graph;
    this.useTransposed = (direction.equals("backward"));
    this.edges = new AllowedEdges(edgesFmt);

    long nbNodes = graph.getNbNodes();
    this.visited = LongArrayBitVector.ofLength(nbNodes);
    this.nodeParent = LongBigArrays.newBigArray(nbNodes);
  }

  // TODO: better separation between internal/external ids (waiting on node types map to be merged)
  private ArrayList<SwhId> convertToSwhIds(ArrayList<Long> nodeIds) {
    ArrayList<SwhId> swhIds = new ArrayList<SwhId>();
    for (long nodeId : nodeIds) {
      swhIds.add(graph.getSwhId(nodeId));
    }
    return swhIds;
  }

  public ArrayList<SwhId> leavesEndpoint(SwhId src) {
    long nodeId = graph.getNodeId(src);
    ArrayList<Long> leavesNodeIds = leavesInternalEndpoint(nodeId);
    return convertToSwhIds(leavesNodeIds);
  }

  public ArrayList<SwhId> neighborsEndpoint(SwhId src) {
    long nodeId = graph.getNodeId(src);
    ArrayList<Long> neighborsNodeIds = neighborsInternalEndpoint(nodeId);
    return convertToSwhIds(neighborsNodeIds);
  }

  public ArrayList<SwhId> walkEndpoint(SwhId src, String dstFmt, String traversal) {
    if (!traversal.matches("dfs|bfs")) {
      throw new IllegalArgumentException("Unknown traversal algorithm: " + traversal);
    }

    long srcNodeId = graph.getNodeId(src);
    long dstNodeId = (traversal.equals("dfs")) ? dfs(srcNodeId, dstFmt) : bfs(srcNodeId, dstFmt);
    if (dstNodeId == -1) {
      throw new IllegalArgumentException("Unable to find destination point: " + dstFmt);
    }

    ArrayList<Long> path = backtracking(srcNodeId, dstNodeId);
    return convertToSwhIds(path);
  }

  public ArrayList<SwhId> visitNodesEndpoint(SwhId src) {
    long nodeId = graph.getNodeId(src);
    ArrayList<Long> nodes = visitNodesInternalEndpoint(nodeId);
    return convertToSwhIds(nodes);
  }

  public ArrayList<ArrayList<SwhId>> visitPathsEndpoint(SwhId src) {
    long nodeId = graph.getNodeId(src);
    ArrayList<ArrayList<Long>> pathNodeIds = new ArrayList<>();
    Stack<Long> currentPath = new Stack<Long>();
    visitPathsInternalEndpoint(nodeId, pathNodeIds, currentPath);

    ArrayList<ArrayList<SwhId>> paths = new ArrayList<>();
    for (ArrayList<Long> nodeIds : pathNodeIds) {
      paths.add(convertToSwhIds(nodeIds));
    }
    return paths;
  }

  private ArrayList<Long> leavesInternalEndpoint(long srcNodeId) {
    ArrayList<Long> leaves = new ArrayList<Long>();
    Stack<Long> stack = new Stack<Long>();
    this.visited.clear();

    stack.push(srcNodeId);
    visited.set(srcNodeId);

    while (!stack.isEmpty()) {
      long currentNodeId = stack.pop();
      SwhId currentSwhId = graph.getSwhId(currentNodeId);

      long neighborsCnt = 0;
      for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, currentSwhId)) {
        neighborsCnt++;
        if (!visited.getBoolean(neighborNodeId)) {
          stack.push(neighborNodeId);
          visited.set(neighborNodeId);
        }
      }

      if (neighborsCnt == 0) {
        leaves.add(currentNodeId);
      }
    }

    return leaves;
  }

  private ArrayList<Long> neighborsInternalEndpoint(long srcNodeId) {
    SwhId srcSwhId = graph.getSwhId(srcNodeId);
    ArrayList<Long> neighbors = new ArrayList<Long>();
    for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, srcSwhId)) {
      neighbors.add(neighborNodeId);
    }
    return neighbors;
  }

  private ArrayList<Long> visitNodesInternalEndpoint(long srcNodeId) {
    // No specific destination point
    String dstFmt = null;
    dfs(srcNodeId, dstFmt);

    ArrayList<Long> nodes = new ArrayList<Long>();
    for (long nodeId = 0; nodeId < graph.getNbNodes(); nodeId++) {
      if (this.visited.getBoolean(nodeId)) {
        nodes.add(nodeId);
      }
    }
    return nodes;
  }

  private void visitPathsInternalEndpoint(
      long currentNodeId, ArrayList<ArrayList<Long>> paths, Stack<Long> currentPath) {
    SwhId currentSwhId = graph.getSwhId(currentNodeId);
    currentPath.push(currentNodeId);

    long visitedNeighbors = 0;
    for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, currentSwhId)) {
      visitPathsInternalEndpoint(neighborNodeId, paths, currentPath);
      visitedNeighbors++;
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

  private long dfs(long srcNodeId, String dstFmt) {
    Stack<Long> stack = new Stack<Long>();
    this.visited.clear();

    stack.push(srcNodeId);
    visited.set(srcNodeId);

    while (!stack.isEmpty()) {
      long currentNodeId = stack.pop();
      SwhId currentSwhId = graph.getSwhId(currentNodeId);
      if (isDestinationNode(currentSwhId, dstFmt)) {
        return currentNodeId;
      }

      for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, currentSwhId)) {
        if (!visited.getBoolean(neighborNodeId)) {
          stack.push(neighborNodeId);
          visited.set(neighborNodeId);
          LongBigArrays.set(nodeParent, neighborNodeId, currentNodeId);
        }
      }
    }

    return -1;
  }

  private long bfs(long srcNodeId, String dstFmt) {
    Queue<Long> queue = new LinkedList<Long>();
    this.visited.clear();

    queue.add(srcNodeId);
    visited.set(srcNodeId);

    while (!queue.isEmpty()) {
      long currentNodeId = queue.poll();
      SwhId currentSwhId = graph.getSwhId(currentNodeId);
      if (isDestinationNode(currentSwhId, dstFmt)) {
        return currentNodeId;
      }

      for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, currentSwhId)) {
        if (!visited.getBoolean(neighborNodeId)) {
          queue.add(neighborNodeId);
          visited.set(neighborNodeId);
          LongBigArrays.set(nodeParent, neighborNodeId, currentNodeId);
        }
      }
    }

    return -1;
  }

  private boolean isDestinationNode(SwhId swhId, String dstFmt) {
    // No destination node, early exit
    if (dstFmt == null) {
      return false;
    }

    // SwhId as destination node
    if (swhId.toString().equals(dstFmt)) {
      return true;
    }

    // Node.Type as destination node
    try {
      Node.Type dstType = Node.Type.fromStr(dstFmt);
      return (swhId.getType().equals(dstType));
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}
