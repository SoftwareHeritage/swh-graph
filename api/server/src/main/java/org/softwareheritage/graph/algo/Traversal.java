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
    this.edges = new AllowedEdges(graph, edgesFmt);

    long nbNodes = graph.getNbNodes();
    this.visited = LongArrayBitVector.ofLength(nbNodes);
    this.nodeParent = LongBigArrays.newBigArray(nbNodes);
  }

  public ArrayList<Long> leaves(long srcNodeId) {
    ArrayList<Long> nodeIds = new ArrayList<Long>();
    Stack<Long> stack = new Stack<Long>();
    this.visited.fill(false);

    stack.push(srcNodeId);
    visited.set(srcNodeId);

    while (!stack.isEmpty()) {
      long currentNodeId = stack.pop();

      long neighborsCnt = 0;
      for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, currentNodeId)) {
        neighborsCnt++;
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

  public ArrayList<Long> neighbors(long srcNodeId) {
    ArrayList<Long> nodeIds = new ArrayList<Long>();
    for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, srcNodeId)) {
      nodeIds.add(neighborNodeId);
    }
    return nodeIds;
  }

  public ArrayList<Long> visitNodes(long srcNodeId) {
    ArrayList<Long> nodeIds = new ArrayList<Long>();
    Stack<Long> stack = new Stack<Long>();
    this.visited.fill(false);

    stack.push(srcNodeId);
    visited.set(srcNodeId);

    while (!stack.isEmpty()) {
      long currentNodeId = stack.pop();
      nodeIds.add(currentNodeId);

      for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, currentNodeId)) {
        if (!visited.getBoolean(neighborNodeId)) {
          stack.push(neighborNodeId);
          visited.set(neighborNodeId);
        }
      }
    }

    return nodeIds;
  }

  public ArrayList<ArrayList<Long>> visitPaths(long srcNodeId) {
    ArrayList<ArrayList<Long>> paths = new ArrayList<>();
    Stack<Long> currentPath = new Stack<Long>();
    visitPathsInternal(srcNodeId, paths, currentPath);
    return paths;
  }

  private void visitPathsInternal(
      long currentNodeId, ArrayList<ArrayList<Long>> paths, Stack<Long> currentPath) {
    currentPath.push(currentNodeId);

    long visitedNeighbors = 0;
    for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, currentNodeId)) {
      visitPathsInternal(neighborNodeId, paths, currentPath);
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

  private <T> long walkInternalDfs(long srcNodeId, T dst) {
    Stack<Long> stack = new Stack<Long>();
    this.visited.fill(false);

    stack.push(srcNodeId);
    visited.set(srcNodeId);

    while (!stack.isEmpty()) {
      long currentNodeId = stack.pop();
      if (isDstNode(currentNodeId, dst)) {
        return currentNodeId;
      }

      for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, currentNodeId)) {
        if (!visited.getBoolean(neighborNodeId)) {
          stack.push(neighborNodeId);
          visited.set(neighborNodeId);
          LongBigArrays.set(nodeParent, neighborNodeId, currentNodeId);
        }
      }
    }

    return -1;
  }

  private <T> long walkInternalBfs(long srcNodeId, T dst) {
    Queue<Long> queue = new LinkedList<Long>();
    this.visited.fill(false);

    queue.add(srcNodeId);
    visited.set(srcNodeId);

    while (!queue.isEmpty()) {
      long currentNodeId = queue.poll();
      if (isDstNode(currentNodeId, dst)) {
        return currentNodeId;
      }

      for (long neighborNodeId : new Neighbors(graph, useTransposed, edges, currentNodeId)) {
        if (!visited.getBoolean(neighborNodeId)) {
          queue.add(neighborNodeId);
          visited.set(neighborNodeId);
          LongBigArrays.set(nodeParent, neighborNodeId, currentNodeId);
        }
      }
    }

    return -1;
  }

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
