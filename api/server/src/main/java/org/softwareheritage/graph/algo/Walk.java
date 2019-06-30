package org.softwareheritage.graph.algo;

import java.util.Stack;
import java.util.Queue;
import java.util.LinkedList;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.longs.LongBigArrays;

import org.softwareheritage.graph.Edges;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.SwhPath;

public class Walk {
  Graph graph;
  boolean useTransposed;
  Edges edges;

  SwhPath path;
  long[][] nodeParent;

  public Walk(
      Graph graph, SwhId src, String dstFmt, String edgesFmt, String direction, String traversal) {
    if (!traversal.matches("dfs|bfs")) {
      throw new IllegalArgumentException("Unknown traversal algorithm: " + traversal);
    }
    if (!direction.matches("forward|backward")) {
      throw new IllegalArgumentException("Unknown traversal direction: " + direction);
    }

    this.graph = graph;
    this.useTransposed = (direction.equals("backward"));
    this.edges = new Edges(edgesFmt);
    this.path = new SwhPath();
    this.nodeParent = LongBigArrays.newBigArray(graph.getNbNodes());
    LongBigArrays.fill(nodeParent, -1);

    long srcNodeId = graph.getNodeId(src);
    long dstNodeId;
    if (traversal.equals("dfs")) {
      dstNodeId = dfs(srcNodeId, dstFmt);
    } else {
      dstNodeId = bfs(srcNodeId, dstFmt);
    }

    if (dstNodeId == -1) {
      throw new IllegalArgumentException("Unable to find destination point: " + dstFmt);
    } else {
      backtracking(srcNodeId, dstNodeId);
    }
  }

  public SwhPath getPath() {
    return path;
  }

  private void backtracking(long srcNodeId, long dstNodeId) {
    long currentNodeId = dstNodeId;
    while (currentNodeId != srcNodeId) {
      path.add(graph.getSwhId(currentNodeId));
      currentNodeId = LongBigArrays.get(nodeParent, currentNodeId);
    }
    path.add(graph.getSwhId(srcNodeId));
    path.reverse();
  }

  private long dfs(long srcNodeId, String dstFmt) {
    Stack<Long> stack = new Stack<Long>();
    LongArrayBitVector visited = LongArrayBitVector.ofLength(graph.getNbNodes());

    stack.push(srcNodeId);
    visited.set(srcNodeId);

    while (!stack.isEmpty()) {
      long currentNodeId = stack.pop();
      SwhId currentSwhId = graph.getSwhId(currentNodeId);
      if (isDestinationNode(currentSwhId, dstFmt)) {
        return currentNodeId;
      }

      long degree = graph.degree(currentNodeId, useTransposed);
      LazyLongIterator neighbors = graph.neighbors(currentNodeId, useTransposed);

      while (degree-- > 0) {
        long neighborNodeId = neighbors.nextLong();
        SwhId neighborSwhId = graph.getSwhId(neighborNodeId);
        if (!visited.getBoolean(neighborNodeId)
            && edges.isAllowed(currentSwhId.getType(), neighborSwhId.getType())) {
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
    LongArrayBitVector visited = LongArrayBitVector.ofLength(graph.getNbNodes());

    queue.add(srcNodeId);
    visited.set(srcNodeId);

    while (!queue.isEmpty()) {
      long currentNodeId = queue.poll();
      SwhId currentSwhId = graph.getSwhId(currentNodeId);
      if (isDestinationNode(currentSwhId, dstFmt)) {
        return currentNodeId;
      }

      long degree = graph.degree(currentNodeId, useTransposed);
      LazyLongIterator neighbors = graph.neighbors(currentNodeId, useTransposed);

      while (degree-- > 0) {
        long neighborNodeId = neighbors.nextLong();
        SwhId neighborSwhId = graph.getSwhId(neighborNodeId);
        if (!visited.getBoolean(neighborNodeId)
            && edges.isAllowed(currentSwhId.getType(), neighborSwhId.getType())) {
          queue.add(neighborNodeId);
          visited.set(neighborNodeId);
          LongBigArrays.set(nodeParent, neighborNodeId, currentNodeId);
        }
      }
    }

    return -1;
  }

  private boolean isDestinationNode(SwhId swhId, String dstFmt) {
    // dstFmt is either a SwhId...
    if (swhId.toString().equals(dstFmt)) {
      return true;
    }

    // ...or a Node.Type
    try {
      Node.Type dstType = Node.Type.fromStr(dstFmt);
      return (swhId.getType().equals(dstType));
    } catch (IllegalArgumentException e) {
      return false;
    }
  }
}
