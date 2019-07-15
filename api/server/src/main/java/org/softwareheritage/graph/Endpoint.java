package org.softwareheritage.graph;

import java.util.ArrayList;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.SwhPath;
import org.softwareheritage.graph.algo.Traversal;

public class Endpoint {
  Graph graph;
  Traversal traversal;

  public Endpoint(Graph graph, String direction, String edgesFmt) {
    this.graph = graph;
    this.traversal = new Traversal(graph, direction, edgesFmt);
  }

  private ArrayList<SwhId> convertNodesToSwhIds(ArrayList<Long> nodeIds) {
    ArrayList<SwhId> swhIds = new ArrayList<>();
    for (long nodeId : nodeIds) {
      swhIds.add(graph.getSwhId(nodeId));
    }
    return swhIds;
  }

  private SwhPath convertNodesToSwhPath(ArrayList<Long> nodeIds) {
    SwhPath path = new SwhPath();
    for (long nodeId : nodeIds) {
      path.add(graph.getSwhId(nodeId));
    }
    return path;
  }

  private ArrayList<SwhPath> convertPathsToSwhIds(ArrayList<ArrayList<Long>> pathsNodeId) {
    ArrayList<SwhPath> paths = new ArrayList<>();
    for (ArrayList<Long> path : pathsNodeId) {
      paths.add(convertNodesToSwhPath(path));
    }
    return paths;
  }

  public ArrayList<SwhId> leaves(SwhId src) {
    long srcNodeId = graph.getNodeId(src);
    ArrayList<Long> nodeIds = traversal.leaves(srcNodeId);
    return convertNodesToSwhIds(nodeIds);
  }

  public ArrayList<SwhId> neighbors(SwhId src) {
    long srcNodeId = graph.getNodeId(src);
    ArrayList<Long> nodeIds = traversal.neighbors(srcNodeId);
    return convertNodesToSwhIds(nodeIds);
  }

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

  public ArrayList<SwhId> visitNodes(SwhId src) {
    long srcNodeId = graph.getNodeId(src);
    ArrayList<Long> nodeIds = traversal.visitNodes(srcNodeId);
    return convertNodesToSwhIds(nodeIds);
  }

  public ArrayList<SwhPath> visitPaths(SwhId src) {
    long srcNodeId = graph.getNodeId(src);
    ArrayList<ArrayList<Long>> paths = traversal.visitPaths(srcNodeId);
    return convertPathsToSwhIds(paths);
  }
}
