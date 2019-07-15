package org.softwareheritage.graph;

import java.io.IOException;

import it.unimi.dsi.big.webgraph.BVGraph;

import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.backend.NodeIdMap;
import org.softwareheritage.graph.backend.NodeTypesMap;

public class Graph {
  public static final String PID_TO_NODE = ".pid2node.csv";
  public static final String NODE_TO_PID = ".node2pid.csv";
  public static final String NODE_TO_TYPE = ".node2type.map";

  BVGraph graph;
  BVGraph graphTransposed;
  String path;
  NodeIdMap nodeIdMap;
  NodeTypesMap nodeTypesMap;

  public Graph(String path) throws IOException {
    this.graph = BVGraph.load(path);
    this.graphTransposed = BVGraph.load(path + "-transposed");
    this.path = path;
    this.nodeIdMap = new NodeIdMap(path, getNbNodes());
    this.nodeTypesMap = new NodeTypesMap(path);
  }

  public void cleanUp() throws IOException {
    nodeIdMap.close();
  }

  public String getPath() {
    return path;
  }

  public long getNodeId(SwhId swhId) {
    return nodeIdMap.getNodeId(swhId);
  }

  public SwhId getSwhId(long nodeId) {
    return nodeIdMap.getSwhId(nodeId);
  }

  public Node.Type getNodeType(long nodeId) {
    return nodeTypesMap.getType(nodeId);
  }

  public long getNbNodes() {
    return graph.numNodes();
  }

  public long getNbEdges() {
    return graph.numArcs();
  }

  public long[][] successors(long nodeId) {
    return graph.successorBigArray(nodeId);
  }

  public long outdegree(long nodeId) {
    return graph.outdegree(nodeId);
  }

  public long[][] predecessors(long nodeId) {
    return graphTransposed.successorBigArray(nodeId);
  }

  public long indegree(long nodeId) {
    return graphTransposed.outdegree(nodeId);
  }

  public long degree(long nodeId, boolean useTransposed) {
    return (useTransposed) ? indegree(nodeId) : outdegree(nodeId);
  }

  public long[][] neighbors(long nodeId, boolean useTransposed) {
    return (useTransposed) ? predecessors(nodeId) : successors(nodeId);
  }
}
