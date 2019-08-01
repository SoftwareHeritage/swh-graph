package org.softwareheritage.graph;

import java.io.IOException;

import it.unimi.dsi.big.webgraph.BVGraph;

import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.backend.NodeIdMap;
import org.softwareheritage.graph.backend.NodeTypesMap;

/**
 * Main class storing the compressed graph and node id mappings.
 *
 * @author Thibault Allançon
 * @version 0.0.1
 * @since 0.0.1
 */

public class Graph {
  /** File extension for the SWH PID to long node id map */
  public static final String PID_TO_NODE = ".pid2node.csv";
  /** File extension for the long node id to SWH PID map */
  public static final String NODE_TO_PID = ".node2pid.csv";
  /** File extension for the long node id to node typ map */
  public static final String NODE_TO_TYPE = ".node2type.map";

  /** Compressed graph stored as a {@link it.unimi.dsi.big.webgraph.BVGraph} */
  BVGraph graph;
  /** Transposed compressed graph (used for backward traversals) */
  BVGraph graphTransposed;
  /** Path and basename of the compressed graph */
  String path;
  /** Mapping long id &harr; SWH PIDs */
  NodeIdMap nodeIdMap;
  /** Mapping long id &rarr; node types */
  NodeTypesMap nodeTypesMap;

  /**
   * Constructor.
   *
   * @param path path and basename of the compressed graph to load
   */
  public Graph(String path) throws IOException {
    this.graph = BVGraph.load(path);
    this.graphTransposed = BVGraph.load(path + "-transposed");
    this.path = path;
    this.nodeIdMap = new NodeIdMap(path, getNbNodes());
    this.nodeTypesMap = new NodeTypesMap(path);
  }

  /**
   * Cleans up graph resources after use.
   */
  public void cleanUp() throws IOException {
    nodeIdMap.close();
  }

  /**
   * Returns the graph full path.
   *
   * @return graph full path
   */
  public String getPath() {
    return path;
  }

  /**
   * Converts {@link SwhId} node to long.
   *
   * @param swhId node specified as a {@link SwhId}
   * @return internal long node id
   * @see org.softwareheritage.graph.SwhId
   */
  public long getNodeId(SwhId swhId) {
    return nodeIdMap.getNodeId(swhId);
  }

  /**
   * Converts long id node to {@link SwhId}.
   *
   * @param nodeId node specified as a long id
   * @return external SWH PID
   * @see org.softwareheritage.graph.SwhId
   */
  public SwhId getSwhId(long nodeId) {
    return nodeIdMap.getSwhId(nodeId);
  }

  /**
   * Returns node type.
   *
   * @param nodeId node specified as a long id
   * @return corresponding node type
   * @see org.softwareheritage.graph.Node.Type
   */
  public Node.Type getNodeType(long nodeId) {
    return nodeTypesMap.getType(nodeId);
  }

  /**
   * Returns number of nodes in the graph.
   *
   * @return number of nodes in the graph
   */
  public long getNbNodes() {
    return graph.numNodes();
  }

  /**
   * Returns number of edges in the graph.
   *
   * @return number of edges in the graph
   */
  public long getNbEdges() {
    return graph.numArcs();
  }

  /**
   * Returns list of successors of a node.
   *
   * @param nodeId node specified as a long id
   * @return list of successors of the node, specified as a {@link LongBigArrays}
   * @see it.unimi.dsi.fastutil.longs.LongBigArrays
   */
  public long[][] successors(long nodeId) {
    return graph.successorBigArray(nodeId);
  }

  /**
   * Returns the outdegree of a node.
   *
   * @param nodeId node specified as a long id
   * @return outdegree of a node
   */
  public long outdegree(long nodeId) {
    return graph.outdegree(nodeId);
  }

  /**
   * Returns list of predecessors of a node.
   *
   * @param nodeId node specified as a long id
   * @return list of predecessors of the node, specified as a {@link LongBigArrays}
   * @see it.unimi.dsi.fastutil.longs.LongBigArrays
   */
  public long[][] predecessors(long nodeId) {
    return graphTransposed.successorBigArray(nodeId);
  }

  /**
   * Returns the indegree of a node.
   *
   * @param nodeId node specified as a long id
   * @return indegree of a node
   */
  public long indegree(long nodeId) {
    return graphTransposed.outdegree(nodeId);
  }

  /**
   * Returns the degree of a node, depending on graph orientation.
   *
   * @param nodeId node specified as a long id
   * @param useTransposed boolean value to use transposed graph
   * @return degree of a node
   */
  public long degree(long nodeId, boolean useTransposed) {
    return (useTransposed) ? indegree(nodeId) : outdegree(nodeId);
  }

  /**
   * Returns the neighbors of a node, depending on graph orientation.
   *
   * @param nodeId node specified as a long id
   * @param useTransposed boolean value to use transposed graph
   * @return list of neighbors of the node, specified as a {@link LongBigArrays}
   * @see it.unimi.dsi.fastutil.longs.LongBigArrays
   */
  public long[][] neighbors(long nodeId, boolean useTransposed) {
    return (useTransposed) ? predecessors(nodeId) : successors(nodeId);
  }
}
