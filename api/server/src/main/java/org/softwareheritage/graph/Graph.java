package org.softwareheritage.graph;

import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;

import org.softwareheritage.graph.NodeIdMap;
import org.softwareheritage.graph.SwhId;

public class Graph {
  BVGraph graph;
  BVGraph graphTransposed;
  String path;
  NodeIdMap nodeIdMap;

  public Graph(String graphPath) throws Exception {
    this.graph = BVGraph.load(graphPath);
    this.graphTransposed = BVGraph.load(graphPath + "-transposed");
    this.path = graphPath;
    this.nodeIdMap = new NodeIdMap(graphPath, getNbNodes());
  }

  public void cleanUp() {
    nodeIdMap.close();
  }

  public String getPath() {
    return path;
  }

  public long getNode(SwhId swhId) {
    return nodeIdMap.getNode(swhId);
  }

  public SwhId getSwhId(long node) {
    return nodeIdMap.getSwhId(node);
  }

  public long getNbNodes() {
    return graph.numNodes();
  }

  public long getNbEdges() {
    return graph.numArcs();
  }

  public LazyLongIterator successors(long node) {
    return graph.successors(node);
  }

  public long outdegree(long node) {
    return graph.outdegree(node);
  }

  public LazyLongIterator predecessors(long node) {
    return graphTransposed.successors(node);
  }

  public long indegree(long node) {
    return graphTransposed.outdegree(node);
  }

  public long degree(long node, boolean isTransposed) {
    if (isTransposed) {
      return indegree(node);
    } else {
      return outdegree(node);
    }
  }

  public LazyLongIterator neighbors(long node, boolean isTransposed) {
    if (isTransposed) {
      return predecessors(node);
    } else {
      return successors(node);
    }
  }
}
