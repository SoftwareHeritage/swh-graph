package org.softwareheritage.graph;

import it.unimi.dsi.big.webgraph.BVGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;

import org.softwareheritage.graph.NodeIdMap;
import org.softwareheritage.graph.SwhId;

public class Graph {
  BVGraph graph;
  String path;
  NodeIdMap nodeIdMap;

  public Graph(String graphPath) throws Exception {
    this.graph = BVGraph.load(graphPath);
    this.path = graphPath;
    this.nodeIdMap = new NodeIdMap(graphPath);
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
}
