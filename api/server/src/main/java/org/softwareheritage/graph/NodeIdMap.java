package org.softwareheritage.graph;

import org.softwareheritage.graph.SwhId;

// TODO: decide on how to do the disk-based node id map
public class NodeIdMap {
  String graphPath;

  public NodeIdMap(String graphPath) {
    this.graphPath = graphPath;
  }

  public long getNode(SwhId swhId) {
    return 42;
  }

  public SwhId getSwhId(long node) {
    return null;
  }

  public void dump() {
  }
}
