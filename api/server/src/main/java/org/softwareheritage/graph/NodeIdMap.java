package org.softwareheritage.graph;

// TODO: decide on how to do the disk-based node id map
public class NodeIdMap {
  String graphPath;

  public NodeIdMap(String graphPath) {
    this.graphPath = graphPath;
  }

  public long getNode(String hash) {
    return 42;
  }

  public String getHash(long node) {
    return null;
  }

  public void dump() {
  }
}
