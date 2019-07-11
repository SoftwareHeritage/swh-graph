package org.softwareheritage.graph.backend;

import java.io.IOException;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigList;

import org.softwareheritage.graph.Node;

public class NodeTypesMap {
  LongBigList nodeTypesMap;

  public NodeTypesMap(String graphPath) throws IOException {
    try {
      nodeTypesMap = (LongBigList) BinIO.loadObject(graphPath + ".nodeTypesMap");
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("The .nodeTypesMap file has unknown class object: " + e);
    }
  }

  public Node.Type getType(long nodeId) {
    long type = nodeTypesMap.getLong(nodeId);
    return Node.Type.fromInt((int) type);
  }
}
