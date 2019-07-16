package org.softwareheritage.graph.backend;

import java.io.IOException;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.longs.LongBigList;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Node;

/**
 * Mapping between long node id and SWH node type as described in the <a
 * href="https://docs.softwareheritage.org/devel/swh-model/data-model.html">data model</a>.
 *
 * @author Thibault Allançon
 * @version 1.0
 * @since 1.0
 */

public class NodeTypesMap {
  /**
   * Array storing for each node its type
   * @see it.unimi.dsi.fastutil.longs.LongBigList
   */
  LongBigList nodeTypesMap;

  /**
   * Constructor.
   *
   * @param graphPath full path of the compressed graph
   */
  public NodeTypesMap(String graphPath) throws IOException {
    try {
      nodeTypesMap = (LongBigList) BinIO.loadObject(graphPath + Graph.NODE_TO_TYPE);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Unknown class object: " + e);
    }
  }

  /**
   * Returns node type from a node long id.
   *
   * @param nodeId node as a long id
   * @return corresponding {@link Node.Type} value
   * @see org.softwareheritage.graph.Node.Type
   */
  public Node.Type getType(long nodeId) {
    long type = nodeTypesMap.getLong(nodeId);
    return Node.Type.fromInt((int) type);
  }
}
