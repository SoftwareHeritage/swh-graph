package org.softwareheritage.graph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A node in the Software Heritage graph.
 *
 * @author Thibault Allançon
 * @version 1.0
 * @since 1.0
 */

public class Node {
  /**
   * Software Heritage graph node types, as described in the
   * <a href="https://docs.softwareheritage.org/devel/swh-model/data-model.html">data model</a>.
   */
  public enum Type {
    /** Content node */
    CNT,
    /** Directory node */
    DIR,
    /** Origin node */
    ORI,
    /** Release node */
    REL,
    /** Revision node */
    REV,
    /** Snapshot node */
    SNP;

    /**
     * Converts integer to corresponding SWH node type.
     *
     * @param intType node type represented as an integer
     * @return the corresponding {@link Node.Type} value
     * @see org.softwareheritage.graph.Node.Type
     */
    public static Node.Type fromInt(int intType) {
      switch (intType) {
        case 0:
          return CNT;
        case 1:
          return DIR;
        case 2:
          return ORI;
        case 3:
          return REL;
        case 4:
          return REV;
        case 5:
          return SNP;
      }
      return null;
    }

    /**
     * Parses SWH node type from string.
     *
     * @param strType node type represented as a string
     * @return the corresponding {@link Node.Type} value
     * @see org.softwareheritage.graph.Node.Type
     */
    public static Node.Type fromStr(String strType) {
      if (!strType.matches("cnt|dir|ori|rel|rev|snp")) {
        throw new IllegalArgumentException("Unknown node type: " + strType);
      }
      return Node.Type.valueOf(strType.toUpperCase());
    }

    /**
     * Parses SWH node type possible values from formatted string (TODO: link API doc).
     *
     * @param strFmtType node types represented as a formatted string (TODO: link API doc)
     * @return a list containing the {@link Node.Type} values
     * @see org.softwareheritage.graph.Node.Type
     */
    public static ArrayList<Node.Type> parse(String strFmtType) {
      ArrayList<Node.Type> types = new ArrayList<>();

      if (strFmtType.equals("*")) {
        List<Node.Type> nodeTypes = Arrays.asList(Node.Type.values());
        types.addAll(nodeTypes);
      } else {
        types.add(Node.Type.fromStr(strFmtType));
      }

      return types;
    }
  }
}
