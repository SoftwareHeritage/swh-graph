package org.softwareheritage.graph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Node {
  public enum Type {
    CNT,
    DIR,
    REL,
    REV,
    SNP;

    public static Node.Type fromStr(String strType) {
      return Node.Type.valueOf(strType.toUpperCase());
    }

    public static ArrayList<Node.Type> parse(String strType) {
      ArrayList<Node.Type> types = new ArrayList<>();

      if (strType.equals("*")) {
        List<Node.Type> nodeTypes = Arrays.asList(Node.Type.values());
        types.addAll(nodeTypes);
      } else {
        types.add(Node.Type.fromStr(strType));
      }

      return types;
    }
  }
}
