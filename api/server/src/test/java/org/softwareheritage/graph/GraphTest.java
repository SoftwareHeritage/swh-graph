package org.softwareheritage.graph;

import org.junit.BeforeClass;

import org.softwareheritage.graph.Graph;

public class GraphTest {
  static Graph graph;

  @BeforeClass
  public static void setUp() {
    String graphPath = System.getProperty("graphPath");
    graph = new Graph(graphPath);
  }

  public Graph getGraph() {
    return graph;
  }
}
