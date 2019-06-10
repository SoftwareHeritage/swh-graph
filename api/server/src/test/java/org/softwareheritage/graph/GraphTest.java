package org.softwareheritage.graph;

import java.io.IOException;

import org.junit.BeforeClass;

import org.softwareheritage.graph.Graph;

public class GraphTest {
  static Graph graph;

  @BeforeClass
  public static void setUp() throws IOException {
    String graphPath = System.getProperty("graphPath");
    graph = new Graph(graphPath);
  }

  public Graph getGraph() {
    return graph;
  }
}
