package org.softwareheritage.graph;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.BeforeClass;

import org.softwareheritage.graph.Graph;

public class GraphTest {
  static Graph graph;

  @BeforeClass
  public static void setUp() throws IOException {
    Path graphPath = Paths.get("src", "test", "dataset", "graph");
    graph = new Graph(graphPath.toString());
  }

  public Graph getGraph() {
    return graph;
  }
}
