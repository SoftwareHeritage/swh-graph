package org.softwareheritage.graph.benchmark;

import java.io.IOException;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.algo.Traversal;

public class LinuxLog {
  public static void main(String[] args) throws IOException {
    String path = args[0];
    Graph graph = new Graph(path);

    // A linux kernel commit on Sun Dec 31
    final SwhId commit = new SwhId("swh:1:rev:f39d7d78b70e0f39facb1e4fab77ad3df5c52a35");
    final long expectedCount = 723640;
    System.out.println("git log " + commit);
    System.out.println("Expecting " + expectedCount + " commits");

    long startTime = System.nanoTime();
    Traversal traversal = new Traversal(graph, "forward", "rev:rev");
    long count = traversal.visitNodesEndpoint(commit).size();
    if (count != expectedCount) {
      throw new IllegalArgumentException("Counted " + count + " commits");
    }
    long endTime = System.nanoTime();
    double duration = (double) (endTime - startTime) / 1_000_000_000;
    System.out.println("Visit operation done in: " + duration + " seconds");

  }
}
