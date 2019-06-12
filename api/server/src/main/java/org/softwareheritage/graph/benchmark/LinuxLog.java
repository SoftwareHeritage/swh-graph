package org.softwareheritage.graph.benchmark;

import java.io.IOException;
import java.util.Stack;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.bits.LongArrayBitVector;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.algo.Visit;

public class LinuxLog {
  public static void main(String[] args) throws IOException {
    String path = args[0];
    Graph graph = new Graph(path);

    final SwhId commit = new SwhId("swh:1:rev:f39d7d78b70e0f39facb1e4fab77ad3df5c52a35");
    final long expectedCount = 723640;
    System.out.println("git log " + commit);

    long count = countCommits(graph, commit);
    System.out.println("Counted " + count + " commits");
    if (count != expectedCount) {
      throw new IllegalArgumentException("Expected " + expectedCount + " commits");
    }

    long startTime = System.nanoTime();
    Visit visit = new Visit(graph, commit, "rev", "dfs", "backward");
    long endTime = System.nanoTime();
    double duration = (double) (endTime - startTime) / 1_000_000_000;
    System.out.println("Visit operation done in: " + duration + " seconds");
  }

  static long countCommits(Graph graph, SwhId commit) {
    final boolean useTransposed = true;

    Stack<Long> stack = new Stack<Long>();
    LongArrayBitVector visited = LongArrayBitVector.ofLength(graph.getNbNodes());
    long startNodeId = graph.getNodeId(commit);
    long count = 0;

    stack.push(startNodeId);
    visited.set(startNodeId);
    while (!stack.empty()) {
      long currentNodeId = stack.pop();
      count++;

      long degree = graph.degree(currentNodeId, useTransposed);
      LazyLongIterator revisions = graph.neighbors(currentNodeId, useTransposed);
      while (degree-- > 0) {
        long nextNodeId = revisions.nextLong();
        if (!visited.getBoolean(nextNodeId)) {
          visited.set(nextNodeId);
          stack.push(nextNodeId);
        }
      }
    }

    return count;
  }
}
