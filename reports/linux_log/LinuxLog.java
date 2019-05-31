package org.softwareheritage.graph;

import java.util.Stack;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.bits.LongArrayBitVector;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.SwhId;
import org.softwareheritage.graph.algo.Visit;

public class LinuxLog {
  private static final String COMMIT_HASH = "f39d7d78b70e0f39facb1e4fab77ad3df5c52a35";
  private static final long EXPECTED_COUNT = 723640;
  private static final boolean TRANSPOSED = true;

  public static void main(String[] args) throws Exception {
    String graphPath = args[0];
    Graph graph = new Graph(graphPath);
    SwhId commit = new SwhId(COMMIT_HASH);
    long startNode = graph.getNode(commit);

    System.out.println("git log " + COMMIT_HASH);
    System.out.println("Expecting " + EXPECTED_COUNT + " commits");

    long count = countCommits(graph, startNode);
    if (count != EXPECTED_COUNT) {
      throw new Exception("Counted " + count + " commits");
    }

    long startTime = System.nanoTime();
    Visit visit = new Visit(graph, commit, "rev", "dfs", "backward");
    long endTime = System.nanoTime();
    double duration = (double) (endTime - startTime) / 1_000_000_000;
    System.out.println("Visit done in: " + duration + " seconds");
  }

  static long countCommits(Graph graph, long startNode) {
    Stack<Long> stack = new Stack<Long>();
    LongArrayBitVector visited = LongArrayBitVector.ofLength(graph.getNbNodes());
    long count = 0;

    stack.push(startNode);
    visited.set(startNode);
    while (!stack.empty()) {
      long currentNode = stack.pop();
      count++;

      long degree = graph.degree(currentNode, TRANSPOSED);
      LazyLongIterator revisions = graph.neighbors(currentNode, TRANSPOSED);
      while (degree-- > 0) {
        long nextNode = revisions.nextLong();
        if (!visited.getBoolean(nextNode)) {
          visited.set(nextNode);
          stack.push(nextNode);
        }
      }
    }

    return count;
  }
}
