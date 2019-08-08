package org.softwareheritage.graph.benchmark;

import java.io.IOException;
import java.util.ArrayList;

import it.unimi.dsi.big.webgraph.LazyLongIterator;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.utils.Random;
import org.softwareheritage.graph.utils.Statistics;
import org.softwareheritage.graph.utils.Timing;

/**
 * Benchmark to time edge access time.
 *
 * @author Thibault Allançon
 * @version 0.0.1
 * @since 0.0.1
 */

public class AccessEdge {
  /**
   * Main entrypoint.
   *
   * @param args command line arguments
   */
  public static void main(String[] args) throws IOException {
    String path = args[0];
    Graph graph = new Graph(path);

    final long seed = 42;
    final int nbNodes = 1_000_000;
    Random random = new Random(seed);
    long[] nodeIds = random.generateNodeIds(graph, nbNodes);

    ArrayList<Double> timings = new ArrayList<>();
    for (long nodeId : nodeIds) {
      long startTime = Timing.start();
      LazyLongIterator neighbors = graph.successors(nodeId);
      long firstNeighbor = neighbors.nextLong();
      double duration = (double) Timing.stop(startTime);
      timings.add(duration);
    }

    System.out.println("Used " + nbNodes + " random edges (results are in seconds):");
    Statistics stats = new Statistics(timings);
    stats.printAll();
  }
}
