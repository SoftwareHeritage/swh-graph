package org.softwareheritage.graph.benchmark;

import java.io.IOException;

import org.softwareheritage.graph.Endpoint;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.benchmark.Common;
import org.softwareheritage.graph.benchmark.utils.Random;

/**
 * Benchmark Software Heritage <a
 * href="https://docs.softwareheritage.org/devel/swh-graph/use-cases.html#vault">vault
 * use-case scenario</a>.
 *
 * @author Thibault Allançon
 * @version 0.0.1
 * @since 0.0.1
 */

public class Vault {
  /**
   * Main entrypoint.
   *
   * @param args command line arguments
   */
  public static void main(String[] args) throws IOException {
    String path = args[0];
    Graph graph = new Graph(path);

    final long seed = 42;
    final int nbNodes = 100_000;
    Random random = new Random(seed);
    long[] nodeIds = random.generateNodeIds(graph, nbNodes);

    Endpoint endpoint = new Endpoint(graph, "forward", "*");

    System.out.println("Used " + nbNodes + " random nodes (results are in seconds):");
    System.out.println("\n'git bundle' use-case");
    Common.timeEndpoint(graph, nodeIds, endpoint::visitNodes);
  }
}
