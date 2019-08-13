package org.softwareheritage.graph.benchmark;

import java.io.IOException;

import org.softwareheritage.graph.Endpoint;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.benchmark.Common;
import org.softwareheritage.graph.benchmark.utils.Random;

/**
 * Benchmark Software Heritage browsing use-cases scenarios: <a
 * href="https://docs.softwareheritage.org/devel/swh-graph/use-cases.html">use cases</a>.
 *
 * @author Thibault Allançon
 * @version 0.0.1
 * @since 0.0.1
 */

public class Browsing {
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
    long[] dirNodeIds = random.generateNodeIdsOfType(graph, nbNodes, Node.Type.DIR);
    long[] revNodeIds = random.generateNodeIdsOfType(graph, nbNodes, Node.Type.REV);

    Endpoint dirEndpoint = new Endpoint(graph, "forward", "dir:cnt,dir:dir");
    Endpoint revEndpoint = new Endpoint(graph, "forward", "rev:rev");

    System.out.println("Used " + nbNodes + " random nodes (results are in seconds):");
    System.out.println("\n'ls' use-case");
    Common.timeEndpoint(graph, dirNodeIds, dirEndpoint::neighbors);
    System.out.println("\n'ls -R' use-case");
    Common.timeEndpoint(graph, dirNodeIds, dirEndpoint::visitPaths);
    System.out.println("\n'git log' use-case");
    Common.timeEndpoint(graph, revNodeIds, revEndpoint::visitNodes);
  }
}
