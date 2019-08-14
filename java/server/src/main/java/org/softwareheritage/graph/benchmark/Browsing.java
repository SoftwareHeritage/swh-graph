package org.softwareheritage.graph.benchmark;

import java.io.IOException;

import com.martiansoftware.jsap.JSAPException;

import org.softwareheritage.graph.Endpoint;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.benchmark.Common;
import org.softwareheritage.graph.benchmark.utils.Random;

/**
 * Benchmark Software Heritage <a
 * href="https://docs.softwareheritage.org/devel/swh-graph/use-cases.html#browsing">browsing
 * use-cases scenarios</a>.
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
  public static void main(String[] args) throws IOException, JSAPException {
    Common.BenchArgs benchArgs = Common.parseCommandLineArgs(args);

    Graph graph = new Graph(benchArgs.graphPath);
    Random random = (benchArgs.seed == null) ? new Random() : new Random(benchArgs.seed);

    long[] dirNodeIds = random.generateNodeIdsOfType(graph, benchArgs.nbNodes, Node.Type.DIR);
    long[] revNodeIds = random.generateNodeIdsOfType(graph, benchArgs.nbNodes, Node.Type.REV);

    Endpoint dirEndpoint = new Endpoint(graph, "forward", "dir:cnt,dir:dir");
    Endpoint revEndpoint = new Endpoint(graph, "forward", "rev:rev");

    System.out.println("Used " + benchArgs.nbNodes + " random nodes (results are in seconds):");
    System.out.println("\n'ls' use-case");
    Common.timeEndpoint(graph, dirNodeIds, dirEndpoint::neighbors);
    System.out.println("\n'ls -R' use-case");
    Common.timeEndpoint(graph, dirNodeIds, dirEndpoint::visitPaths);
    System.out.println("\n'git log' use-case");
    Common.timeEndpoint(graph, revNodeIds, revEndpoint::visitNodes);
  }
}
