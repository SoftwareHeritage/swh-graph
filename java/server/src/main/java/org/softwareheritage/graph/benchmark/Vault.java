package org.softwareheritage.graph.benchmark;

import java.io.IOException;

import com.martiansoftware.jsap.JSAPException;

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
  public static void main(String[] args) throws IOException, JSAPException {
    Common.BenchArgs benchArgs = Common.parseCommandLineArgs(args);

    Graph graph = new Graph(benchArgs.graphPath);
    Random random = (benchArgs.seed == null) ? new Random() : new Random(benchArgs.seed);

    long[] nodeIds = random.generateNodeIds(graph, benchArgs.nbNodes);

    Endpoint endpoint = new Endpoint(graph, "forward", "*");

    System.out.println("Used " + benchArgs.nbNodes + " random nodes (results are in seconds):");
    System.out.println("\n'git bundle' use-case");
    Common.timeEndpoint(graph, nodeIds, endpoint::visitNodes);
  }
}
