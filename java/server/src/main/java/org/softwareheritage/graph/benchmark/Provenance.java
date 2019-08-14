package org.softwareheritage.graph.benchmark;

import java.io.IOException;

import com.martiansoftware.jsap.JSAPException;

import org.softwareheritage.graph.Endpoint;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.benchmark.Common;
import org.softwareheritage.graph.benchmark.utils.Random;

/**
 * Benchmark Software Heritage <a
 * href="https://docs.softwareheritage.org/devel/swh-graph/use-cases.html#provenance">provenance
 * use-cases scenarios</a>.
 *
 * @author Thibault Allançon
 * @version 0.0.1
 * @since 0.0.1
 */

public class Provenance {
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

    Endpoint commitProvenanceEndpoint = new Endpoint(graph, "backward", "dir:dir,cnt:dir,dir:rev");
    Endpoint originProvenanceEndpoint = new Endpoint(graph, "backward", "*");

    System.out.println("Used " + benchArgs.nbNodes + " random nodes (results are in seconds):");

    System.out.println("\n'commit provenance' use-case (using dfs)");
    Common.timeEndpoint(graph, nodeIds, commitProvenanceEndpoint::walk, "rev", "dfs");
    System.out.println("\n'commit provenance' use-case (using bfs)");
    Common.timeEndpoint(graph, nodeIds, commitProvenanceEndpoint::walk, "rev", "bfs");
    System.out.println("\n'complete commit provenance' use-case");
    Common.timeEndpoint(graph, nodeIds, commitProvenanceEndpoint::leaves);

    System.out.println("\n'origin provenance' use-case (using dfs)");
    Common.timeEndpoint(graph, nodeIds, originProvenanceEndpoint::walk, "ori", "dfs");
    System.out.println("\n'origin provenance' use-case (using bfs)");
    Common.timeEndpoint(graph, nodeIds, originProvenanceEndpoint::walk, "ori", "bfs");
    System.out.println("\n'complete origin provenance' use-case");
    Common.timeEndpoint(graph, nodeIds, originProvenanceEndpoint::leaves);
  }
}
