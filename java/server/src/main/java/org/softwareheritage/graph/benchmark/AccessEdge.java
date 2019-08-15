package org.softwareheritage.graph.benchmark;

import java.io.IOException;
import java.util.ArrayList;

import com.martiansoftware.jsap.JSAPException;
import it.unimi.dsi.big.webgraph.LazyLongIterator;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.benchmark.Common;
import org.softwareheritage.graph.benchmark.utils.Statistics;
import org.softwareheritage.graph.benchmark.utils.Timing;

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
  public static void main(String[] args) throws IOException, JSAPException {
    Common.BenchArgs benchArgs = Common.parseCommandLineArgs(args);

    Graph graph = new Graph(benchArgs.graphPath);

    long[] nodeIds = benchArgs.random.generateNodeIds(graph, benchArgs.nbNodes);

    ArrayList<Double> timings = new ArrayList<>();
    for (long nodeId : nodeIds) {
      long startTime = Timing.start();
      LazyLongIterator neighbors = graph.successors(nodeId);
      long firstNeighbor = neighbors.nextLong();
      double duration = Timing.stop(startTime);
      timings.add(duration);
    }

    System.out.println("Used " + benchArgs.nbNodes + " random edges (results are in seconds):");
    Statistics stats = new Statistics(timings);
    stats.printAll();
  }
}
