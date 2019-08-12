package org.softwareheritage.graph.benchmark;

import java.util.ArrayList;
import java.util.function.Function;

import org.softwareheritage.graph.Endpoint;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.SwhPID;
import org.softwareheritage.graph.utils.Statistics;

/**
 * Benchmark common utility functions.
 *
 * @author Thibault Allançon
 * @version 0.0.1
 * @since 0.0.1
 */

public class Common {
  /**
   * Times a specific endpoint and prints aggregated statistics.
   *
   * @param graph compressed graph used in the benchmark
   * @param nodeIds node ids to use as starting point for the endpoint traversal
   * @param operation endpoint function to benchmark
   */
  public static void timeEndpoint(
      Graph graph, long[] nodeIds, Function<SwhPID, Endpoint.Output> operation) {
    ArrayList<Double> timings = new ArrayList<>();
    ArrayList<Double> timingsNormalized = new ArrayList<>();

    for (long nodeId : nodeIds) {
      SwhPID swhPID = graph.getSwhPID(nodeId);

      Endpoint.Output output = operation.apply(swhPID);

      timings.add(output.meta.timings.traversal);
      if (output.meta.nbEdgesAccessed != 0) {
        timingsNormalized.add(output.meta.timings.traversal / output.meta.nbEdgesAccessed);
      }
    }

    System.out.println("timings:");
    Statistics stats = new Statistics(timings);
    stats.printAll();

    System.out.println("timings normalized:");
    Statistics statsNormalized = new Statistics(timingsNormalized);
    statsNormalized.printAll();
  }
}
