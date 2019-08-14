package org.softwareheritage.graph.benchmark;

import java.util.ArrayList;
import java.util.function.Function;

import org.softwareheritage.graph.Endpoint;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.SwhPID;
import org.softwareheritage.graph.benchmark.utils.Statistics;

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
   * @param dstFmt destination formatted string as described in the <a
   * href="https://docs.softwareheritage.org/devel/swh-graph/api.html#walk">API</a>
   * @param algorithm traversal algorithm used in endpoint call (either "dfs" or "bfs")
   */
  public static void timeEndpoint(Graph graph, long[] nodeIds,
      Function<Endpoint.Input, Endpoint.Output> operation, String dstFmt, String algorithm) {
    ArrayList<Double> timings = new ArrayList<>();
    ArrayList<Double> timingsNormalized = new ArrayList<>();

    for (long nodeId : nodeIds) {
      SwhPID swhPID = graph.getSwhPID(nodeId);

      Endpoint.Output output = (dstFmt == null)
          ? operation.apply(new Endpoint.Input(swhPID))
          : operation.apply(new Endpoint.Input(swhPID, dstFmt, algorithm));

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

  /**
   * Same as {@link timeEndpoint} but without destination or algorithm specified to endpoint call.
   */
  public static void timeEndpoint(
      Graph graph, long[] nodeIds, Function<Endpoint.Input, Endpoint.Output> operation) {
    timeEndpoint(graph, nodeIds, operation, null, null);
  }
}
