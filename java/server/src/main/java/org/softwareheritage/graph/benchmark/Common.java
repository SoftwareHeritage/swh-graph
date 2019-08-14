package org.softwareheritage.graph.benchmark;

import java.util.ArrayList;
import java.util.function.Function;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.UnflaggedOption;

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
   * Benchmark input arguments.
   */
  public static class BenchArgs {
    /** Basename of the compressed graph */
    public String graphPath;
    /** Number of random nodes to use for the benchmark */
    public int nbNodes;
    /** Random generator seed */
    public Long seed;
  }

  /**
   * Parses benchmark command line arguments.
   *
   * @param args command line arguments
   * @return parsed arguments as a {@link BenchArgs}
   */
  public static BenchArgs parseCommandLineArgs(String[] args) throws JSAPException {
    SimpleJSAP jsap = new SimpleJSAP(Common.class.getName(),
        "Benchmark tool for Software Heritage use-cases scenarios.",
        new Parameter[] {
            new UnflaggedOption("graphPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED,
                JSAP.NOT_GREEDY, "The basename of the compressed graph."),
            new FlaggedOption("nbNodes", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'n',
                "nb-nodes", "Number of random nodes used to do the benchmark."),
            new FlaggedOption("seed", JSAP.LONG_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 's',
                "seed", "Random generator seed."),
        });

    JSAPResult config = jsap.parse(args);
    if (jsap.messagePrinted()) {
      System.exit(1);
    }

    BenchArgs benchArgs = new BenchArgs();
    benchArgs.graphPath = config.getString("graphPath");
    benchArgs.nbNodes = config.getInt("nbNodes");
    benchArgs.seed = config.getLong("seed");

    return benchArgs;
  }

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
