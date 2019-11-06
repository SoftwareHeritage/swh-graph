package org.softwareheritage.graph.benchmark;

import java.io.IOException;

import com.martiansoftware.jsap.JSAPException;

import org.softwareheritage.graph.Endpoint;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.benchmark.Benchmark;

/**
 * Benchmark Software Heritage <a
 * href="https://docs.softwareheritage.org/devel/swh-graph/use-cases.html#vault">vault
 * use-case scenario</a>.
 *
 * @author The Software Heritage developers
 */

public class Vault {
    /**
     * Main entrypoint.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) throws IOException, JSAPException {
        Benchmark bench = new Benchmark();
        bench.parseCommandLineArgs(args);

        Graph graph = new Graph(bench.args.graphPath);

        long[] nodeIds = bench.args.random.generateNodeIds(graph, bench.args.nbNodes);

        Endpoint endpoint = new Endpoint(graph, "forward", "*");

        System.out.println("Used " + bench.args.nbNodes + " random nodes (results are in seconds):");
        bench.createCSVLogFile();
        bench.timeEndpoint("git bundle", graph, nodeIds, endpoint::visitNodes);
    }
}
