package org.softwareheritage.graph.benchmark;

import java.io.IOException;
import java.util.ArrayList;

import com.martiansoftware.jsap.JSAPException;
import it.unimi.dsi.big.webgraph.LazyLongIterator;

import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.benchmark.Benchmark;
import org.softwareheritage.graph.benchmark.utils.Statistics;
import org.softwareheritage.graph.benchmark.utils.Timing;

/**
 * Benchmark to time edge access time.
 *
 * @author The Software Heritage developers
 */

public class AccessEdge {
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

        ArrayList<Double> timings = new ArrayList<>();
        for (long nodeId : nodeIds) {
            long startTime = Timing.start();
            LazyLongIterator neighbors = graph.successors(nodeId);
            long firstNeighbor = neighbors.nextLong();
            double duration = Timing.stop(startTime);
            timings.add(duration);
        }

        System.out.println("Used " + bench.args.nbNodes + " random edges (results are in seconds):");
        Statistics stats = new Statistics(timings);
        stats.printAll();
    }
}
