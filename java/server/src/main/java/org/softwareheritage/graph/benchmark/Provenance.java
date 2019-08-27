package org.softwareheritage.graph.benchmark;

import java.io.IOException;

import com.martiansoftware.jsap.JSAPException;

import org.softwareheritage.graph.Endpoint;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.benchmark.Benchmark;

/**
 * Benchmark Software Heritage <a
 * href="https://docs.softwareheritage.org/devel/swh-graph/use-cases.html#provenance">provenance
 * use-cases scenarios</a>.
 *
 * @author The Software Heritage developers
 */

public class Provenance {
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

        Endpoint commitProvenanceEndpoint = new Endpoint(graph, "backward", "dir:dir,cnt:dir,dir:rev");
        Endpoint originProvenanceEndpoint = new Endpoint(graph, "backward", "*");

        System.out.println("Used " + bench.args.nbNodes + " random nodes (results are in seconds):");
        bench.createCSVLogFile();

        bench.timeEndpoint(
            "commit provenance (dfs)", graph, nodeIds, commitProvenanceEndpoint::walk, "rev", "dfs");
        bench.timeEndpoint(
            "commit provenance (bfs)", graph, nodeIds, commitProvenanceEndpoint::walk, "rev", "bfs");
        bench.timeEndpoint(
            "complete commit provenance", graph, nodeIds, commitProvenanceEndpoint::leaves);

        bench.timeEndpoint(
            "origin provenance (dfs)", graph, nodeIds, originProvenanceEndpoint::walk, "ori", "dfs");
        bench.timeEndpoint(
            "origin provenance (bfs)", graph, nodeIds, originProvenanceEndpoint::walk, "ori", "bfs");
        bench.timeEndpoint(
            "complete origin provenance", graph, nodeIds, originProvenanceEndpoint::leaves);
    }
}
