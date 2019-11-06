package org.softwareheritage.graph.benchmark;

import java.io.IOException;

import com.martiansoftware.jsap.JSAPException;

import org.softwareheritage.graph.Endpoint;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.benchmark.Benchmark;

/**
 * Benchmark Software Heritage <a
 * href="https://docs.softwareheritage.org/devel/swh-graph/use-cases.html#browsing">browsing
 * use-cases scenarios</a>.
 *
 * @author The Software Heritage developers
 */

public class Browsing {
    /**
     * Main entrypoint.
     *
     * @param args command line arguments
     */
    public static void main(String[] args) throws IOException, JSAPException {
        Benchmark bench = new Benchmark();
        bench.parseCommandLineArgs(args);

        Graph graph = new Graph(bench.args.graphPath);

        long[] dirNodeIds =
            bench.args.random.generateNodeIdsOfType(graph, bench.args.nbNodes, Node.Type.DIR);
        long[] revNodeIds =
            bench.args.random.generateNodeIdsOfType(graph, bench.args.nbNodes, Node.Type.REV);

        Endpoint dirEndpoint = new Endpoint(graph, "forward", "dir:cnt,dir:dir");
        Endpoint revEndpoint = new Endpoint(graph, "forward", "rev:rev");

        System.out.println("Used " + bench.args.nbNodes + " random nodes (results are in seconds):");
        bench.createCSVLogFile();
        bench.timeEndpoint("ls", graph, dirNodeIds, dirEndpoint::neighbors);
        bench.timeEndpoint("ls -R", graph, dirNodeIds, dirEndpoint::visitPaths);
        bench.timeEndpoint("git log", graph, revNodeIds, revEndpoint::visitNodes);
    }
}
