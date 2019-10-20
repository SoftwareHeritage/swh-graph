package org.softwareheritage.graph.benchmark;

import org.softwareheritage.graph.Graph;

import com.martiansoftware.jsap.*;

import it.unimi.dsi.big.webgraph.algo.ParallelBreadthFirstVisit;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.IOException;


public class ParallelBFS {
    private Graph graph;

    private void load_graph(String graphBasename) throws IOException {
        System.err.println("Loading graph " + graphBasename + " ...");
        this.graph = new Graph(graphBasename);
        System.err.println("Graph loaded.");
    }

    private static JSAPResult parse_args(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(
                ParallelBFS.class.getName(),
                "",
                new Parameter[] {
                    new FlaggedOption("graphPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED,
                            'g', "graph", "Basename of the compressed graph"),

                    new FlaggedOption("numThreads", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED,
                            't', "numthreads", "Number of threads"),
                }
            );

            config = jsap.parse(args);
            if (jsap.messagePrinted()) {
                System.exit(1);
            }
        } catch (JSAPException e) {
            e.printStackTrace();
        }
        return config;
    }

    public static void main(String[] args) {
        JSAPResult config = parse_args(args);

        String graphPath = config.getString("graphPath");
        int numThreads = config.getInt("numThreads");
        ProgressLogger logger = new ProgressLogger();
        long startTime;
        double totalTime;


        ParallelBFS bfs = new ParallelBFS();
        try {
            bfs.load_graph(graphPath);
        } catch (IOException e) {
            System.out.println("Could not load graph: " + e);
            System.exit(2);
        }

        ParallelBreadthFirstVisit visit =
            new ParallelBreadthFirstVisit(bfs.graph.getBVGraph(false),
                                          numThreads, true, logger);
        logger.start("Parallel BFS visit...");
        visit.visitAll();
        logger.done();
    }
}
