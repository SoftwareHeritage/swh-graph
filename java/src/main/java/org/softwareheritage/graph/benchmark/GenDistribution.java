package org.softwareheritage.graph.benchmark;

import com.martiansoftware.jsap.*;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.algo.Traversal;
import org.softwareheritage.graph.benchmark.utils.Timing;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.*;

public class GenDistribution {
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
                GenDistribution.class.getName(),
                "",
                new Parameter[] {
                    new FlaggedOption("graphPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED,
                            'g', "graph", "Basename of the compressed graph"),
                    new FlaggedOption("srcType", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED,
                            's', "srctype", "Source node type"),
                    new FlaggedOption("dstType", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED,
                            'd', "dsttype", "Destination node type"),
                    new FlaggedOption("edgesFmt", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED,
                            'e', "edges", "Edges constraints"),

                    new FlaggedOption("numThreads", JSAP.INTEGER_PARSER, "128", JSAP.NOT_REQUIRED,
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
        Node.Type srcType = Node.Type.fromStr(config.getString("srcType"));
        Node.Type dstType = Node.Type.fromStr(config.getString("dstType"));
        String edgesFmt = config.getString("edgesFmt");
        int numThreads = config.getInt("numThreads");

        GenDistribution tp = new GenDistribution();
        try {
            tp.load_graph(graphPath);
        } catch (IOException e) {
            System.out.println("Could not load graph: " + e);
            System.exit(2);
        }

        final long END_OF_QUEUE = -1L;

        ArrayBlockingQueue<Long> queue = new ArrayBlockingQueue<>(numThreads);
        ExecutorService service = Executors.newFixedThreadPool(numThreads + 1);

        service.submit(() -> {
            try {
                Scanner input = new Scanner(System.in);
                while (input.hasNextLong()) {
                    long node = input.nextLong();
                    if (tp.graph.getNodeType(node) == srcType) {
                        queue.put(node);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                for (int i = 0; i < numThreads; ++i) {
                    try {
                        queue.put(END_OF_QUEUE);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        });

        for (int i = 0; i < numThreads; ++i) {
            service.submit(() -> {
                Graph thread_graph = tp.graph.copy();
                long startTime;
                double totalTime;

                while (true) {
                    Long node = null;
                    try {
                        node = queue.take();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    if (node == null || node == END_OF_QUEUE) {
                        return;
                    }

                    Traversal t = new Traversal(thread_graph, "backward", edgesFmt);
                    int[] count = { 0 };

                    startTime = Timing.start();
                    t.visitNodesVisitor(node, (curnode) -> {
                        if (tp.graph.getNodeType(curnode) == dstType) {
                            count[0]++;
                        }
                    });
                    totalTime = Timing.stop(startTime);
                    System.out.format("%d %d %d %d %f\n",
                            node, count[0], t.getNbNodesAccessed(),
                            t.getNbEdgesAccessed(), totalTime
                    );
                }
            });
        }

        service.shutdown();
    }
}
