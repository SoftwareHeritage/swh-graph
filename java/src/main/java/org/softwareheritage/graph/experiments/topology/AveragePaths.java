/*
 * Copyright (c) 2020 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.experiments.topology;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.Util;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import org.softwareheritage.graph.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.*;

public class AveragePaths {
    private final SwhBidirectionalGraph graph;
    private final Subgraph subgraph;
    private final ConcurrentHashMap<Long, Long> result;
    private final String outdir;

    public AveragePaths(String graphBasename, String allowedNodes, String outdir) throws IOException {
        System.err.println("Loading graph " + graphBasename + " ...");
        this.graph = SwhBidirectionalGraph.loadMapped(graphBasename);
        this.subgraph = new Subgraph(this.graph, new AllowedNodes(allowedNodes));
        this.outdir = outdir;
        System.err.println("Graph loaded.");

        result = new ConcurrentHashMap<>();
    }

    private static JSAPResult parse_args(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(AveragePaths.class.getName(), "",
                    new Parameter[]{
                            new FlaggedOption("graphPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'g',
                                    "graph", "Basename of the compressed graph"),
                            new FlaggedOption("nodeTypes", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 's',
                                    "nodetypes", "Node type constraints"),
                            new FlaggedOption("outdir", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o',
                                    "outdir", "Directory where to put the results"),
                            new FlaggedOption("numThreads", JSAP.INTEGER_PARSER, "32", JSAP.NOT_REQUIRED, 't',
                                    "numthreads", "Number of threads"),});

            config = jsap.parse(args);
            if (jsap.messagePrinted()) {
                System.exit(1);
            }
        } catch (JSAPException e) {
            e.printStackTrace();
        }
        return config;
    }

    private void run(int numThreads) throws InterruptedException {
        final long END_OF_QUEUE = -1L;

        ArrayBlockingQueue<Long> queue = new ArrayBlockingQueue<>(numThreads);
        ExecutorService service = Executors.newFixedThreadPool(numThreads + 1);

        service.submit(() -> {
            try {
                SwhBidirectionalGraph thread_graph = graph.copy();
                Subgraph thread_subgraph = subgraph.copy();

                long[][] randomPerm = Util.identity(thread_graph.numNodes());
                LongBigArrays.shuffle(randomPerm, new XoRoShiRo128PlusRandom());
                long n = thread_graph.numNodes();

                ProgressLogger pl = new ProgressLogger();
                pl.expectedUpdates = n;
                pl.itemsName = "nodes";
                pl.start("Filling processor queue...");

                for (long j = 0; j < n; ++j) {
                    long node = BigArrays.get(randomPerm, j);
                    if (thread_subgraph.nodeExists(node) && thread_subgraph.outdegree(node) == 0) {
                        queue.put(node);
                    }
                    if (j % 10000 == 0) {
                        printResult();
                    }
                    pl.update();
                }
            } catch (Exception e) {
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
                try {
                    Subgraph thread_subgraph = subgraph.copy();
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

                        bfsAt(thread_subgraph, node);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }

        service.shutdown();
        service.awaitTermination(365, TimeUnit.DAYS);
    }

    private void bfsAt(Subgraph graph, long srcNodeId) {
        ArrayDeque<Long> queue = new ArrayDeque<>();
        HashSet<Long> visited = new HashSet<>();

        long FRONTIER_MARKER = -1;

        queue.addLast(srcNodeId);
        visited.add(srcNodeId);

        long distance = 0;
        queue.addLast(FRONTIER_MARKER);

        while (!queue.isEmpty()) {
            long currentNodeId = queue.removeFirst();
            // System.err.println("curr: " + currentNodeId);
            if (currentNodeId == FRONTIER_MARKER) {
                if (queue.isEmpty()) // avoid infinite loops
                    break;
                ++distance;
                queue.addLast(FRONTIER_MARKER);
                continue;
            }
            if (graph.indegree(currentNodeId) == 0) {
                result.merge(distance, 1L, Long::sum);
            }

            LazyLongIterator it = graph.predecessors(currentNodeId);
            for (long neighborNodeId; (neighborNodeId = it.nextLong()) != -1;) {
                if (!visited.contains(neighborNodeId)) {
                    queue.addLast(neighborNodeId);
                    visited.add(neighborNodeId);
                }
            }
        }
    }

    public void printResult() throws IOException {
        new File(outdir).mkdirs();

        PrintWriter f = new PrintWriter(new FileWriter(outdir + "/distribution.txt"));
        TreeMap<Long, Long> sortedDistribution = new TreeMap<>(result);
        for (Map.Entry<Long, Long> entry : sortedDistribution.entrySet()) {
            f.println(entry.getKey() + " " + entry.getValue());
        }
        f.close();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        JSAPResult config = parse_args(args);

        String graphPath = config.getString("graphPath");
        String outdir = config.getString("outdir");
        String allowedNodes = config.getString("nodeTypes");
        int numThreads = config.getInt("numThreads");

        AveragePaths tp = new AveragePaths(graphPath, allowedNodes, outdir);
        tp.run(numThreads);
        tp.printResult();
    }
}
