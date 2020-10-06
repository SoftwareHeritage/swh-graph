package org.softwareheritage.graph.experiments.topology;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.martiansoftware.jsap.*;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.logging.ProgressLogger;
import org.slf4j.LoggerFactory;
import org.softwareheritage.graph.Graph;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class ClusteringCoefficient {
    private Graph graph;

    private void load_graph(String graphBasename) throws IOException {
        System.err.println("Loading graph " + graphBasename + " ...");
        this.graph = new Graph(graphBasename).symmetrize();
        System.err.println("Graph loaded.");
    }

    private static JSAPResult parse_args(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(ClusteringCoefficient.class.getName(), "",
                    new Parameter[]{
                            new FlaggedOption("graphPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'g',
                                    "graph", "Basename of the compressed graph"),
                            new FlaggedOption("outdir", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o',
                                    "outdir", "Directory where to put the results"),});

            config = jsap.parse(args);
            if (jsap.messagePrinted()) {
                System.exit(1);
            }
        } catch (JSAPException e) {
            e.printStackTrace();
        }
        return config;
    }

    private long oppositeEdges(ImmutableGraph graph, long node) {
        System.out.format("%d\n", node);
        HashSet<Long> neighborhood = new HashSet<>();
        long succ;
        final LazyLongIterator iterator = graph.successors(node);
        while ((succ = iterator.nextLong()) != -1) {
            System.out.format("%d neighbor add %d\n", node, succ);
            neighborhood.add(succ);
        }

        long closed_triplets = 0;
        for (Long neighbor : neighborhood) {
            System.out.format("%d neighbor visit %d\n", node, neighbor);
            final LazyLongIterator it = graph.successors(neighbor);
            while ((succ = it.nextLong()) != -1) {
                System.out.format("%d neighbor visit %d succ %d\n", node, neighbor, succ);
                if (neighborhood.contains(succ)) {
                    closed_triplets++;
                }
            }
        }
        return closed_triplets;
    }

    public void compute(ProgressLogger pl, Formatter out_local, Formatter out_global) {
        final long n = this.graph.numNodes();

        pl.expectedUpdates = n;
        pl.itemsName = "nodes";

        long nodes_d2 = 0;
        double cum_lcc = 0;
        double cum_lcc_c0 = 0;
        double cum_lcc_c1 = 0;
        HashMap<Double, Long> distribution = new HashMap<>();

        for (long node = 0; node < n; node++) {
            long d = graph.outdegree(node);
            if (d >= 2) {
                double t = (d * (d - 1));
                double m = oppositeEdges(graph, node);
                double lcc = m / t;

                distribution.merge(lcc, 1L, Long::sum);

                cum_lcc += lcc;
                nodes_d2++;
            } else {
                cum_lcc_c1++;
            }
            pl.update();
        }
        pl.done();

        for (Map.Entry<Double, Long> entry : distribution.entrySet()) {
            out_local.format("%f %d\n", entry.getKey(), entry.getValue());
        }

        double gC = cum_lcc / nodes_d2;
        double gC0 = cum_lcc_c0 / n;
        double gC1 = cum_lcc_c1 / n;

        out_global.format("C: %f\n", gC);
        out_global.format("C0: %f\n", gC0);
        out_global.format("C1: %f\n", gC1);
    }

    public void compute_approx(Formatter out_global) {
        final long n = this.graph.numNodes();

        long trials = 0;
        long triangles = 0;

        while (true) {
            long node = ThreadLocalRandom.current().nextLong(0, n);
            long d = graph.outdegree(node);
            if (d >= 2) {
                Long u = null;
                Long v = null;

                long u_idx = ThreadLocalRandom.current().nextLong(0, d);
                long v_idx = ThreadLocalRandom.current().nextLong(0, d - 1);
                if (v_idx >= u_idx) {
                    v_idx++;
                }

                long succ;
                final LazyLongIterator node_iterator = graph.successors(node);
                for (long succ_idx = 0; (succ = node_iterator.nextLong()) != -1; succ_idx++) {
                    if (succ_idx == u_idx) {
                        u = succ;
                    }
                    if (succ_idx == v_idx) {
                        v = succ;
                    }
                }

                final LazyLongIterator u_iterator = graph.successors(u);
                while ((succ = u_iterator.nextLong()) != -1) {
                    if (succ == v)
                        triangles++;
                }
            }
            trials++;

            if (trials % 100 == 0 || true) {
                double gC = (double) triangles / (double) trials;
                out_global.format("C: %f (triangles: %d, trials: %d)\n", gC, triangles, trials);
                System.out.format("C: %f (triangles: %d, trials: %d)\n", gC, triangles, trials);
            }
        }
    }

    public static void main(String[] args) {
        JSAPResult config = parse_args(args);

        String graphPath = config.getString("graphPath");
        String outdirPath = config.getString("outdir");

        ClusteringCoefficient ccoef = new ClusteringCoefficient();
        try {
            ccoef.load_graph(graphPath);
        } catch (IOException e) {
            System.out.println("Could not load graph: " + e);
            System.exit(2);
        }

        Logger rootLogger = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(Level.DEBUG);

        new File(outdirPath).mkdirs();

        try {
            ccoef.compute_approx(new Formatter(outdirPath + "/local.txt"));
            /*
             * ccoef.compute( symmetric, new ProgressLogger(rootLogger), new Formatter(outdirPath +
             * "/local.txt"), new Formatter(outdirPath + "/global.txt") );
             */
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
