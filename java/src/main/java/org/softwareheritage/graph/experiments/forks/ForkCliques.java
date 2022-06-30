/*
 * Copyright (c) 2020 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.experiments.forks;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.google.common.primitives.Longs;
import com.martiansoftware.jsap.*;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.logging.ProgressLogger;
import org.slf4j.LoggerFactory;
import org.softwareheritage.graph.SwhBidirectionalGraph;
import org.softwareheritage.graph.SwhType;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class ForkCliques {
    private SwhBidirectionalGraph graph;
    private LongArrayBitVector whitelist;

    private void load_graph(String graphBasename) throws IOException {
        System.err.println("Loading graph " + graphBasename + " ...");
        this.graph = SwhBidirectionalGraph.loadMapped(graphBasename);
        System.err.println("Graph loaded.");
        this.whitelist = null;
    }

    private static JSAPResult parse_args(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(ForkCliques.class.getName(), "",
                    new Parameter[]{
                            new FlaggedOption("graphPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'g',
                                    "graph", "Basename of the compressed graph"),
                            new FlaggedOption("whitelistPath", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 't',
                                    "whitelist", "Whitelist of origins"),
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

    private ArrayList<Long> dfsAt(Long baseNode) {
        ArrayList<Long> res = new ArrayList<>();

        final Deque<Long> stack = new ArrayDeque<>();
        HashSet<Long> seen = new HashSet<>();
        stack.push(baseNode);

        while (!stack.isEmpty()) {
            final Long currentNode = stack.pop();

            final LazyLongIterator iterator = this.graph.predecessors(currentNode);
            long succ;
            while ((succ = iterator.nextLong()) != -1) {
                if (!seen.contains(succ)) {
                    SwhType nt = this.graph.getNodeType(succ);
                    if (nt == SwhType.DIR || nt == SwhType.CNT)
                        continue;
                    if (nt == SwhType.ORI && (this.whitelist == null || this.whitelist.getBoolean(succ))) {
                        res.add(succ);
                    } else {
                        stack.push(succ);
                        seen.add(succ);
                    }
                }
            }
        }

        Collections.sort(res);
        return res;
    }

    private boolean isBaseRevision(Long node) {
        if (this.graph.getNodeType(node) != SwhType.REV)
            return false;

        final LazyLongIterator iterator = this.graph.successors(node);
        long succ;
        while ((succ = iterator.nextLong()) != -1) {
            if (this.graph.getNodeType(succ) == SwhType.REV)
                return false;
        }
        return true;
    }

    static private String fingerprint(ArrayList<Long> cluster) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
        for (Long n : cluster)
            digest.update(Longs.toByteArray(n));

        return new String(digest.digest());
    }

    private ArrayList<ArrayList<Long>> compute(ProgressLogger pl) {
        final long n = this.graph.numNodes();

        HashSet<String> fingerprints = new HashSet<>();
        ArrayList<ArrayList<Long>> clusters = new ArrayList<>();

        pl.expectedUpdates = n;
        pl.itemsName = "nodes";
        pl.start("Starting topological sort...");

        for (long i = 0; i < n; i++) {
            if (isBaseRevision(i)) {
                ArrayList<Long> currentCluster = dfsAt(i);
                String clusterFp = fingerprint(currentCluster);
                if (!fingerprints.contains(clusterFp)) {
                    fingerprints.add(clusterFp);
                    clusters.add(currentCluster);
                }
            }
            pl.update();
        }
        pl.done();

        return clusters;
    }

    private static void printDistribution(ArrayList<ArrayList<Long>> components, Formatter out) {
        TreeMap<Long, Long> distribution = new TreeMap<>();
        for (ArrayList<Long> component : components) {
            distribution.merge((long) component.size(), 1L, Long::sum);
        }

        for (Map.Entry<Long, Long> entry : distribution.entrySet()) {
            out.format("%d %d\n", entry.getKey(), entry.getValue());
        }
    }

    private static void printLargestComponent(ArrayList<ArrayList<Long>> components, Formatter out) {
        int indexLargest = 0;
        for (int i = 1; i < components.size(); ++i) {
            if (components.get(i).size() > components.get(indexLargest).size())
                indexLargest = i;
        }

        ArrayList<Long> component = components.get(indexLargest);
        for (Long node : component) {
            out.format("%d\n", node);
        }
    }

    private static void printAllComponents(ArrayList<ArrayList<Long>> components, Formatter out) {
        for (int i = 1; i < components.size(); ++i) {
            ArrayList<Long> component = components.get(i);
            for (Long node : component) {
                out.format("%d ", node);
            }
            out.format("\n");
        }
    }

    private void parseWhitelist(String path) {
        System.err.println("Loading whitelist " + path + " ...");
        this.whitelist = LongArrayBitVector.ofLength(this.graph.numNodes());
        Scanner scanner;
        try {
            scanner = new Scanner(new File(path));
            while (scanner.hasNextLong()) {
                whitelist.set(scanner.nextLong());
            }
            System.err.println("Whitelist loaded.");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        JSAPResult config = parse_args(args);

        String graphPath = config.getString("graphPath");
        String whitelistPath = config.getString("whitelistPath");
        String outdirPath = config.getString("outdir");

        ForkCliques forkCliques = new ForkCliques();
        try {
            forkCliques.load_graph(graphPath);
        } catch (IOException e) {
            System.out.println("Could not load graph: " + e);
            System.exit(2);
        }

        if (whitelistPath != null) {
            forkCliques.parseWhitelist(whitelistPath);
        }

        Logger rootLogger = (ch.qos.logback.classic.Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        rootLogger.setLevel(Level.DEBUG);

        ProgressLogger logger = new ProgressLogger(rootLogger);
        ArrayList<ArrayList<Long>> components = forkCliques.compute(logger);

        // noinspection ResultOfMethodCallIgnored
        new File(outdirPath).mkdirs();
        try {
            printDistribution(components, new Formatter(outdirPath + "/distribution.txt"));
            printLargestComponent(components, new Formatter(outdirPath + "/largest_clique.txt"));
            printAllComponents(components, new Formatter(outdirPath + "/all_cliques.txt"));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        logger.done();
    }
}
