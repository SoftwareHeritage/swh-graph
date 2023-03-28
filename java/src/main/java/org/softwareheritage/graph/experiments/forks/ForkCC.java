/*
 * Copyright (c) 2019 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.experiments.forks;

import com.google.common.primitives.Longs;
import com.martiansoftware.jsap.*;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.io.ByteDiskQueue;
import it.unimi.dsi.logging.ProgressLogger;
import org.softwareheritage.graph.SwhBidirectionalGraph;
import org.softwareheritage.graph.SwhType;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class ForkCC {
    public Boolean includeRootDir;
    private SwhBidirectionalGraph graph;
    private Long emptySnapshot;
    private LongArrayBitVector visited;
    private LongArrayBitVector whitelist;

    private static JSAPResult parse_args(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(ForkCC.class.getName(), "",
                    new Parameter[]{
                            new FlaggedOption("graphPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'g',
                                    "graph", "Basename of the compressed graph"),
                            new FlaggedOption("whitelistPath", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 't',
                                    "whitelist", "Whitelist of origins"),
                            new FlaggedOption("includeRootDir", JSAP.BOOLEAN_PARSER, "false", JSAP.NOT_REQUIRED, 'R',
                                    "includerootdir", "Include root directory (default: false)"),
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

    private static void printDistribution(ArrayList<ArrayList<Long>> components) {
        TreeMap<Long, Long> distribution = new TreeMap<>();
        for (ArrayList<Long> component : components) {
            distribution.merge((long) component.size(), 1L, Long::sum);
        }

        for (Map.Entry<Long, Long> entry : distribution.entrySet()) {
            System.out.format("%d %d\n", entry.getKey(), entry.getValue());
        }
    }

    private static void printLargestComponent(ArrayList<ArrayList<Long>> components) {
        int indexLargest = 0;
        for (int i = 1; i < components.size(); ++i) {
            if (components.get(i).size() > components.get(indexLargest).size())
                indexLargest = i;
        }

        ArrayList<Long> component = components.get(indexLargest);
        for (Long node : component) {
            System.out.println(node);
        }
    }

    private void load_graph(String graphBasename) throws IOException {
        System.err.println("Loading graph " + graphBasename + " ...");
        this.graph = SwhBidirectionalGraph.loadMapped(graphBasename).symmetrize();
        System.err.println("Graph loaded.");
        this.emptySnapshot = null;
        this.whitelist = null;
        this.visited = null;
        this.includeRootDir = null;
    }

    private boolean nodeIsEmptySnapshot(Long node) {
        if (this.emptySnapshot == null && this.graph.getNodeType(node) == SwhType.SNP
                && this.graph.outdegree(node) == 0) {
            System.err.println("Found empty snapshot: " + node);
            this.emptySnapshot = node;
        }
        return node.equals(this.emptySnapshot);
    }

    private Boolean shouldVisit(Long node) {
        SwhType nt = this.graph.getNodeType(node);
        if (nt == SwhType.CNT) {
            return false;
        }
        if (nt == SwhType.DIR && !includeRootDir)
            return false;
        if (this.nodeIsEmptySnapshot(node))
            return false;
        if (visited.getBoolean(node))
            return false;
        return true;
    }

    private ArrayList<ArrayList<Long>> compute(ProgressLogger pl) throws IOException {
        final long n = graph.numNodes();
        // Allow enough memory to behave like in-memory queue
        int bufferSize = (int) Math.min(Arrays.MAX_ARRAY_SIZE & ~0x7, 8L * n);

        // Use a disk based queue to store BFS frontier
        final File queueFile = File.createTempFile(ForkCC.class.getSimpleName(), "queue");
        final ByteDiskQueue queue = ByteDiskQueue.createNew(queueFile, bufferSize, true);
        final byte[] byteBuf = new byte[Long.BYTES];
        // WARNING: no 64-bit version of this data-structure, but it can support
        // indices up to 2^37
        visited = LongArrayBitVector.ofLength(n);
        pl.expectedUpdates = n;
        pl.itemsName = "nodes";
        pl.start("Starting connected components visit...");

        ArrayList<ArrayList<Long>> components = new ArrayList<>();

        for (long i = 0; i < n; i++) {
            if (!shouldVisit(i) || this.graph.getNodeType(i) == SwhType.DIR)
                continue;

            ArrayList<Long> component = new ArrayList<>();

            queue.enqueue(Longs.toByteArray(i));
            visited.set(i);

            while (!queue.isEmpty()) {
                queue.dequeue(byteBuf);
                final long currentNode = Longs.fromByteArray(byteBuf);
                SwhType cur_nt = this.graph.getNodeType(currentNode);
                if (cur_nt == SwhType.ORI && (this.whitelist == null || this.whitelist.getBoolean(currentNode))) {
                    // TODO: add a check that the origin has >=1 non-empty snapshot
                    component.add(currentNode);
                }

                final LazyLongIterator iterator = graph.successors(currentNode);
                long succ;
                while ((succ = iterator.nextLong()) != -1) {
                    if (!shouldVisit(succ))
                        continue;
                    if (this.graph.getNodeType(succ) == SwhType.DIR && cur_nt != SwhType.REV)
                        continue;
                    visited.set(succ);
                    queue.enqueue(Longs.toByteArray(succ));
                }

                pl.update();
            }

            if (component.size() > 0) {
                components.add(component);
            }
        }
        pl.done();
        queue.close();

        return components;
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
        boolean includeRootDir = config.getBoolean("includeRootDir");
        String outdirPath = config.getString("outdir");

        ForkCC forkCc = new ForkCC();
        try {
            forkCc.load_graph(graphPath);
            forkCc.includeRootDir = includeRootDir;
        } catch (IOException e) {
            System.out.println("Could not load graph: " + e);
            System.exit(2);
        }

        if (whitelistPath != null) {
            forkCc.parseWhitelist(whitelistPath);
        }

        ProgressLogger logger = new ProgressLogger();
        // noinspection ResultOfMethodCallIgnored
        new File(outdirPath).mkdirs();
        try {
            ArrayList<ArrayList<Long>> components = forkCc.compute(logger);
            printDistribution(components, new Formatter(outdirPath + "/distribution.txt"));
            printLargestComponent(components, new Formatter(outdirPath + "/largest_clique.txt"));
            printAllComponents(components, new Formatter(outdirPath + "/all_cliques.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.done();
    }
}
