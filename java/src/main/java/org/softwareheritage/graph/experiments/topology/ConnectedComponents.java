/*
 * Copyright (c) 2020 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.experiments.topology;

import com.google.common.primitives.Longs;
import com.martiansoftware.jsap.*;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.io.ByteDiskQueue;
import it.unimi.dsi.logging.ProgressLogger;
import org.softwareheritage.graph.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class ConnectedComponents {
    private Subgraph graph;

    private void load_graph(String graphBasename, String nodeTypes) throws IOException {
        System.err.println("Loading graph " + graphBasename + " ...");
        var underlyingGraph = SwhBidirectionalGraph.loadMapped(graphBasename);
        var underlyingGraphSym = underlyingGraph.symmetrize();
        graph = new Subgraph(underlyingGraphSym, new AllowedNodes(nodeTypes));
        System.err.println("Graph loaded.");
    }

    private static JSAPResult parse_args(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(ConnectedComponents.class.getName(), "",
                    new Parameter[]{
                            new FlaggedOption("graphPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'g',
                                    "graph", "Basename of the compressed graph"),
                            new FlaggedOption("outdir", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'o',
                                    "outdir", "Directory where to put the results"),
                            new Switch("byOrigins", JSAP.NO_SHORTFLAG, "by-origins",
                                    "Compute size of components by number of origins"),
                            new FlaggedOption("nodeTypes", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'n',
                                    "nodetypes", "Allowed node types (comma-separated)"),});

            config = jsap.parse(args);
            if (jsap.messagePrinted()) {
                System.exit(1);
            }
        } catch (JSAPException e) {
            e.printStackTrace();
        }
        return config;
    }

    private HashMap<Long, Long> /* ArrayList<ArrayList<Long>> */ compute(ProgressLogger pl, boolean byOrigin)
            throws IOException {
        final long n = graph.numNodes();
        final long maxN = graph.maxNodeNumber();

        // Allow enough memory to behave like in-memory queue
        int bufferSize = (int) Math.min(Arrays.MAX_ARRAY_SIZE & ~0x7, 8L * maxN);

        // Use a disk based queue to store BFS frontier
        final File queueFile = File.createTempFile(ConnectedComponents.class.getSimpleName(), "queue");
        final ByteDiskQueue queue = ByteDiskQueue.createNew(queueFile, bufferSize, true);
        final byte[] byteBuf = new byte[Long.BYTES];
        // WARNING: no 64-bit version of this data-structure, but it can support
        // indices up to 2^37
        LongArrayBitVector visited = LongArrayBitVector.ofLength(maxN);

        pl.expectedUpdates = n;
        pl.itemsName = "nodes";
        pl.start("Starting connected components visit...");

        // ArrayList<ArrayList<Long>> components = new ArrayList<>();
        HashMap<Long, Long> componentDistribution = new HashMap<>();

        var it = graph.nodeIterator();
        while (it.hasNext()) {
            long i = it.nextLong();

            if (visited.getBoolean(i))
                continue;

            // ArrayList<Long> component = new ArrayList<>();
            long componentNodes = 0;

            queue.enqueue(Longs.toByteArray(i));
            visited.set(i);

            while (!queue.isEmpty()) {
                queue.dequeue(byteBuf);
                final long currentNode = Longs.fromByteArray(byteBuf);
                // component.add(currentNode);

                if (!byOrigin || graph.getNodeType(currentNode) == SwhType.ORI)
                    componentNodes += 1;

                final LazyLongIterator iterator = graph.successors(currentNode);
                long succ;
                while ((succ = iterator.nextLong()) != -1) {
                    if (visited.getBoolean(succ))
                        continue;
                    visited.set(succ);
                    queue.enqueue(Longs.toByteArray(succ));
                }

                pl.update();
            }

            /*
             * if (component.size() > 0) { components.add(component); }
             */
            if (componentNodes > 0)
                componentDistribution.merge(componentNodes, 1L, Long::sum);
        }
        pl.done();
        // return components;
        return componentDistribution;
    }

    private static void printDistribution(ArrayList<ArrayList<Long>> components, Formatter out) {
        TreeMap<Long, Long> distribution = new TreeMap<>();
        for (ArrayList<Long> component : components) {
            distribution.merge((long) component.size(), 1L, Long::sum);
        }

        for (Map.Entry<Long, Long> entry : distribution.entrySet()) {
            out.format("%d %d\n", entry.getKey(), entry.getValue());
        }
        out.close();
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
        out.close();
    }

    private static void printAllComponents(ArrayList<ArrayList<Long>> components, Formatter out) {
        for (int i = 1; i < components.size(); ++i) {
            ArrayList<Long> component = components.get(i);
            for (Long node : component) {
                out.format("%d ", node);
            }
            out.format("\n");
        }
        out.close();
    }

    public static void main(String[] args) {
        JSAPResult config = parse_args(args);

        String graphPath = config.getString("graphPath");
        String outdirPath = config.getString("outdir");
        String nodeTypes = config.getString("nodeTypes");
        boolean byOrigin = config.getBoolean("byOrigins");

        ConnectedComponents connectedComponents = new ConnectedComponents();
        try {
            connectedComponents.load_graph(graphPath, nodeTypes);
        } catch (IOException e) {
            System.out.println("Could not load graph: " + e);
            System.exit(2);
        }

        ProgressLogger logger = new ProgressLogger();
        // noinspection ResultOfMethodCallIgnored
        new File(outdirPath).mkdirs();
        try {
            // ArrayList<ArrayList<Long>> components = connectedComponents.compute(logger);
            // components.sort(Comparator.comparing(ArrayList<Long>::size).reversed());

            // printDistribution(components, new Formatter(outdirPath + "/distribution.txt"));
            // printLargestComponent(components, new Formatter(outdirPath + "/largest_component.txt"));
            // printAllComponents(components, new Formatter(outdirPath + "/all_components.txt"));

            HashMap<Long, Long> componentDistribution = connectedComponents.compute(logger, byOrigin);
            PrintWriter f = new PrintWriter(new FileWriter(outdirPath + "/distribution.txt"));
            TreeMap<Long, Long> sortedDistribution = new TreeMap<>(componentDistribution);
            for (Map.Entry<Long, Long> entry : sortedDistribution.entrySet()) {
                f.println(entry.getKey() + " " + entry.getValue());
            }
            f.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.done();
    }
}
