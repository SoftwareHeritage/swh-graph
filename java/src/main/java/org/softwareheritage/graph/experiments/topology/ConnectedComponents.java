package org.softwareheritage.graph.experiments.topology;

import com.google.common.primitives.Longs;
import com.martiansoftware.jsap.*;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.io.ByteDiskQueue;
import it.unimi.dsi.logging.ProgressLogger;
import org.softwareheritage.graph.Graph;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;

public class ConnectedComponents {
    private Graph graph;

    private void load_graph(String graphBasename) throws IOException {
        System.err.println("Loading graph " + graphBasename + " ...");
        this.graph = new Graph(graphBasename).symmetrize();
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

    private HashMap<Long, Long> /* ArrayList<ArrayList<Long>> */ compute(ProgressLogger pl) throws IOException {
        final long n = graph.numNodes();

        // Allow enough memory to behave like in-memory queue
        int bufferSize = (int) Math.min(Arrays.MAX_ARRAY_SIZE & ~0x7, 8L * n);

        // Use a disk based queue to store BFS frontier
        final File queueFile = File.createTempFile(ConnectedComponents.class.getSimpleName(), "queue");
        final ByteDiskQueue queue = ByteDiskQueue.createNew(queueFile, bufferSize, true);
        final byte[] byteBuf = new byte[Long.BYTES];
        // WARNING: no 64-bit version of this data-structure, but it can support
        // indices up to 2^37
        LongArrayBitVector visited = LongArrayBitVector.ofLength(n);

        pl.expectedUpdates = n;
        pl.itemsName = "nodes";
        pl.start("Starting connected components visit...");

        // ArrayList<ArrayList<Long>> components = new ArrayList<>();
        HashMap<Long, Long> componentDistribution = new HashMap<>();

        for (long i = 0; i < n; i++) {
            // ArrayList<Long> component = new ArrayList<>();
            long componentNodes = 0;

            queue.enqueue(Longs.toByteArray(i));
            visited.set(i);

            while (!queue.isEmpty()) {
                queue.dequeue(byteBuf);
                final long currentNode = Longs.fromByteArray(byteBuf);
                // component.add(currentNode);
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

        ConnectedComponents connectedComponents = new ConnectedComponents();
        try {
            connectedComponents.load_graph(graphPath);
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

            HashMap<Long, Long> componentDistribution = connectedComponents.compute(logger);
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
