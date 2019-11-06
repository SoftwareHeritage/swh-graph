package org.softwareheritage.graph.benchmark;

import com.google.common.primitives.Longs;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.io.ByteDiskQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareheritage.graph.Graph;

import com.martiansoftware.jsap.*;

import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.fastutil.Arrays;

import java.io.File;
import java.io.IOException;


public class BFS {
    private Graph graph;

    private void load_graph(String graphBasename) throws IOException {
        System.err.println("Loading graph " + graphBasename + " ...");
        this.graph = new Graph(graphBasename);
        System.err.println("Graph loaded.");
    }

    private final static Logger LOGGER = LoggerFactory.getLogger(BFS.class);

    // Partly inlined from it.unimi.dsi.law.big.graph.BFS
    private static void bfsperm(final ImmutableGraph graph) throws IOException {
        final long n = graph.numNodes();
        // Allow enough memory to behave like in-memory queue
        int bufferSize = (int)Math.min(Arrays.MAX_ARRAY_SIZE & ~0x7, 8L * n);

        // Use a disk based queue to store BFS frontier
        final File queueFile = File.createTempFile(BFS.class.getSimpleName(), "queue");
        final ByteDiskQueue queue = ByteDiskQueue.createNew(queueFile, bufferSize, true);
        final byte[] byteBuf = new byte[Long.BYTES];
        // WARNING: no 64-bit version of this data-structure, but it can support
        // indices up to 2^37
        final LongArrayBitVector visited = LongArrayBitVector.ofLength(n);
        final ProgressLogger pl = new ProgressLogger(LOGGER);
        pl.expectedUpdates = n;
        pl.itemsName = "nodes";
        pl.start("Starting breadth-first visit...");

        long pos = 0;

        for (long i = 0; i < n; i++) {
            if (visited.getBoolean(i)) continue;
            queue.enqueue(Longs.toByteArray(i));
            visited.set(i);

            while (!queue.isEmpty()) {
                queue.dequeue(byteBuf);
                final long currentNode = Longs.fromByteArray(byteBuf);

                final LazyLongIterator iterator = graph.successors(currentNode);
                long succ;
                while((succ = iterator.nextLong()) != -1) {
                    if (!visited.getBoolean(succ)) {
                        visited.set(succ);
                        queue.enqueue(Longs.toByteArray(succ));
                    }
                }

                pl.update();
            }
        }

        pl.done();
        queue.close();
    }


    private static JSAPResult parse_args(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(
                    BFS.class.getName(),
                    "",
                    new Parameter[] {
                            new FlaggedOption("graphPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED,
                                    'g', "graph", "Basename of the compressed graph"),

                            new FlaggedOption("useTransposed", JSAP.BOOLEAN_PARSER, "false", JSAP.NOT_REQUIRED,
                                    'T', "transposed", "Use transposed graph (default: false)"),
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
        boolean useTransposed = config.getBoolean("useTransposed");

        ProgressLogger logger = new ProgressLogger();
        long startTime;
        double totalTime;


        BFS bfs = new BFS();
        try {
            bfs.load_graph(graphPath);
        } catch (IOException e) {
            System.out.println("Could not load graph: " + e);
            System.exit(2);
        }

        logger.start("Parallel BFS visit...");
        try {
            BFS.bfsperm(bfs.graph.getBVGraph(useTransposed));
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.done();
    }
}
