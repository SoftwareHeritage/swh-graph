package org.softwareheritage.graph.benchmark;

import com.google.common.primitives.Longs;
import com.martiansoftware.jsap.*;
import it.unimi.dsi.big.webgraph.ImmutableGraph;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.io.ByteDiskQueue;
import it.unimi.dsi.logging.ProgressLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareheritage.graph.Graph;

import java.io.File;
import java.io.IOException;

public class BFS {
    private final static Logger LOGGER = LoggerFactory.getLogger(BFS.class);
    private final ImmutableGraph graph;

    public BFS(ImmutableGraph graph) {
        this.graph = graph;
    }

    private static JSAPResult parse_args(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(BFS.class.getName(), "",
                    new Parameter[]{
                            new FlaggedOption("graphPath", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'g',
                                    "graph", "Basename of the compressed graph"),

                            new FlaggedOption("useTransposed", JSAP.BOOLEAN_PARSER, "false", JSAP.NOT_REQUIRED, 'T',
                                    "transposed", "Use transposed graph (default: false)"),});

            config = jsap.parse(args);
            if (jsap.messagePrinted()) {
                System.exit(1);
            }
        } catch (JSAPException e) {
            e.printStackTrace();
        }
        return config;
    }

    public static void main(String[] args) throws IOException {
        JSAPResult config = parse_args(args);
        String graphPath = config.getString("graphPath");
        boolean useTransposed = config.getBoolean("useTransposed");

        System.err.println("Loading graph " + graphPath + " ...");
        Graph graph = new Graph(graphPath);
        System.err.println("Graph loaded.");

        if (useTransposed)
            graph = graph.transpose();

        BFS bfs = new BFS(graph);
        bfs.bfsperm();
    }

    // Partly inlined from it.unimi.dsi.law.big.graph.BFS
    private void bfsperm() throws IOException {
        final long n = graph.numNodes();
        // Allow enough memory to behave like in-memory queue
        int bufferSize = (int) Math.min(Arrays.MAX_ARRAY_SIZE & ~0x7, 8L * n);

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

        for (long i = 0; i < n; i++) {
            if (visited.getBoolean(i))
                continue;
            queue.enqueue(Longs.toByteArray(i));
            visited.set(i);

            while (!queue.isEmpty()) {
                queue.dequeue(byteBuf);
                final long currentNode = Longs.fromByteArray(byteBuf);

                final LazyLongIterator iterator = graph.successors(currentNode);
                long succ;
                while ((succ = iterator.nextLong()) != -1) {
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
}
