package org.softwareheritage.graph.experiments.topology;

import com.google.common.primitives.Longs;
import com.martiansoftware.jsap.*;
import it.unimi.dsi.Util;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.io.ByteDiskQueue;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.XoRoShiRo128PlusRandom;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.experiments.forks.ForkCC;

import java.io.*;

public class SubdatasetSizeFunction {

    private SubdatasetSizeFunction() {
    }

    public static void run(final Graph graph) throws IOException {
        final ProgressLogger pl = new ProgressLogger();
        pl.itemsName = "nodes";
        pl.expectedUpdates = graph.numNodes();

        long n = graph.numNodes();
        LongArrayBitVector visited = LongArrayBitVector.ofLength(n);

        int bufferSize = (int) Math.min(Arrays.MAX_ARRAY_SIZE & ~0x7, 8L * n);
        final File queueFile = File.createTempFile(ForkCC.class.getSimpleName(), "queue");
        final ByteDiskQueue queue = ByteDiskQueue.createNew(queueFile, bufferSize, true);
        final byte[] byteBuf = new byte[Long.BYTES];

        long[][] randomPerm = Util.identity(graph.numNodes());
        LongBigArrays.shuffle(randomPerm, new XoRoShiRo128PlusRandom());

        long visitedNodes = 0;
        long visitedEdges = 0;
        long visitedOrigins = 0;
        long visitedContents = 0;
        pl.start("Running traversal starting from origins...");
        for (long j = 0; j < n; ++j) {
            long i = BigArrays.get(randomPerm, j);
            if (visited.getBoolean(i) || graph.getNodeType(i) != Node.Type.ORI) {
                continue;
            }
            visitedOrigins++;
            queue.enqueue(Longs.toByteArray(i));
            visited.set(i);

            while (!queue.isEmpty()) {
                queue.dequeue(byteBuf);
                final long currentNode = Longs.fromByteArray(byteBuf);

                visitedNodes++;
                if (graph.getNodeType(currentNode) == Node.Type.CNT)
                    visitedContents++;

                final LazyLongIterator iterator = graph.successors(currentNode);
                long succ;
                while ((succ = iterator.nextLong()) != -1) {
                    visitedEdges++;
                    if (visited.getBoolean(succ))
                        continue;
                    visited.set(succ);
                    queue.enqueue(Longs.toByteArray(succ));
                }

                pl.update();
            }

            if (visitedOrigins % 10000 == 0)
                System.out.println(visitedNodes + " " + visitedEdges + " " + visitedContents);
        }
        pl.done();
    }

    static public void main(final String[] arg)
            throws IllegalArgumentException, SecurityException, JSAPException, IOException {
        final SimpleJSAP jsap = new SimpleJSAP(SubdatasetSizeFunction.class.getName(),
                "Computes subdataset size functions using a random uniform order",
                new Parameter[]{new UnflaggedOption("basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED,
                        JSAP.NOT_GREEDY, "The basename of the graph."),});

        final JSAPResult jsapResult = jsap.parse(arg);
        if (jsap.messagePrinted())
            System.exit(1);

        final String basename = jsapResult.getString("basename");

        Graph graph = Graph.loadMapped(basename);
        run(graph);
    }
}
