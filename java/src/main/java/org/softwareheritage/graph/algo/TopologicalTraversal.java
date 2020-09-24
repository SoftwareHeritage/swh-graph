package org.softwareheritage.graph.algo;

import com.google.common.primitives.Longs;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.io.ByteDiskQueue;
import it.unimi.dsi.logging.ProgressLogger;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Traversal;
import org.softwareheritage.graph.experiments.forks.ForkCC;

import java.io.File;
import java.io.IOException;

public class TopologicalTraversal {
    public static void run(final Graph graph, Traversal.NodeIdConsumer cb) throws IOException {
        final long[][] indegree = LongBigArrays.newBigArray(graph.numNodes());
        final ProgressLogger pl = new ProgressLogger();

        pl.itemsName = "nodes";
        pl.expectedUpdates = graph.numNodes();

        pl.start("Fetching indegrees...");
        long n = graph.numNodes();
        for (long i = 0; i < graph.numNodes(); ++i) {
            BigArrays.add(indegree, i, graph.indegree(i));
        }
        pl.done();

        LongArrayBitVector visited = LongArrayBitVector.ofLength(n);

        int bufferSize = (int) Math.min(Arrays.MAX_ARRAY_SIZE & ~0x7, 8L * n);
        final File queueFile = File.createTempFile(ForkCC.class.getSimpleName(), "queue");
        final ByteDiskQueue queue = ByteDiskQueue.createNew(queueFile, bufferSize, true);
        final byte[] byteBuf = new byte[Long.BYTES];

        pl.start("Traversal in topological order...");
        for (long i = 0; i < graph.numNodes(); ++i) {
            if (visited.getBoolean(i) || BigArrays.get(indegree, i) != 0L) {
                continue;
            }

            queue.enqueue(Longs.toByteArray(i));
            visited.set(i);

            while (!queue.isEmpty()) {
                queue.dequeue(byteBuf);
                final long currentNode = Longs.fromByteArray(byteBuf);

                cb.accept(currentNode);

                final LazyLongIterator iterator = graph.successors(currentNode);
                long succ;
                while ((succ = iterator.nextLong()) != -1) {
                    BigArrays.add(indegree, succ, -1L);
                    if (visited.getBoolean(succ) || BigArrays.get(indegree, succ) != 0)
                        continue;
                    visited.set(succ);
                    queue.enqueue(Longs.toByteArray(succ));
                }

                pl.update();
            }
        }
        pl.done();
    }
}
