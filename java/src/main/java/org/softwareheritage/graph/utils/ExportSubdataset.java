/*
 * Copyright (c) 2021 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.utils;

import com.google.common.primitives.Longs;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.objects.Object2LongFunction;
import it.unimi.dsi.io.ByteDiskQueue;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.LineIterator;
import org.softwareheritage.graph.SwhBidirectionalGraph;
import org.softwareheritage.graph.SWHID;
import org.softwareheritage.graph.experiments.topology.ConnectedComponents;
import org.softwareheritage.graph.maps.NodeIdMap;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class ExportSubdataset {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        System.err.print("Loading everything...");
        String graphPath = args[0];
        SwhBidirectionalGraph graph = SwhBidirectionalGraph.loadMapped(graphPath);
        Object2LongFunction<byte[]> mphMap = NodeIdMap.loadMph(graphPath + ".mph");
        System.err.println(" done.");

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

        FastBufferedReader buffer = new FastBufferedReader(new InputStreamReader(System.in, StandardCharsets.US_ASCII));
        LineIterator lineIterator = new LineIterator(buffer);

        while (lineIterator.hasNext()) {
            String line = lineIterator.next().toString();
            long i;
            try {
                // i = mphMap.getLong(line.getBytes(StandardCharsets.UTF_8));
                i = graph.getNodeId(new SWHID(line));
            } catch (IllegalArgumentException e) {
                continue;
            }

            queue.enqueue(Longs.toByteArray(i));
            visited.set(i);

            while (!queue.isEmpty()) {
                queue.dequeue(byteBuf);
                final long currentNode = Longs.fromByteArray(byteBuf);
                SWHID currentNodeSWHID = graph.getSWHID(currentNode);

                final LazyLongIterator iterator = graph.successors(currentNode);
                long succ;
                while ((succ = iterator.nextLong()) != -1) {
                    System.out.format("%s %s\n", currentNodeSWHID, graph.getSWHID(succ));
                    if (visited.getBoolean(succ))
                        continue;
                    visited.set(succ);
                    queue.enqueue(Longs.toByteArray(succ));
                }
            }

        }
    }
}
