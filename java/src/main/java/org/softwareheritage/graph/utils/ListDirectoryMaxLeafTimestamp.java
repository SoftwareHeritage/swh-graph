/*
 * Copyright (c) 2023 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.utils;

import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.longs.LongMappedBigList;
import it.unimi.dsi.logging.ProgressLogger;
import org.softwareheritage.graph.*;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.concurrent.*;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Given as input a binary file containing an array of timestamps which is,
 * for every content, the date of first occurrence of that content in a revision.
 * Produces a binary file in the same format, which contains for each directory,
 * the max of these values for all contents in that directory.
 *
 * If the <path/to/provenance_timestamps.bin> parameter is passed, then this file
 * is written as an array of longs, which can be loaded with LongMappedBigList.
 *
 * This new date is guaranteed to be lower or equal to the one in the input.
 */

public class ListDirectoryMaxLeafTimestamp {

    private class MyLongBigArrayBigList extends LongBigArrayBigList {
        public MyLongBigArrayBigList(long size) {
            super(size);

            // Allow setting directly in the array without repeatedly calling
            // .add() first
            this.size = size;
        }
    }

    private int NUM_THREADS = 96;

    private String graphPath;
    private SwhUnidirectionalGraph graph;
    private ThreadLocal<SwhUnidirectionalGraph> threadGraph;
    private SwhUnidirectionalGraph transposeGraph;
    private MyLongBigArrayBigList unvisitedChildren;
    private LongMappedBigList timestamps;
    private LongBigArrayBigList maxTimestamps;
    private CSVPrinter csvPrinter;
    private String binOutputPath;
    final static Logger logger = LoggerFactory.getLogger(ListDirectoryMaxLeafTimestamp.class);

    // Queue of the DFS
    private LongBigArrayBigList toVisit;
    private long toVisitOffset;

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, ExecutionException {
        if (args.length < 2 || args.length > 3) {
            System.err.println(
                    "Syntax: java org.softwareheritage.graph.utils.ListDirectoryMaxLeafTimestamp <path/to/graph> <path/to/earliest_timestamps.bin> [<path/to/provenance_timestamps.bin>]");
            System.exit(1);
        }

        ListDirectoryMaxLeafTimestamp ldmlt = new ListDirectoryMaxLeafTimestamp();

        ldmlt.graphPath = args[0];
        ldmlt.binOutputPath = null;
        if (args.length >= 3) {
            ldmlt.binOutputPath = args[2];
        }

        String timestampsPath = args[1];
        System.err.println("Loading timestamps from " + timestampsPath + " ...");
        RandomAccessFile raf = new RandomAccessFile(timestampsPath, "r");
        ldmlt.timestamps = LongMappedBigList.map(raf.getChannel());

        System.err.println("Loading graph " + ldmlt.graphPath + " ...");
        ldmlt.graph = SwhUnidirectionalGraph.loadMapped(ldmlt.graphPath);
        ldmlt.threadGraph = new ThreadLocal<SwhUnidirectionalGraph>();

        ldmlt.run();
    }

    public void run() throws IOException, InterruptedException, ExecutionException {
        System.err.println("Allocating memory...");
        long numNodes = graph.numNodes();
        unvisitedChildren = new MyLongBigArrayBigList(numNodes);

        final ProgressLogger pl1 = new ProgressLogger(logger);
        pl1.logInterval = 60000;
        pl1.itemsName = "nodes";
        pl1.expectedUpdates = numNodes;
        pl1.start("Initializing unvisitedChildren array...");
        final long numChunks = NUM_THREADS * 1000;

        ExecutorService service = Executors.newFixedThreadPool(NUM_THREADS);
        Vector<Future> futures = new Vector<Future>();
        for (long i = 0; i < numChunks; ++i) {
            final long chunkId = i;
            futures.add(service.submit(() -> {
                initializeUnvisitedChildrenChunk(chunkId, numChunks, pl1);
            }));
        }

        service.shutdown();
        service.awaitTermination(365, TimeUnit.DAYS);
        pl1.done();

        // Error if any exception occurred
        for (Future future : futures) {
            future.get();
        }
        futures.clear();

        service = Executors.newFixedThreadPool(NUM_THREADS);

        System.err.println("Deallocating " + graphPath + " ...");
        SwhGraphProperties properties = graph.getProperties();
        graph = null;

        futures.add(service.submit(() -> {
            System.err.println("Loading graph " + graphPath + "-transposed ...");
            try {
                transposeGraph = SwhUnidirectionalGraph.loadGraphOnly(SwhUnidirectionalGraph.LoadMethod.MAPPED,
                        graphPath + "-transposed", null, null);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            transposeGraph.setProperties(properties);
            System.err.println("Done loading graph " + graphPath + "-transposed.");
        }));

        futures.add(service.submit(() -> {
            System.err.println("Allocating maxTimestamps array...");
            maxTimestamps = new LongBigArrayBigList(numNodes);
            for (long i = 0; i < numNodes; ++i) {
                maxTimestamps.add(Long.MIN_VALUE);
            }
            System.err.println("Done allocating maxTimestamps array.");
        }));

        System.err.format("Initializing BFS (preallocated %d for toVisit)...\n", numNodes / 10);
        toVisit = new LongBigArrayBigList(numNodes / 10);
        ProgressLogger pl2 = new ProgressLogger(logger);
        pl2.logInterval = 60000;
        pl2.itemsName = "nodes";
        pl2.expectedUpdates = numNodes;
        pl2.start("Initializing BFS...");
        ThreadLocal<LongBigArrayBigList> threadBuf = new ThreadLocal<LongBigArrayBigList>();
        for (long i = 0; i < numChunks; ++i) {
            final long chunkId = i;
            futures.add(service.submit(() -> {
                long chunkSize = numNodes / numChunks;
                long chunkStart = chunkSize * chunkId;
                long chunkEnd = chunkId == numChunks - 1 ? numNodes : chunkSize * (chunkId + 1);

                if (threadBuf.get() == null) {
                    threadBuf.set(new LongBigArrayBigList(chunkSize));
                }
                LongBigArrayBigList buf = threadBuf.get();

                for (long j = chunkStart; j < chunkEnd; j++) {
                    if (properties.getNodeType(j) == SwhType.CNT) {
                        buf.add(j);
                    }
                }

                // toVisit drops elements when multiple threads append at the same time
                // at the same place
                synchronized (toVisit) {
                    pl2.update(chunkSize);
                    toVisit.addAll(buf);
                }
                buf.clear();
            }));
        }
        service.shutdown();
        service.awaitTermination(365, TimeUnit.DAYS);

        // Error if any exception occurred
        for (Future future : futures) {
            future.get();
        }

        System.err.format("Done initializing BFS (starting from %d contents).\n", toVisit.size64());

        toVisitOffset = 0;
        bfs();

        if (binOutputPath != null) {
            System.err.format("Writing binary output to %s\n", binOutputPath);
            BinIO.storeLongs(maxTimestamps.elements(), binOutputPath);
        }
    }

    private void initializeUnvisitedChildrenChunk(long chunkId, long numChunks, ProgressLogger pl) {
        if (threadGraph.get() == null) {
            threadGraph.set(this.graph.copy());
        }
        SwhUnidirectionalGraph graph = threadGraph.get();
        long numNodes = graph.numNodes();
        long chunkSize = numNodes / numChunks;
        long chunkStart = chunkSize * chunkId;
        long chunkEnd = chunkId == numChunks - 1 ? numNodes : chunkSize * (chunkId + 1);
        for (long i = chunkStart; i < chunkEnd; i++) {
            long children = 0;
            if (graph.getNodeType(i) == SwhType.DIR) {
                LazyLongIterator it = graph.successors(i);

                for (long successorId; (successorId = it.nextLong()) != -1;) {
                    if (graph.getNodeType(successorId) == SwhType.DIR
                            || graph.getNodeType(successorId) == SwhType.CNT) {
                        children++;
                    }

                }
            }

            unvisitedChildren.set(i, children);
        }
        pl.update(chunkSize);
    }

    private void bfs() {
        ProgressLogger pl = new ProgressLogger(logger);
        pl.logInterval = 60000;
        pl.itemsName = "nodes";
        pl.expectedUpdates = Double.valueOf(transposeGraph.numNodes() / 1.2).longValue(); // rough estimate of the
                                                                                          // number of contents and
                                                                                          // directories
        pl.start("Running BFS...");

        long nodeId;
        while (toVisitOffset < toVisit.size64()) {
            pl.lightUpdate();
            nodeId = toVisit.getLong(toVisitOffset++);

            visitNode(nodeId);
        }

        System.err.format("BFS done (visited %d nodes).\n", toVisitOffset);
    }

    private void visitNode(long nodeId) {
        long nodeMaxTimestamp = Long.MIN_VALUE;

        if (transposeGraph.getNodeType(nodeId) == SwhType.CNT) {
            nodeMaxTimestamp = timestamps.getLong(nodeId);
        } else if (transposeGraph.getNodeType(nodeId) == SwhType.DIR) {
            nodeMaxTimestamp = maxTimestamps.getLong(nodeId);
        } else {
            System.err.format("%s has unexpected type %s\n", transposeGraph.getSWHID(nodeId),
                    transposeGraph.getNodeType(nodeId).toString());
            System.exit(4);
        }

        LazyLongIterator it = transposeGraph.successors(nodeId);

        for (long ancestorId; (ancestorId = it.nextLong()) != -1;) {
            if (transposeGraph.getNodeType(ancestorId) == SwhType.DIR) {
                long ancestorUnvisitedChildren = unvisitedChildren.getLong(ancestorId);
                long ancestorMaxTimestamp = maxTimestamps.getLong(ancestorId);

                if (nodeMaxTimestamp > ancestorMaxTimestamp) {
                    ancestorMaxTimestamp = nodeMaxTimestamp;
                    maxTimestamps.set(ancestorId, ancestorMaxTimestamp);
                }

                ancestorUnvisitedChildren--;
                unvisitedChildren.set(ancestorId, ancestorUnvisitedChildren);

                if (ancestorUnvisitedChildren < 0) {
                    System.err.format("%s has more children visits than expected (current child: %s)\n",
                            transposeGraph.getSWHID(ancestorId), transposeGraph.getSWHID(nodeId));
                    System.exit(5);
                } else if (ancestorUnvisitedChildren == 0) {
                    // We are visiting the last remaining child of this parent
                    toVisit.add(ancestorId);
                }
            }
        }
    }
}
