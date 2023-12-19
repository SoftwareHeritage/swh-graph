/*
 * Copyright (c) 2023 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.utils;

import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongMappedBigList;
import it.unimi.dsi.logging.ProgressLogger;
import org.softwareheritage.graph.*;
import org.softwareheritage.graph.labels.DirEntry;

import java.io.IOException;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.RandomAccessFile;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Given as stdin a CSV with header "max_author_date,dir_SWHID" containing, for each
 * directory, the newest date of first occurrence of any of the content in its subtree
 * (well, DAG), ie., max_{for all content} (min_{for all occurrence of content} occurrence).
 * Produces the "provenance frontier", as defined in
 * https://gitlab.softwareheritage.org/swh/devel/swh-provenance/-/blob/ae09086a3bd45c7edbc22691945b9d61200ec3c2/swh/provenance/algos/revision.py#L210
 */

public class ComputeDirectoryFrontier {

    private class MyLongBigArrayBigList extends LongBigArrayBigList {
        public MyLongBigArrayBigList(long size) {
            super(size);

            // Allow setting directly in the array without repeatedly calling
            // .add() first
            this.size = size;
        }
    }

    private int NUM_THREADS = 96;
    private int batchSize; /* Number of revisions to process in each task */

    private SwhBidirectionalGraph graph;
    private ThreadLocal<SwhBidirectionalGraph> threadGraph;
    private LongMappedBigList maxTimestamps; /*
                                              * The parsed input.
                                              */
    private CSVPrinter csvPrinter;
    final static Logger logger = LoggerFactory.getLogger(ComputeDirectoryFrontier.class);

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, ExecutionException {
        if (args.length != 3) {
            System.err.println(
                    "Syntax: java org.softwareheritage.graph.utils.ComputeDirectoryFrontier <path/to/graph> <path/to/provenance_timestamps.bin> <batch_size>");
            System.exit(1);
        }

        ComputeDirectoryFrontier cdf = new ComputeDirectoryFrontier();

        String graphPath = args[0];
        String timestampsPath = args[1];
        cdf.batchSize = Integer.parseInt(args[2]);

        System.err.println("Loading graph " + graphPath + " ...");
        cdf.graph = SwhBidirectionalGraph.loadLabelledMapped(graphPath);
        System.err.println("Loading timestamps from " + graphPath + " ...");
        cdf.graph.loadAuthorTimestamps();
        System.err.println("Loading label names from " + graphPath + " ...");
        cdf.graph.loadLabelNames();
        cdf.threadGraph = new ThreadLocal<SwhBidirectionalGraph>();

        System.err.println("Loading provenance timestamps from " + timestampsPath + " ...");
        RandomAccessFile raf = new RandomAccessFile(timestampsPath, "r");
        cdf.maxTimestamps = LongMappedBigList.map(raf.getChannel());

        cdf.run();
    }

    public void run() throws IOException, InterruptedException, ExecutionException {
        BufferedWriter bufferedStdout = new BufferedWriter(new OutputStreamWriter(System.out));
        csvPrinter = new CSVPrinter(bufferedStdout, CSVFormat.RFC4180);
        csvPrinter.printRecord("max_author_date", "frontier_dir_SWHID", "rev_author_date", "rev_SWHID", "path");

        csvPrinter.flush();
        bufferedStdout.flush();

        findFrontiers();

        csvPrinter.flush();
        bufferedStdout.flush();
    }

    private void findFrontiers() throws InterruptedException, ExecutionException {
        final long numChunks = NUM_THREADS * 1000;

        System.err.println("Initializing traversals...");
        ExecutorService service = Executors.newFixedThreadPool(NUM_THREADS);
        Vector<Future> futures = new Vector<Future>();
        List<Long> range = new ArrayList<Long>();
        for (long i = 0; i < numChunks; i++) {
            range.add(i);
        }
        Collections.shuffle(range); // Make workload homogeneous over time

        ProgressLogger pl = new ProgressLogger(logger);
        pl.logInterval = 60000;
        pl.itemsName = "nodes";
        pl.expectedUpdates = graph.numNodes();
        pl.start("Visiting revisions' directories...");
        for (long i : range) {
            final long chunkId = i;
            futures.add(service.submit(() -> {
                try {
                    findFrontiersInRevrelChunk(chunkId, numChunks, pl);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }

            }));
        }

        for (Future future : futures) {
            future.get();
        }

        service.shutdown();
        service.awaitTermination(365, TimeUnit.DAYS);
        pl.done();

        // Error if any exception occurred
        for (Future future : futures) {
            future.get();
        }
    }

    private void findFrontiersInRevrelChunk(long chunkId, long numChunks, ProgressLogger pl) throws IOException {
        if (threadGraph.get() == null) {
            threadGraph.set(this.graph.copy());
        }
        SwhBidirectionalGraph graph = threadGraph.get();
        long numNodes = graph.numNodes();
        long chunkSize = numNodes / numChunks;
        long chunkStart = chunkSize * chunkId;
        long chunkEnd = chunkId == numChunks - 1 ? numNodes : chunkSize * (chunkId + 1);

        // Each line is about 114 bytes ("timestamp,SWHID,SWHID\r\n").
        // Assume revision have generally less than ~10 frontier directories
        // -> ~1140 bytes per revision.
        // Also this is an overapproximation, as not all nodes in the chunk are
        // revisions.
        BigStringBuffer buf = new BigStringBuffer(batchSize * 1140);
        CSVPrinter csvPrinter = new CSVPrinter(buf, CSVFormat.RFC4180);
        long flushThreshold = batchSize * 1140 * 500; // Avoids wasting RAM for too long
        long previousLength = 0;
        long newLength;
        for (long i = chunkStart; i < chunkEnd; i++) {
            if (graph.getNodeType(i) == SwhType.REL) {
                // Allow releases
            } else if (graph.getNodeType(i) == SwhType.REV) {
                // Allow revisions if they are a snapshot head or are pointed by a revision
                boolean isSnapshotHead = false;
                LazyLongIterator it = graph.predecessors(i);
                for (long predecessorId; (predecessorId = it.nextLong()) != -1;) {
                    if (graph.getNodeType(predecessorId) == SwhType.SNP
                            || graph.getNodeType(predecessorId) == SwhType.REL) {
                        isSnapshotHead = true;
                    }
                }
                if (!isSnapshotHead) {
                    continue;
                }
            } else {
                // Reject all other objects
                continue;
            }

            try {
                if (graph.getNodeType(i) == SwhType.REL) {
                    findFrontiersInRelease(graph.getForwardGraph(), i, csvPrinter);
                } else {
                    findFrontiersInRevision(graph.getForwardGraph(), i, csvPrinter);
                }
            } catch (OutOfMemoryError e) {
                newLength = buf.length();
                System.err.format("OOMed while processing %s (buffer grew from %d to %d): %s\n", graph.getSWHID(i),
                        previousLength, newLength, e);
                throw new RuntimeException(e);
            }

            newLength = buf.length();
            if (newLength - previousLength > (2L << 30)) {
                System.err.format("Frontier CSV for %s is suspiciously large: %d GB\n", graph.getSWHID(i),
                        (newLength - previousLength) / 1000000000);
            }

            if (newLength > flushThreshold) {
                csvPrinter.flush();
                buf.flushToStdout();
                buf = new BigStringBuffer(batchSize * 1140);
                csvPrinter = new CSVPrinter(buf, CSVFormat.RFC4180);
                newLength = 0;
            }
            previousLength = newLength;
        }
        csvPrinter.flush();

        buf.flushToStdout();

        synchronized (pl) {
            pl.update(chunkSize);
        }
    }

    /* Calls findFrontiersInDirectory on the root directory of a release */
    private void findFrontiersInRelease(SwhUnidirectionalGraph graph, long relId, CSVPrinter csvPrinter)
            throws IOException {
        SWHID relSWHID = graph.getSWHID(relId);

        Long boxedReleaseTimestamp = graph.getAuthorTimestamp(relId);
        if (boxedReleaseTimestamp == null) {
            return;
        }
        long releaseTimestamp = boxedReleaseTimestamp;

        long rootDirectory = -1;
        LazyLongIterator it = graph.successors(relId);
        for (long successorId; (successorId = it.nextLong()) != -1;) {
            if (graph.getNodeType(successorId) != SwhType.DIR) {
                continue;
            }
            if (rootDirectory == -1) {
                rootDirectory = successorId;
                continue;
            }
            System.err.format("%s has more than one directory successor: %s and %s\n", relSWHID,
                    graph.getSWHID(rootDirectory), graph.getSWHID(successorId));
            System.exit(6);
        }

        if (rootDirectory == -1) {
            // Release does not have a direct directory successor, try with a revision
            // in-between
            it = graph.successors(relId);
            long rootRevision = -1;
            for (long successorId; (successorId = it.nextLong()) != -1;) {
                if (graph.getNodeType(successorId) != SwhType.REV) {
                    continue;
                }
                if (rootRevision == -1) {
                    rootRevision = successorId;
                    continue;
                }
                System.err.format("%s has more than one revision successor: %s and %s\n", relSWHID,
                        graph.getSWHID(rootRevision), graph.getSWHID(successorId));
                System.exit(6);
            }

            if (rootRevision == -1) {
                // points neither to a directory or a revision, ignore
                return;
            }

            SWHID revSWHID = graph.getSWHID(rootRevision);
            it = graph.successors(rootRevision);
            for (long successorId; (successorId = it.nextLong()) != -1;) {
                if (graph.getNodeType(successorId) != SwhType.DIR) {
                    continue;
                }
                if (rootDirectory == -1) {
                    rootDirectory = successorId;
                    continue;
                }
                System.err.format("%s (via %s) has more than one directory successor: %s and %s\n", relSWHID, revSWHID,
                        graph.getSWHID(rootDirectory), graph.getSWHID(successorId));
                System.exit(6);
            }

            if (rootDirectory == -1) {
                // probably unknown revision
                return;
            }
        }

        if (rootDirectory == -1) {
            System.err.format("Could not find the root directory for %s\n", relSWHID);
            System.exit(7);
        }

        findFrontiersInDirectory(graph, relId, relSWHID, releaseTimestamp, rootDirectory, csvPrinter);
    }

    /* Calls findFrontiersInDirectory on the root directory of a revision */
    private void findFrontiersInRevision(SwhUnidirectionalGraph graph, long revId, CSVPrinter csvPrinter)
            throws IOException {
        SWHID revSWHID = graph.getSWHID(revId);

        Long boxedRevisionTimestamp = graph.getAuthorTimestamp(revId);
        if (boxedRevisionTimestamp == null) {
            return;
        }
        long revisionTimestamp = boxedRevisionTimestamp;

        long rootDirectory = -1;
        LazyLongIterator it = graph.successors(revId);
        for (long successorId; (successorId = it.nextLong()) != -1;) {
            if (graph.getNodeType(successorId) != SwhType.DIR) {
                continue;
            }
            if (rootDirectory == -1) {
                rootDirectory = successorId;
                continue;
            }
            System.err.format("%s has more than one directory successor: %s and %s\n", revSWHID,
                    graph.getSWHID(rootDirectory), graph.getSWHID(successorId));
            System.exit(6);
        }

        findFrontiersInDirectory(graph, revId, revSWHID, revisionTimestamp, rootDirectory, csvPrinter);
    }

    /* Performs a BFS, stopping at frontier directories. */
    private void findFrontiersInDirectory(SwhUnidirectionalGraph graph, long revrelId, SWHID revrelSWHID,
            long revrelTimestamp, long rootDirectory, CSVPrinter csvPrinter) throws IOException {

        // TODO: reuse these across calls instead of reallocating?
        LongArrayList stack = new LongArrayList();
        LongOpenHashSet visited = new LongOpenHashSet();

        // For each item in the stack, stores a sequence of filenameId.
        // Sequences are separated by -1 values.
        LongArrayList pathStack = new LongArrayList();

        /*
         * Do not push the root directory directly on the stack, because it is not allowed to be considered
         * a frontier. Instead, we push the root directory's successors.
         */
        ArcLabelledNodeIterator.LabelledArcIterator itl = graph.labelledSuccessors(rootDirectory);
        for (long successorId; (successorId = itl.nextLong()) != -1;) {
            if (graph.getNodeType(successorId) == SwhType.DIR) {
                stack.push(successorId);

                pathStack.push(-1L);
                DirEntry[] labels = (DirEntry[]) itl.label().get();
                if (labels.length != 0) {
                    pathStack.push(labels[0].filenameId);
                }
            }
        }

        long nodeId, maxTimestamp;
        boolean isFrontier;
        LongArrayList path = new LongArrayList();
        LazyLongIterator it;
        while (!stack.isEmpty()) {
            nodeId = stack.popLong();
            path.clear();
            for (long filenameId; (filenameId = pathStack.popLong()) != -1L;) {
                path.push(filenameId);
            }
            Collections.reverse(path);

            maxTimestamp = maxTimestamps.getLong(nodeId);
            if (maxTimestamp == Long.MIN_VALUE || visited.contains(nodeId)) {
                continue;
            }
            visited.add(nodeId);

            /*
             * Detect if a node is a frontier according to
             * https://gitlab.softwareheritage.org/swh/devel/swh-provenance/-/blob/
             * ae09086a3bd45c7edbc22691945b9d61200ec3c2/swh/provenance/algos/revision.py#L210
             */
            isFrontier = false;
            if (maxTimestamp < revrelTimestamp) {
                /* No need to check if it's depth > 1, given that we excluded the root dir above */
                it = graph.successors(nodeId);
                for (long successorId; (successorId = it.nextLong()) != -1;) {
                    if (graph.getNodeType(successorId) == SwhType.CNT) {
                        isFrontier = true;
                        break;
                    }
                }
            }

            if (isFrontier) {
                Vector<String> pathParts = new Vector<String>(path.size());
                for (long filenameId : path) {
                    pathParts.add(new String(graph.getLabelName(filenameId)));
                }
                pathParts.add("");
                String pathString = String.join("/", pathParts);
                csvPrinter.printRecord(maxTimestamp, graph.getSWHID(nodeId), Instant.ofEpochSecond(revrelTimestamp),
                        revrelSWHID, pathString);
            } else {
                /* Look if the subdirectories are frontiers */
                itl = graph.labelledSuccessors(nodeId);
                for (long successorId; (successorId = itl.nextLong()) != -1;) {
                    if (graph.getNodeType(successorId) == SwhType.DIR && !visited.contains(successorId)) {
                        stack.add(successorId);
                        pathStack.push(-1L);
                        for (long filenameId : path) {
                            pathStack.push(filenameId);
                        }
                        DirEntry[] labels = (DirEntry[]) itl.label().get();
                        if (labels.length != 0) {
                            pathStack.push(labels[0].filenameId);
                        }
                    }
                }
            }
        }
    }
}
