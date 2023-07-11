/*
 * Copyright (c) 2023 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.utils;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongMappedBigList;
import it.unimi.dsi.logging.ProgressLogger;
import org.softwareheritage.graph.*;

import java.io.IOException;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.io.RandomAccessFile;
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

    private SwhUnidirectionalGraph graph;
    private ThreadLocal<SwhUnidirectionalGraph> threadGraph;
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
        cdf.graph = SwhUnidirectionalGraph.loadMapped(graphPath);
        System.err.println("Loading timestamps from " + graphPath + " ...");
        cdf.graph.loadAuthorTimestamps();
        cdf.threadGraph = new ThreadLocal<SwhUnidirectionalGraph>();

        System.err.println("Loading provenance timestamps from " + timestampsPath + " ...");
        RandomAccessFile raf = new RandomAccessFile(timestampsPath, "r");
        cdf.maxTimestamps = LongMappedBigList.map(raf.getChannel());

        cdf.run();
    }

    public void run() throws IOException, InterruptedException, ExecutionException {
        BufferedWriter bufferedStdout = new BufferedWriter(new OutputStreamWriter(System.out));
        csvPrinter = new CSVPrinter(bufferedStdout, CSVFormat.RFC4180);
        csvPrinter.printRecord("max_author_date", "frontier_dir_SWHID", "rev_SWHID");

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
                    findFrontiersInRevisionChunk(chunkId, numChunks, pl);
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

    // Like {@class StringBuffer}, but can grow over 2^31 bytes.
    private class BigStringBuffer implements Appendable {
        Vector<StringBuffer> buffers;

        BigStringBuffer(int capacity) {
            buffers = new Vector<StringBuffer>();
            buffers.add(new StringBuffer(capacity));
        }

        void ensureCanAppend(int length) {
            if (buffers.lastElement().length() + length + 3 < 0) {
                // System.err.format("adding new buffer. buffers.length()==%d, buffers.lastElement().size()==%d\n",
                // buffers.size(), buffers.lastElement().length());
                buffers.add(new StringBuffer(Integer.MAX_VALUE - 2));
            }
        }

        public BigStringBuffer append(char c) {
            ensureCanAppend(1);
            try {
                buffers.lastElement().append(c);
            } catch (OutOfMemoryError e) {
                System.err.format("append1 OOMed. buffers.length()==%d, buffers.lastElement().size()==%d: %s\n",
                        buffers.size(), buffers.lastElement().length(), e);
                throw new RuntimeException(e);
            }
            return this;
        }

        public BigStringBuffer append(CharSequence csq) {
            ensureCanAppend(csq.length());
            try {
                buffers.lastElement().append(csq);
            } catch (OutOfMemoryError e) {
                System.err.format(
                        "append2 OOMed. buffers.length()==%d, buffers.lastElement().size()==%d, csq.length()==%d: %s\n",
                        buffers.size(), buffers.lastElement().length(), csq.length(), e);
                throw new RuntimeException(e);
            }
            return this;
        }

        public BigStringBuffer append(CharSequence csq, int start, int end) {
            ensureCanAppend(end - start);
            try {
                buffers.lastElement().append(csq, start, end);
            } catch (OutOfMemoryError e) {
                System.err.format(
                        "append3 OOMed. buffers.length()==%d, buffers.lastElement().size()==%d, start=%d, end=%d: %s\n",
                        buffers.size(), buffers.lastElement().length(), start, end, e);
                throw new RuntimeException(e);
            }
            return this;
        }

        long length() {
            long r = 0;
            for (StringBuffer buffer : buffers) {
                r += buffer.length();
            }
            return r;
        }

        void flushToStdout() throws IOException {
            Vector<byte[]> arrays = new Vector<byte[]>(buffers.size());
            for (StringBuffer buffer : buffers) {
                arrays.add(buffer.toString().getBytes());
            }
            synchronized (System.out) {
                for (byte[] array : arrays) {
                    System.out.write(array);
                }
            }
        }
    }

    private void findFrontiersInRevisionChunk(long chunkId, long numChunks, ProgressLogger pl) throws IOException {
        if (threadGraph.get() == null) {
            threadGraph.set(this.graph.copy());
        }
        SwhUnidirectionalGraph graph = threadGraph.get();
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
            if (graph.getNodeType(i) != SwhType.REV) {
                continue;
            }

            try {
                findFrontiersInRevision(graph, i, csvPrinter);
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

    /* Performs a BFS, stopping at frontier directories. */
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

        // TODO: reuse these across calls instead of reallocating?
        LongArrayList stack = new LongArrayList();
        LongOpenHashSet visited = new LongOpenHashSet();

        /*
         * Do not push the root directory directly on the stack, because it is not allowed to be considered
         * a frontier. Instead, we push the root directory's successors.
         */
        it = graph.successors(rootDirectory);
        for (long successorId; (successorId = it.nextLong()) != -1;) {
            if (graph.getNodeType(successorId) == SwhType.DIR) {
                stack.push(successorId);
            }
        }

        long nodeId, maxTimestamp;
        boolean isFrontier;
        while (!stack.isEmpty()) {
            nodeId = stack.popLong();
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
            if (maxTimestamp < revisionTimestamp) {
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
                csvPrinter.printRecord(maxTimestamp, graph.getSWHID(nodeId), revSWHID);
            } else {
                /* Look if the subdirectories are frontiers */
                it = graph.successors(nodeId);
                for (long successorId; (successorId = it.nextLong()) != -1;) {
                    if (graph.getNodeType(successorId) == SwhType.DIR && !visited.contains(successorId)) {
                        stack.add(successorId);
                        break;
                    }
                }
            }
        }
    }
}
