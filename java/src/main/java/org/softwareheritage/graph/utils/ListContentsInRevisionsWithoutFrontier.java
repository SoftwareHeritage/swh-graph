/*
 * Copyright (c) 2023 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.utils;

import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.fastutil.booleans.BooleanBigArrayBigList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.logging.ProgressLogger;
import org.softwareheritage.graph.*;
import org.softwareheritage.graph.labels.DirEntry;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Given as stdin a CSV with header "frontier_dir_SWHID" containing a single column
 * with frontier directories, produces a CSV with header "cnt_SWHID,rev_SWHID,path",
 * listing contents reachable from release/revisions without going through any frontier
 * directory.
 */

public class ListContentsInRevisionsWithoutFrontier {

    private class MyBooleanBigArrayBigList extends BooleanBigArrayBigList {
        public MyBooleanBigArrayBigList(long size) {
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
    private CSVPrinter csvPrinter;
    private MyBooleanBigArrayBigList frontierDirectories;
    final static Logger logger = LoggerFactory.getLogger(ListContentsInRevisionsWithoutFrontier.class);

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, ExecutionException {
        if (args.length != 2) {
            System.err.println(
                    "Syntax: java org.softwareheritage.graph.utils.ListContentsInRevisionsWithoutFrontier <path/to/graph> <batch_size>");
            System.exit(1);
        }

        ListContentsInRevisionsWithoutFrontier lcirwf = new ListContentsInRevisionsWithoutFrontier();

        String graphPath = args[0];
        lcirwf.batchSize = Integer.parseInt(args[1]);

        System.err.println("Loading graph " + graphPath + " ...");
        lcirwf.graph = SwhBidirectionalGraph.loadLabelledMapped(graphPath);
        System.err.println("Loading label names from " + graphPath + " ...");
        lcirwf.graph.loadLabelNames();
        lcirwf.graph.loadAuthorTimestamps();
        lcirwf.threadGraph = new ThreadLocal<SwhBidirectionalGraph>();

        lcirwf.run();
    }

    public void run() throws IOException, InterruptedException, ExecutionException {
        System.err.println("Loading frontier directories...");
        loadFrontierDirectories();

        BufferedWriter bufferedStdout = new BufferedWriter(new OutputStreamWriter(System.out));
        csvPrinter = new CSVPrinter(bufferedStdout, CSVFormat.RFC4180);
        csvPrinter.printRecord("cnt_SWHID", "rev_author_date", "rev_SWHID", "path");

        csvPrinter.flush();
        bufferedStdout.flush();

        listContents();

        csvPrinter.flush();
        bufferedStdout.flush();
    }

    private void loadFrontierDirectories() throws IOException {
        frontierDirectories = new MyBooleanBigArrayBigList(graph.numNodes());

        BufferedReader bufferedStdin = new BufferedReader(new InputStreamReader(System.in));
        CSVParser csvParser = CSVParser.parse(bufferedStdin, CSVFormat.RFC4180);

        Iterator<CSVRecord> recordIterator = csvParser.iterator();

        CSVRecord header = recordIterator.next();

        if (!header.get(0).equals("frontier_dir_SWHID")) {
            throw new RuntimeException("First column of input is not 'SWHID'");
        }

        ProgressLogger pl = new ProgressLogger(logger);
        pl.itemsName = "directories";
        pl.start("Listing frontier directories...");

        while (recordIterator.hasNext()) {
            pl.lightUpdate();
            frontierDirectories.set(graph.getNodeId(recordIterator.next().get(0)), true);
        }

    }

    private void listContents() throws InterruptedException, ExecutionException {
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
                    listContentsInRevrelChunk(chunkId, numChunks, pl);
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

    private void listContentsInRevrelChunk(long chunkId, long numChunks, ProgressLogger pl) throws IOException {
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
                // Allow revisions if they are a snapshot head
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
                listContentsInRevrel(graph.getForwardGraph(), i, csvPrinter);
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
    private void listContentsInRevrel(SwhUnidirectionalGraph graph, long revrelId, CSVPrinter csvPrinter)
            throws IOException {
        SWHID revrelSWHID = graph.getSWHID(revrelId);

        // TODO: reuse these across calls instead of reallocating?
        LongArrayList stack = new LongArrayList();
        LongOpenHashSet visited = new LongOpenHashSet();

        // For each item in the stack, stores a sequence of filenameId.
        // Sequences are separated by -1 values.
        LongArrayList pathStack = new LongArrayList();

        // Initialize traversal with the revision's/release's root directory
        LazyLongIterator it = graph.successors(revrelId);
        for (long successorId; (successorId = it.nextLong()) != -1;) {
            if (graph.getNodeType(successorId) == SwhType.DIR && !frontierDirectories.getBoolean(successorId)) {
                stack.push(successorId);
                pathStack.push(-1L);
            }
        }

        long nodeId, maxTimestamp;
        boolean isFrontier;
        Long revrelTimestamp = graph.properties.getAuthorTimestamp(revrelId);
        String revrelDate = revrelTimestamp == null ? "" : Instant.ofEpochSecond(revrelTimestamp).toString();
        LongArrayList path = new LongArrayList();
        ArcLabelledNodeIterator.LabelledArcIterator itl;
        while (!stack.isEmpty()) {
            nodeId = stack.popLong();
            path.clear();
            for (long filenameId; (filenameId = pathStack.popLong()) != -1L;) {
                path.push(filenameId);
            }
            Collections.reverse(path);

            visited.add(nodeId);

            itl = graph.labelledSuccessors(nodeId);
            for (long successorId; (successorId = itl.nextLong()) != -1;) {
                // If the successor is a non-frontier directory, add it to the stack
                if (graph.getNodeType(successorId) == SwhType.DIR && !visited.contains(successorId)
                        && !frontierDirectories.getBoolean(successorId)) {
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

                // If the successor is a content, print it.
                if (graph.getNodeType(successorId) == SwhType.CNT) {
                    String pathPart;
                    long pathLength = 0;
                    Vector<String> pathParts = new Vector<String>(path.size());
                    for (long filenameId : path) {
                        pathPart = new String(graph.getLabelName(filenameId));
                        pathLength += 1L + pathPart.length();
                        pathParts.add(pathPart);
                    }

                    // Add the file's own name
                    DirEntry[] labels = (DirEntry[]) itl.label().get();
                    if (labels.length != 0) {
                        pathPart = new String(graph.getLabelName(labels[0].filenameId));
                        pathLength += 1L + pathPart.length();
                        pathParts.add(pathPart);
                    }

                    String pathString = pathLength < 1000000 ? String.join("/", pathParts) : "";
                    csvPrinter.printRecord(graph.getSWHID(successorId), revrelDate, revrelSWHID, pathString);
                }
            }
        }
    }
}
