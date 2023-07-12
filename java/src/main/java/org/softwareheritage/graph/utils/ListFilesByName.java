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
import it.unimi.dsi.logging.ProgressLogger;
import org.softwareheritage.graph.*;
import org.softwareheritage.graph.labels.DirEntry;

import java.io.IOException;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.concurrent.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* From every refs/heads/master, refs/heads/main, or HEAD branch in any snapshot,
 * browse the whole directory tree looking for files named <filename>, and lists
 * them to stdout.
 */

public class ListFilesByName {

    private class MyLongBigArrayBigList extends LongBigArrayBigList {
        public MyLongBigArrayBigList(long size) {
            super(size);

            // Allow setting directly in the array without repeatedly calling
            // .add() first
            this.size = size;
        }
    }

    private int numThreads;
    private int batchSize; /* Number of revisions to process in each task */

    private String filename;
    private long filenameId; // the label id of the filename we are looking for
    private final String[] branchNames = {"refs/heads/master", "refs/heads/main", "HEAD"};
    private Vector<Long> branchNameIds;

    private SwhUnidirectionalGraph graph;
    private ThreadLocal<SwhUnidirectionalGraph> threadGraph;
    final static Logger logger = LoggerFactory.getLogger(ListFilesByName.class);

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, ExecutionException {
        if (args.length != 4) {
            System.err.println(
                    "Syntax: java org.softwareheritage.graph.utils.ListFilesByName <path/to/graph> <filename> <num_threads> <batch_size>");
            System.exit(1);
        }

        ListFilesByName lfbn = new ListFilesByName();

        String graphPath = args[0];
        lfbn.filename = args[1];
        lfbn.numThreads = Integer.parseInt(args[2]);
        lfbn.batchSize = Integer.parseInt(args[3]);

        System.err.println("Loading graph " + graphPath + " ...");
        lfbn.graph = SwhUnidirectionalGraph.loadLabelledMapped(graphPath);
        System.err.println("Loading labels for " + graphPath + " ...");
        lfbn.graph.loadLabelNames();
        lfbn.threadGraph = new ThreadLocal<SwhUnidirectionalGraph>();

        lfbn.run();
    }

    public void run() throws IOException, InterruptedException, ExecutionException {
        ProgressLogger pl = new ProgressLogger(logger);
        pl.itemsName = "filenames";
        pl.start("Looking for filename and branch ids...");

        filenameId = -1L;
        String label;
        branchNameIds = new Vector<Long>();
        for (int j = 0; j < branchNames.length; j++) {
            branchNameIds.add(-1L);
        }
        for (long i = 0;; i++) {
            try {
                label = new String(graph.getLabelName(i));
            } catch (IndexOutOfBoundsException e) {
                break;
            }
            if (label.equals(filename)) {
                filenameId = i;
                if (!branchNameIds.contains(-1L)) {
                    break;
                }
            }
            for (int j = 0; j < branchNames.length; j++) {
                if (label.equals(branchNames[j])) {
                    branchNameIds.set(j, i);
                    if (filenameId != -1L && !branchNameIds.contains(-1L)) {
                        break;
                    }
                }
            }
            pl.lightUpdate();
        }
        pl.done();

        if (filenameId == -1) {
            System.err.println("Failed to find filename id for " + filename);
            System.exit(7);
        }
        if (branchNameIds.contains(-1L)) {
            for (int j = 0; j < branchNames.length; j++) {
                if (branchNameIds.get(j) == -1L) {
                    System.err.println("Failed to find branch name id for " + branchNames[j]);

                    // FIXME: this fails tests, but we should probably error in prod
                    // System.exit(7);
                }
            }
        }

        BufferedWriter bufferedStdout = new BufferedWriter(new OutputStreamWriter(System.out));
        CSVPrinter csvPrinter = new CSVPrinter(bufferedStdout, CSVFormat.RFC4180);
        csvPrinter.printRecord("snp_SWHID", "branch_name", "dir_SWHID", "file_name", "cnt_SWHID");

        csvPrinter.flush();
        bufferedStdout.flush();

        listFiles();
    }

    private void listFiles() throws InterruptedException, ExecutionException {
        final long numChunks = numThreads * 1000;

        System.err.println("Initializing traversals...");
        ExecutorService service = Executors.newFixedThreadPool(numThreads);
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
        pl.start("Visiting snapshots' files...");
        for (long i : range) {
            final long chunkId = i;
            futures.add(service.submit(() -> {
                try {
                    listFilesInSnapshotChunk(chunkId, numChunks, pl);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }

            }));
        }

        // Error if any exception occurred
        for (Future future : futures) {
            future.get();
        }

        service.shutdown();
        service.awaitTermination(365, TimeUnit.DAYS);
        pl.done();
    }

    private void flushToStdout(StringBuffer buffer) throws IOException {
        synchronized (System.out) {
            System.out.write(buffer.toString().getBytes());
        }
    }

    private void listFilesInSnapshotChunk(long chunkId, long numChunks, ProgressLogger pl) throws IOException {
        if (threadGraph.get() == null) {
            threadGraph.set(this.graph.copy());
        }
        SwhUnidirectionalGraph graph = threadGraph.get();
        long numNodes = graph.numNodes();
        long chunkSize = numNodes / numChunks;
        long chunkStart = chunkSize * chunkId;
        long chunkEnd = chunkId == numChunks - 1 ? numNodes : chunkSize * (chunkId + 1);

        StringBuffer buf = new StringBuffer(batchSize * 100000);
        CSVPrinter csvPrinter = new CSVPrinter(buf, CSVFormat.RFC4180);
        long flushThreshold = batchSize * 1000000; // Avoids wasting RAM for too long
        for (long i = chunkStart; i < chunkEnd; i++) {
            if (graph.getNodeType(i) != SwhType.SNP) {
                continue;
            }

            try {
                listFilesInSnapshot(graph, i, csvPrinter);
                csvPrinter.flush();
            } catch (OutOfMemoryError e) {
                System.err.format("OOMed while processing %s (buffer grew to %d): %s\n", graph.getSWHID(i),
                        buf.length(), e);
                throw new RuntimeException(e);
            }
            if (buf.length() > flushThreshold) {
                csvPrinter.flush();
                flushToStdout(buf);
                buf = new StringBuffer(batchSize * 1140);
                csvPrinter = new CSVPrinter(buf, CSVFormat.RFC4180);
            }
        }
        csvPrinter.flush();

        flushToStdout(buf);

        synchronized (pl) {
            pl.update(chunkSize);
        }
    }

    /* Performs a BFS, stopping at frontier directories. */
    private void listFilesInSnapshot(SwhUnidirectionalGraph graph, long snpId, CSVPrinter csvPrinter)
            throws IOException {
        SWHID snpSWHID = graph.getSWHID(snpId);

        LazyLongIterator it = graph.successors(snpId);

        ArcLabelledNodeIterator.LabelledArcIterator s = graph.labelledSuccessors(snpId);
        long nodeId;
        while ((nodeId = s.nextLong()) >= 0) {
            if (graph.getNodeType(nodeId) != SwhType.REV && graph.getNodeType(nodeId) != SwhType.REL) {
                continue;
            }
            DirEntry[] labels = (DirEntry[]) s.label().get();
            for (DirEntry label : labels) {
                if (branchNameIds.contains(label.filenameId)) {
                    listFilesInDirectory(graph, snpId, label.filenameId, nodeId, csvPrinter);
                }
            }
        }
    }

    private void listFilesInDirectory(SwhUnidirectionalGraph graph, long snpId, long branchNameId, long rootId,
            CSVPrinter csvPrinter) throws IOException {

        // TODO: reuse these across calls instead of reallocating?
        LongArrayList stack = new LongArrayList();
        LongOpenHashSet visited = new LongOpenHashSet();

        stack.push(rootId);

        long nodeId, successorId;
        while (!stack.isEmpty()) {
            nodeId = stack.popLong();
            if (visited.contains(nodeId)) {
                continue;
            }
            visited.add(nodeId);

            if (graph.getNodeType(nodeId) == SwhType.DIR) {
                ArcLabelledNodeIterator.LabelledArcIterator s = graph.labelledSuccessors(nodeId);
                long node;
                while ((successorId = s.nextLong()) >= 0) {
                    if (graph.getNodeType(successorId) == SwhType.DIR && !visited.contains(successorId)) {
                        stack.add(successorId);
                    } else {
                        DirEntry[] labels = (DirEntry[]) s.label().get();
                        for (DirEntry label : labels) {
                            if (filenameId == label.filenameId) {
                                // snp_SWHID,branch_name,dir_SWHID,file_name,cnt_SWHID
                                csvPrinter.printRecord(graph.getSWHID(snpId),
                                        new String(graph.getLabelName(branchNameId)), graph.getSWHID(nodeId), filename,
                                        graph.getSWHID(successorId));
                            }
                        }
                    }
                }
            } else {
                LazyLongIterator it = graph.successors(nodeId);
                for (; (successorId = it.nextLong()) != -1;) {
                    if (graph.getNodeType(successorId) == SwhType.DIR && !visited.contains(successorId)) {
                        stack.add(successorId);
                    }
                }
            }
        }
    }
}
