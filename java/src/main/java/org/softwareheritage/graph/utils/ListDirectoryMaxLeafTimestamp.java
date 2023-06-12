/*
 * Copyright (c) 2023 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.utils;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.logging.ProgressLogger;
import org.softwareheritage.graph.*;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Given as stdin a CSV with header "author_date,revrel_SWHID,cntdir_SWHID"  containing all
 * directory and content SWHIDs along with the first revision/release they occur in
 * and its date (as produced by ListEarliestRevisions), produces a new CSV with header
 * "max_author_date,dir_SWHID", which contains, for every directory, the newest date
 * of any of the contents its subtree (well, sub-DAG) contains.
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
    private LongBigArrayBigList maxTimestamps;
    private CSVPrinter csvPrinter;
    final static Logger logger = LoggerFactory.getLogger(ListDirectoryMaxLeafTimestamp.class);

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, ExecutionException {
        if (args.length != 1) {
            System.err.println(
                    "Syntax: java org.softwareheritage.graph.utils.ListDirectoryMaxLeafTimestamp <path/to/graph>");
            System.exit(1);
        }

        ListDirectoryMaxLeafTimestamp ldmlt = new ListDirectoryMaxLeafTimestamp();

        ldmlt.graphPath = args[0];

        System.err.println("Loading graph " + ldmlt.graphPath + " ...");
        ldmlt.graph = SwhUnidirectionalGraph.loadMapped(ldmlt.graphPath);
        ldmlt.threadGraph = new ThreadLocal<SwhUnidirectionalGraph>();

        ldmlt.run();
    }

    public void run() throws IOException, InterruptedException, ExecutionException {
        BufferedReader bufferedStdin = new BufferedReader(new InputStreamReader(System.in));
        String firstLine = bufferedStdin.readLine().strip();
        if (!firstLine.equals("author_date,revrel_SWHID,cntdir_SWHID")) {
            System.err.format("Unexpected header: %s\n", firstLine);
            System.exit(2);
        }

        CSVParser parser = CSVParser.parse(bufferedStdin, CSVFormat.RFC4180);

        System.err.println("Allocating memory...");
        long numNodes = graph.numNodes();
        unvisitedChildren = new MyLongBigArrayBigList(numNodes);
        maxTimestamps = new LongBigArrayBigList(numNodes);

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

        System.err.println("Deallocating " + graphPath + " ...");
        SwhGraphProperties properties = graph.getProperties();
        graph = null;

        System.err.println("Loading graph " + graphPath + "-transposed ...");
        transposeGraph = SwhUnidirectionalGraph.loadGraphOnly(SwhUnidirectionalGraph.LoadMethod.MAPPED,
                graphPath + "-transposed", null, null);
        transposeGraph.setProperties(properties);

        ProgressLogger pl = new ProgressLogger(logger);
        pl.logInterval = 60000;
        pl.itemsName = "nodes";
        pl.expectedUpdates = numNodes;
        pl.start("Initializing maxTimestamps array...");
        for (long i = 0; i < numNodes; i++) {
            pl.lightUpdate();
            maxTimestamps.add(Long.MIN_VALUE);
        }
        pl.done();

        BufferedWriter bufferedStdout = new BufferedWriter(new OutputStreamWriter(System.out));
        csvPrinter = new CSVPrinter(bufferedStdout, CSVFormat.RFC4180);
        csvPrinter.printRecord("max_author_date", "dir_SWHID");
        pl = new ProgressLogger(logger);
        pl.logInterval = 60000;
        pl.itemsName = "nodes";
        pl.start("Visiting contents and directories...");
        String previousDate = "";
        for (CSVRecord record : parser) {
            pl.lightUpdate();
            String date = record.get(0);
            String nodeSWHID = record.get(2);

            if (date.compareTo(previousDate) < 0) {
                System.err.format("Dates are not correctly ordered (%s follow %s)\n", date, previousDate);
                System.exit(3);
            }
            previousDate = date;

            visitNode(date, nodeSWHID);
        }
        pl.done();

        csvPrinter.flush();
        bufferedStdout.flush();
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
            pl.lightUpdate();
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
    }

    private void visitNode(String date, String nodeSWHID) throws IOException {
        long nodeId = transposeGraph.getNodeId(nodeSWHID);
        long nodeMaxTimestamp = Long.MIN_VALUE;

        if (transposeGraph.getNodeType(nodeId) == SwhType.CNT) {
            nodeMaxTimestamp = Instant.parse(date + "Z").getEpochSecond();
        } else if (transposeGraph.getNodeType(nodeId) == SwhType.DIR) {
            nodeMaxTimestamp = maxTimestamps.getLong(nodeId);
        } else {
            System.err.format("%s has unexpected type %s\n", nodeSWHID, transposeGraph.getNodeType(nodeId).toString());
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
                            transposeGraph.getSWHID(ancestorId), nodeSWHID);
                    System.exit(5);
                } else if (ancestorUnvisitedChildren == 0) {
                    // We are visiting the last remaining child of this parent
                    csvPrinter.printRecord(date, transposeGraph.getSWHID(ancestorId));
                }
            }
        }
    }
}
