/*
 * Copyright (c) 2023 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.utils;

import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator;
import com.martiansoftware.jsap.*;
import org.softwareheritage.graph.*;
import org.softwareheritage.graph.labels.DirEntry;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.booleans.BooleanBigArrayBigList;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.*;
import java.util.concurrent.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Reads a list of directory SWHIDs on stdin, and returns, for each content in the
 * directory's sub-"tree", a line with both their SWHIDs, and the path between the two
 *
 * Syntax:
 *
 * <code>java org.softwareheritage.graph.utils.ListContentsInDirectories &lt;path/to/graph&gt; &lt;columnIndex&gt;</code>
 *
 * where <code>&lt;columnIndex&gt;</code> is the (1-indexed) number of the column
 * containing SWHIDs in the input CSV.
 */

public class ListContentsInDirectories {
    private class MyBooleanBigArrayBigList extends BooleanBigArrayBigList {
        public MyBooleanBigArrayBigList(long size) {
            super(size);

            // Allow setting directly in the array without repeatedly calling
            // .add() first
            this.size = size;
        }
    }

    private SwhUnidirectionalGraph graph; // Labelled graph
    /*
     * A copy of the graph for each thread to reuse between calls to processChunk
     */
    private ThreadLocal<SwhUnidirectionalGraph> threadGraph;
    private int NUM_THREADS = 96;
    private int columnIndex; /* Which column of the input CSV contains SWHIDs */
    private final int BATCH_SIZE = 100; /* Number of CSV records to process at once */
    private final long MIN_CONTENT_SIZE = 3; /* Ignore all contents smaller than this */
    private CSVParser csvParser;
    private MyBooleanBigArrayBigList processedDirectories;

    final static Logger logger = LoggerFactory.getLogger(ListContentsInDirectories.class);

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException, ExecutionException {
        if (args.length != 2) {
            System.err.println(
                    "Syntax: java org.softwareheritage.graph.utils.ListContentsInDirectories <path/to/graph> <columnIndex>");
            System.exit(1);
        }
        String graphPath = args[0];

        BufferedReader bufferedStdin = new BufferedReader(new InputStreamReader(System.in));

        ListContentsInDirectories lcid = new ListContentsInDirectories();

        lcid.columnIndex = Integer.parseInt(args[1]) - 1;
        lcid.threadGraph = new ThreadLocal<SwhUnidirectionalGraph>();
        lcid.csvParser = CSVParser.parse(bufferedStdin, CSVFormat.RFC4180);

        lcid.loadGraph(graphPath);

        lcid.run();
    }

    public void loadGraph(String graphBasename) throws IOException {
        System.err.println("Loading graph " + graphBasename + " ...");
        graph = SwhUnidirectionalGraph.loadLabelledGraphOnly(SwhUnidirectionalGraph.LoadMethod.MAPPED, graphBasename,
                null, null);
        graph.loadProperties(graphBasename);
        graph.properties.loadLabelNames();
        System.err.println("Graph loaded.");
        processedDirectories = new MyBooleanBigArrayBigList(graph.numNodes());
    }

    public void run() throws InterruptedException, IOException, ExecutionException {

        long totalNodes = graph.numNodes();

        Iterator<CSVRecord> recordIterator = csvParser.iterator();

        CSVRecord header = recordIterator.next();
        if (!header.get(columnIndex).contains("SWHID")) {
            throw new RuntimeException("Header of column " + (columnIndex + 1) + " does not contain 'SWHID'");
        }

        CSVPrinter csvPrinter = new CSVPrinter(System.out, CSVFormat.RFC4180);
        csvPrinter.printRecord("cnt_SWHID", "dir_SWHID", "path");
        csvPrinter.flush();

        ProgressLogger pl = new ProgressLogger(logger);
        pl.itemsName = "directories";
        pl.start("Listing contents in directories...");

        ExecutorService service = Executors.newFixedThreadPool(NUM_THREADS);
        Vector<Future> futures = new Vector<Future>();
        for (long i = 0; i < NUM_THREADS; ++i) {
            futures.add(service.submit(() -> {
                try {
                    process(recordIterator, pl);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }));
        }

        service.shutdown();
        service.awaitTermination(365, TimeUnit.DAYS);

        // Error if any exception occurred
        for (Future future : futures) {
            future.get();
        }

        pl.done();

    }

    /*
     * Worker thread, which reads the <code>recordIterator</code> in chunks in a synchronized block and
     * calls <code>processBatch</code> for each of them.
     */
    private void process(Iterator<CSVRecord> recordIterator, ProgressLogger pl) throws IOException {
        SwhUnidirectionalGraph graph = this.graph.copy();

        CSVRecord[] records = new CSVRecord[BATCH_SIZE];

        while (true) {
            // Keep the critical section minimal: only read CSV records to the buffer
            synchronized (csvParser) {
                if (!recordIterator.hasNext()) {
                    break;
                }
                for (int i = 0; i < BATCH_SIZE; i++) {
                    try {
                        records[i] = recordIterator.next();
                    } catch (NoSuchElementException e) {
                        // Reached end of file
                        records[i] = null;
                    }
                }
            }

            // Do the actual work outside the critical section
            processBatch(graph, records, pl);
        }
    }

    /*
     * Given an array of CSV records, computes the most popular path of each record and prints it to the
     * shared <code>csvPrinter</code>.
     *
     * <code>graph</code> should be a thread-local copy of the transposed graph at
     * <code>this.graph</code>
     */
    private void processBatch(SwhUnidirectionalGraph graph, CSVRecord[] records, ProgressLogger pl) throws IOException {

        long totalNodes = graph.numNodes();

        for (CSVRecord record : records) {
            if (record == null) {
                // Reached end of file
                break;
            }
            String dirSwhid = record.get(columnIndex);

            processDirectory(graph, dirSwhid);
        }

        synchronized (pl) {
            pl.update(records.length);
        }
    }

    void processDirectory(SwhUnidirectionalGraph graph, String rootSwhid) throws IOException {
        long rootId = graph.getNodeId(rootSwhid);

        synchronized (processedDirectories) {
            // Avoid processing duplicates twice.
            if (processedDirectories.getBoolean(rootId)) {
                return;
            }
            processedDirectories.set(rootId, true);
        }

        BigStringBuffer buf = new BigStringBuffer(10000000); // 10MB
        CSVPrinter csvPrinter = new CSVPrinter(buf, CSVFormat.RFC4180);

        // TODO: reuse these across calls instead of reallocating?
        LongArrayList stack = new LongArrayList();
        LongOpenHashSet visited = new LongOpenHashSet();

        // For each item in the stack, stores a sequence of filenameId.
        // Sequences are separated by -1 values.
        LongArrayList pathStack = new LongArrayList();

        ArcLabelledNodeIterator.LabelledArcIterator itl;

        stack.push(rootId);
        pathStack.push(-1L);

        String directoryPathString = null;

        LongArrayList path = new LongArrayList();

        long nodeId;
        while (!stack.isEmpty()) {
            nodeId = stack.popLong();
            path.clear();
            for (long filenameId; (filenameId = pathStack.popLong()) != -1L;) {
                path.push(filenameId);
            }
            Collections.reverse(path);

            visited.add(nodeId);

            directoryPathString = null;
            itl = graph.labelledSuccessors(nodeId);
            for (long successorId; (successorId = itl.nextLong()) != -1;) {
                if (visited.contains(successorId)) {
                    continue;
                }
                if (graph.getNodeType(successorId) == SwhType.DIR) {
                    stack.add(successorId);
                    pathStack.push(-1L);
                    for (long filenameId : path) {
                        pathStack.push(filenameId);
                    }
                    DirEntry[] labels = (DirEntry[]) itl.label().get();
                    if (labels.length != 0) {
                        pathStack.push(labels[0].filenameId);
                    }
                } else if (graph.getNodeType(successorId) == SwhType.CNT) {
                    visited.add(successorId);
                    if (directoryPathString == null) {
                        // Compute the directory's path if this is the first
                        // content we find in the directory.
                        Vector<String> pathParts = new Vector<String>(path.size());
                        for (long filenameId : path) {
                            pathParts.add(new String(graph.getLabelName(filenameId)));
                        }
                        pathParts.add("");
                        directoryPathString = String.join("/", pathParts);
                    }
                    DirEntry[] labels = (DirEntry[]) itl.label().get();
                    String contentPathString;
                    if (labels.length == 0) {
                        // Shouldn't happen
                        contentPathString = directoryPathString;
                    } else {
                        contentPathString = directoryPathString + new String(graph.getLabelName(labels[0].filenameId));
                    }
                    csvPrinter.printRecord(graph.getSWHID(successorId), rootSwhid, contentPathString);
                }
            }
        }

        csvPrinter.flush();
        buf.flushToStdout();

    }
}
