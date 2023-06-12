/*
 * Copyright (c) 2021-2023 The Software Heritage developers
 * Copyright (c) 2021 Antoine Pietri
 * Copyright (c) 2021 Stefano Zacchiroli
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.utils;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import org.softwareheritage.graph.*;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

/* sample invocation on granet.internal.softwareheritage.org for benchmarking
 * purposes, with the main swh-graph service already running:
 *
 *   $ java -cp ~/swh-environment/swh-graph/java/target/swh-graph-0.3.0.jar -Xmx300G -XX:PretenureSizeThreshold=512M -XX:MaxNewSize=4G -XX:+UseLargePages -XX:+UseTransparentHugePages -XX:+UseNUMA -XX:+UseTLAB -XX:+ResizeTLAB org.softwareheritage.graph.utils.FindEarliestRevision --timing /dev/shm/swh-graph/default/graph
 *
 */

public class FindEarliestRevision {
    final private boolean LOG_NO_REVISION = false; /*
                                                    * Whether to write to stderr when no revisions point to a given
                                                    * content
                                                    */
    private int NUM_THREADS = 96;
    private final int BATCH_SIZE = 1000; /* Number of CSV records to read at once */

    final private boolean timing;
    private long lineCount;
    private Duration elapsed;
    private SwhBidirectionalGraph graph;
    private CSVParser csvParser;
    private CSVPrinter csvPrinter;

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, ExecutionException {
        String graphPath = args[0];
        boolean timing = false;

        if (args.length >= 2 && (args[0].equals("-t") || args[0].equals("--timing"))) {
            timing = true;
            graphPath = args[1];
            System.err.println("started with timing option, will keep track of elapsed time");
        }

        FindEarliestRevision fer = new FindEarliestRevision(timing);
        fer.run(graphPath);
    }

    public FindEarliestRevision(boolean timing) {
        this.timing = timing;
        lineCount = 0;
    }

    public void run(String graphPath) throws IOException, InterruptedException, ExecutionException {
        long ts, elapsedNanos;
        System.err.println("loading transposed graph...");
        ts = System.nanoTime();
        graph = SwhBidirectionalGraph.loadMapped(graphPath).transpose();
        elapsed = Duration.ofNanos(System.nanoTime() - ts);
        System.err.println(String.format("transposed graph loaded (duration: %s).", elapsed));

        System.err.println("loading revision timestamps...");
        ts = System.nanoTime();
        graph.loadCommitterTimestamps();
        elapsed = Duration.ofNanos(System.nanoTime() - ts);
        System.err.println(String.format("revision timestamps loaded (duration: %s).", elapsed));

        BufferedWriter bufferedStdout = new BufferedWriter(new OutputStreamWriter(System.out));
        csvPrinter = new CSVPrinter(bufferedStdout, CSVFormat.RFC4180);
        BufferedReader bufferedStdin = new BufferedReader(new InputStreamReader(System.in));
        csvParser = CSVParser.parse(bufferedStdin, CSVFormat.RFC4180);

        Iterator<CSVRecord> recordIterator = csvParser.iterator();

        if (timing) {
            System.err.println("starting SWHID processing...");
            elapsed = Duration.ZERO;
        }

        // print CSV header line
        csvPrinter.printRecord("swhid", "earliest_swhid", "earliest_ts", "rev_occurrences");

        ExecutorService service = Executors.newFixedThreadPool(NUM_THREADS);
        Vector<Future> futures = new Vector<Future>();
        for (long i = 0; i < NUM_THREADS; ++i) {
            futures.add(service.submit(() -> {
                try {
                    process(recordIterator);
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

        if (timing)
            System.err.printf("processed %d SWHIDs in %s (%s avg)\n", lineCount, elapsed, elapsed.dividedBy(lineCount));

        csvPrinter.flush();
        bufferedStdout.flush();
    }

    /*
     * Worker thread, which reads the <code>recordIterator</code> in chunks in a synchronized block and
     * calls <code>processBatch</code> for each of them.
     */
    private void process(Iterator<CSVRecord> recordIterator) throws IOException {
        SwhBidirectionalGraph graph = this.graph.copy();

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
            processBatch(graph, records);
        }
    }

    /*
     * Given an array of CSV records, computes the most popular path of each record and prints it to the
     * shared <code>csvPrinter</code>.
     */
    private void processBatch(SwhBidirectionalGraph graph, CSVRecord[] records) throws IOException {
        AllowedEdges edges = new AllowedEdges("cnt:dir,dir:dir,dir:rev");
        String rawSWHID = null;
        SWHID srcSWHID = null;
        long srcNodeId = -1;
        long ts = 0, elapsedNanos;
        boolean timing = this.timing;
        for (CSVRecord record : records) {
            if (timing)
                ts = System.nanoTime();
            rawSWHID = record.get(0);
            lineCount++;
            try {
                srcSWHID = new SWHID(rawSWHID);
                srcNodeId = graph.getNodeId(srcSWHID);
            } catch (IllegalArgumentException e) {
                System.err
                        .println(String.format("skipping invalid or unknown SWHID %s on line %d", rawSWHID, lineCount));
                continue;
            }

            if (timing)
                System.err.println("starting traversal for: " + srcSWHID.toString());
            Stack<Long> stack = new Stack<>();
            HashSet<Long> visited = new HashSet<>();
            stack.push(srcNodeId);
            visited.add(srcNodeId);

            long minRevId = -1;
            long minTimestamp = Long.MAX_VALUE;
            long visitedRevisions = 0;
            while (!stack.isEmpty()) {
                long currentNodeId = stack.pop();
                if (graph.getNodeType(currentNodeId) == SwhType.REV) {
                    visitedRevisions++;
                    Long committerTs = graph.getCommitterTimestamp(currentNodeId);
                    if (committerTs == null) {
                        continue;
                    }
                    if (committerTs < minTimestamp && committerTs != Long.MIN_VALUE && committerTs != 0) {
                        // exclude missing and zero (= epoch) as plausible earliest timestamps
                        // as they are almost certainly bogus values
                        minRevId = currentNodeId;
                        minTimestamp = committerTs;
                    }
                }

                LazyLongIterator it = graph.successors(currentNodeId);
                for (long neighborNodeId; (neighborNodeId = it.nextLong()) != -1;) {
                    if (!edges.isAllowed(graph.getNodeType(currentNodeId), graph.getNodeType(neighborNodeId))) {
                        continue;
                    }
                    if (!visited.contains(neighborNodeId)) {
                        stack.push(neighborNodeId);
                        visited.add(neighborNodeId);
                    }
                }
            }

            if (minRevId == -1) {
                if (LOG_NO_REVISION) {
                    System.err.println("no revision found containing: " + srcSWHID.toString());
                }
            } else {
                csvPrinter.printRecord(srcSWHID, graph.getSWHID(minRevId), minTimestamp, visitedRevisions);
            }
            if (timing) {
                elapsedNanos = System.nanoTime() - ts; // processing time for current SWHID
                synchronized (this.elapsed) {
                    elapsed = elapsed.plus(Duration.ofNanos(elapsedNanos)); // cumulative processing time for all SWHIDs
                }
                System.err.printf("visit time (s):\t%.6f\n", (double) elapsedNanos / 1_000_000_000);
            }
        }
    }
}
