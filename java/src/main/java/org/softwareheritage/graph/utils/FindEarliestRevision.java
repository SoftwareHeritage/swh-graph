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
import java.time.Duration;
import java.util.HashSet;
import java.util.Stack;
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
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String graphPath = args[0];
        boolean timing = false;
        long ts, elapsedNanos;
        Duration elapsed;

        if (args.length >= 2 && (args[0].equals("-t") || args[0].equals("--timing"))) {
            timing = true;
            graphPath = args[1];
            System.err.println("started with timing option, will keep track of elapsed time");
        }

        System.err.println("loading transposed graph...");
        ts = System.nanoTime();
        SwhBidirectionalGraph graph = SwhBidirectionalGraph.loadMapped(graphPath).transpose();
        elapsed = Duration.ofNanos(System.nanoTime() - ts);
        System.err.println(String.format("transposed graph loaded (duration: %s).", elapsed));

        System.err.println("loading revision timestamps...");
        ts = System.nanoTime();
        graph.loadCommitterTimestamps();
        elapsed = Duration.ofNanos(System.nanoTime() - ts);
        System.err.println(String.format("revision timestamps loaded (duration: %s).", elapsed));

        BufferedWriter bufferedStdout = new BufferedWriter(new OutputStreamWriter(System.out));
        CSVPrinter csvPrinter = new CSVPrinter(bufferedStdout, CSVFormat.RFC4180);
        BufferedReader bufferedStdin = new BufferedReader(new InputStreamReader(System.in));
        CSVParser csvParser = CSVParser.parse(bufferedStdin, CSVFormat.RFC4180);

        AllowedEdges edges = new AllowedEdges("cnt:dir,dir:dir,dir:rev");
        String rawSWHID = null;
        SWHID srcSWHID = null;
        long lineCount = 0;
        long srcNodeId = -1;
        if (timing) {
            System.err.println("starting SWHID processing...");
            elapsed = Duration.ZERO;
        }
        // print CSV header line
        csvPrinter.printRecord("swhid", "earliest_swhid", "earliest_ts", "rev_occurrences");
        for (CSVRecord record : csvParser) {
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
                System.err.println("no revision found containing: " + srcSWHID.toString());
            } else {
                csvPrinter.printRecord(srcSWHID, graph.getSWHID(minRevId), minTimestamp, visitedRevisions);
            }
            if (timing) {
                elapsedNanos = System.nanoTime() - ts; // processing time for current SWHID
                elapsed = elapsed.plus(Duration.ofNanos(elapsedNanos)); // cumulative processing time for all SWHIDs
                System.err.printf("visit time (s):\t%.6f\n", (double) elapsedNanos / 1_000_000_000);
            }
        }
        if (timing)
            System.err.printf("processed %d SWHIDs in %s (%s avg)\n", lineCount, elapsed, elapsed.dividedBy(lineCount));

        csvPrinter.flush();
        bufferedStdout.flush();
    }
}
