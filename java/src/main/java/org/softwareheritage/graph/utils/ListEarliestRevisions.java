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
import it.unimi.dsi.fastutil.booleans.BooleanBigArrayBigList;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.io.BinIO;
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

/* Given as stdin a CSV with header "date,SWHID" containing a list of revisions and releases
 * SWHIDs sorted by author date, produces a new CSV "date,revrel_SWHID,cntdir_SWHID" containing all
 * directory and content SWHIDs along with the first revision/release they occur in
 * and its date.
 */

public class ListEarliestRevisions {

    private SwhBidirectionalGraph graph;
    private BooleanBigArrayBigList visited;
    private LongBigArrayBigList timestamps;
    private CSVPrinter csvPrinter;
    private String binOutputPath;
    final static Logger logger = LoggerFactory.getLogger(ListEarliestRevisions.class);

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length < 1 || args.length > 2) {
            System.err.println(
                    "Syntax: java org.softwareheritage.graph.utils.ListEarliestRevisions <path/to/graph> [<path/to/earliest_timestamps.bin>]");
            System.exit(1);
        }

        String graphPath = args[0];

        ListEarliestRevisions ler = new ListEarliestRevisions();

        ler.binOutputPath = null;
        if (args.length >= 2) {
            ler.binOutputPath = args[1];
        }

        System.err.println("Loading graph " + graphPath + " ...");
        ler.graph = SwhBidirectionalGraph.loadMapped(graphPath);
        ler.timestamps = null;

        ler.run();
    }

    public void run() throws IOException {

        BufferedReader bufferedStdin = new BufferedReader(new InputStreamReader(System.in));
        String firstLine = bufferedStdin.readLine().strip();
        if (!firstLine.equals("author_date,SWHID")) {
            System.err.format("Unexpected header: %s\n", firstLine);
            System.exit(2);
        }

        CSVParser parser = CSVParser.parse(bufferedStdin, CSVFormat.RFC4180);

        System.err.println("Allocating memory...");
        long numNodes = graph.numNodes();
        visited = new BooleanBigArrayBigList(numNodes);
        if (binOutputPath != null) {
            timestamps = new LongBigArrayBigList(numNodes);
        }

        ProgressLogger pl = new ProgressLogger(logger);
        pl.logInterval = 60000;
        pl.itemsName = "nodes";
        pl.expectedUpdates = numNodes;
        pl.start("Initializing arrays...");
        for (long i = 0; i < numNodes; i++) {
            pl.lightUpdate();
            visited.add(false);
            if (timestamps != null) {
                timestamps.add(Long.MIN_VALUE);
            }
        }
        pl.done();

        BufferedWriter bufferedStdout = new BufferedWriter(new OutputStreamWriter(System.out));
        csvPrinter = new CSVPrinter(bufferedStdout, CSVFormat.RFC4180);
        csvPrinter.printRecord("author_date", "revrel_SWHID", "cntdir_SWHID");
        pl = new ProgressLogger(logger);
        pl.logInterval = 60000;
        pl.itemsName = "nodes";
        pl.start("Visiting revisions and releases...");
        String previousDate = "";
        for (CSVRecord record : parser) {
            pl.lightUpdate();
            String date = record.get(0);
            String nodeSWHID = record.get(1);

            if (date.compareTo(previousDate) < 0) {
                System.err.format("Dates are not correctly ordered (%s follow %s)\n", date, previousDate);
                System.exit(3);
            }
            previousDate = date;

            long timestamp = Instant.parse(date + "Z").getEpochSecond();
            visitNode(date, timestamp, nodeSWHID);
        }
        pl.done();

        csvPrinter.flush();
        bufferedStdout.flush();

        if (binOutputPath != null) {
            System.err.format("Writing binary output to %s\n", binOutputPath);
            BinIO.storeLongs(timestamps.elements(), binOutputPath);
        }
    }

    private void visitNode(String date, long timestamp, String revrelSWHID) throws IOException {
        long nodeId = graph.getNodeId(revrelSWHID);

        if (graph.getNodeType(nodeId) == SwhType.REL) {
            // Allow releases
        } else if (graph.getNodeType(nodeId) == SwhType.REV) {
            // Allow revisions if they are a snapshot head
            boolean isSnapshotHead = false;
            LazyLongIterator it = graph.predecessors(nodeId);
            for (long predecessorId; (predecessorId = it.nextLong()) != -1;) {
                if (graph.getNodeType(predecessorId) == SwhType.SNP
                        || graph.getNodeType(predecessorId) == SwhType.REL) {
                    isSnapshotHead = true;
                }
            }
            if (!isSnapshotHead) {
                return;
            }
        } else {
            System.err.format("%s has unexpected type\n", graph.getNodeType(nodeId).toString());
            return;
        }

        LongArrayList toVisit = new LongArrayList(1000000);
        toVisit.push(nodeId);

        while (!toVisit.isEmpty()) {
            nodeId = toVisit.popLong();

            if (graph.getNodeType(nodeId) == SwhType.DIR || graph.getNodeType(nodeId) == SwhType.CNT) {
                timestamps.set(nodeId, timestamp);
            }

            LazyLongIterator it = graph.successors(nodeId);

            for (long successorId; (successorId = it.nextLong()) != -1;) {
                if (visited.getBoolean(successorId)) {
                    continue;
                }
                if (graph.getNodeType(successorId) != SwhType.DIR && graph.getNodeType(successorId) != SwhType.CNT) {
                    continue;
                }
                toVisit.push(successorId);
                csvPrinter.printRecord(date, revrelSWHID, graph.getSWHID(successorId));
                visited.set(successorId, true);
            }
        }
    }
}
