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
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.logging.ProgressLogger;
import org.softwareheritage.graph.*;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
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
    private SwhUnidirectionalGraph graph;
    private BooleanBigArrayBigList visited;
    private LongArrayList toVisit;
    private CSVPrinter csvPrinter;
    final static Logger logger = LoggerFactory.getLogger(ListEarliestRevisions.class);

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        if (args.length != 1) {
            System.err.println("Syntax: java org.softwareheritage.graph.utils.ListEarliestRevisions <path/to/graph>");
            System.exit(1);
        }

        String graphPath = args[0];

        ListEarliestRevisions ler = new ListEarliestRevisions();

        System.err.println("Loading graph " + graphPath + " ...");
        ler.graph = SwhUnidirectionalGraph.loadMapped(graphPath);
        ler.toVisit = new LongArrayList();

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

        ProgressLogger pl = new ProgressLogger(logger);
        pl.logInterval = 60000;
        pl.itemsName = "nodes";
        pl.expectedUpdates = numNodes;
        pl.start("Initializing visited array...");
        for (long i = 0; i < numNodes; i++) {
            pl.lightUpdate();
            visited.add(false);
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

            visitNode(date, nodeSWHID);
        }
        pl.done();

        csvPrinter.flush();
        bufferedStdout.flush();
    }

    private void visitNode(String date, String revrelSWHID) throws IOException {
        long nodeId = graph.getNodeId(revrelSWHID);
        if (graph.getNodeType(nodeId) != SwhType.REV && graph.getNodeType(nodeId) != SwhType.REL) {
            System.err.format("%s has unexpected type %s\n", graph.getNodeType(nodeId).toString());
            return;
        }

        toVisit.push(nodeId);

        while (!toVisit.isEmpty()) {
            nodeId = toVisit.popLong();

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
