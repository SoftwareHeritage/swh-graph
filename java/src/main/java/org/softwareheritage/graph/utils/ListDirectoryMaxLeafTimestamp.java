/*
 * Copyright (c) 2023 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.utils;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.fastutil.longs.LongMappedBigList;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.logging.ProgressLogger;
import org.softwareheritage.graph.*;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.*;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.CSVParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Given as argument a binary file containing an array of timestamps which is,
 * for every content, the date of first occurrence of that content in a revision,
 * and as stdin a backward topological order.
 * Produces a binary file in the same format, which contains for each directory,
 * the max of these values for all contents in that directory.
 *
 * If the <path/to/provenance_timestamps.bin> parameter is passed, then this file
 * is written as an array of longs, which can be loaded with LongMappedBigList.
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
    private SwhUnidirectionalGraph transposeGraph;
    private LongMappedBigList timestamps;
    private LongBigArrayBigList maxTimestamps;
    private CSVPrinter csvPrinter;
    private String binOutputPath;
    final static Logger logger = LoggerFactory.getLogger(ListDirectoryMaxLeafTimestamp.class);

    // Queue of the DFS
    private LongBigArrayBigList toVisit;
    private long toVisitOffset;

    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException, ExecutionException {
        if (args.length < 2 || args.length > 3) {
            System.err.println(
                    "Syntax: java org.softwareheritage.graph.utils.ListDirectoryMaxLeafTimestamp <path/to/graph> <path/to/earliest_timestamps.bin> [<path/to/provenance_timestamps.bin>]");
            System.exit(1);
        }

        ListDirectoryMaxLeafTimestamp ldmlt = new ListDirectoryMaxLeafTimestamp();

        ldmlt.graphPath = args[0];
        ldmlt.binOutputPath = null;
        if (args.length >= 3) {
            ldmlt.binOutputPath = args[2];
        }

        String timestampsPath = args[1];
        System.err.println("Loading timestamps from " + timestampsPath + " ...");
        RandomAccessFile raf = new RandomAccessFile(timestampsPath, "r");
        ldmlt.timestamps = LongMappedBigList.map(raf.getChannel());

        System.err.println("Loading graph " + ldmlt.graphPath + " ...");
        ldmlt.transposeGraph = SwhUnidirectionalGraph.loadGraphOnly(SwhUnidirectionalGraph.LoadMethod.MAPPED,
                ldmlt.graphPath + "-transposed", null, null);
        ldmlt.transposeGraph.loadProperties(ldmlt.graphPath);

        ldmlt.run();
    }

    public void run() throws IOException, InterruptedException, ExecutionException {
        long numNodes = transposeGraph.numNodes();

        BufferedReader bufferedStdin = new BufferedReader(new InputStreamReader(System.in));

        CSVParser parser = CSVParser.parse(bufferedStdin, CSVFormat.RFC4180);

        CSVRecord firstRow = parser.iterator().next();
        if (!firstRow.get(0).equals("SWHID")) {
            System.err.format("Unexpected header: %s\n", firstRow);
            System.exit(2);
        }

        System.err.println("Allocating maxTimestamps array...");
        maxTimestamps = new LongBigArrayBigList(numNodes);
        for (long i = 0; i < numNodes; ++i) {
            maxTimestamps.add(Long.MIN_VALUE);
        }

        ProgressLogger pl = new ProgressLogger(logger);
        pl.logInterval = 60000;
        pl.itemsName = "nodes";
        pl.expectedUpdates = numNodes;
        pl.start("Visiting contents...");
        long nbContents = 0;
        for (long nodeId = 0; nodeId < numNodes; nodeId++) {
            pl.lightUpdate();
            long children = 0;
            if (transposeGraph.getNodeType(nodeId) == SwhType.CNT) {
                setMaxTimestampToAncestors(nodeId, timestamps.getLong(nodeId));
                nbContents++;
            }
        }

        pl = new ProgressLogger(logger);
        pl.logInterval = 60000;
        pl.itemsName = "nodes";
        pl.expectedUpdates = numNodes - nbContents;
        pl.start("Visiting directories...");
        long nodeId;
        for (CSVRecord record : parser) {
            pl.lightUpdate();
            String nodeSWHID = record.get(0);

            nodeId = transposeGraph.getNodeId(nodeSWHID);
            if (transposeGraph.getNodeType(nodeId) == SwhType.DIR) {
                setMaxTimestampToAncestors(nodeId, maxTimestamps.getLong(nodeId));
            }
        }

        if (binOutputPath != null) {
            System.err.format("Writing binary output to %s\n", binOutputPath);
            BinIO.storeLongs(maxTimestamps.elements(), binOutputPath);
        }

    }

    private void setMaxTimestampToAncestors(long nodeId, long timestamp) {
        LazyLongIterator it = transposeGraph.successors(nodeId);

        for (long ancestorId; (ancestorId = it.nextLong()) != -1;) {
            if (transposeGraph.getNodeType(ancestorId) == SwhType.DIR) {
                long ancestorMaxTimestamp = maxTimestamps.getLong(ancestorId);

                if (timestamp > ancestorMaxTimestamp) {
                    ancestorMaxTimestamp = timestamp;
                    maxTimestamps.set(ancestorId, ancestorMaxTimestamp);
                }
            }
        }
    }
}
