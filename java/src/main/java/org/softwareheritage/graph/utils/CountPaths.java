/*
 * Copyright (c) 2023 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.utils;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.fastutil.doubles.DoubleBigArrayBigList;
import it.unimi.dsi.logging.ProgressLogger;
import org.softwareheritage.graph.*;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Counts the number of (non-singleton) paths reaching each node, from all other nodes.
 *
 * Counts in the output may be large enough to overflow long integer, so they are computed with double-precision floating point number and printed as such.
 *
 * Sample invocation:
 *
 *   $ zstdcat /poolswh/softwareheritage/vlorentz/2022-04-25_toposort_ori,snp,rel,rev,dir.txt.zst
 *      | pv --line-mode --wait
 *      | java -cp ~/swh-environment/swh-graph/java/target/swh-graph-*.jar -Xmx1000G -XX:PretenureSizeThreshold=512M -XX:MaxNewSize=4G -XX:+UseLargePages -XX:+UseTransparentHugePages -XX:+UseNUMA -XX:+UseTLAB -XX:+ResizeTLAB org.softwareheritage.graph.utils.CountPaths /dev/shm/swh-graph/default/graph forward \
 *      | zstdmt \
 *      > /poolswh/softwareheritage/vlorentz/2022-04-25_path_counts_forward_ori,snp,rel,rev,dir.txt.zst
 */

public class CountPaths {
    private SwhBidirectionalGraph graph;

    final static Logger logger = LoggerFactory.getLogger(CountPaths.class);
    private CSVParser csvParser;
    private CSVPrinter csvPrinter;

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        if (args.length != 2) {
            System.err.println(
                    "Syntax: java org.softwareheritage.graph.utils.CountPaths <path/to/graph> {forward|backward}");
            System.exit(1);
        }

        String graphPath = args[0];
        String directionString = args[1];

        CountPaths countPaths = new CountPaths();

        countPaths.loadGraph(graphPath);

        if (directionString.equals("backward")) {
            countPaths.graph = countPaths.graph.transpose();
        } else if (!directionString.equals("forward")) {
            System.err.println("Invalid direction " + directionString);
            System.exit(1);
        }
        System.err.println("Starting...");

        BufferedWriter bufferedStdout = new BufferedWriter(new OutputStreamWriter(System.out));
        countPaths.csvPrinter = new CSVPrinter(bufferedStdout, CSVFormat.RFC4180);

        countPaths.countPaths();

        countPaths.csvPrinter.flush();
        bufferedStdout.flush();
    }

    public void loadGraph(String graphBasename) throws IOException {
        System.err.println("Loading graph " + graphBasename + " ...");
        graph = SwhBidirectionalGraph.loadMapped(graphBasename);
    }

    public void countPaths() throws IOException {
        BufferedReader bufferedStdin = new BufferedReader(new InputStreamReader(System.in));

        String firstLine = bufferedStdin.readLine().strip();
        if (!firstLine.equals("SWHID,ancestors,successors,sample_ancestor1,sample_ancestor2")) {
            System.err.format("Unexpected header: %s\n", firstLine);
            System.exit(2);
        }

        CSVParser parser = CSVParser.parse(bufferedStdin, CSVFormat.RFC4180);

        long numNodes = graph.numNodes();
        DoubleBigArrayBigList countsFromRoots = new DoubleBigArrayBigList(numNodes);
        DoubleBigArrayBigList countsFromAll = new DoubleBigArrayBigList(numNodes);

        ProgressLogger pl = new ProgressLogger(logger);
        pl.logInterval = 60000;
        pl.itemsName = "nodes";
        pl.expectedUpdates = graph.numNodes();
        pl.start("Initializing counts...");
        for (long i = 0; i < numNodes; i++) {
            pl.lightUpdate();
            countsFromRoots.add(0);
            countsFromAll.add(0);
        }
        pl.done();

        pl = new ProgressLogger(logger);
        pl.itemsName = "nodes";
        pl.expectedUpdates = graph.numNodes();
        pl.start("Counting paths...");

        csvPrinter.printRecord("SWHID", "paths_from_roots", "all_paths");
        for (CSVRecord record : parser) {
            pl.lightUpdate();
            String nodeSWHID = record.get(0);
            long nodeId = graph.getNodeId(nodeSWHID);
            double countFromRoots = countsFromRoots.getDouble(nodeId);
            double countFromAll = countsFromAll.getDouble(nodeId);

            /* Print counts for this node */
            csvPrinter.printRecord(nodeSWHID, countFromRoots, countFromAll);

            /* Add counts of paths coming from this node to all successors */
            countFromAll++;
            if (countFromRoots == 0) {
                /* This node is itself a root */
                countFromRoots++;
            }
            LazyLongIterator it = graph.successors(nodeId);
            for (long successorId; (successorId = it.nextLong()) != -1;) {
                countsFromAll.set(successorId, countsFromAll.getDouble(successorId) + countFromAll);
                countsFromRoots.set(successorId, countsFromRoots.getDouble(successorId) + countFromRoots);
            }

        }

        pl.done();
    }
}
