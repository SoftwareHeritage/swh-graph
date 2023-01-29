/*
 * Copyright (c) 2023 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.utils;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.logging.ProgressLogger;
import org.softwareheritage.graph.*;

import java.io.IOException;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Counts the number of (non-singleton) paths reaching each node, from all other nodes.
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

    final static Logger logger = LoggerFactory.getLogger(TopoSort.class);

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

        countPaths.countPaths();
    }

    public void loadGraph(String graphBasename) throws IOException {
        System.err.println("Loading graph " + graphBasename + " ...");
        graph = SwhBidirectionalGraph.loadMapped(graphBasename);
    }

    public void countPaths() {
        Scanner stdin = new Scanner(System.in);

        String firstLine = stdin.nextLine().strip();
        if (!firstLine.equals("SWHID,ancestors,successors,sample_ancestor1,sample_ancestor2")) {
            System.err.format("Unexpected header: %s\n", firstLine);
            System.exit(2);
        }

        long numNodes = graph.numNodes();
        LongBigArrayBigList countsFromRoots = new LongBigArrayBigList(numNodes);
        LongBigArrayBigList countsFromAll = new LongBigArrayBigList(numNodes);

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

        System.out.println("swhid,paths_from_roots,all_paths");
        while (stdin.hasNextLine()) {
            pl.lightUpdate();
            String cells[] = stdin.nextLine().strip().split(",", -1);
            SWHID nodeSWHID = new SWHID(cells[0]);
            long nodeId = graph.getNodeId(nodeSWHID);
            long countFromRoots = countsFromRoots.getLong(nodeId);
            long countFromAll = countsFromAll.getLong(nodeId);

            /* Print counts for this node */
            System.out.format("%s,%d,%d\n", cells[0], countFromRoots, countFromAll);

            /* Add counts of paths coming from this node to all successors */
            countFromAll++;
            if (countFromRoots == 0) {
                /* This node is itself a root */
                countFromRoots++;
            }
            LazyLongIterator it = graph.successors(nodeId);
            for (long successorId; (successorId = it.nextLong()) != -1;) {
                countsFromAll.set(successorId, countsFromAll.getLong(successorId) + countFromAll);
                countsFromRoots.set(successorId, countsFromRoots.getLong(successorId) + countFromRoots);
            }

        }

        pl.done();
    }
}
