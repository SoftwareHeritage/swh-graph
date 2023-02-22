/*
 * Copyright (c) 2022-2023 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.utils;

import com.martiansoftware.jsap.*;
import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.fastutil.longs.LongBigArrayBigList;
import it.unimi.dsi.logging.ProgressLogger;
import org.softwareheritage.graph.*;

import java.io.IOException;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

/* Lists all nodes nodes of the types given as argument, in topological order,
 * from leaves (contents, if selected) to the top (origins, if selected).
 *
 * This uses a DFS, so nodes are likely to be close to their neighbors.
 *
 * Some extra information is provided to allow more efficient consumption
 * of the output: number of ancestors, successors, and a sample of two ancestors.
 *
 * Sample invocation:
 *
 *   $ java -cp ~/swh-environment/swh-graph/java/target/swh-graph-*.jar -Xmx1000G -XX:PretenureSizeThreshold=512M -XX:MaxNewSize=4G -XX:+UseLargePages -XX:+UseTransparentHugePages -XX:+UseNUMA -XX:+UseTLAB -XX:+ResizeTLAB org.softwareheritage.graph.utils.TopoSort /dev/shm/swh-graph/default/graph dfs backward 'rev,rel,snp,ori' \
 *      | pv --line-mode --wait \
 *      | zstdmt \
 *      > /poolswh/softwareheritage/vlorentz/2022-04-25_toposort_rev,rel,snp,ori.txt.zst
 */

public class TopoSort {
    private SwhBidirectionalGraph underlyingGraph;
    private SwhBidirectionalGraph transposedUnderlyingGraph;
    private Subgraph graph;
    private Subgraph transposedGraph;
    private boolean dfs; // Whether to run in BFS or DFS
    private LongBigArrayBigList ready;
    private long endIndex; // In BFS mode, index of the end of the queue where to pop from; in DFS mode, index of the
                           // top of the stack

    final static Logger logger = LoggerFactory.getLogger(TopoSort.class);

    private class MyLongBigArrayBigList extends LongBigArrayBigList {
        public MyLongBigArrayBigList(long size) {
            super(size);

            // Allow setting directly in the array without repeatedly calling
            // .add() first
            this.size = size;
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        if (args.length != 4) {
            System.err.println(
                    "Syntax: java org.softwareheritage.graph.utils.TopoSort <path/to/graph> {dfs|bfs} {forward|backward} <nodeTypes>");
            System.exit(1);
        }
        String graphPath = args[0];
        String algorithm = args[1];
        String directionString = args[2];
        String nodeTypes = args[3];

        TopoSort toposort = new TopoSort();

        toposort.loadGraph(graphPath, nodeTypes);

        if (directionString.equals("forward")) {
            toposort.swapGraphs();
        } else if (!directionString.equals("backward")) {
            System.err.println("Invalid direction " + directionString);
            System.exit(1);
        }
        if (algorithm.equals("dfs")) {
            toposort.dfs = true;
        } else if (algorithm.equals("bfs")) {
            toposort.dfs = false;
        } else {
            System.err.println("Invalid algorithm " + algorithm);
            System.exit(1);
        }

        toposort.endIndex = 0;
        toposort.toposort();
    }

    public void swapGraphs() {
        Subgraph tmp;
        tmp = graph;
        graph = transposedGraph;
        transposedGraph = tmp;

        SwhBidirectionalGraph tmp2;
        tmp2 = underlyingGraph;
        underlyingGraph = transposedUnderlyingGraph;
        transposedUnderlyingGraph = tmp2;
    }

    public void loadGraph(String graphBasename, String nodeTypes) throws IOException {
        System.err.println("Loading graph " + graphBasename + " ...");
        underlyingGraph = SwhBidirectionalGraph.loadMapped(graphBasename);
        transposedUnderlyingGraph = underlyingGraph.transpose();
        System.err.println("Selecting subgraphs.");
        graph = new Subgraph(underlyingGraph, new AllowedNodes(nodeTypes));
        transposedGraph = graph.transpose();
        System.err.println("Graph loaded.");

        ready = new LongBigArrayBigList(graph.numNodes());
    }

    private void pushReady(long nodeId) {
        if (dfs) {
            endIndex++;
        }
        ready.add(nodeId);
    }

    private long popReady() {
        if (dfs) {
            return ready.removeLong(--endIndex);
        } else {
            return ready.getLong(endIndex++);
        }
    }

    private long readySize() {
        if (dfs) {
            return endIndex;
        } else {
            return ready.size64() - endIndex;
        }
    }

    private boolean readyNodes() {
        return readySize() > 0;
    }

    /* Prints nodes in topological order, based on a DFS or BFS. */
    public void toposort() throws IOException {
        MyLongBigArrayBigList unvisitedAncestors = new MyLongBigArrayBigList(underlyingGraph.numNodes());

        BufferedWriter bufferedStdout = new BufferedWriter(new OutputStreamWriter(System.out));
        CSVPrinter csvPrinter = new CSVPrinter(bufferedStdout, CSVFormat.RFC4180);

        /* First, push all leaves to the stack */
        ProgressLogger pl = new ProgressLogger(logger);
        pl.itemsName = "nodes";
        pl.expectedUpdates = graph.numNodes();
        pl.start("Listing leaves...");

        long total_nodes = 0;
        long total_edges = 0;
        NodeIterator nodeIterator = graph.nodeIterator();
        while (nodeIterator.hasNext()) {
            pl.update();
            long currentNodeId = nodeIterator.nextLong();
            total_nodes++;
            long nbAncestors = graph.outdegree(currentNodeId);
            unvisitedAncestors.set(currentNodeId, nbAncestors);
            total_edges += nbAncestors;
            if (nbAncestors != 0) {
                /* The node has ancestor, so it is not a leaf. */
                continue;
            }
            pushReady(currentNodeId);
        }
        pl.done();
        System.err.println("Leaves listed, starting traversal.");

        csvPrinter.printRecord("SWHID", "ancestors", "successors", "sample_ancestor1", "sample_ancestor2");
        pl = new ProgressLogger(logger);
        pl.itemsName = "edges";
        pl.expectedUpdates = total_edges;
        pl.start("Sorting...");
        while (readyNodes()) {
            long currentNodeId = popReady();

            /* Find its successors which are ready */
            LazyLongIterator successors = transposedUnderlyingGraph.successors(currentNodeId);
            long successorCount = 0;
            for (long successorNodeId; (successorNodeId = successors.nextLong()) != -1;) {
                if (!graph.nodeExists(successorNodeId)) {
                    /* Arc destination is filtered out from the subgraph, ignore it */
                    continue;
                }
                pl.update();
                successorCount++;
                long nbUnvisitedAncestors = unvisitedAncestors.getLong(successorNodeId);
                nbUnvisitedAncestors--;
                if (nbUnvisitedAncestors == 0) {
                    pushReady(successorNodeId);
                } else if (nbUnvisitedAncestors < 0) {
                    System.err.format("[BUG] %s has negative number of unvisited ancestors: %d\n",
                            graph.getSWHID(successorNodeId), nbUnvisitedAncestors);
                }
                unvisitedAncestors.set(successorNodeId, nbUnvisitedAncestors);
            }

            String[] sampleAncestors = {"", ""};
            long ancestorCount = 0;
            LazyLongIterator ancestors = graph.successors(currentNodeId);
            for (long ancestorNodeId; (ancestorNodeId = ancestors.nextLong()) != -1;) {
                if (ancestorCount < sampleAncestors.length) {
                    sampleAncestors[(int) ancestorCount] = graph.getSWHID(ancestorNodeId).toString();
                }
                ancestorCount++;
            }

            /*
             * Print the node
             *
             * TODO: print its depth too?
             */
            SWHID currentNodeSWHID = graph.getSWHID(currentNodeId);
            csvPrinter.printRecord(currentNodeSWHID, ancestorCount, successorCount, sampleAncestors[0],
                    sampleAncestors[1]);
        }
        pl.done();

        csvPrinter.flush();
        bufferedStdout.flush();
    }
}
