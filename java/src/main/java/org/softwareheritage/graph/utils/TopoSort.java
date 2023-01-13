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
import org.softwareheritage.graph.*;

import java.io.IOException;
import java.util.*;

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
 *   $ java -cp ~/swh-environment/swh-graph/java/target/swh-graph-*.jar -Xmx1000G -XX:PretenureSizeThreshold=512M -XX:MaxNewSize=4G -XX:+UseLargePages -XX:+UseTransparentHugePages -XX:+UseNUMA -XX:+UseTLAB -XX:+ResizeTLAB org.softwareheritage.graph.utils.TopoSort /dev/shm/swh-graph/default/graph backward 'rev,rel,snp,ori' \
 *      | pv --line-mode --wait \
 *      | zstdmt \
 *      > /poolswh/softwareheritage/vlorentz/2022-04-25_toposort_rev,rel,snp,ori.txt.zst
 */

public class TopoSort {
    private Subgraph graph;
    private Subgraph transposedGraph;

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        if (args.length != 3) {
            System.err.println(
                    "Syntax: java org.softwareheritage.graph.utils.TopoSort <path/to/graph> {forward|backward} <nodeTypes>");
            System.exit(1);
        }
        String graphPath = args[0];
        String directionString = args[1];
        String nodeTypes = args[2];

        TopoSort toposort = new TopoSort();

        toposort.loadGraph(graphPath, nodeTypes);

        if (directionString.equals("forward")) {
            toposort.swapGraphs();
        } else if (!directionString.equals("backward")) {
            System.err.println("Invalid direction " + directionString);
            System.exit(1);
        }

        toposort.toposortDFS();
    }

    public void swapGraphs() {
        Subgraph tmp;
        tmp = graph;
        graph = transposedGraph;
        transposedGraph = tmp;
    }

    public void loadGraph(String graphBasename, String nodeTypes) throws IOException {
        System.err.println("Loading graph " + graphBasename + " ...");
        var underlyingGraph = SwhBidirectionalGraph.loadMapped(graphBasename);
        System.err.println("Selecting subgraphs.");
        graph = new Subgraph(underlyingGraph, new AllowedNodes(nodeTypes));
        transposedGraph = graph.transpose();
        System.err.println("Graph loaded.");
    }

    /* Prints nodes in topological order, based on a DFS. */
    public void toposortDFS() {
        HashSet<Long> visited = new HashSet<Long>();
        Stack<Long> ready = new Stack<>();

        /* First, push all leaves to the stack */
        System.err.println("Listing leaves.");
        long total_nodes = 0;
        NodeIterator nodeIterator = graph.nodeIterator();
        while (nodeIterator.hasNext()) {
            long currentNodeId = nodeIterator.nextLong();
            total_nodes++;
            long firstSuccessor = graph.successors(currentNodeId).nextLong();
            if (firstSuccessor != -1) {
                /* The node has ancestor, so it is not a leaf. */
                continue;
            }
            ready.push(currentNodeId);
            if (ready.size() % 10000000 == 0) {
                float ready_size_f = ready.size();
                float total_nodes_f = total_nodes;
                System.err.printf("Listed %.02f B leaves (out of %.02f B nodes)\n", ready_size_f / 1000000000.,
                        total_nodes_f / 1000000000.);
            }
        }
        System.err.println("Leaves loaded, starting DFS.");

        System.out.format("SWHID,ancestors,successors,sample_ancestor1,sample_ancestor2\n");
        long printed_nodes = 0;
        while (!ready.isEmpty()) {
            long currentNodeId = ready.pop();
            visited.add(currentNodeId);

            /* Find its successors which are ready */
            LazyLongIterator successors = transposedGraph.successors(currentNodeId);
            long successorCount = 0;
            for (long successorNodeId; (successorNodeId = successors.nextLong()) != -1;) {
                successorCount++;
                LazyLongIterator successorAncestors = graph.successors(successorNodeId);
                boolean isReady = true;
                for (long successorAncestorNodeId; (successorAncestorNodeId = successorAncestors.nextLong()) != -1;) {
                    if (!visited.contains(successorAncestorNodeId)) {
                        /*
                         * This ancestor of the successor is not yet visited, so the ancestor is not ready.
                         */
                        isReady = false;
                        break;
                    }
                }
                if (isReady) {
                    ready.push(successorNodeId);
                }
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
            System.out.format("%s,%d,%d,%s,%s\n", currentNodeSWHID, ancestorCount, successorCount, sampleAncestors[0],
                    sampleAncestors[1]);
            printed_nodes += 1;
            if (printed_nodes % 10000000 == 0) {
                float printed_nodes_f = printed_nodes;
                float total_nodes_f = total_nodes;
                System.err.printf("Sorted %.02f B nodes (out of %.02f B)\n", printed_nodes_f / 1000000000.,
                        total_nodes_f / 1000000000.);
            }
        }
    }
}
