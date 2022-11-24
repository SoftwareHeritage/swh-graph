/*
 * Copyright (c) 2022 The Software Heritage developers
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

public class TopoSort {
    private Subgraph graph;
    private Subgraph transposedGraph;

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        if (args.length != 2) {
            System.err.println("Syntax: java org.softwareheritage.graph.utils.TopoSort <path/to/graph> <nodeTypes>");
            System.exit(1);
        }
        String graphPath = args[0];
        String nodeTypes = args[1];

        TopoSort toposort = new TopoSort();

        toposort.load_graph(graphPath, nodeTypes);
        toposort.toposortDFS();
    }

    public void load_graph(String graphBasename, String nodeTypes) throws IOException {
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
        for (long currentNodeId = nodeIterator.nextLong(); nodeIterator
                .hasNext(); currentNodeId = nodeIterator.nextLong()) {
            total_nodes++;
            // SWHID currentNodeSWHID = graph.getSWHID(currentNodeId);
            // if (graph.outdegree(currentNodeId) > 0) {
            long firstSuccessor = graph.successors(currentNodeId).nextLong();
            if (firstSuccessor != -1) {
                /* The node has ancestor, so it is not a leaf. */
                // SWHID firstSuccessorNodeSWHID = graph.getSWHID(firstSuccessor);
                // System.err.format("not leaf: %s, has succ: %s\n", currentNodeSWHID, firstSuccessorNodeSWHID);
                continue;
            }
            // System.err.format("is leaf: %s\n", currentNodeSWHID);
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
                         * This ancestor of the success is not yet visited, so the ancestor is not ready.
                         */
                        // SWHID successorNodeSWHID = graph.getSWHID(successorNodeId);
                        // SWHID successorAncestorNodeSWHID = graph.getSWHID(successorAncestorNodeId);
                        // System.err.format("successor %s of %s is not ready, has unvisited ancestor: %s\n",
                        // successorNodeSWHID, currentNodeSWHID, successorAncestorNodeSWHID);
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
             * Print the node TODO: print its depth too?
             */
            SWHID currentNodeSWHID = graph.getSWHID(currentNodeId);
            System.out.format("%s,%d,%d,%s,%s\n", currentNodeSWHID, ancestorCount, successorCount, sampleAncestors[0],
                    sampleAncestors[1]);
        }
    }
}
