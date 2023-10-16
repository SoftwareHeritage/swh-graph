/*
 * Copyright (c) 2020-2023 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.utils;

import it.unimi.dsi.big.webgraph.NodeIterator;
import it.unimi.dsi.logging.ProgressLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareheritage.graph.SwhBidirectionalGraph;
import org.softwareheritage.graph.SwhUnidirectionalGraph;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Read a compressed Software Heritage graph and print to stdout the edges of both the directed
 * (corresponding to the original Merkle DAG) and transposed graph.
 */
public class ReadBiGraph {
    final static Logger logger = LoggerFactory.getLogger(ReadLabelledGraph.class);

    private static void printEdges(SwhUnidirectionalGraph graph, ProgressLogger pl) {
        pl.expectedUpdates = graph.numArcs();
        pl.start("Reading graph...");
        NodeIterator it = graph.nodeIterator();
        while (it.hasNext()) {
            long srcNode = it.nextLong();

            var s = graph.successors(srcNode);
            long dstNode;
            while ((dstNode = s.nextLong()) >= 0) {
                System.out.format("  %2d (%s) -> %2d (%s)\n", srcNode, graph.getSWHID(srcNode), dstNode,
                        graph.getSWHID(dstNode));
                pl.lightUpdate();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        String graphPath = args[0];

        SwhBidirectionalGraph graph;
        ProgressLogger pl = new ProgressLogger(logger, 10, TimeUnit.SECONDS);
        if (args.length > 1 && (args[1].equals("--mapped") || args[1].equals("-m"))) {
            graph = SwhBidirectionalGraph.loadMapped(graphPath, pl);
        } else {
            graph = SwhBidirectionalGraph.load(graphPath, pl);
        }

        System.out.println("Direct (forward) graph:");
        printEdges(graph.getForwardGraph(), pl);
        System.out.println("Transposed (backward) graph:");
        printEdges(graph.getBackwardGraph(), pl);
    }
}
