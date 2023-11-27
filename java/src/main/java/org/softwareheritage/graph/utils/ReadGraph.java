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
import org.softwareheritage.graph.SwhUnidirectionalGraph;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Read a compressed Software Heritage graph (as a unidirectional graph corresponding to the
 * original Merkle DAG) and print all its edges to stdout.
 */
public class ReadGraph {
    final static Logger logger = LoggerFactory.getLogger(ReadLabelledGraph.class);

    public static void main(String[] args) throws IOException {
        String graphPath = args[0];

        SwhUnidirectionalGraph graph;
        ProgressLogger pl = new ProgressLogger(logger, 10, TimeUnit.SECONDS);
        if (args.length > 1 && (args[1].equals("--mapped") || args[1].equals("-m"))) {
            graph = SwhUnidirectionalGraph.loadMapped(graphPath, pl);
        } else {
            graph = SwhUnidirectionalGraph.load(graphPath, pl);
        }

        pl.expectedUpdates = graph.numArcs();
        pl.start("Reading graph...");
        NodeIterator it = graph.nodeIterator();
        while (it.hasNext()) {
            long srcNode = it.nextLong();

            var s = it.successors();
            long dstNode;
            while ((dstNode = s.nextLong()) >= 0) {
                System.out.format("%s %s\n", graph.getSWHID(srcNode), graph.getSWHID(dstNode));
                pl.lightUpdate();
            }
        }
    }
}
