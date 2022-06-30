/*
 * Copyright (c) 2020-2022 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.utils;

import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.logging.ProgressLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareheritage.graph.SwhUnidirectionalGraph;
import org.softwareheritage.graph.labels.DirEntry;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class ReadLabelledGraph {
    final static Logger logger = LoggerFactory.getLogger(ReadLabelledGraph.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String graphPath = args[0];

        ProgressLogger pl = new ProgressLogger(logger, 10, TimeUnit.SECONDS);
        SwhUnidirectionalGraph graph;
        if (args.length > 1 && (args[1].equals("--mapped") || args[1].equals("-m"))) {
            graph = SwhUnidirectionalGraph.loadLabelledMapped(graphPath, pl);
        } else {
            graph = SwhUnidirectionalGraph.loadLabelled(graphPath, pl);
        }

        graph.properties.loadLabelNames();

        ArcLabelledNodeIterator it = graph.labelledNodeIterator();
        while (it.hasNext()) {
            long srcNode = it.nextLong();

            ArcLabelledNodeIterator.LabelledArcIterator s = it.successors();
            long dstNode;
            while ((dstNode = s.nextLong()) >= 0) {
                DirEntry[] labels = (DirEntry[]) s.label().get();
                if (labels.length > 0) {
                    for (DirEntry label : labels) {
                        System.out.format("%s %s %s %d\n", graph.getSWHID(srcNode), graph.getSWHID(dstNode),
                                new String(graph.properties.getLabelName(label.filenameId)), label.permission);
                    }
                } else {
                    System.out.format("%s %s\n", graph.getSWHID(srcNode), graph.getSWHID(dstNode));
                }
            }
        }
    }
}
