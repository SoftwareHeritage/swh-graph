/*
 * Copyright (c) 2022 The Software Heritage developers
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

public class DumpProperties {
    final static Logger logger = LoggerFactory.getLogger(DumpProperties.class);

    public static void main(String[] args) throws IOException {
        String graphPath = args[0];

        ProgressLogger pl = new ProgressLogger(logger, 10, TimeUnit.SECONDS);
        SwhUnidirectionalGraph graph;
        if (args.length > 1 && (args[1].equals("--mapped") || args[1].equals("-m"))) {
            graph = SwhUnidirectionalGraph.loadLabelledMapped(graphPath, pl);
        } else {
            graph = SwhUnidirectionalGraph.loadLabelled(graphPath, pl);
        }
        graph.loadContentLength();
        graph.loadContentIsSkipped();
        graph.loadPersonIds();
        graph.loadAuthorTimestamps();
        graph.loadCommitterTimestamps();
        graph.loadMessages();
        graph.loadTagNames();
        graph.loadLabelNames();

        ArcLabelledNodeIterator it = graph.labelledNodeIterator();
        while (it.hasNext()) {
            long node = it.nextLong();
            System.out.format("%s: %s\n", node, graph.getSWHID(node));

            var s = it.successors();
            System.out.println("  successors:");
            for (long succ; (succ = s.nextLong()) >= 0;) {
                DirEntry[] labels = (DirEntry[]) s.label().get();
                if (labels.length > 0) {
                    for (DirEntry label : labels) {
                        System.out.format("    %s %s [perms: %s]\n", graph.getSWHID(succ),
                                new String(graph.getLabelName(label.filenameId)), label.permission);
                    }
                } else {
                    System.out.format("    %s\n", graph.getSWHID(succ));
                }
            }

            switch (graph.getNodeType(node)) {
                case CNT:
                    System.out.format("  length: %s\n", graph.getContentLength(node));
                    System.out.format("  is_skipped: %s\n", graph.isContentSkipped(node));
                    break;
                case REV:
                    System.out.format("  author: %s\n", graph.getAuthorId(node));
                    System.out.format("  committer: %s\n", graph.getCommitterId(node));
                    System.out.format("  date: %s (offset: %s)\n", graph.getAuthorTimestamp(node),
                            graph.getAuthorTimestampOffset(node));
                    System.out.format("  committer_date: %s (offset: %s)\n", graph.getCommitterTimestamp(node),
                            graph.getCommitterTimestampOffset(node));
                    byte[] msg = graph.getMessage(node);
                    if (msg != null) {
                        System.out.format("  message: %s\n", (new String(msg)).replace("\n", "\\n"));
                    }
                    break;
                case REL:
                    System.out.format("  author: %s\n", graph.getAuthorId(node));
                    System.out.format("  date: %s (offset: %s)\n", graph.getAuthorTimestamp(node),
                            graph.getAuthorTimestamp(node));
                    byte[] tagMsg = graph.getMessage(node);
                    if (tagMsg != null) {
                        System.out.format("  message: %s\n", (new String(tagMsg)).replace("\n", "\\n"));
                    }
                    byte[] tagName = graph.getTagName(node);
                    if (tagName != null) {
                        System.out.format("  name: %s\n", (new String(tagName)));
                    }
                    break;
                case ORI:
                    System.out.format("  url: %s\n", graph.getUrl(node));
            }

            System.out.println();
        }
    }
}
