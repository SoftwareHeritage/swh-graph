package org.softwareheritage.graph.utils;

import it.unimi.dsi.big.webgraph.NodeIterator;
import org.softwareheritage.graph.SwhUnidirectionalGraph;
import org.softwareheritage.graph.labels.DirEntry;

import java.io.IOException;

public class DumpProperties {
    public static void main(String[] args) throws IOException {
        String graphPath = args[0];

        SwhUnidirectionalGraph graph = SwhUnidirectionalGraph.loadLabelled(graphPath);
        graph.loadContentLength();
        graph.loadContentIsSkipped();
        graph.loadPersonIds();
        graph.loadAuthorTimestamps();
        graph.loadCommitterTimestamps();
        graph.loadMessages();
        graph.loadTagNames();
        graph.loadLabelNames();

        NodeIterator it = graph.nodeIterator();
        while (it.hasNext()) {
            long node = it.nextLong();
            System.out.format("%s: %s\n", node, graph.getSWHID(node));

            var s = graph.labelledSuccessors(node);
            long succ;
            System.out.println("  successors:");
            while ((succ = s.nextLong()) >= 0) {
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
                    System.out.format("  message: %s\n", (new String(graph.getMessage(node))).replace("\n", "\\n"));
                    System.out.format("  tag name: %s\n", new String(graph.getTagName(node)));
                    break;
                case ORI:
                    System.out.format("  url: %s\n", graph.getUrl(node));
            }

            System.out.println();
        }
    }
}
