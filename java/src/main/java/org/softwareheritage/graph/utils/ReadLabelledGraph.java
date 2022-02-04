package org.softwareheritage.graph.utils;

import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator;
import org.softwareheritage.graph.SwhUnidirectionalGraph;
import org.softwareheritage.graph.labels.DirEntry;

import java.io.IOException;

public class ReadLabelledGraph {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String graphPath = args[0];

        SwhUnidirectionalGraph graph = SwhUnidirectionalGraph.loadLabelled(graphPath);
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
