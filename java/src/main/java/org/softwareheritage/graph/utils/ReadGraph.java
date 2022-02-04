package org.softwareheritage.graph.utils;

import it.unimi.dsi.big.webgraph.NodeIterator;
import org.softwareheritage.graph.SwhUnidirectionalGraph;

import java.io.IOException;

public class ReadGraph {
    public static void main(String[] args) throws IOException {
        String graphPath = args[0];

        SwhUnidirectionalGraph graph = SwhUnidirectionalGraph.load(graphPath);

        NodeIterator it = graph.nodeIterator();
        while (it.hasNext()) {
            long srcNode = it.nextLong();

            var s = it.successors();
            long dstNode;
            while ((dstNode = s.nextLong()) >= 0) {
                System.out.format("%s %s\n", graph.getSWHID(srcNode), graph.getSWHID(dstNode));
            }
        }
    }
}
