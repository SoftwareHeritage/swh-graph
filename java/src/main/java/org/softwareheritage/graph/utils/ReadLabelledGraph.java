package org.softwareheritage.graph.utils;

import it.unimi.dsi.big.util.FrontCodedStringBigList;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledImmutableGraph;
import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator;
import it.unimi.dsi.big.webgraph.labelling.BitStreamArcLabelledImmutableGraph;
import it.unimi.dsi.fastutil.io.BinIO;
import org.softwareheritage.graph.labels.DirEntry;
import org.softwareheritage.graph.maps.NodeIdMap;

import java.io.IOException;

public class ReadLabelledGraph {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String graphPath = args[0];

        ArcLabelledImmutableGraph graph = BitStreamArcLabelledImmutableGraph.loadOffline(graphPath + "-labelled");
        NodeIdMap nodeMap = new NodeIdMap(graphPath, graph.numNodes());
        FrontCodedStringBigList filenameMap = (FrontCodedStringBigList) BinIO.loadObject(graphPath + "-labels.fcl");

        ArcLabelledNodeIterator it = graph.nodeIterator();
        while (it.hasNext()) {
            long srcNode = it.nextLong();

            ArcLabelledNodeIterator.LabelledArcIterator s = it.successors();
            long dstNode;
            while ((dstNode = s.nextLong()) >= 0) {
                DirEntry[] labels = (DirEntry[]) s.label().get();
                if (labels.length > 0) {
                    for (DirEntry label : labels) {
                        System.out.format("%s %s %s %d\n", nodeMap.getSWHID(srcNode), nodeMap.getSWHID(dstNode),
                                filenameMap.get(label.filenameId), label.permission);
                    }
                } else {
                    System.out.format("%s %s\n", nodeMap.getSWHID(srcNode), nodeMap.getSWHID(dstNode));
                }
            }
        }
    }
}
