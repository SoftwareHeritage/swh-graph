package org.softwareheritage.graph.utils;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import org.softwareheritage.graph.AllowedEdges;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.SWHID;

import java.io.IOException;
import java.util.HashSet;
import java.util.Stack;

public class FindEarliestRevision {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String graphPath = args[0];
        SWHID srcSWHID = new SWHID(args[1]);

        System.err.println("loading graph.");
        Graph graph = new Graph(graphPath).transpose();
        long srcNodeId = graph.getNodeId(srcSWHID);

        System.err.println("loading timestamps.");
        long[][] committerTimestamps = BinIO.loadLongsBig(graphPath + "-rev_committer_timestamps.bin");

        System.err.println("starting traversal.");
        AllowedEdges edges = new AllowedEdges("cnt:dir,dir:dir,dir:rev,rev:rev");
        Stack<Long> stack = new Stack<>();
        HashSet<Long> visited = new HashSet<>();
        stack.push(srcNodeId);
        visited.add(srcNodeId);

        long minRevId = -1;
        long minTimestamp = Long.MAX_VALUE;
        while (!stack.isEmpty()) {
            long currentNodeId = stack.pop();
            if (graph.getNodeType(currentNodeId) == Node.Type.REV) {
                long committerTs = BigArrays.get(committerTimestamps, currentNodeId);
                if (committerTs < minTimestamp) {
                    minRevId = currentNodeId;
                    minTimestamp = committerTs;
                }
            }

            LazyLongIterator it = graph.successors(currentNodeId, edges);
            for (long neighborNodeId; (neighborNodeId = it.nextLong()) != -1;) {
                if (!visited.contains(neighborNodeId)) {
                    stack.push(neighborNodeId);
                    visited.add(neighborNodeId);
                }
            }
        }

        if (minRevId == -1) {
            System.out.println("No revision found containing this object");
        } else {
            System.out.println("Earliest revision containing this object: " + graph.getSWHID(minRevId).toString());
        }
    }
}
