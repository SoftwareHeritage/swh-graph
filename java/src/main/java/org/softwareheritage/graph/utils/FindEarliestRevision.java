package org.softwareheritage.graph.utils;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import org.softwareheritage.graph.AllowedEdges;
import org.softwareheritage.graph.Graph;
import org.softwareheritage.graph.Node;
import org.softwareheritage.graph.SWHID;

import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Stack;

public class FindEarliestRevision {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String graphPath = args[0];
        long ts, elapsedNanos;
        Duration elapsed;

        System.err.println("loading transposed graph...");
        ts = System.nanoTime();
        Graph graph = new Graph(graphPath).transpose();
        elapsed = Duration.ofNanos(System.nanoTime() - ts);
        System.err.println(String.format("transposed graph loaded (duration: %s).", elapsed));

        System.err.println("loading revision timestamps...");
        ts = System.nanoTime();
        long[][] committerTimestamps = BinIO.loadLongsBig(graphPath + "-rev_committer_timestamps.bin");
        elapsed = Duration.ofNanos(System.nanoTime() - ts);
        System.err.println(String.format("revision timestamps loaded (duration: %s).", elapsed));

        Scanner stdin = new Scanner(System.in);
        AllowedEdges edges = new AllowedEdges("cnt:dir,dir:dir,dir:rev");
        String rawSWHID = null;
        SWHID srcSWHID = null;
        long lineCount = 0;
        System.err.println("starting SWHID processing...");
        elapsed = Duration.ZERO;
        while (stdin.hasNextLine()) {
            ts = System.nanoTime();
            rawSWHID = stdin.nextLine().strip();
            lineCount++;
            try {
                srcSWHID = new SWHID(rawSWHID);
            } catch (IllegalArgumentException e) {
                System.err.println(String.format("skipping invalid SWHID %s on line %d", rawSWHID, lineCount));
                continue;
            }
            long srcNodeId = graph.getNodeId(srcSWHID);

            System.err.println("starting traversal for: " + srcSWHID.toString());
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
                System.err.println("no revision found containing: " + srcSWHID.toString());
            } else {
                System.out.println(srcSWHID.toString() + "\t" + graph.getSWHID(minRevId).toString());
            }
            elapsedNanos = System.nanoTime() - ts; // processing time for current SWHID
            elapsed = elapsed.plus(Duration.ofNanos(elapsedNanos)); // cumulative processing time for all SWHIDs
            System.err.println(String.format("visit time (s):\t%.6f", (double) elapsedNanos / 1_000_000_000));
        }
        System.err.println(
                String.format("processed %d SWHIDs in %s (%s avg)", lineCount, elapsed, elapsed.dividedBy(lineCount)));
    }
}
