package org.softwareheritage.graph.utils;

import it.unimi.dsi.big.webgraph.LazyLongIterator;
import org.softwareheritage.graph.*;

import java.io.IOException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Stack;

/* sample invocation on granet.internal.softwareheritage.org for benchmarking
 * purposes, with the main swh-graph service already running:
 *
 *   $ java -cp ~/swh-environment/swh-graph/java/target/swh-graph-0.3.0.jar -Xmx300G -XX:PretenureSizeThreshold=512M -XX:MaxNewSize=4G -XX:+UseLargePages -XX:+UseTransparentHugePages -XX:+UseNUMA -XX:+UseTLAB -XX:+ResizeTLAB org.softwareheritage.graph.utils.FindEarliestRevision --timing /dev/shm/swh-graph/default/graph
 *
 */

public class FindEarliestRevision {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String graphPath = args[0];
        boolean timing = false;
        long ts, elapsedNanos;
        Duration elapsed;

        if (args.length >= 2 && (args[0].equals("-t") || args[0].equals("--timing"))) {
            timing = true;
            graphPath = args[1];
            System.err.println("started with timing option, will keep track of elapsed time");
        }

        System.err.println("loading transposed graph...");
        ts = System.nanoTime();
        SwhBidirectionalGraph graph = SwhBidirectionalGraph.loadMapped(graphPath).transpose();
        elapsed = Duration.ofNanos(System.nanoTime() - ts);
        System.err.println(String.format("transposed graph loaded (duration: %s).", elapsed));

        System.err.println("loading revision timestamps...");
        ts = System.nanoTime();
        graph.loadCommitterTimestamps();
        elapsed = Duration.ofNanos(System.nanoTime() - ts);
        System.err.println(String.format("revision timestamps loaded (duration: %s).", elapsed));

        Scanner stdin = new Scanner(System.in);
        AllowedEdges edges = new AllowedEdges("cnt:dir,dir:dir,dir:rev");
        String rawSWHID = null;
        SWHID srcSWHID = null;
        long lineCount = 0;
        long srcNodeId = -1;
        if (timing) {
            System.err.println("starting SWHID processing...");
            elapsed = Duration.ZERO;
        }
        while (stdin.hasNextLine()) {
            if (timing)
                ts = System.nanoTime();
            rawSWHID = stdin.nextLine().strip();
            lineCount++;
            try {
                srcSWHID = new SWHID(rawSWHID);
                srcNodeId = graph.getNodeId(srcSWHID);
            } catch (IllegalArgumentException e) {
                System.err
                        .println(String.format("skipping invalid or unknown SWHID %s on line %d", rawSWHID, lineCount));
                continue;
            }

            if (timing)
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
                    long committerTs = graph.getCommitterTimestamp(currentNodeId);
                    if (committerTs < minTimestamp) {
                        minRevId = currentNodeId;
                        minTimestamp = committerTs;
                    }
                }

                LazyLongIterator it = graph.successors(currentNodeId);
                for (long neighborNodeId; (neighborNodeId = it.nextLong()) != -1;) {
                    if (!edges.isAllowed(graph.getNodeType(currentNodeId), graph.getNodeType(neighborNodeId))) {
                        continue;
                    }
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
            if (timing) {
                elapsedNanos = System.nanoTime() - ts; // processing time for current SWHID
                elapsed = elapsed.plus(Duration.ofNanos(elapsedNanos)); // cumulative processing time for all SWHIDs
                System.err.printf("visit time (s):\t%.6f\n", (double) elapsedNanos / 1_000_000_000);
            }
        }
        if (timing)
            System.err.printf("processed %d SWHIDs in %s (%s avg)\n", lineCount, elapsed, elapsed.dividedBy(lineCount));
    }
}
