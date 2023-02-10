/*
 * Copyright (c) 2020-2023 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.utils;

import it.unimi.dsi.big.webgraph.labelling.ArcLabelledNodeIterator;
import com.martiansoftware.jsap.*;
import org.softwareheritage.graph.*;
import org.softwareheritage.graph.labels.DirEntry;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/* Lists all nodes nodes of the types given as argument, in topological order,
 * from leaves (contents, if selected) to the top (origins, if selected).
 *
 * This uses a DFS, so nodes are likely to be close to their neighbors.
 *
 * Some extra information is provided to allow more efficient consumption
 * of the output: number of ancestors, successors, and a sample of two ancestors.
 *
 * Sample invocation:
 *
 *   $ java -cp ~/swh-environment/swh-graph/java/target/swh-graph-*.jar -Xmx500G -XX:PretenureSizeThreshold=512M -XX:MaxNewSize=4G -XX:+UseLargePages -XX:+UseTransparentHugePages -XX:+UseNUMA -XX:+UseTLAB -XX:+ResizeTLAB org.softwareheritage.graph.utils.PopularContents /dev/shm/swh-graph/default/graph \
 *      | pv --line-mode --wait \
 *      | zstdmt \
 *      > /poolswh/softwareheritage/vlorentz/2022-04-25_popular_contents.txt.zst
 */

public class PopularContents {
    private SwhBidirectionalGraph graph;
    private int NUM_THREADS = 96;

    final static Logger logger = LoggerFactory.getLogger(PopularContents.class);

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 3) {
            System.err.println(
                    "Syntax: java org.softwareheritage.graph.utils.PopularContents <path/to/graph> <max_results_per_cnt> <popularity_threshold>");
            System.exit(1);
        }
        String graphPath = args[0];
        int maxResults = Integer.parseInt(args[1]);
        long popularityThreshold = Long.parseLong(args[2]);

        PopularContents popular_contents = new PopularContents();

        popular_contents.loadGraph(graphPath);

        popular_contents.run(maxResults, popularityThreshold);
    }

    public void loadGraph(String graphBasename) throws IOException {
        System.err.println("Loading graph " + graphBasename + " ...");
        graph = SwhBidirectionalGraph.loadLabelledMapped(graphBasename);
        graph.properties.loadLabelNames();
        graph.properties.loadContentLength();
        System.err.println("Graph loaded.");
    }

    public void run(int maxResults, long popularityThreshold) throws InterruptedException {
        System.out.format("SWHID,length,filename,occurrences\n");

        long totalNodes = graph.numNodes();
        long numChunks = NUM_THREADS * 10000;

        ProgressLogger pl = new ProgressLogger(logger);
        pl.itemsName = "nodes";
        pl.expectedUpdates = graph.numNodes();
        pl.start("Listing contents...");

        ExecutorService service = Executors.newFixedThreadPool(NUM_THREADS);
        for (long i = 0; i < numChunks; ++i) {
            final long chunkId = i;
            service.submit(() -> {
                processChunk(numChunks, chunkId, maxResults, popularityThreshold, pl);
            });
        }

        service.shutdown();
        service.awaitTermination(365, TimeUnit.DAYS);

        pl.done();

    }

    private void processChunk(long numChunks, long chunkId, int maxResults, long popularityThreshold,
            ProgressLogger pl) {
        long totalNodes = graph.numNodes();
        HashMap<Long, Long> names = new HashMap<>();
        SwhUnidirectionalGraph backwardGraph = graph.getBackwardGraph().copy();

        long chunkSize = totalNodes / numChunks;
        long chunkStart = chunkSize * chunkId;
        long chunkEnd = chunkId == numChunks - 1 ? totalNodes : chunkSize * (chunkId + 1);

        /*
         * priority heap used to only print filenames with the most occurrences for each content
         */
        PriorityQueue<Long> heap = new PriorityQueue<Long>((maxResults > 0) ? maxResults : 1, new SortByHashmap(names));

        for (long cntNode = chunkStart; cntNode < chunkEnd; cntNode++) {
            pl.update();

            if (graph.getNodeType(cntNode) != SwhType.CNT) {
                continue;
            }

            names.clear();

            ArcLabelledNodeIterator.LabelledArcIterator s = backwardGraph.labelledSuccessors(cntNode);
            long dirNode;
            while ((dirNode = s.nextLong()) >= 0) {
                if (graph.getNodeType(dirNode) != SwhType.DIR) {
                    continue;
                }
                DirEntry[] labels = (DirEntry[]) s.label().get();
                for (DirEntry label : labels) {
                    names.put(label.filenameId, names.getOrDefault(label.filenameId, 0L) + 1);

                }
            }

            Long contentLength = graph.properties.getContentLength(cntNode);
            if (contentLength == null) {
                contentLength = -1L;
            }
            if (names.size() == 0) {
                /* No filename at all */
                continue;
            } else if (maxResults <= 0 || maxResults >= names.size()) {
                /* Print everything */
                for (Map.Entry<Long, Long> entry : names.entrySet()) {
                    long filenameId = entry.getKey();
                    Long count = entry.getValue();
                    if (count < popularityThreshold) {
                        continue;
                    }
                    String filename = getFilename(filenameId, dirNode);
                    if (filename == null) {
                        continue;
                    }
                    System.out.format("%s,%d,%s,%d\n", graph.getSWHID(cntNode), contentLength, filename, count);
                }
            } else if (maxResults == 1) {
                /*
                 * Print only the result with the most occurrence. This case could be merged with the one below, but
                 * avoiding the priority heap has much better performance.
                 */
                long maxFilenameId = 0;
                long maxCount = 0;

                for (Map.Entry<Long, Long> entry : names.entrySet()) {
                    Long count = entry.getValue();
                    if (count > maxCount) {
                        maxFilenameId = entry.getKey();
                        maxCount = count;
                    }
                }

                if (maxCount > 0) {
                    String filename = getFilename(maxFilenameId, dirNode);
                    if (filename == null) {
                        continue;
                    }
                    System.out.format("%s,%d,%s,%d\n", graph.getSWHID(cntNode), contentLength, filename, maxCount);
                }
            } else {
                /* Print only results with the most occurrences */
                int nbResultsInHeap = 0;
                for (Map.Entry<Long, Long> entry : names.entrySet()) {
                    Long filenameId = entry.getKey();
                    Long count = entry.getValue();
                    if (count < popularityThreshold) {
                        continue;
                    }
                    heap.add(filenameId);
                    if (nbResultsInHeap == maxResults) {
                        heap.poll();
                    } else {
                        nbResultsInHeap++;
                    }
                }

                for (Long filenameId : heap) {
                    String filename = getFilename(filenameId, dirNode);
                    if (filename == null) {
                        continue;
                    }
                    System.out.format("%s,%d,%s,%d\n", graph.getSWHID(cntNode), contentLength, filename,
                            names.get(filenameId));
                }
                heap.clear();
            }
        }
    }

    private String getFilename(long filenameId, long dirNode) {
        try {
            return new String(graph.properties.getLabelName(filenameId));
        } catch (IllegalArgumentException e) {
            /*
             * https://gitlab.softwareheritage.org/swh/devel/swh-graph/-/issues/4759
             *
             * Caused by: java.lang.IllegalArgumentException: Input byte array has incorrect ending byte at 36
             * at java.base/java.util.Base64$Decoder.decode0(Base64.java:875) at
             * java.base/java.util.Base64$Decoder.decode(Base64.java:566) at
             * org.softwareheritage.graph.SwhGraphProperties.getLabelName(SwhGraphProperties.java:333) at
             * org.softwareheritage.graph.utils.PopularContents.lambda$run$0(PopularContents.java:103)
             */

            System.err.printf("Failed to read filename %d of directory %s: %s\n", filenameId, graph.getSWHID(dirNode),
                    e.toString());
            return null;
        }
    }

    private class SortByHashmap implements Comparator<Long> {
        private HashMap<Long, Long> map;
        public SortByHashmap(HashMap<Long, Long> map) {
            this.map = map;
        }

        public int compare(Long l1, Long l2) {
            return map.get(l1).compareTo(map.get(l2));
        }
    }
}
