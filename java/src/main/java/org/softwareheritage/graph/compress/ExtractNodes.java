/*
 * Copyright (c) 2022-2023 The Software Heritage developers
 * See the AUTHORS file at the top-level directory of this distribution
 * License: GNU General Public License version 3, or any later version
 * See top-level LICENSE file for more information
 */

package org.softwareheritage.graph.compress;

import com.github.luben.zstd.ZstdOutputStream;
import com.martiansoftware.jsap.*;
import it.unimi.dsi.logging.ProgressLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.softwareheritage.graph.SwhType;
import org.softwareheritage.graph.utils.Sort;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import org.softwareheritage.graph.AllowedNodes;

/**
 * Read a graph dataset and extract all the unique node SWHIDs it contains, including the ones that
 * are not stored as actual objects in the graph, but only <em>referred to</em> by the edges.
 * Additionally, extract the set of all unique edge labels in the graph.
 *
 * <ul>
 * <li>The set of nodes is written in <code>${outputBasename}.nodes.csv.zst</code>, as a
 * zst-compressed sorted list of SWHIDs, one per line.</li>
 * <li>The set of edge labels is written in <code>${outputBasename}.labels.csv.zst</code>, as a
 * zst-compressed sorted list of labels encoded in base64, one per line.</li>
 * <li>The number of unique nodes referred to in the graph is written in a text file,
 * <code>${outputBasename}.nodes.count.txt</code></li>
 * <li>The number of unique edges referred to in the graph is written in a text file,
 * <code>${outputBasename}.edges.count.txt</code></li>
 * <li>The number of unique edge labels is written in a text file,
 * <code>${outputBasename}.labels.count.txt</code></li>
 * <li>Statistics on the number of nodes of each type are written in a text file,
 * <code>${outputBasename}.nodes.stats.txt</code></li>
 * <li>Statistics on the number of edges of each type are written in a text file,
 * <code>${outputBasename}.edges.stats.txt</code></li>
 * </ul>
 *
 * <p>
 * <strong>Rationale:</strong> Because the graph can contain holes, loose objects and dangling
 * objects, some nodes that are referred to as destinations in the edge relationships might not
 * actually be stored in the graph itself. However, to compress the graph using a graph compression
 * library, it is necessary to have a list of <em>all</em> the nodes in the graph, including the
 * ones that are simply referred to by the edges but not actually stored as concrete objects.
 * </p>
 *
 * <p>
 * This class reads the entire graph dataset, and uses <code>sort -u</code> to extract the set of
 * all the unique nodes and unique labels that will be needed as an input for the compression
 * process.
 * </p>
 */
public class ExtractNodes {
    private final static Logger logger = LoggerFactory.getLogger(ExtractNodes.class);

    // Create one thread per processor.
    final static int numThreads = Runtime.getRuntime().availableProcessors();

    // Allocate up to 20% of maximum memory for sorting subprocesses.
    final static long sortBufferSize = (long) (Runtime.getRuntime().maxMemory() * 0.2 / numThreads / 2);

    private static JSAPResult parseArgs(String[] args) {
        JSAPResult config = null;
        try {
            SimpleJSAP jsap = new SimpleJSAP(ComposePermutations.class.getName(), "", new Parameter[]{
                    new UnflaggedOption("dataset", JSAP.STRING_PARSER, JSAP.REQUIRED, "Path to the edges dataset"),
                    new UnflaggedOption("outputBasename", JSAP.STRING_PARSER, JSAP.REQUIRED,
                            "Basename of the output files"),

                    new FlaggedOption("format", JSAP.STRING_PARSER, "orc", JSAP.NOT_REQUIRED, 'f', "format",
                            "Format of the input dataset (orc, csv)"),
                    new FlaggedOption("sortBufferSize", JSAP.STRING_PARSER, String.valueOf(sortBufferSize) + "b",
                            JSAP.NOT_REQUIRED, 'S', "sort-buffer-size",
                            "Size of the memory buffer used by each sort process"),
                    new FlaggedOption("sortTmpDir", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 'T', "temp-dir",
                            "Path to the temporary directory used by sort"),
                    new FlaggedOption("allowedNodeTypes", JSAP.STRING_PARSER, "*", JSAP.NOT_REQUIRED, 'N',
                            "allowed-node-types",
                            "Node types to include in the graph, eg. 'ori,snp,rel,rev' to exclude directories and contents"),});

            config = jsap.parse(args);
            if (jsap.messagePrinted()) {
                System.exit(1);
            }
        } catch (JSAPException e) {
            System.err.println("Usage error: " + e.getMessage());
            System.exit(1);
        }
        return config;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        JSAPResult parsedArgs = parseArgs(args);
        String datasetPath = parsedArgs.getString("dataset");
        String outputBasename = parsedArgs.getString("outputBasename");

        String datasetFormat = parsedArgs.getString("format");
        String sortBufferSize = parsedArgs.getString("sortBufferSize");
        String sortTmpPath = parsedArgs.getString("sortTmpDir", null);
        AllowedNodes allowedNodeTypes = new AllowedNodes(parsedArgs.getString("allowedNodeTypes"));

        File sortTmpDir = new File(sortTmpPath);
        sortTmpDir.mkdirs();

        // Open edge dataset
        GraphDataset dataset;
        if (datasetFormat.equals("orc")) {
            dataset = new ORCGraphDataset(datasetPath, allowedNodeTypes);
        } else if (datasetFormat.equals("csv")) {
            dataset = new CSVEdgeDataset(datasetPath, allowedNodeTypes);
        } else {
            throw new IllegalArgumentException("Unknown dataset format: " + datasetFormat);
        }

        extractNodes(dataset, outputBasename, sortBufferSize, sortTmpDir);
    }

    public static void extractNodes(GraphDataset dataset, String outputBasename, String sortBufferSize, File sortTmpDir)
            throws IOException, InterruptedException {
        // Read the dataset and write the nodes and labels to the sorting processes
        AtomicLong edgeCount = new AtomicLong(0);
        AtomicLongArray edgeCountByType = new AtomicLongArray(SwhType.values().length * SwhType.values().length);

        int numThreads = Runtime.getRuntime().availableProcessors();
        ForkJoinPool forkJoinPool = new ForkJoinPool(numThreads);

        Process[] nodeSorters = new Process[numThreads];
        File[] nodeBatchPaths = new File[numThreads];
        Process[] labelSorters = new Process[numThreads];
        File[] labelBatches = new File[numThreads];
        long[] progressCounts = new long[numThreads];

        AtomicInteger nextThreadId = new AtomicInteger(0);
        ThreadLocal<Integer> threadLocalId = ThreadLocal.withInitial(nextThreadId::getAndIncrement);

        ProgressLogger pl = new ProgressLogger(logger, 10, TimeUnit.SECONDS);
        pl.itemsName = "edges";
        pl.start("Reading node/edge files and writing sorted batches.");

        GraphDataset.NodeCallback nodeCallback = (node) -> {
            int threadId = threadLocalId.get();
            if (nodeSorters[threadId] == null) {
                nodeBatchPaths[threadId] = File.createTempFile("nodes", ".txt", sortTmpDir);
                nodeSorters[threadId] = Sort.spawnSort(sortBufferSize, sortTmpDir.getPath(),
                        List.of("-o", nodeBatchPaths[threadId].getPath()));
            }
            OutputStream nodeOutputStream = nodeSorters[threadId].getOutputStream();
            nodeOutputStream.write(node);
            nodeOutputStream.write('\n');
        };

        GraphDataset.NodeCallback labelCallback = (label) -> {
            int threadId = threadLocalId.get();
            if (labelSorters[threadId] == null) {
                labelBatches[threadId] = File.createTempFile("labels", ".txt", sortTmpDir);
                labelSorters[threadId] = Sort.spawnSort(sortBufferSize, sortTmpDir.getPath(),
                        List.of("-o", labelBatches[threadId].getPath()));
            }
            OutputStream labelOutputStream = labelSorters[threadId].getOutputStream();
            labelOutputStream.write(label);
            labelOutputStream.write('\n');
        };

        try {
            forkJoinPool.submit(() -> {
                try {
                    dataset.readEdges((node) -> {
                        nodeCallback.onNode(node);
                    }, (src, dst, label, perm) -> {
                        nodeCallback.onNode(src);
                        nodeCallback.onNode(dst);

                        if (label != null) {
                            labelCallback.onNode(label);
                        }
                        edgeCount.incrementAndGet();
                        // Extract type of src and dst from their SWHID: swh:1:XXX
                        byte[] srcTypeBytes = Arrays.copyOfRange(src, 6, 6 + 3);
                        byte[] dstTypeBytes = Arrays.copyOfRange(dst, 6, 6 + 3);
                        int srcType = SwhType.byteNameToInt(srcTypeBytes);
                        int dstType = SwhType.byteNameToInt(dstTypeBytes);
                        if (srcType != -1 && dstType != -1) {
                            edgeCountByType.incrementAndGet(srcType * SwhType.values().length + dstType);
                        } else {
                            System.err.println("Invalid edge type: " + new String(srcTypeBytes) + " -> "
                                    + new String(dstTypeBytes));
                            System.exit(1);
                        }

                        int threadId = threadLocalId.get();
                        if (++progressCounts[threadId] > 1000) {
                            synchronized (pl) {
                                pl.update(progressCounts[threadId]);
                            }
                            progressCounts[threadId] = 0;
                        }
                    });
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).get();
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        // Close all the sorters stdin
        for (int i = 0; i < numThreads; i++) {
            if (nodeSorters[i] != null) {
                nodeSorters[i].getOutputStream().close();
            }
            if (labelSorters[i] != null) {
                labelSorters[i].getOutputStream().close();
            }
        }

        // Wait for sorting processes to finish
        for (int i = 0; i < numThreads; i++) {
            if (nodeSorters[i] != null) {
                nodeSorters[i].waitFor();
            }
            if (labelSorters[i] != null) {
                labelSorters[i].waitFor();
            }
        }
        pl.done();

        ArrayList<String> nodeSortMergerOptions = new ArrayList<>(List.of("-m"));
        ArrayList<String> labelSortMergerOptions = new ArrayList<>(List.of("-m"));
        for (int i = 0; i < numThreads; i++) {
            if (nodeBatchPaths[i] != null) {
                nodeSortMergerOptions.add(nodeBatchPaths[i].getPath());
            }
            if (labelBatches[i] != null) {
                labelSortMergerOptions.add(labelBatches[i].getPath());
            }
        }

        // Spawn node merge-sorting process
        Process nodeSortMerger = Sort.spawnSort(sortBufferSize, sortTmpDir.getPath(), nodeSortMergerOptions);
        nodeSortMerger.getOutputStream().close();
        OutputStream nodesFileOutputStream = new ZstdOutputStream(
                new BufferedOutputStream(new FileOutputStream(outputBasename + ".nodes.csv.zst")));
        NodesOutputThread nodesOutputThread = new NodesOutputThread(
                new BufferedInputStream(nodeSortMerger.getInputStream()), nodesFileOutputStream);
        nodesOutputThread.start();

        // Spawn label merge-sorting process
        Process labelSortMerger = Sort.spawnSort(sortBufferSize, sortTmpDir.getPath(), labelSortMergerOptions);
        labelSortMerger.getOutputStream().close();
        OutputStream labelsFileOutputStream = new ZstdOutputStream(
                new BufferedOutputStream(new FileOutputStream(outputBasename + ".labels.csv.zst")));
        /*
         * Workaround for <https://github.com/luben/zstd-jni/issues/249>, which happens when
         * allowedNodeTypes does not contain REV or REL)
         */
        labelsFileOutputStream.write(new byte[]{});
        LabelsOutputThread labelsOutputThread = new LabelsOutputThread(
                new BufferedInputStream(labelSortMerger.getInputStream()), labelsFileOutputStream);
        labelsOutputThread.start();

        pl.logger().info("Waiting for merge-sort and writing output files...");
        nodeSortMerger.waitFor();
        labelSortMerger.waitFor();
        nodesOutputThread.join();
        labelsOutputThread.join();

        long[][] edgeCountByTypeArray = new long[SwhType.values().length][SwhType.values().length];
        for (int i = 0; i < edgeCountByTypeArray.length; i++) {
            for (int j = 0; j < edgeCountByTypeArray[i].length; j++) {
                edgeCountByTypeArray[i][j] = edgeCountByType.get(i * SwhType.values().length + j);
            }
        }

        // Write node, edge and label counts/statistics
        printEdgeCounts(outputBasename, edgeCount.get(), edgeCountByTypeArray);
        printNodeCounts(outputBasename, nodesOutputThread.getNodeCount(), nodesOutputThread.getNodeTypeCounts());
        printLabelCounts(outputBasename, labelsOutputThread.getLabelCount());

        // Clean up sorted batches
        for (int i = 0; i < numThreads; i++) {
            if (nodeBatchPaths[i] != null) {
                nodeBatchPaths[i].delete();
            }
            if (labelBatches[i] != null) {
                labelBatches[i].delete();
            }
        }
    }

    private static void printEdgeCounts(String basename, long edgeCount, long[][] edgeTypeCounts) throws IOException {
        PrintWriter nodeCountWriter = new PrintWriter(basename + ".edges.count.txt");
        nodeCountWriter.println(edgeCount);
        nodeCountWriter.close();

        PrintWriter nodeTypesCountWriter = new PrintWriter(basename + ".edges.stats.txt");
        TreeMap<String, Long> edgeTypeCountsMap = new TreeMap<>();
        for (SwhType src : SwhType.values()) {
            for (SwhType dst : SwhType.values()) {
                long cnt = edgeTypeCounts[SwhType.toInt(src)][SwhType.toInt(dst)];
                if (cnt > 0)
                    edgeTypeCountsMap.put(src.toString().toLowerCase() + ":" + dst.toString().toLowerCase(), cnt);
            }
        }
        for (Map.Entry<String, Long> entry : edgeTypeCountsMap.entrySet()) {
            nodeTypesCountWriter.println(entry.getKey() + " " + entry.getValue());
        }
        nodeTypesCountWriter.close();
    }

    private static void printNodeCounts(String basename, long nodeCount, long[] nodeTypeCounts) throws IOException {
        PrintWriter nodeCountWriter = new PrintWriter(basename + ".nodes.count.txt");
        nodeCountWriter.println(nodeCount);
        nodeCountWriter.close();

        PrintWriter nodeTypesCountWriter = new PrintWriter(basename + ".nodes.stats.txt");
        TreeMap<String, Long> nodeTypeCountsMap = new TreeMap<>();
        for (SwhType v : SwhType.values()) {
            nodeTypeCountsMap.put(v.toString().toLowerCase(), nodeTypeCounts[SwhType.toInt(v)]);
        }
        for (Map.Entry<String, Long> entry : nodeTypeCountsMap.entrySet()) {
            nodeTypesCountWriter.println(entry.getKey() + " " + entry.getValue());
        }
        nodeTypesCountWriter.close();
    }

    private static void printLabelCounts(String basename, long labelCount) throws IOException {
        PrintWriter nodeCountWriter = new PrintWriter(basename + ".labels.count.txt");
        nodeCountWriter.println(labelCount);
        nodeCountWriter.close();
    }

    private static class NodesOutputThread extends Thread {
        private final InputStream sortedNodesStream;
        private final OutputStream nodesOutputStream;

        private long nodeCount = 0;
        private final long[] nodeTypeCounts = new long[SwhType.values().length];

        NodesOutputThread(InputStream sortedNodesStream, OutputStream nodesOutputStream) {
            this.sortedNodesStream = sortedNodesStream;
            this.nodesOutputStream = nodesOutputStream;
        }

        @Override
        public void run() {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(sortedNodesStream, StandardCharsets.UTF_8));
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    nodesOutputStream.write(line.getBytes(StandardCharsets.UTF_8));
                    nodesOutputStream.write('\n');
                    nodeCount++;
                    try {
                        SwhType nodeType = SwhType.fromStr(line.split(":")[2]);
                        nodeTypeCounts[SwhType.toInt(nodeType)]++;
                    } catch (ArrayIndexOutOfBoundsException e) {
                        System.err.println("Error parsing SWHID: " + line);
                        System.exit(1);
                    }
                }
                nodesOutputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public long getNodeCount() {
            return nodeCount;
        }

        public long[] getNodeTypeCounts() {
            return nodeTypeCounts;
        }
    }

    private static class LabelsOutputThread extends Thread {
        private final InputStream sortedLabelsStream;
        private final OutputStream labelsOutputStream;

        private long labelCount = 0;

        LabelsOutputThread(InputStream sortedLabelsStream, OutputStream labelsOutputStream) {
            this.labelsOutputStream = labelsOutputStream;
            this.sortedLabelsStream = sortedLabelsStream;
        }

        @Override
        public void run() {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(sortedLabelsStream, StandardCharsets.UTF_8));
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    labelsOutputStream.write(line.getBytes(StandardCharsets.UTF_8));
                    labelsOutputStream.write('\n');
                    labelCount++;
                }
                labelsOutputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public long getLabelCount() {
            return labelCount;
        }
    }
}
